use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context};
use serde_json::Value;

use crate::logging::{log_event, log_progress};
pub(crate) fn bridge_skipped_total(value: &Value) -> u64 {
    if let Some(skipped) = value.get("skipped").and_then(Value::as_u64) {
        return skipped;
    }
    let skipped_exists = value
        .get("skipped_exists")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let skipped_errors = value
        .get("skipped_errors")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    skipped_exists + skipped_errors
}

pub(crate) fn bridge_error_total(value: &Value) -> u64 {
    value
        .get("errors")
        .and_then(Value::as_u64)
        .or_else(|| value.get("skipped_errors").and_then(Value::as_u64))
        .unwrap_or(0)
}

fn python_bridge_max_retries() -> usize {
    std::env::var("RAGMAIL_PY_BRIDGE_MAX_RETRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(2)
}

fn python_bridge_retry_delay_ms() -> u64 {
    std::env::var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(250)
}

fn retry_delay_for_attempt(attempt: usize, base_delay_ms: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6) as u32;
    let factor = 1_u64 << exponent;
    let delay = base_delay_ms.saturating_mul(factor).min(10_000);
    Duration::from_millis(delay)
}

fn should_retry_bridge_failure(exit_code: Option<i32>, stdout: &str, stderr: &str) -> bool {
    let retryable_exit = matches!(exit_code, Some(75 | 111 | 124 | 137));
    if retryable_exit {
        return true;
    }
    let combined = format!("{stdout}\n{stderr}").to_lowercase();
    [
        "temporary",
        "temporarily unavailable",
        "timeout",
        "timed out",
        "connection reset",
        "connection refused",
        "connection error",
        "api connection error",
        "service unavailable",
        "too many requests",
        "rate limit",
        "429",
        "503",
        "504",
        "try again",
    ]
    .iter()
    .any(|needle| combined.contains(needle))
}

fn repo_root_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn resolve_python_bridge_bin() -> Option<PathBuf> {
    if let Ok(value) = std::env::var("RAGMAIL_PY_BRIDGE_BIN") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            let sibling = parent.join("ragmail-py");
            if sibling.is_file() {
                return Some(sibling);
            }
        }
    }

    let repo_root = repo_root_dir();
    [
        repo_root.join(".venv/bin/ragmail-py"),
        repo_root.join("python/.venv/bin/ragmail-py"),
    ]
    .into_iter()
    .find(|candidate| candidate.is_file())
}

fn build_python_stage_command(stage: &str) -> ProcessCommand {
    if let Some(bin) = resolve_python_bridge_bin() {
        let mut cmd = ProcessCommand::new(bin);
        cmd.arg("py");
        cmd.arg(stage);
        return cmd;
    }

    let python = std::env::var("RAGMAIL_PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
    let mut cmd = ProcessCommand::new(python);
    cmd.arg("-m");
    cmd.arg("ragmail.cli");
    cmd.arg("py");
    cmd.arg(stage);
    cmd.current_dir(repo_root_dir());
    cmd
}

fn build_python_passthrough_command(args: &[String]) -> ProcessCommand {
    if let Some(bin) = resolve_python_bridge_bin() {
        let mut cmd = ProcessCommand::new(bin);
        cmd.args(args);
        return cmd;
    }

    let python = std::env::var("RAGMAIL_PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
    let mut cmd = ProcessCommand::new(python);
    cmd.arg("-m");
    cmd.arg("ragmail.cli");
    cmd.args(args);
    cmd.current_dir(repo_root_dir());
    cmd
}

fn spawn_bridge_reader<T: std::io::Read + Send + 'static>(
    reader: T,
    is_stderr: bool,
    tx: mpsc::Sender<(bool, String)>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            let read = match buf_reader.read_line(&mut line) {
                Ok(value) => value,
                Err(_) => break,
            };
            if read == 0 {
                break;
            }
            let _ = tx.send((is_stderr, line.trim_end_matches('\n').to_string()));
        }
    })
}

pub(crate) fn run_python_bridge<F>(
    stage: &str,
    args: &[String],
    logs_dir: &Path,
    mut on_event: F,
) -> anyhow::Result<Value>
where
    F: FnMut(&Value),
{
    let max_retries = python_bridge_max_retries();
    let total_attempts = max_retries + 1;
    let base_delay_ms = python_bridge_retry_delay_ms();
    for attempt in 1..=total_attempts {
        let mut command = build_python_stage_command(stage);
        command.args(args);
        command.stdin(Stdio::null());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        let rendered = format!(
            "{} {}",
            command.get_program().to_string_lossy(),
            command
                .get_args()
                .map(|arg| arg.to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
        log_event(
            logs_dir,
            stage,
            "INFO",
            format!("python bridge command (attempt {attempt}/{total_attempts}): {rendered}"),
        );

        let mut child = match command.spawn() {
            Ok(value) => value,
            Err(err) => {
                let retryable = matches!(
                    err.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                );
                if retryable && attempt < total_attempts {
                    let delay = retry_delay_for_attempt(attempt, base_delay_ms);
                    log_event(
                        logs_dir,
                        stage,
                        "WARN",
                        format!(
                            "python bridge execution error (attempt {attempt}/{total_attempts}), retrying in {}ms: {}",
                            delay.as_millis(),
                            err
                        ),
                    );
                    std::thread::sleep(delay);
                    continue;
                }
                return Err(err).with_context(|| {
                    format!("python bridge stage '{stage}' failed to spawn/execute")
                });
            }
        };
        let stdout = child
            .stdout
            .take()
            .context("python bridge missing stdout pipe")?;
        let stderr = child
            .stderr
            .take()
            .context("python bridge missing stderr pipe")?;
        let (tx, rx) = mpsc::channel::<(bool, String)>();
        let stdout_handle = spawn_bridge_reader(stdout, false, tx.clone());
        let stderr_handle = spawn_bridge_reader(stderr, true, tx);

        let mut stdout_text = String::new();
        let mut stderr_text = String::new();
        let mut final_payload: Option<Value> = None;
        while let Ok((is_stderr, line)) = rx.recv() {
            if line.trim().is_empty() {
                continue;
            }
            if is_stderr {
                stderr_text.push_str(&line);
                stderr_text.push('\n');
            } else {
                stdout_text.push_str(&line);
                stdout_text.push('\n');
            }
            if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
                if value.get("event").is_some() {
                    on_event(&value);
                    if let Some(event) = value.get("event").and_then(Value::as_str) {
                        if event == "progress" {
                            if let Some(processed) = value.get("processed").and_then(Value::as_u64)
                            {
                                let skipped =
                                    value.get("skipped").and_then(Value::as_u64).unwrap_or(0);
                                let errors = value
                                    .get("errors")
                                    .and_then(Value::as_u64)
                                    .or_else(|| value.get("skipped_errors").and_then(Value::as_u64))
                                    .unwrap_or(0);
                                log_progress(
                                    logs_dir,
                                    stage,
                                    processed,
                                    None,
                                    Some(skipped),
                                    Some(errors),
                                );
                            }
                        } else if event == "compaction" {
                            let phase = value
                                .get("phase")
                                .and_then(Value::as_str)
                                .unwrap_or("update");
                            log_event(logs_dir, stage, "INFO", format!("compaction phase={phase}"));
                        }
                    }
                    continue;
                }
                final_payload = Some(value);
                continue;
            }

            if is_stderr {
                log_event(logs_dir, stage, "WARN", format!("python stderr: {line}"));
            } else {
                log_event(logs_dir, stage, "INFO", format!("python stdout: {line}"));
            }
        }
        let status = child.wait()?;
        let _ = stdout_handle.join();
        let _ = stderr_handle.join();
        if !status.success() {
            let code = status.code();
            if matches!(code, Some(130)) {
                bail!("python bridge stage '{stage}' interrupted");
            }
            let retryable = should_retry_bridge_failure(code, &stdout_text, &stderr_text);
            if retryable && attempt < total_attempts {
                let delay = retry_delay_for_attempt(attempt, base_delay_ms);
                log_event(
                    logs_dir,
                    stage,
                    "WARN",
                    format!(
                        "python bridge failed (attempt {attempt}/{total_attempts}, exit {}), retrying in {}ms: stderr='{}'",
                        code.unwrap_or(-1),
                        delay.as_millis(),
                        stderr_text.trim()
                    ),
                );
                std::thread::sleep(delay);
                continue;
            }
            bail!(
                "python bridge stage '{}' failed after {} attempt(s) (exit {}): stdout='{}' stderr='{}'",
                stage,
                attempt,
                code.unwrap_or(-1),
                stdout_text.trim(),
                stderr_text.trim()
            );
        }

        let value = final_payload.with_context(|| {
            format!(
                "python bridge stage '{}' did not emit parseable JSON output: {}",
                stage,
                stdout_text.trim()
            )
        })?;
        on_event(&value);
        if !stderr_text.trim().is_empty() {
            log_event(
                logs_dir,
                stage,
                "INFO",
                format!("python stderr: {}", stderr_text.trim()),
            );
        }
        return Ok(value);
    }

    bail!("python bridge stage '{stage}' failed unexpectedly without output")
}

pub(crate) fn run_python_passthrough(args: &[String]) -> anyhow::Result<()> {
    let mut command = build_python_passthrough_command(args);
    let rendered = format!(
        "{} {}",
        command.get_program().to_string_lossy(),
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let query_like = is_query_like_passthrough(args);
    if query_like {
        eprintln!("loading: starting query command");
        eprintln!("loading: resolving Python bridge runtime");
        let _ = std::io::stderr().flush();
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to execute python passthrough command: {rendered}"))?;
    if query_like {
        eprintln!("loading: Python bridge launched, waiting for search engine startup");
        let _ = std::io::stderr().flush();
    }
    let status = child.wait()?;

    if status.success() {
        return Ok(());
    }
    bail!(
        "python passthrough command failed (exit {}): {}",
        status.code().unwrap_or(-1),
        rendered
    );
}

fn is_query_like_passthrough(args: &[String]) -> bool {
    matches!(args.first().map(String::as_str), Some("query" | "search"))
}

#[cfg(test)]
mod tests {
    use super::is_query_like_passthrough;

    #[test]
    fn query_like_passthrough_detects_query_aliases() {
        assert!(is_query_like_passthrough(&["query".to_string()]));
        assert!(is_query_like_passthrough(&["search".to_string()]));
        assert!(!is_query_like_passthrough(&["stats".to_string()]));
        assert!(!is_query_like_passthrough(&[]));
    }
}

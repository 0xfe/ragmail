use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use chrono::Local;
use serde_json::Value;
pub(crate) fn details_map(value: Value) -> Option<serde_json::Map<String, Value>> {
    value.as_object().cloned()
}

fn log_timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn write_log_line(logs_dir: &Path, stage: &str, level: &str, message: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(logs_dir)?;
    let log_path = logs_dir.join(format!("{stage}.log"));
    let mut handle = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    writeln!(handle, "{} | {:<5} | {}", log_timestamp(), level, message)?;
    Ok(())
}

pub(crate) fn log_event(logs_dir: &Path, stage: &str, level: &str, message: impl AsRef<str>) {
    if let Err(err) = write_log_line(logs_dir, stage, level, message.as_ref()) {
        eprintln!(
            "warning: failed to write stage log stage={} level={} error={err}",
            stage, level
        );
    }
}

pub(crate) fn log_progress(
    logs_dir: &Path,
    stage: &str,
    processed: u64,
    total: Option<u64>,
    skipped: Option<u64>,
    errors: Option<u64>,
) {
    let mut parts = vec![format!("progress {processed}")];
    if let Some(total_value) = total {
        if total_value > 0 {
            let pct = processed as f64 / total_value as f64 * 100.0;
            parts.push(format!("of {total_value} ({pct:0.1}%)"));
        }
    }
    if let Some(skipped_value) = skipped {
        if skipped_value > 0 {
            parts.push(format!("skipped {skipped_value}"));
        }
    }
    if let Some(errors_value) = errors {
        if errors_value > 0 {
            parts.push(format!("errors {errors_value}"));
        }
    }
    log_event(logs_dir, stage, "INFO", parts.join(" "));
}

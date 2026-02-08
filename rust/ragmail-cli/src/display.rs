use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ragmail_core::workspace::Workspace;
use serde_json::{json, Value};

use crate::file_ops::collect_split_files;
const APP_VERSION: &str = env!("RAGMAIL_VERSION");
const DEFAULT_EMBEDDING_MODEL: &str = "nomic-ai/nomic-embed-text-v1.5";

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_RED: &str = "\x1b[91m";
const ANSI_GREEN: &str = "\x1b[92m";
const ANSI_YELLOW: &str = "\x1b[93m";
const ANSI_BLUE: &str = "\x1b[94m";
const ANSI_CYAN: &str = "\x1b[96m";

#[derive(Clone, Debug)]
struct StageProgress {
    processed: u64,
    total: Option<u64>,
    skipped: u64,
    meta: HashMap<String, Value>,
}

#[derive(Debug)]
struct StageDisplayState {
    stages: Vec<String>,
    status: HashMap<String, String>,
    progress: HashMap<String, StageProgress>,
    durations: HashMap<String, f64>,
    spinner_idx: HashMap<String, usize>,
    stage_width: usize,
    status_width: usize,
    duration_width: usize,
    lines_printed: usize,
    note: String,
    last_render: Instant,
    dirty: bool,
}

#[derive(Debug)]
pub(crate) struct StageDisplay {
    state: Arc<Mutex<StageDisplayState>>,
    stop_spinner: Arc<AtomicBool>,
    spinner_handle: Option<thread::JoinHandle<()>>,
}

impl StageDisplay {
    pub(crate) fn new(stages: &[&str]) -> Self {
        let stage_names = stages
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let mut status = HashMap::new();
        let mut progress = HashMap::new();
        let mut spinner_idx = HashMap::new();
        for stage in &stage_names {
            status.insert(stage.clone(), "pending".to_string());
            progress.insert(
                stage.clone(),
                StageProgress {
                    processed: 0,
                    total: None,
                    skipped: 0,
                    meta: HashMap::new(),
                },
            );
            spinner_idx.insert(stage.clone(), 0);
        }
        let stage_width = stage_names.iter().map(String::len).max().unwrap_or(8);
        let status_width = "pending".len();
        Self {
            state: Arc::new(Mutex::new(StageDisplayState {
                stages: stage_names,
                status,
                progress,
                durations: HashMap::new(),
                spinner_idx,
                stage_width,
                status_width,
                duration_width: 0,
                lines_printed: 0,
                note: String::new(),
                last_render: Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
                dirty: false,
            })),
            stop_spinner: Arc::new(AtomicBool::new(false)),
            spinner_handle: None,
        }
    }

    pub(crate) fn start_spinner(&mut self) {
        if self.spinner_handle.is_some() {
            return;
        }
        let state = Arc::clone(&self.state);
        let stop = Arc::clone(&self.stop_spinner);
        self.spinner_handle = Some(thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                {
                    let mut guard = state.lock().expect("stage display lock");
                    for stage in guard.stages.clone() {
                        if let Some(status) = guard.status.get(&stage).map(String::as_str) {
                            if is_active_status(status) {
                                let spinner = guard.spinner_idx.entry(stage).or_insert(0);
                                *spinner = (*spinner + 1) % spinner_frames().len();
                            }
                        }
                    }
                }
                StageDisplay::render_locked(&state, false);
                thread::sleep(Duration::from_millis(200));
            }
        }));
    }

    pub(crate) fn stop_spinner(&mut self) {
        self.stop_spinner.store(true, Ordering::SeqCst);
        if let Some(handle) = self.spinner_handle.take() {
            let _ = handle.join();
        }
    }

    pub(crate) fn update_status(&self, stage: &str, status: &str, duration_s: Option<f64>) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            guard.status.insert(stage.to_string(), status.to_string());
            guard.status_width = guard.status_width.max(status.len());
            if let Some(duration) = duration_s {
                guard.durations.insert(stage.to_string(), duration);
                guard.duration_width = guard.duration_width.max(format_time(duration).len());
            }
        }
        self.render(true);
    }

    pub(crate) fn set_total(&self, stage: &str, total: u64) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            if let Some(progress) = guard.progress.get_mut(stage) {
                progress.total = Some(total);
            }
        }
        self.render(true);
    }

    pub(crate) fn update_progress(
        &self,
        stage: &str,
        processed: Option<u64>,
        skipped: Option<u64>,
        meta: Option<HashMap<String, Value>>,
    ) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            if let Some(progress) = guard.progress.get_mut(stage) {
                if let Some(processed_value) = processed {
                    progress.processed = processed_value;
                }
                if let Some(skipped_value) = skipped {
                    progress.skipped = skipped_value;
                }
                if let Some(extra_meta) = meta {
                    for (key, value) in extra_meta {
                        progress.meta.insert(key, value);
                    }
                }
            }
        }
        self.render(false);
    }

    pub(crate) fn note(&self, message: impl Into<String>) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            guard.note = message.into();
        }
        self.render(true);
    }

    pub(crate) fn render(&self, force: bool) {
        Self::render_locked(&self.state, force);
    }

    pub(crate) fn finish(&self) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            guard.note.clear();
        }
        self.render(true);
    }

    fn render_locked(state: &Arc<Mutex<StageDisplayState>>, force: bool) {
        let min_render_interval = Duration::from_millis(200);
        let mut guard = state.lock().expect("stage display lock");
        if !force && guard.last_render.elapsed() < min_render_interval {
            guard.dirty = true;
            return;
        }
        guard.dirty = false;
        clear_lines(guard.lines_printed);

        let mut lines = vec![format!("{ANSI_BOLD}Stages:{ANSI_RESET}")];
        for stage in guard.stages.clone() {
            let status = guard
                .status
                .get(&stage)
                .cloned()
                .unwrap_or_else(|| "pending".to_string());
            let color = stage_status_color(&status);
            let progress = guard
                .progress
                .get(&stage)
                .cloned()
                .unwrap_or(StageProgress {
                    processed: 0,
                    total: None,
                    skipped: 0,
                    meta: HashMap::new(),
                });
            let spinner = if is_active_status(&status) {
                let idx = *guard.spinner_idx.get(&stage).unwrap_or(&0);
                spinner_frames()[idx]
            } else {
                " "
            };
            let mut progress_text = format_progress_text(&stage, &progress);
            if stage == "preprocess" {
                let spam = progress
                    .meta
                    .get("spam")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let errors = progress
                    .meta
                    .get("errors")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                progress_text = format!(
                    "{progress_text}  skipped: {} bulk, {} errors",
                    format_count(spam),
                    format_count(errors)
                );
            } else {
                let skipped_exists = progress
                    .meta
                    .get("skipped_exists")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let skipped_errors = progress
                    .meta
                    .get("skipped_errors")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if skipped_exists > 0 || skipped_errors > 0 {
                    progress_text = format!(
                        "{progress_text}  skipped (exists: {}, errors: {})",
                        format_count(skipped_exists),
                        format_count(skipped_errors),
                    );
                } else if progress.skipped > 0 {
                    progress_text = format!(
                        "{progress_text}  skipped {}",
                        format_count(progress.skipped)
                    );
                }
            }

            let duration_text = guard
                .durations
                .get(&stage)
                .map(|duration| {
                    format!(
                        "  {ANSI_DIM}{:>width$}{ANSI_RESET}",
                        format_time(*duration),
                        width = guard.duration_width
                    )
                })
                .unwrap_or_default();
            lines.push(format!(
                "  {spinner} {color}{stage:<stage_width$}{ANSI_RESET} {color}{status:<status_width$}{ANSI_RESET}  {progress_text}{duration_text}",
                stage_width = guard.stage_width,
                status_width = guard.status_width
            ));
        }
        if !guard.note.is_empty() {
            lines.push(format!("{ANSI_DIM}{}{ANSI_RESET}", guard.note));
        }

        print!("{}", lines.join("\n"));
        println!();
        let _ = std::io::stdout().flush();

        guard.lines_printed = lines.len();
        guard.last_render = Instant::now();
    }
}

impl Drop for StageDisplay {
    fn drop(&mut self) {
        self.stop_spinner();
    }
}

fn spinner_frames() -> [&'static str; 10] {
    ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
}

fn clear_lines(n: usize) {
    for _ in 0..n {
        print!("\x1b[1A\r\x1b[2K");
    }
}

fn stage_status_color(status: &str) -> &'static str {
    match status {
        "done" => ANSI_GREEN,
        "running" | "starting" | "downloading" | "interrupted" => ANSI_YELLOW,
        "failed" => ANSI_RED,
        "skipped" => ANSI_BLUE,
        _ => ANSI_DIM,
    }
}

fn is_active_status(status: &str) -> bool {
    matches!(status, "running" | "starting" | "downloading")
}

fn format_progress_text(stage: &str, progress: &StageProgress) -> String {
    if let Some(startup_text) = progress.meta.get("startup_text").and_then(Value::as_str) {
        if !startup_text.trim().is_empty() {
            return startup_text.to_string();
        }
    }
    let mut text = match progress.total {
        Some(total) if total > 0 => {
            let pct = progress.processed as f64 / total as f64 * 100.0;
            format!(
                "{}/{} ({pct:5.1}%)",
                format_count(progress.processed),
                format_count(total)
            )
        }
        Some(0) => "0/0 (100.0%)".to_string(),
        _ => format_count(progress.processed),
    };
    if stage == "model"
        && (progress.meta.contains_key("downloaded_bytes")
            || progress.meta.contains_key("cache_bytes"))
    {
        let downloaded = progress
            .meta
            .get("downloaded_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cache = progress
            .meta
            .get("cache_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        text = format!(
            "{} downloaded  cache: {}",
            format_bytes(downloaded),
            format_bytes(cache)
        );
        if progress
            .meta
            .get("cache_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            text = format!("cache hit  {text}");
        }
        if let Some(elapsed_s) = progress.meta.get("elapsed_s").and_then(Value::as_f64) {
            if elapsed_s > 0.0 {
                text = format!("{text}  elapsed {}", format_time(elapsed_s));
            }
        }
    } else if stage == "split" && progress.meta.contains_key("bytes_total") {
        let bytes_total = progress
            .meta
            .get("bytes_total")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let bytes_processed = progress
            .meta
            .get("bytes_processed")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let bytes_text = if bytes_total > 0 {
            let pct = bytes_processed as f64 / bytes_total as f64 * 100.0;
            format!(
                "{}/{} ({pct:5.1}%)",
                format_bytes(bytes_processed),
                format_bytes(bytes_total)
            )
        } else {
            format_bytes(bytes_processed)
        };
        text = format!("{} emails  {bytes_text}", format_count(progress.processed));
    }
    text
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + (digits.len() / 3));
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn format_bytes(bytes: u64) -> String {
    let mut value = bytes as f64;
    for unit in ["B", "KB", "MB", "GB"] {
        if value < 1024.0 {
            return format!("{value:.1}{unit}");
        }
        value /= 1024.0;
    }
    format!("{value:.1}TB")
}

fn format_time(seconds: f64) -> String {
    if seconds < 60.0 {
        format!("{seconds:.0}s")
    } else if seconds < 3600.0 {
        let minutes = (seconds / 60.0).floor();
        let rem = seconds % 60.0;
        format!("{minutes:.0}m {rem:.0}s")
    } else {
        let hours = (seconds / 3600.0).floor();
        let minutes = ((seconds % 3600.0) / 60.0).floor();
        format!("{hours:.0}h {minutes:.0}m")
    }
}

pub(crate) fn print_pipeline_header(
    workspace_root: &Path,
    input_mboxes: &[PathBuf],
    years: &[u16],
    resume_effective: bool,
    refresh: bool,
    cache_root: &Path,
) {
    let input_label = if input_mboxes.is_empty() {
        "none".to_string()
    } else if input_mboxes.len() == 1 {
        input_mboxes[0].display().to_string()
    } else {
        format!(
            "{} (+{} more)",
            input_mboxes[0].display(),
            input_mboxes.len() - 1
        )
    };
    let years_label = if years.is_empty() {
        "all".to_string()
    } else {
        years
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",")
    };
    let resume_label = if resume_effective {
        "True".to_string()
    } else if refresh {
        "False (refresh)".to_string()
    } else {
        "False (state reset)".to_string()
    };
    let embedding = std::env::var("RAGMAIL_EMBEDDING_MODEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL.to_string());

    println!("{ANSI_CYAN}{ANSI_BOLD}RAGMail v{APP_VERSION} - running pipeline{ANSI_RESET}");
    println!("Workspace: {}", workspace_root.display());
    println!("Inputs:    {input_label}");
    println!("Years:     {years_label}");
    println!("Resume:    {resume_label}");
    println!("Cache:     {}", cache_root.display());
    println!("Embedding: {embedding}");
    println!();
}

fn stage_detail_u64(state: &Value, stage: &str, key: &str) -> u64 {
    state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get(stage))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn print_pipeline_summary(workspace: &Workspace, total_duration_s: f64) {
    let split_files = collect_split_files(workspace.split_dir().as_path())
        .map(|files| files.len())
        .unwrap_or(0);
    let state = workspace.load_state().unwrap_or_else(|_| json!({}));
    let emails_found = stage_detail_u64(&state, "split", "processed");
    let split_written = stage_detail_u64(&state, "split", "written");
    let split_errors = stage_detail_u64(&state, "split", "errors");
    let preprocess_total = stage_detail_u64(&state, "preprocess", "processed");
    let preprocess_spam = stage_detail_u64(&state, "preprocess", "spam");
    let preprocess_errors = stage_detail_u64(&state, "preprocess", "errors");
    let vectorized = stage_detail_u64(&state, "vectorize", "processed");
    let ingested = stage_detail_u64(&state, "ingest", "processed");
    let ingest_errors = stage_detail_u64(&state, "ingest", "errors");
    let split_error_text = if split_errors == 0 {
        "no errors".to_string()
    } else {
        format!("{} errors", format_count(split_errors))
    };

    println!();
    println!("{ANSI_BOLD}Outputs:{ANSI_RESET}");
    println!("  Mailbox files: {split_files}");
    println!("  Emails found: {}", format_count(emails_found));
    println!(
        "  Split: {} ({split_error_text})",
        format_count(split_written)
    );
    println!(
        "  Preprocessed: {} (ignoring: {} bulk, {} errors)",
        format_count(preprocess_total),
        format_count(preprocess_spam),
        format_count(preprocess_errors)
    );
    println!("  Vectorized: {}", format_count(vectorized));
    println!(
        "  Ingested: {} ({} errors)",
        format_count(ingested),
        format_count(ingest_errors)
    );
    println!("  Embeddings: {}", workspace.embeddings_dir().display());
    println!(
        "  Database: {}",
        workspace.db_dir().join("email_search.lancedb").display()
    );
    println!("  Total time: {}", format_time(total_duration_s));
    println!("  Logs: {}", workspace.logs_dir().display());
    println!();
}

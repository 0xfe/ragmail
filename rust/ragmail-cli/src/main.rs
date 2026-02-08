use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use chrono::Local;
use clap::ArgAction;
use clap::{Parser, Subcommand};
use ragmail_clean::{
    clean_mbox_file, clean_mbox_file_with_progress, default_clean_outputs, default_summary_output,
    CleanOptions,
};
use ragmail_core::workspace::Workspace;
use ragmail_index::{build_index_for_file, BuildOptions};
use ragmail_mbox::{
    split_mbox_by_month, split_mbox_by_month_with_options,
    split_mbox_by_month_with_options_and_progress,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
struct StageDisplay {
    state: Arc<Mutex<StageDisplayState>>,
    stop_spinner: Arc<AtomicBool>,
    spinner_handle: Option<thread::JoinHandle<()>>,
}

impl StageDisplay {
    fn new(stages: &[&str]) -> Self {
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

    fn start_spinner(&mut self) {
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

    fn stop_spinner(&mut self) {
        self.stop_spinner.store(true, Ordering::SeqCst);
        if let Some(handle) = self.spinner_handle.take() {
            let _ = handle.join();
        }
    }

    fn update_status(&self, stage: &str, status: &str, duration_s: Option<f64>) {
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

    fn set_total(&self, stage: &str, total: u64) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            if let Some(progress) = guard.progress.get_mut(stage) {
                progress.total = Some(total);
            }
        }
        self.render(true);
    }

    fn update_progress(
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

    fn note(&self, message: impl Into<String>) {
        {
            let mut guard = self.state.lock().expect("stage display lock");
            guard.note = message.into();
        }
        self.render(true);
    }

    fn render(&self, force: bool) {
        Self::render_locked(&self.state, force);
    }

    fn finish(&self) {
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

fn print_pipeline_header(
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

fn print_pipeline_summary(workspace: &Workspace, total_duration_s: f64) {
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

fn is_interrupted_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("interrupted")
}

struct PipelineRunOptions<'a> {
    input_mboxes: &'a [PathBuf],
    workspace_name: &'a str,
    base_dir: Option<&'a Path>,
    stages_raw: &'a str,
    resume: bool,
    refresh: bool,
    checkpoint_interval: u64,
    years: &'a [u16],
    clean_dir: Option<&'a Path>,
    embeddings_dir: Option<&'a Path>,
    db_path: Option<&'a Path>,
    ingest_batch_size: Option<u64>,
    embedding_batch_size: Option<u64>,
    chunk_size: Option<u64>,
    chunk_overlap: Option<u64>,
    compact_every: Option<u64>,
    skip_exists_check: bool,
    repair_embeddings: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "ragmail",
    version = APP_VERSION,
    about = "Rust-first CLI harness for ragmail"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Rust-native pipeline.
    Pipeline {
        /// Input MBOX files.
        #[arg(value_name = "INPUT_MBOX")]
        input_mbox: Vec<PathBuf>,
        /// Workspace name.
        #[arg(long)]
        workspace: String,
        /// Optional base directory for workspaces (default: `workspaces`).
        #[arg(long, value_name = "DIR")]
        base_dir: Option<PathBuf>,
        /// Comma-separated stage list (`split,preprocess,vectorize,ingest`; optional `model`).
        #[arg(long, default_value = "split,preprocess,vectorize,ingest")]
        stages: String,
        /// Resume from stage checkpoints where available.
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        resume: bool,
        /// Refresh selected stage outputs before running.
        #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
        refresh: bool,
        /// Checkpoint interval in seconds.
        #[arg(long, default_value_t = 30)]
        checkpoint_interval: u64,
        /// Optional repeated year filters (`--years 2024 --years 2025`).
        #[arg(long)]
        years: Vec<u16>,
        /// Use clean JSONL files from this directory (vectorize/ingest only).
        #[arg(long, value_name = "DIR")]
        clean_dir: Option<PathBuf>,
        /// Use embeddings from or write embeddings to this directory.
        #[arg(long, value_name = "DIR")]
        embeddings_dir: Option<PathBuf>,
        /// Override ingest database path (defaults to workspace db path).
        #[arg(long, value_name = "PATH")]
        db_path: Option<PathBuf>,
        /// Emails per write batch.
        #[arg(long)]
        ingest_batch_size: Option<u64>,
        /// Embedding model batch size.
        #[arg(long)]
        embedding_batch_size: Option<u64>,
        /// Max characters per body chunk.
        #[arg(long)]
        chunk_size: Option<u64>,
        /// Chunk overlap in characters.
        #[arg(long)]
        chunk_overlap: Option<u64>,
        /// Run compaction every N ingested emails.
        #[arg(long)]
        compact_every: Option<u64>,
        /// Skip per-email existence check for ingest.
        #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
        skip_exists_check: bool,
        /// Disable automatic repair of missing embeddings during ingest.
        #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
        no_repair_embeddings: bool,
    },
    /// Split an MBOX into monthly files (`YYYY-MM.mbox`).
    Split {
        /// Input MBOX file.
        input: PathBuf,
        /// Output directory for split month files.
        #[arg(long, value_name = "DIR")]
        output_dir: PathBuf,
        /// Optional repeated year filters (`--years 2024 --years 2025`).
        #[arg(long)]
        years: Vec<u16>,
        /// Optional resume offset.
        #[arg(long, default_value_t = 0)]
        start_offset: u64,
        /// Optional checkpoint file path.
        #[arg(long, value_name = "PATH")]
        checkpoint: Option<PathBuf>,
        /// Resume from checkpoint/start offset.
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        resume: bool,
        /// Checkpoint interval in seconds.
        #[arg(long, default_value_t = 30)]
        checkpoint_interval: u64,
    },
    /// Build a byte-offset index JSONL for an MBOX file (compatibility helper).
    #[command(hide = true)]
    Index {
        /// Input MBOX file.
        input: PathBuf,
        /// Value to store in the `mbox_file` field.
        #[arg(long)]
        mbox_file: String,
        /// Output index JSONL path.
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
        /// Optional checkpoint path for resume support.
        #[arg(long, value_name = "PATH")]
        checkpoint: Option<PathBuf>,
        /// Resume from checkpoint/output append mode.
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        resume: bool,
        /// Checkpoint interval in seconds.
        #[arg(long, default_value_t = 30)]
        checkpoint_interval: u64,
    },
    /// Preprocess an MBOX into clean/spam/index outputs.
    #[command(name = "preprocess", alias = "clean")]
    Preprocess {
        /// Input MBOX file.
        input: PathBuf,
        /// Output clean JSONL path (`*.clean.jsonl`).
        #[arg(long, value_name = "PATH")]
        output_clean: Option<PathBuf>,
        /// Output spam JSONL path (`*.spam.jsonl`).
        #[arg(long, value_name = "PATH")]
        output_spam: Option<PathBuf>,
        /// Optional output index JSONL path for this input.
        #[arg(long, value_name = "PATH")]
        index_output: Option<PathBuf>,
        /// Resume offset for partial runs.
        #[arg(long, default_value_t = 0)]
        start_offset: u64,
        /// Append mode for output files.
        #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
        append: bool,
        /// Optional mbox file label to store in each clean row.
        #[arg(long)]
        mbox_file: Option<String>,
        /// Output summary path (`*.mbox.summary`).
        #[arg(long, value_name = "PATH")]
        summary_output: Option<PathBuf>,
    },
    /// Print resolved version.
    Version,
    /// Forward non-Rust commands to the Python bridge CLI.
    #[command(external_subcommand)]
    Passthrough(Vec<String>),
}

fn run_pipeline(options: &PipelineRunOptions<'_>) -> anyhow::Result<()> {
    let pipeline_started = Instant::now();
    let stages = parse_stage_selection(options.stages_raw)?;
    let wants_model = stages.contains("model");
    let wants_split = stages.contains("split");
    let wants_preprocess = stages.contains("preprocess");
    let wants_vectorize = stages.contains("vectorize");
    let wants_ingest = stages.contains("ingest");
    if wants_split && options.input_mboxes.is_empty() {
        bail!("split stage requires at least one input mbox");
    }
    if options.clean_dir.is_some() && (wants_split || wants_preprocess) {
        bail!("--clean-dir can only be used for vectorize/ingest stages");
    }

    let workspace_base = options.base_dir.unwrap_or_else(|| Path::new("workspaces"));
    let workspace_root = workspace_base.join(options.workspace_name);
    let workspace = Workspace::new(options.workspace_name.to_string(), workspace_root.clone());
    workspace.ensure()?;
    if options.refresh {
        workspace.apply_refresh(&stages)?;
    }
    let resume_effective = options.resume && !options.refresh;
    if !resume_effective && !options.refresh {
        workspace.reset_state()?;
    }

    let split_dir = workspace.split_dir();
    let clean_dir = workspace.clean_dir();
    let spam_dir = workspace.spam_dir();
    let reports_dir = workspace.reports_dir();
    let checkpoints_dir = workspace.checkpoints_dir();
    let logs_dir = workspace.logs_dir();
    let cache_root = std::env::var("RAGMAIL_CACHE_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if let Some(base_dir) = options.base_dir {
                base_dir.join(".ragmail-cache")
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .join(".ragmail-cache")
            }
        });
    print_pipeline_header(
        &workspace_root,
        options.input_mboxes,
        options.years,
        resume_effective,
        options.refresh,
        &cache_root,
    );
    let display_stages: Vec<&str> = if wants_model {
        vec!["model", "split", "preprocess", "vectorize", "ingest"]
    } else {
        vec!["split", "preprocess", "vectorize", "ingest"]
    };
    let mut stage_display = StageDisplay::new(display_stages.as_slice());
    stage_display.render(true);
    stage_display.start_spinner();

    let checkpoint_every = Duration::from_secs(options.checkpoint_interval);
    let year_filter = if options.years.is_empty() {
        None
    } else {
        Some(options.years.iter().copied().collect::<BTreeSet<u16>>())
    };
    log_event(
        logs_dir.as_path(),
        "pipeline",
        "INFO",
        format!(
            "start workspace={} stages={} resume={} refresh={} inputs={}",
            workspace_root.display(),
            options.stages_raw,
            resume_effective,
            options.refresh,
            options.input_mboxes.len()
        ),
    );

    let model_should_run = wants_model && (!resume_effective || !workspace.stage_done("model")?);
    if model_should_run {
        let model_started = Instant::now();
        log_event(logs_dir.as_path(), "model", "INFO", "start");
        stage_display.update_status("model", "downloading", None);
        workspace.update_stage("model", "running", None)?;
        let mut args = vec![
            "--workspace".to_string(),
            options.workspace_name.to_string(),
        ];
        if let Some(base) = options.base_dir {
            args.extend(["--base-dir".to_string(), base.display().to_string()]);
        }
        let result = run_python_bridge("model", &args, logs_dir.as_path(), |event| {
            if event.get("event").and_then(Value::as_str) == Some("progress") {
                let downloaded = event
                    .get("downloaded_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cache_bytes = event
                    .get("cache_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let mut meta = HashMap::new();
                meta.insert("downloaded_bytes".to_string(), Value::from(downloaded));
                meta.insert("cache_bytes".to_string(), Value::from(cache_bytes));
                if let Some(elapsed_s) = event.get("elapsed_s").and_then(Value::as_f64) {
                    if let Some(number) = serde_json::Number::from_f64(elapsed_s) {
                        meta.insert("elapsed_s".to_string(), Value::Number(number));
                    }
                }
                stage_display.update_progress("model", Some(0), None, Some(meta));
            }
        });
        let value = match result {
            Ok(payload) => payload,
            Err(err) => {
                log_event(
                    logs_dir.as_path(),
                    "model",
                    "ERROR",
                    format!("failed: {err}"),
                );
                if is_interrupted_error(&err) {
                    stage_display.note(
                        "Interrupt received. Finishing current batch and saving checkpoints...",
                    );
                    stage_display.update_status("model", "interrupted", None);
                    workspace.update_stage(
                        "model",
                        "interrupted",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                } else {
                    stage_display.update_status("model", "failed", None);
                    workspace.update_stage(
                        "model",
                        "failed",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                }
                return Err(err);
            }
        };
        let downloaded_bytes = value
            .get("downloaded_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cache_bytes = value
            .get("cache_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let model_duration_s = model_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "model",
            "done",
            details_map(json!({
                "downloaded_bytes": downloaded_bytes,
                "cache_bytes": cache_bytes,
                "duration_s": model_duration_s
            })),
        )?;
        log_event(
            logs_dir.as_path(),
            "model",
            "INFO",
            format!(
                "done downloaded_bytes={} cache_bytes={} duration_s={:.3}",
                downloaded_bytes, cache_bytes, model_duration_s
            ),
        );
        let mut meta = HashMap::new();
        meta.insert(
            "downloaded_bytes".to_string(),
            Value::from(downloaded_bytes),
        );
        meta.insert("cache_bytes".to_string(), Value::from(cache_bytes));
        meta.insert(
            "cache_hit".to_string(),
            Value::from(downloaded_bytes == 0 && cache_bytes > 0),
        );
        meta.insert("elapsed_s".to_string(), Value::Null);
        stage_display.update_progress("model", Some(0), None, Some(meta));
        stage_display.update_status("model", "done", Some(model_duration_s));
    } else if wants_model {
        log_event(logs_dir.as_path(), "model", "INFO", "skipped");
        stage_display.update_status("model", "skipped", None);
    }

    let existing_split_files = collect_split_files(split_dir.as_path())?;
    let split_outputs_exist = !existing_split_files.is_empty();
    let split_should_run = wants_split
        && (!resume_effective || !workspace.stage_done("split")? || !split_outputs_exist);
    if split_should_run {
        let split_started = Instant::now();
        log_event(logs_dir.as_path(), "split", "INFO", "start");
        stage_display.update_status("split", "starting", None);
        let split_inputs = options
            .input_mboxes
            .iter()
            .map(|path| Value::String(path.display().to_string()))
            .collect::<Vec<_>>();
        workspace.update_stage(
            "split",
            "running",
            details_map(json!({"inputs": split_inputs})),
        )?;
        let split_checkpoint_dir = checkpoints_dir.join("split-rs");
        if !resume_effective && split_checkpoint_dir.exists() {
            std::fs::remove_dir_all(&split_checkpoint_dir)?;
        }
        std::fs::create_dir_all(&split_checkpoint_dir)?;
        let split_total_bytes = options
            .input_mboxes
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|meta| meta.len())
            .sum::<u64>();
        stage_display.update_progress(
            "split",
            Some(0),
            Some(0),
            Some(HashMap::from([
                (
                    "startup_text".to_string(),
                    Value::from("initializing split inputs"),
                ),
                ("bytes_total".to_string(), Value::from(split_total_bytes)),
                ("bytes_processed".to_string(), Value::from(0)),
            ])),
        );
        let mut split_processed = 0_u64;
        let mut split_written = 0_u64;
        let mut split_skipped = 0_u64;
        let mut split_errors = 0_u64;
        let mut split_bytes_processed = 0_u64;
        let split_file_count = options.input_mboxes.len();
        let mut split_processing_started = false;
        let split_result = (|| -> anyhow::Result<()> {
            for (split_input_idx, input) in options.input_mboxes.iter().enumerate() {
                let file_name = input
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                    .to_string();
                stage_display.update_progress(
                    "split",
                    Some(split_processed),
                    Some(split_skipped),
                    Some(HashMap::from([
                        (
                            "startup_text".to_string(),
                            Value::from(format!(
                                "opening file {}/{}: {}",
                                split_input_idx + 1,
                                split_file_count,
                                file_name
                            )),
                        ),
                        ("bytes_total".to_string(), Value::from(split_total_bytes)),
                        (
                            "bytes_processed".to_string(),
                            Value::from(split_bytes_processed.min(split_total_bytes)),
                        ),
                    ])),
                );
                let checkpoint_path = split_checkpoint_path(&split_checkpoint_dir, input);
                let start_offset = if resume_effective {
                    load_last_position(&checkpoint_path)?
                } else {
                    0
                };
                log_event(
                    logs_dir.as_path(),
                    "split",
                    "INFO",
                    format!(
                        "input={} start_offset={} checkpoint={}",
                        input.display(),
                        start_offset,
                        checkpoint_path.display()
                    ),
                );
                let base_processed = split_processed;
                let base_written = split_written;
                let base_skipped = split_skipped;
                let base_errors = split_errors;
                let input_total_bytes =
                    std::fs::metadata(input).map(|meta| meta.len()).unwrap_or(0);
                let bytes_before = split_bytes_processed;
                let mut on_progress = |stats: &ragmail_mbox::SplitStats| {
                    if !split_processing_started {
                        split_processing_started = true;
                        stage_display.update_status("split", "running", None);
                    }
                    let current_processed = base_processed + stats.processed;
                    let current_skipped = base_skipped + stats.skipped;
                    let current_errors = base_errors + stats.errors;
                    let current_bytes = bytes_before + stats.last_position.min(input_total_bytes);
                    log_progress(
                        logs_dir.as_path(),
                        "split",
                        current_processed,
                        None,
                        Some(current_skipped),
                        Some(current_errors),
                    );
                    let mut meta = HashMap::new();
                    meta.insert("startup_text".to_string(), Value::from(""));
                    meta.insert("bytes_total".to_string(), Value::from(split_total_bytes));
                    meta.insert(
                        "bytes_processed".to_string(),
                        Value::from(current_bytes.min(split_total_bytes)),
                    );
                    stage_display.update_progress(
                        "split",
                        Some(current_processed),
                        Some(current_skipped),
                        Some(meta),
                    );
                };
                let stats = split_mbox_by_month_with_options_and_progress(
                    input,
                    split_dir.as_path(),
                    start_offset,
                    year_filter.as_ref(),
                    Some(checkpoint_path.as_path()),
                    checkpoint_every,
                    Duration::from_millis(250),
                    &mut on_progress,
                )
                .with_context(|| format!("split failed for {}", input.display()))?;
                split_processed = base_processed + stats.processed;
                split_written = base_written + stats.written;
                split_skipped = base_skipped + stats.skipped;
                split_errors = base_errors + stats.errors;
                split_bytes_processed = bytes_before + input_total_bytes;
                if !split_processing_started {
                    split_processing_started = true;
                    stage_display.update_status("split", "running", None);
                    stage_display.update_progress(
                        "split",
                        Some(split_processed),
                        Some(split_skipped),
                        Some(HashMap::from([(
                            "startup_text".to_string(),
                            Value::from(""),
                        )])),
                    );
                }
                if checkpoint_path.exists() {
                    std::fs::remove_file(checkpoint_path)?;
                }
            }
            Ok(())
        })();
        if let Err(err) = split_result {
            log_event(
                logs_dir.as_path(),
                "split",
                "ERROR",
                format!("failed: {err}"),
            );
            workspace.update_stage(
                "split",
                "failed",
                details_map(json!({"error": err.to_string()})),
            )?;
            if is_interrupted_error(&err) {
                stage_display
                    .note("Interrupt received. Finishing current batch and saving checkpoints...");
                stage_display.update_status("split", "interrupted", None);
                workspace.update_stage(
                    "split",
                    "interrupted",
                    details_map(json!({"error": err.to_string()})),
                )?;
            } else {
                stage_display.update_status("split", "failed", None);
            }
            return Err(err);
        }
        let split_duration_s = split_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "split",
            "done",
            details_map(json!({
                "output_dir": split_dir.display().to_string(),
                "processed": split_processed,
                "written": split_written,
                "skipped": split_skipped,
                "errors": split_errors,
                "duration_s": split_duration_s
            })),
        )?;
        log_event(
            logs_dir.as_path(),
            "split",
            "INFO",
            format!(
                "done processed={} written={} skipped={} errors={} duration_s={:.3}",
                split_processed, split_written, split_skipped, split_errors, split_duration_s
            ),
        );
        stage_display.update_progress(
            "split",
            Some(split_processed),
            Some(split_skipped),
            Some(HashMap::from([(
                "startup_text".to_string(),
                Value::from(""),
            )])),
        );
        stage_display.update_status("split", "done", Some(split_duration_s));
    } else if wants_split {
        log_event(logs_dir.as_path(), "split", "INFO", "skipped");
        stage_display.update_status("split", "skipped", None);
    }

    let split_files = collect_split_files(split_dir.as_path())?;
    if wants_preprocess && split_files.is_empty() {
        log_event(
            logs_dir.as_path(),
            "split",
            "ERROR",
            "no split outputs found",
        );
        bail!("no split outputs found in {}", split_dir.display());
    }

    let preprocess_checkpoint_dir = checkpoints_dir.join("preprocess-rs");
    let preprocess_index_parts_dir = preprocess_checkpoint_dir.join("index-parts");
    let output_index = split_dir.join("mbox_index.jsonl");

    let expected_preprocess_missing = split_files.iter().any(|split_file| {
        let stem = split_file
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let mbox_name = split_file
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let clean_path = clean_dir.join(format!("{stem}.clean.jsonl"));
        let spam_path = spam_dir.join(format!("{stem}.spam.jsonl"));
        let summary_path = reports_dir.join(format!("{mbox_name}.summary"));
        let index_part_path = preprocess_index_parts_dir.join(format!("{mbox_name}.jsonl"));
        !(clean_path.exists()
            && spam_path.exists()
            && summary_path.exists()
            && index_part_path.exists())
    });
    let preprocess_should_run = wants_preprocess
        && (!resume_effective
            || !workspace.stage_done("preprocess")?
            || !output_index.exists()
            || expected_preprocess_missing);
    if preprocess_should_run {
        let preprocess_started = Instant::now();
        log_event(logs_dir.as_path(), "preprocess", "INFO", "start");
        stage_display.update_status("preprocess", "starting", None);
        workspace.update_stage(
            "preprocess",
            "running",
            details_map(json!({"files": split_files.len()})),
        )?;
        let mut preprocess_total = 0_u64;
        let preprocess_file_count = split_files.len();
        for (idx, split_file) in split_files.iter().enumerate() {
            let file_name = split_file
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_string();
            stage_display.update_progress(
                "preprocess",
                Some(0),
                Some(0),
                Some(HashMap::from([(
                    "startup_text".to_string(),
                    Value::from(format!(
                        "scanning split files {}/{}: {}",
                        idx + 1,
                        preprocess_file_count,
                        file_name
                    )),
                )])),
            );
            preprocess_total += count_mbox_envelopes(split_file).unwrap_or(0);
        }
        stage_display.set_total("preprocess", preprocess_total);
        stage_display.update_status("preprocess", "running", None);
        stage_display.update_progress(
            "preprocess",
            Some(0),
            Some(0),
            Some(HashMap::from([(
                "startup_text".to_string(),
                Value::from(""),
            )])),
        );
        if !resume_effective {
            if output_index.exists() {
                std::fs::remove_file(&output_index)?;
            }
            if preprocess_checkpoint_dir.exists() {
                std::fs::remove_dir_all(&preprocess_checkpoint_dir)?;
            }
        }
        std::fs::create_dir_all(&preprocess_index_parts_dir)?;

        let mut preprocess_processed = 0_u64;
        let mut preprocess_written = 0_u64;
        let mut preprocess_spam = 0_u64;
        let mut preprocess_errors = 0_u64;
        let preprocess_result = (|| -> anyhow::Result<()> {
            for split_file in &split_files {
                let stem = split_file
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid split file stem: {}", split_file.display())
                    })?;
                let mbox_name = split_file
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid split file name: {}", split_file.display())
                    })?
                    .to_string();
                let clean_output = clean_dir.join(format!("{stem}.clean.jsonl"));
                let spam_output = spam_dir.join(format!("{stem}.spam.jsonl"));
                let summary_output = reports_dir.join(format!("{mbox_name}.summary"));
                let index_part_output =
                    preprocess_index_parts_dir.join(format!("{mbox_name}.jsonl"));
                if resume_effective
                    && clean_output.exists()
                    && spam_output.exists()
                    && summary_output.exists()
                    && index_part_output.exists()
                {
                    log_event(
                        logs_dir.as_path(),
                        "preprocess",
                        "INFO",
                        format!(
                            "skip existing outputs clean={} spam={} summary={} index_part={}",
                            clean_output.display(),
                            spam_output.display(),
                            summary_output.display(),
                            index_part_output.display()
                        ),
                    );
                    preprocess_processed += count_non_empty_lines(&clean_output)?;
                    stage_display.update_progress(
                        "preprocess",
                        Some(preprocess_processed),
                        Some(preprocess_spam + preprocess_errors),
                        Some(HashMap::from([
                            ("spam".to_string(), Value::from(preprocess_spam)),
                            ("errors".to_string(), Value::from(preprocess_errors)),
                        ])),
                    );
                    continue;
                }
                let options = CleanOptions {
                    start_offset: 0,
                    append: false,
                    mbox_file_name: Some(mbox_name),
                    summary_output: Some(summary_output),
                    index_output: Some(index_part_output.clone()),
                };
                let base_processed = preprocess_processed;
                let base_written = preprocess_written;
                let base_spam = preprocess_spam;
                let base_errors = preprocess_errors;
                let mut on_progress = |stats: &ragmail_clean::CleanStats| {
                    let current_processed = base_processed + stats.processed;
                    let current_spam = base_spam + stats.spam;
                    let current_errors = base_errors + stats.errors;
                    log_progress(
                        logs_dir.as_path(),
                        "preprocess",
                        current_processed,
                        Some(preprocess_total),
                        Some(current_spam + current_errors),
                        Some(current_errors),
                    );
                    stage_display.update_progress(
                        "preprocess",
                        Some(current_processed),
                        Some(current_spam + current_errors),
                        Some(HashMap::from([
                            ("spam".to_string(), Value::from(current_spam)),
                            ("errors".to_string(), Value::from(current_errors)),
                        ])),
                    );
                };
                let stats = clean_mbox_file_with_progress(
                    split_file,
                    &clean_output,
                    &spam_output,
                    &options,
                    Duration::from_millis(250),
                    Some(&mut on_progress),
                )
                .with_context(|| {
                    format!("preprocess failed for split file {}", split_file.display())
                })?;
                preprocess_processed = base_processed + stats.processed;
                preprocess_written = base_written + stats.clean;
                preprocess_spam = base_spam + stats.spam;
                preprocess_errors = base_errors + stats.errors;
            }
            merge_index_parts(
                preprocess_index_parts_dir.as_path(),
                &split_files,
                output_index.as_path(),
            )?;
            Ok(())
        })();
        if let Err(err) = preprocess_result {
            log_event(
                logs_dir.as_path(),
                "preprocess",
                "ERROR",
                format!("failed: {err}"),
            );
            workspace.update_stage(
                "preprocess",
                "failed",
                details_map(json!({"error": err.to_string()})),
            )?;
            if is_interrupted_error(&err) {
                stage_display
                    .note("Interrupt received. Finishing current batch and saving checkpoints...");
                stage_display.update_status("preprocess", "interrupted", None);
                workspace.update_stage(
                    "preprocess",
                    "interrupted",
                    details_map(json!({"error": err.to_string()})),
                )?;
            } else {
                stage_display.update_status("preprocess", "failed", None);
            }
            return Err(err);
        }
        let preprocess_duration_s = preprocess_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "preprocess",
            "done",
            details_map(json!({
                "clean_files": split_files.len(),
                "processed": preprocess_processed,
                "written": preprocess_written,
                "skipped": preprocess_spam + preprocess_errors,
                "spam": preprocess_spam,
                "errors": preprocess_errors,
                "index_output": output_index.display().to_string(),
                "duration_s": preprocess_duration_s
            })),
        )?;
        log_event(
            logs_dir.as_path(),
            "preprocess",
            "INFO",
            format!(
                "done processed={} clean={} spam={} errors={} duration_s={:.3}",
                preprocess_processed,
                preprocess_written,
                preprocess_spam,
                preprocess_errors,
                preprocess_duration_s
            ),
        );
        stage_display.update_progress(
            "preprocess",
            Some(preprocess_processed),
            Some(preprocess_spam + preprocess_errors),
            Some(HashMap::from([
                ("startup_text".to_string(), Value::from("")),
                ("spam".to_string(), Value::from(preprocess_spam)),
                ("errors".to_string(), Value::from(preprocess_errors)),
            ])),
        );
        stage_display.update_status("preprocess", "done", Some(preprocess_duration_s));
    } else if wants_preprocess {
        log_event(logs_dir.as_path(), "preprocess", "INFO", "skipped");
        stage_display.update_status("preprocess", "skipped", None);
    }

    let clean_source_dir = options.clean_dir.unwrap_or(clean_dir.as_path());
    let clean_files = collect_clean_files(clean_source_dir)?;
    if (wants_vectorize || wants_ingest) && clean_files.is_empty() {
        log_event(
            logs_dir.as_path(),
            "preprocess",
            "ERROR",
            "no clean outputs found",
        );
        bail!("no clean outputs found in {}", clean_source_dir.display());
    }

    let embeddings_dir = options
        .embeddings_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| workspace.embeddings_dir());
    let db_path = options
        .db_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| workspace.db_dir().join("email_search.lancedb"));

    let vectorize_should_run =
        wants_vectorize && (!resume_effective || !workspace.stage_done("vectorize")?);
    if vectorize_should_run {
        let vectorize_started = Instant::now();
        log_event(logs_dir.as_path(), "vectorize", "INFO", "start");
        let vectorize_total = count_non_empty_lines_for_paths(&clean_files)?;
        stage_display.update_status("vectorize", "starting", None);
        stage_display.set_total("vectorize", vectorize_total);
        stage_display.update_progress(
            "vectorize",
            Some(0),
            Some(0),
            Some(HashMap::from([(
                "startup_text".to_string(),
                Value::from("initializing vectorization"),
            )])),
        );
        workspace.update_stage(
            "vectorize",
            "running",
            details_map(json!({"files": clean_files.len(), "total": vectorize_total})),
        )?;
        let mut args = vec![
            "--workspace".to_string(),
            options.workspace_name.to_string(),
            "--checkpoint-interval".to_string(),
            options.checkpoint_interval.to_string(),
        ];
        if resume_effective {
            args.push("--resume".to_string());
        } else {
            args.push("--no-resume".to_string());
        }
        if let Some(base) = options.base_dir {
            args.extend(["--base-dir".to_string(), base.display().to_string()]);
        }
        if let Some(clean_root) = options.clean_dir {
            args.extend(["--clean-dir".to_string(), clean_root.display().to_string()]);
        }
        args.extend([
            "--embeddings-dir".to_string(),
            embeddings_dir.display().to_string(),
        ]);
        if let Some(value) = options.ingest_batch_size {
            args.extend(["--ingest-batch-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.embedding_batch_size {
            args.extend(["--embedding-batch-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.chunk_size {
            args.extend(["--chunk-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.chunk_overlap {
            args.extend(["--chunk-overlap".to_string(), value.to_string()]);
        }
        let mut vectorize_running = false;
        let result = run_python_bridge("vectorize", &args, logs_dir.as_path(), |event| {
            if event.get("event").and_then(Value::as_str) != Some("progress") {
                return;
            }
            let processed = event.get("processed").and_then(Value::as_u64);
            let skipped = event.get("skipped").and_then(Value::as_u64).or_else(|| {
                Some(
                    event
                        .get("skipped_exists")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        + event
                            .get("skipped_errors")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                )
            });
            let mut meta = HashMap::new();
            if let Some(value) = event.get("skipped_exists").and_then(Value::as_u64) {
                meta.insert("skipped_exists".to_string(), Value::from(value));
            }
            if let Some(value) = event.get("skipped_errors").and_then(Value::as_u64) {
                meta.insert("skipped_errors".to_string(), Value::from(value));
            }
            if let Some(value) = event.get("startup_text").and_then(Value::as_str) {
                meta.insert("startup_text".to_string(), Value::from(value.to_string()));
            }
            let has_startup_text = meta
                .get("startup_text")
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if !vectorize_running && (processed.unwrap_or(0) > 0 || !has_startup_text) {
                vectorize_running = true;
                stage_display.update_status("vectorize", "running", None);
                meta.insert("startup_text".to_string(), Value::from(""));
            }
            stage_display.update_progress(
                "vectorize",
                processed,
                skipped,
                if meta.is_empty() { None } else { Some(meta) },
            );
        });
        let value = match result {
            Ok(payload) => payload,
            Err(err) => {
                log_event(
                    logs_dir.as_path(),
                    "vectorize",
                    "ERROR",
                    format!("failed: {err}"),
                );
                if is_interrupted_error(&err) {
                    stage_display.note(
                        "Interrupt received. Finishing current batch and saving checkpoints...",
                    );
                    stage_display.update_status("vectorize", "interrupted", None);
                    workspace.update_stage(
                        "vectorize",
                        "interrupted",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                } else {
                    stage_display.update_status("vectorize", "failed", None);
                    workspace.update_stage(
                        "vectorize",
                        "failed",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                }
                return Err(err);
            }
        };
        let processed = value.get("processed").and_then(Value::as_u64).unwrap_or(0);
        let skipped = bridge_skipped_total(&value);
        let errors = bridge_error_total(&value);
        let files = value
            .get("files")
            .and_then(Value::as_u64)
            .unwrap_or(clean_files.len() as u64);
        let vectorize_duration_s = vectorize_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "vectorize",
            "done",
            details_map(json!({
                "output_dir": embeddings_dir.display().to_string(),
                "processed": processed,
                "written": processed,
                "files": files,
                "total": vectorize_total,
                "skipped": skipped,
                "errors": errors,
                "duration_s": vectorize_duration_s
            })),
        )?;
        log_progress(
            logs_dir.as_path(),
            "vectorize",
            processed,
            Some(vectorize_total),
            Some(skipped),
            Some(errors),
        );
        log_event(
            logs_dir.as_path(),
            "vectorize",
            "INFO",
            format!(
                "done processed={} total={} files={} skipped={} errors={} duration_s={:.3}",
                processed, vectorize_total, files, skipped, errors, vectorize_duration_s
            ),
        );
        let mut meta = HashMap::new();
        meta.insert(
            "skipped_exists".to_string(),
            Value::from(
                value
                    .get("skipped_exists")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            ),
        );
        meta.insert(
            "skipped_errors".to_string(),
            Value::from(
                value
                    .get("skipped_errors")
                    .and_then(Value::as_u64)
                    .unwrap_or(errors),
            ),
        );
        meta.insert("startup_text".to_string(), Value::from(""));
        stage_display.update_progress("vectorize", Some(processed), Some(skipped), Some(meta));
        stage_display.update_status("vectorize", "done", Some(vectorize_duration_s));
    } else if wants_vectorize {
        log_event(logs_dir.as_path(), "vectorize", "INFO", "skipped");
        stage_display.update_status("vectorize", "skipped", None);
    }

    let ingest_should_run =
        wants_ingest && (!resume_effective || !workspace.stage_done("ingest")?);
    if ingest_should_run {
        let ingest_started = Instant::now();
        log_event(logs_dir.as_path(), "ingest", "INFO", "start");
        let ingest_total = count_non_empty_lines_for_paths(&clean_files)?;
        stage_display.update_status("ingest", "starting", None);
        stage_display.set_total("ingest", ingest_total);
        stage_display.update_progress(
            "ingest",
            Some(0),
            Some(0),
            Some(HashMap::from([(
                "startup_text".to_string(),
                Value::from("initializing ingest"),
            )])),
        );
        workspace.update_stage(
            "ingest",
            "running",
            details_map(json!({"files": clean_files.len(), "total": ingest_total})),
        )?;
        let mut args = vec![
            "--workspace".to_string(),
            options.workspace_name.to_string(),
            "--db-path".to_string(),
            db_path.display().to_string(),
            "--embeddings-dir".to_string(),
            embeddings_dir.display().to_string(),
            "--checkpoint-interval".to_string(),
            options.checkpoint_interval.to_string(),
        ];
        if resume_effective {
            args.push("--resume".to_string());
        } else {
            args.push("--no-resume".to_string());
        }
        if let Some(base) = options.base_dir {
            args.extend(["--base-dir".to_string(), base.display().to_string()]);
        }
        if let Some(clean_root) = options.clean_dir {
            args.extend(["--clean-dir".to_string(), clean_root.display().to_string()]);
        }
        if options.skip_exists_check {
            args.extend(["--skip-exists-check".to_string(), "true".to_string()]);
        }
        if let Some(value) = options.ingest_batch_size {
            args.extend(["--ingest-batch-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.embedding_batch_size {
            args.extend(["--embedding-batch-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.chunk_size {
            args.extend(["--chunk-size".to_string(), value.to_string()]);
        }
        if let Some(value) = options.chunk_overlap {
            args.extend(["--chunk-overlap".to_string(), value.to_string()]);
        }
        if let Some(value) = options.compact_every {
            args.extend(["--compact-every".to_string(), value.to_string()]);
        }
        if !options.repair_embeddings {
            args.push("--no-repair-embeddings".to_string());
        }
        let mut ingest_running = false;
        let result = run_python_bridge("ingest", &args, logs_dir.as_path(), |event| {
            match event.get("event").and_then(Value::as_str) {
                Some("progress") => {
                    let processed = event.get("processed").and_then(Value::as_u64);
                    let skipped = event.get("skipped").and_then(Value::as_u64).or_else(|| {
                        Some(
                            event
                                .get("skipped_exists")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                                + event
                                    .get("skipped_errors")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0),
                        )
                    });
                    let mut meta = HashMap::new();
                    if let Some(value) = event.get("skipped_exists").and_then(Value::as_u64) {
                        meta.insert("skipped_exists".to_string(), Value::from(value));
                    }
                    if let Some(value) = event.get("skipped_errors").and_then(Value::as_u64) {
                        meta.insert("skipped_errors".to_string(), Value::from(value));
                    }
                    if let Some(value) = event.get("startup_text").and_then(Value::as_str) {
                        meta.insert("startup_text".to_string(), Value::from(value.to_string()));
                    }
                    let has_startup_text = meta
                        .get("startup_text")
                        .and_then(Value::as_str)
                        .map(|value| !value.trim().is_empty())
                        .unwrap_or(false);
                    if !ingest_running && (processed.unwrap_or(0) > 0 || !has_startup_text) {
                        ingest_running = true;
                        stage_display.update_status("ingest", "running", None);
                        meta.insert("startup_text".to_string(), Value::from(""));
                    }
                    stage_display.update_progress(
                        "ingest",
                        processed,
                        skipped,
                        if meta.is_empty() { None } else { Some(meta) },
                    );
                }
                Some("compaction") => {
                    if !ingest_running {
                        ingest_running = true;
                        stage_display.update_status("ingest", "running", None);
                    }
                    if let Some(phase) = event.get("phase").and_then(Value::as_str) {
                        stage_display.note(format!("Ingest {phase}..."));
                    }
                }
                _ => {}
            }
        });
        let value = match result {
            Ok(payload) => payload,
            Err(err) => {
                log_event(
                    logs_dir.as_path(),
                    "ingest",
                    "ERROR",
                    format!("failed: {err}"),
                );
                if is_interrupted_error(&err) {
                    stage_display.note(
                        "Interrupt received. Finishing current batch and saving checkpoints...",
                    );
                    stage_display.update_status("ingest", "interrupted", None);
                    workspace.update_stage(
                        "ingest",
                        "interrupted",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                } else {
                    stage_display.update_status("ingest", "failed", None);
                    workspace.update_stage(
                        "ingest",
                        "failed",
                        details_map(json!({"error": err.to_string()})),
                    )?;
                }
                return Err(err);
            }
        };
        let processed = value.get("processed").and_then(Value::as_u64).unwrap_or(0);
        let skipped = bridge_skipped_total(&value);
        let errors = bridge_error_total(&value);
        let files = value
            .get("files")
            .and_then(Value::as_u64)
            .unwrap_or(clean_files.len() as u64);
        let ingest_duration_s = ingest_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "ingest",
            "done",
            details_map(json!({
                "db": db_path.display().to_string(),
                "processed": processed,
                "written": processed,
                "files": files,
                "total": ingest_total,
                "skipped": skipped,
                "errors": errors,
                "duration_s": ingest_duration_s
            })),
        )?;
        log_progress(
            logs_dir.as_path(),
            "ingest",
            processed,
            Some(ingest_total),
            Some(skipped),
            Some(errors),
        );
        log_event(
            logs_dir.as_path(),
            "ingest",
            "INFO",
            format!(
                "done processed={} total={} files={} skipped={} errors={} duration_s={:.3}",
                processed, ingest_total, files, skipped, errors, ingest_duration_s
            ),
        );
        let mut meta = HashMap::new();
        meta.insert(
            "skipped_exists".to_string(),
            Value::from(
                value
                    .get("skipped_exists")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            ),
        );
        meta.insert(
            "skipped_errors".to_string(),
            Value::from(
                value
                    .get("skipped_errors")
                    .and_then(Value::as_u64)
                    .unwrap_or(errors),
            ),
        );
        meta.insert("startup_text".to_string(), Value::from(""));
        stage_display.update_progress("ingest", Some(processed), Some(skipped), Some(meta));
        stage_display.update_status("ingest", "done", Some(ingest_duration_s));
    } else if wants_ingest {
        log_event(logs_dir.as_path(), "ingest", "INFO", "skipped");
        stage_display.update_status("ingest", "skipped", None);
    }

    let pipeline_duration_s = pipeline_started.elapsed().as_secs_f64();
    stage_display.finish();
    stage_display.stop_spinner();
    print_pipeline_summary(&workspace, pipeline_duration_s);
    log_event(
        logs_dir.as_path(),
        "pipeline",
        "INFO",
        format!(
            "complete workspace={} duration_s={pipeline_duration_s:.3}",
            workspace_root.display()
        ),
    );
    Ok(())
}

fn details_map(value: Value) -> Option<serde_json::Map<String, Value>> {
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

fn log_event(logs_dir: &Path, stage: &str, level: &str, message: impl AsRef<str>) {
    if let Err(err) = write_log_line(logs_dir, stage, level, message.as_ref()) {
        eprintln!(
            "warning: failed to write stage log stage={} level={} error={err}",
            stage, level
        );
    }
}

fn log_progress(
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

fn parse_stage_selection(raw: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for stage in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match stage {
            "download" | "model" => {
                out.insert("model".to_string());
            }
            "split" => {
                out.insert("split".to_string());
            }
            "clean" | "index" | "preprocess" => {
                out.insert("preprocess".to_string());
            }
            "vectorize" | "ingest" => {
                out.insert(stage.to_string());
            }
            other => bail!("unsupported stage for rust pipeline: {other}"),
        }
    }
    if out.is_empty() {
        bail!("stage list cannot be empty");
    }
    Ok(out)
}

fn split_checkpoint_path(checkpoint_dir: &Path, input: &Path) -> PathBuf {
    let digest = Sha256::digest(input.to_string_lossy().as_bytes());
    let digest_hex = format!("{digest:x}");
    checkpoint_dir.join(format!(
        "{}-{}.checkpoint.json",
        input
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("mbox"),
        &digest_hex[..12]
    ))
}

fn load_last_position(checkpoint_path: &Path) -> anyhow::Result<u64> {
    if !checkpoint_path.exists() {
        return Ok(0);
    }
    let raw = std::fs::read_to_string(checkpoint_path)?;
    let value = serde_json::from_str::<Value>(&raw)?;
    Ok(value
        .get("last_position")
        .and_then(|entry| entry.as_u64())
        .unwrap_or(0))
}

fn collect_split_files(split_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(split_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if is_month_split_name(name) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn collect_clean_files(clean_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(clean_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(".clean.jsonl") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn is_month_split_name(name: &str) -> bool {
    if !name.ends_with(".mbox") || name.len() != 12 {
        return false;
    }
    let bytes = name.as_bytes();
    bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
}

fn count_non_empty_lines(path: &Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0_u64;
    for line in reader.lines() {
        if !line?.trim().is_empty() {
            total += 1;
        }
    }
    Ok(total)
}

fn count_non_empty_lines_for_paths(paths: &[PathBuf]) -> anyhow::Result<u64> {
    let mut total = 0_u64;
    for path in paths {
        total += count_non_empty_lines(path)?;
    }
    Ok(total)
}

fn count_mbox_envelopes(path: &Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0_u64;
    for line in reader.lines() {
        let line = line?;
        if line.starts_with("From ") {
            total += 1;
        }
    }
    Ok(total)
}

fn bridge_skipped_total(value: &Value) -> u64 {
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

fn bridge_error_total(value: &Value) -> u64 {
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

fn run_python_bridge<F>(
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

fn run_python_passthrough(args: &[String]) -> anyhow::Result<()> {
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
    let status = command
        .status()
        .with_context(|| format!("failed to execute python passthrough command: {rendered}"))?;
    if status.success() {
        return Ok(());
    }
    bail!(
        "python passthrough command failed (exit {}): {}",
        status.code().unwrap_or(-1),
        rendered
    );
}

fn merge_index_parts(
    parts_dir: &Path,
    split_files: &[PathBuf],
    output: &Path,
) -> anyhow::Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out_file = File::create(output)?;
    let mut writer = BufWriter::new(out_file);
    for split_file in split_files {
        let mbox_name = split_file
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid split file name: {}", split_file.display()))?;
        let part_path = parts_dir.join(format!("{mbox_name}.jsonl"));
        if !part_path.exists() {
            bail!("missing index part output {}", part_path.display());
        }
        let mut reader = BufReader::new(File::open(part_path)?);
        std::io::copy(&mut reader, &mut writer)?;
    }
    writer.flush()?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Pipeline {
            input_mbox,
            workspace,
            base_dir,
            stages,
            resume,
            refresh,
            checkpoint_interval,
            years,
            clean_dir,
            embeddings_dir,
            db_path,
            ingest_batch_size,
            embedding_batch_size,
            chunk_size,
            chunk_overlap,
            compact_every,
            skip_exists_check,
            no_repair_embeddings,
        }) => {
            let options = PipelineRunOptions {
                input_mboxes: &input_mbox,
                workspace_name: &workspace,
                base_dir: base_dir.as_deref(),
                stages_raw: &stages,
                resume,
                refresh,
                checkpoint_interval,
                years: &years,
                clean_dir: clean_dir.as_deref(),
                embeddings_dir: embeddings_dir.as_deref(),
                db_path: db_path.as_deref(),
                ingest_batch_size,
                embedding_batch_size,
                chunk_size,
                chunk_overlap,
                compact_every,
                skip_exists_check,
                repair_embeddings: !no_repair_embeddings,
            };
            run_pipeline(&options)?;
        }
        Some(Command::Split {
            input,
            output_dir,
            years,
            start_offset,
            checkpoint,
            resume,
            checkpoint_interval,
        }) => {
            let year_filter = if years.is_empty() {
                None
            } else {
                Some(years.into_iter().collect::<std::collections::BTreeSet<_>>())
            };
            let effective_start_offset = if resume { start_offset } else { 0 };
            let stats = if checkpoint.is_some() {
                split_mbox_by_month_with_options(
                    &input,
                    &output_dir,
                    effective_start_offset,
                    year_filter.as_ref(),
                    checkpoint.as_deref(),
                    Duration::from_secs(checkpoint_interval),
                )
            } else {
                split_mbox_by_month(
                    &input,
                    &output_dir,
                    effective_start_offset,
                    year_filter.as_ref(),
                )
            }
            .with_context(|| format!("failed to split input mbox: {}", input.display()))?;
            println!(
                "split complete: processed={} written={} skipped={} errors={} last_position={}",
                stats.processed, stats.written, stats.skipped, stats.errors, stats.last_position
            );
            println!("output_dir={}", output_dir.display());
            if let Some(checkpoint_path) = checkpoint {
                println!("checkpoint={}", checkpoint_path.display());
            }
        }
        Some(Command::Index {
            input,
            mbox_file,
            output,
            checkpoint,
            resume,
            checkpoint_interval,
        }) => {
            let options = BuildOptions {
                checkpoint_path: checkpoint,
                resume,
                checkpoint_every: Duration::from_secs(checkpoint_interval),
            };
            let stats =
                build_index_for_file(&input, &mbox_file, &output, &options).with_context(|| {
                    format!("failed to build index from input: {}", input.display())
                })?;
            println!(
                "index complete: indexed={} last_position={}",
                stats.indexed, stats.last_position
            );
            println!("output={}", output.display());
        }
        Some(Command::Preprocess {
            input,
            output_clean,
            output_spam,
            index_output,
            start_offset,
            append,
            mbox_file,
            summary_output,
        }) => {
            let (default_clean, default_spam) = default_clean_outputs(&input);
            let default_summary = default_summary_output(&input);
            let clean_path = output_clean.unwrap_or(default_clean);
            let spam_path = output_spam.unwrap_or(default_spam);
            let options = CleanOptions {
                start_offset,
                append,
                mbox_file_name: mbox_file,
                summary_output: Some(summary_output.unwrap_or(default_summary)),
                index_output,
            };
            let stats = clean_mbox_file(&input, &clean_path, &spam_path, &options)
                .with_context(|| format!("failed to preprocess input mbox: {}", input.display()))?;
            println!(
                "clean complete: processed={} clean={} spam={} errors={}",
                stats.processed, stats.clean, stats.spam, stats.errors
            );
            println!("clean_output={}", clean_path.display());
            println!("spam_output={}", spam_path.display());
            if let Some(summary_path) = options.summary_output {
                println!("summary_output={}", summary_path.display());
            }
            if let Some(index_path) = options.index_output {
                println!("index_output={}", index_path.display());
            }
        }
        Some(Command::Passthrough(args)) => {
            run_python_passthrough(&args)?;
        }
        Some(Command::Version) | None => {
            println!("{APP_VERSION}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_fixture() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../python/tests/fixtures/sample.mbox")
            .canonicalize()
            .expect("sample fixture path")
    }

    fn temp_base(name: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("ragmail-cli-{name}-{}-{now}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(unix)]
    fn make_mock_bridge_script(base: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let script = base.join("mock-bridge.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo '{"status":"ok","stage":"vectorize","processed":11,"files":2}'
  exit 0
fi
if [ "$stage" = "ingest" ]; then
  echo '{"status":"ok","stage":"ingest","processed":7,"files":2}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
        )
        .expect("write mock bridge");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");
        script
    }

    fn count_lines(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .map(|raw| raw.lines().count())
            .unwrap_or(0)
    }

    #[test]
    fn parse_stage_selection_rejects_unknown_stage() {
        let err = parse_stage_selection("split,index,unknown").expect_err("unknown stage");
        assert!(err.to_string().contains("unsupported stage"));
    }

    #[test]
    fn parse_stage_selection_accepts_python_bridge_stages() {
        let stages = parse_stage_selection("vectorize,ingest,split").expect("stages");
        assert!(stages.contains("vectorize"));
        assert!(stages.contains("ingest"));
        assert!(stages.contains("split"));
    }

    #[test]
    fn pipeline_writes_state_and_outputs() {
        let temp = temp_base("state");
        let input = sample_fixture();
        let inputs = [input];
        let options = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-state",
            base_dir: Some(temp.as_path()),
            stages_raw: "split,preprocess",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        run_pipeline(&options).expect("pipeline run");

        let root = temp.join("rs-pipeline-state");
        assert!(root.join("workspace.json").exists());
        assert!(root.join("state.json").exists());
        assert!(root.join("split/mbox_index.jsonl").exists());
        assert!(root.join("logs/pipeline.log").exists());
        assert!(root.join("logs/split.log").exists());
        assert!(root.join("logs/preprocess.log").exists());
        let clean_count = std::fs::read_dir(root.join("clean"))
            .expect("clean dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .count();
        let spam_count = std::fs::read_dir(root.join("spam"))
            .expect("spam dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .count();
        let report_count = std::fs::read_dir(root.join("reports"))
            .expect("reports dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().and_then(|ext| ext.to_str()) == Some("summary")
            })
            .count();
        assert!(clean_count >= 1, "expected at least one clean output");
        assert!(spam_count >= 1, "expected at least one spam output");
        assert!(report_count >= 1, "expected at least one summary output");

        let state_raw = std::fs::read_to_string(root.join("state.json")).expect("state");
        let state: Value = serde_json::from_str(&state_raw).expect("state json");
        for stage in ["split", "preprocess"] {
            let stage_entry = state
                .get("stages")
                .and_then(Value::as_object)
                .and_then(|stages| stages.get(stage))
                .and_then(Value::as_object);
            let status = stage_entry
                .and_then(|entry| entry.get("status"))
                .and_then(Value::as_str);
            assert_eq!(status, Some("done"), "stage {stage} should be done");
            let duration = stage_entry
                .and_then(|entry| entry.get("details"))
                .and_then(Value::as_object)
                .and_then(|details| details.get("duration_s"))
                .and_then(Value::as_f64)
                .or_else(|| {
                    stage_entry
                        .and_then(|entry| entry.get("details"))
                        .and_then(Value::as_object)
                        .and_then(|details| details.get("duration_s"))
                        .and_then(Value::as_u64)
                        .map(|value| value as f64)
                });
            assert!(
                duration.is_some(),
                "stage {stage} should include details.duration_s"
            );
        }
        let _ = std::fs::remove_dir_all(temp);
    }

    #[test]
    fn pipeline_resume_skips_done_split_stage() {
        let temp = temp_base("resume");
        let input = sample_fixture();
        let inputs = [input];
        let first = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-resume",
            base_dir: Some(temp.as_path()),
            stages_raw: "split",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        run_pipeline(&first).expect("first run");
        let state_path = temp.join("rs-pipeline-resume/state.json");
        let before = std::fs::read_to_string(&state_path).expect("state before");
        let second = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-resume",
            base_dir: Some(temp.as_path()),
            stages_raw: "split",
            resume: true,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        run_pipeline(&second).expect("resume run");
        let after = std::fs::read_to_string(state_path).expect("state after");
        assert_eq!(
            before, after,
            "resume run should skip stage without state mutation"
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[test]
    fn pipeline_refresh_archives_previous_split_outputs() {
        let temp = temp_base("refresh");
        let input = sample_fixture();
        let inputs = [input];
        let first = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-refresh",
            base_dir: Some(temp.as_path()),
            stages_raw: "split",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        run_pipeline(&first).expect("first run");
        let second = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-refresh",
            base_dir: Some(temp.as_path()),
            stages_raw: "split",
            resume: true,
            refresh: true,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        run_pipeline(&second).expect("refresh run");

        let old_dir = temp.join("rs-pipeline-refresh/old");
        assert!(old_dir.exists(), "refresh should create archive root");
        let mut saw_archived_split = false;
        for entry in std::fs::read_dir(old_dir).expect("read old") {
            let entry = entry.expect("old entry");
            let split_dir = entry.path().join("split");
            let has_mbox = split_dir.exists()
                && std::fs::read_dir(split_dir)
                    .expect("split archive dir")
                    .filter_map(Result::ok)
                    .any(|file| {
                        file.path().extension().and_then(|ext| ext.to_str()) == Some("mbox")
                    });
            if has_mbox {
                saw_archived_split = true;
                break;
            }
        }
        assert!(saw_archived_split, "expected archived split outputs");
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_vectorize_and_ingest_use_python_bridge_commands() {
        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("py-bridge");
        let workspace_root = temp.join("rs-pipeline-py");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n").expect("clean 1");
        std::fs::write(clean_dir.join("2024-02.clean.jsonl"), "{}\n").expect("clean 2");
        let args_log = temp.join("bridge-args.log");
        let script = make_mock_bridge_script(&temp);

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-py",
            base_dir: Some(temp.as_path()),
            stages_raw: "vectorize,ingest",
            resume: false,
            refresh: false,
            checkpoint_interval: 5,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
        assert!(result.is_ok(), "pipeline should succeed: {result:?}");

        let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state");
        let state: Value = serde_json::from_str(&state_raw).expect("state json");
        let vectorize_status = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("status"))
            .and_then(Value::as_str);
        let ingest_status = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("status"))
            .and_then(Value::as_str);
        assert_eq!(vectorize_status, Some("done"));
        assert_eq!(ingest_status, Some("done"));
        let vectorize_total = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("total"))
            .and_then(Value::as_u64);
        let ingest_total = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("total"))
            .and_then(Value::as_u64);
        assert_eq!(vectorize_total, Some(3));
        assert_eq!(ingest_total, Some(3));
        let vectorize_duration = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("duration_s"));
        let ingest_duration = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("duration_s"));
        assert!(
            vectorize_duration.is_some(),
            "vectorize stage should include details.duration_s"
        );
        assert!(
            ingest_duration.is_some(),
            "ingest stage should include details.duration_s"
        );

        let args_lines = std::fs::read_to_string(args_log).expect("args log");
        assert!(
            args_lines.contains("py vectorize"),
            "expected vectorize call"
        );
        assert!(args_lines.contains("py ingest"), "expected ingest call");
        assert!(
            args_lines.contains("--no-resume"),
            "expected explicit click boolean flag for resume=false"
        );
        assert!(
            !args_lines.contains("--resume true") && !args_lines.contains("--resume false"),
            "must not pass click boolean options as extra positional values"
        );
        assert!(workspace_root.join("logs/vectorize.log").exists());
        assert!(workspace_root.join("logs/ingest.log").exists());
        let _ = std::fs::remove_dir_all(temp);
    }

    #[test]
    fn pipeline_marks_split_failed_in_state_on_missing_input() {
        let temp = temp_base("split-fail");
        let missing = temp.join("missing-input.mbox");
        let inputs = [missing];
        let options = PipelineRunOptions {
            input_mboxes: &inputs,
            workspace_name: "rs-pipeline-split-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "split",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };

        let result = run_pipeline(&options);
        assert!(result.is_err(), "expected split stage failure");

        let state_path = temp.join("rs-pipeline-split-fail/state.json");
        let state_raw = std::fs::read_to_string(state_path).expect("state");
        let state: Value = serde_json::from_str(&state_raw).expect("state json");
        let split = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("split"))
            .and_then(Value::as_object)
            .expect("split stage entry");
        assert_eq!(
            split.get("status").and_then(Value::as_str),
            Some("failed"),
            "split stage should be marked failed"
        );
        let error = split
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "split failed state should include error details"
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_marks_vectorize_failed_on_bridge_error() {
        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("vectorize-fail");
        let workspace_root = temp.join("rs-pipeline-vectorize-fail");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

        use std::os::unix::fs::PermissionsExt;
        let script = temp.join("mock-bridge-fail.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo "simulated bridge failure" >&2
  exit 3
fi
echo '{"status":"ok","stage":"ingest","processed":0,"files":0}'
"#,
        )
        .expect("write mock bridge fail");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-vectorize-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "vectorize",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        assert!(result.is_err(), "expected vectorize bridge failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let vectorize = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .expect("vectorize stage");
        assert_eq!(
            vectorize.get("status").and_then(Value::as_str),
            Some("failed")
        );
        let error = vectorize
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(error.contains("python bridge stage 'vectorize' failed"));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_retries_vectorize_on_retryable_bridge_failure() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("vectorize-retry");
        let workspace_root = temp.join("rs-pipeline-vectorize-retry");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n{}\n").expect("clean file");

        let args_log = temp.join("bridge-args.log");
        let counter_file = temp.join("bridge-counter.txt");
        let script = temp.join("mock-bridge-vectorize-retry.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
count_file="${RAGMAIL_BRIDGE_COUNTER:?}"
count=0
if [ -f "$count_file" ]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [ "$count" -lt 2 ]; then
  echo "temporary connection error" >&2
  exit 75
fi
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo '{"status":"ok","stage":"vectorize","processed":3,"files":1}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
        )
        .expect("write retry script");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
        std::env::set_var("RAGMAIL_BRIDGE_COUNTER", &counter_file);
        std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "2");
        std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-vectorize-retry",
            base_dir: Some(temp.as_path()),
            stages_raw: "vectorize",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
        std::env::remove_var("RAGMAIL_BRIDGE_COUNTER");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
        assert!(result.is_ok(), "expected retry to recover: {result:?}");

        assert_eq!(count_lines(&args_log), 2, "expected one retry attempt");
        let attempts = std::fs::read_to_string(counter_file)
            .expect("counter")
            .trim()
            .parse::<u64>()
            .expect("counter value");
        assert_eq!(attempts, 2);

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let vectorize = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .expect("vectorize stage");
        assert_eq!(
            vectorize.get("status").and_then(Value::as_str),
            Some("done")
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_does_not_retry_vectorize_on_non_retryable_bridge_failure() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("vectorize-no-retry");
        let workspace_root = temp.join("rs-pipeline-vectorize-no-retry");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

        let args_log = temp.join("bridge-args.log");
        let script = temp.join("mock-bridge-vectorize-no-retry.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
echo "invalid command usage" >&2
exit 2
"#,
        )
        .expect("write no-retry script");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
        std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "3");
        std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-vectorize-no-retry",
            base_dir: Some(temp.as_path()),
            stages_raw: "vectorize",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
        assert!(result.is_err(), "expected non-retryable failure");
        assert_eq!(
            count_lines(&args_log),
            1,
            "non-retryable errors should not retry"
        );

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let vectorize = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("vectorize"))
            .and_then(Value::as_object)
            .expect("vectorize stage");
        assert_eq!(
            vectorize.get("status").and_then(Value::as_str),
            Some("failed")
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_marks_ingest_failed_on_bridge_error() {
        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("ingest-fail");
        let workspace_root = temp.join("rs-pipeline-ingest-fail");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

        use std::os::unix::fs::PermissionsExt;
        let script = temp.join("mock-bridge-ingest-fail.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
stage="${2:-}"
if [ "$stage" = "ingest" ]; then
  echo "simulated ingest failure" >&2
  exit 4
fi
echo '{"status":"ok","stage":"vectorize","processed":0,"files":0}'
"#,
        )
        .expect("write mock bridge fail");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-ingest-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "ingest",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        assert!(result.is_err(), "expected ingest bridge failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let ingest = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .expect("ingest stage");
        assert_eq!(ingest.get("status").and_then(Value::as_str), Some("failed"));
        let error = ingest
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(error.contains("python bridge stage 'ingest' failed"));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_retries_ingest_on_retryable_bridge_failure() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("ingest-retry");
        let workspace_root = temp.join("rs-pipeline-ingest-retry");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n").expect("clean file");

        let args_log = temp.join("bridge-args.log");
        let counter_file = temp.join("bridge-counter.txt");
        let script = temp.join("mock-bridge-ingest-retry.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
count_file="${RAGMAIL_BRIDGE_COUNTER:?}"
count=0
if [ -f "$count_file" ]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [ "$count" -lt 2 ]; then
  echo "temporary service unavailable" >&2
  exit 111
fi
stage="${2:-}"
if [ "$stage" = "ingest" ]; then
  echo '{"status":"ok","stage":"ingest","processed":2,"files":1,"skipped_exists":1,"skipped_errors":1}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
        )
        .expect("write retry script");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
        std::env::set_var("RAGMAIL_BRIDGE_COUNTER", &counter_file);
        std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "2");
        std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-ingest-retry",
            base_dir: Some(temp.as_path()),
            stages_raw: "ingest",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
        std::env::remove_var("RAGMAIL_BRIDGE_COUNTER");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
        assert!(result.is_ok(), "expected retry to recover: {result:?}");

        assert_eq!(count_lines(&args_log), 2, "expected one retry attempt");
        let attempts = std::fs::read_to_string(counter_file)
            .expect("counter")
            .trim()
            .parse::<u64>()
            .expect("counter value");
        assert_eq!(attempts, 2);

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let ingest = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .expect("ingest stage");
        assert_eq!(ingest.get("status").and_then(Value::as_str), Some("done"));
        let details = ingest
            .get("details")
            .and_then(Value::as_object)
            .expect("ingest details");
        assert_eq!(details.get("total").and_then(Value::as_u64), Some(2));
        assert_eq!(details.get("skipped").and_then(Value::as_u64), Some(2));
        assert_eq!(details.get("errors").and_then(Value::as_u64), Some(1));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_does_not_retry_ingest_on_non_retryable_bridge_failure() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = env_lock().lock().expect("env lock");
        let temp = temp_base("ingest-no-retry");
        let workspace_root = temp.join("rs-pipeline-ingest-no-retry");
        let clean_dir = workspace_root.join("clean");
        std::fs::create_dir_all(&clean_dir).expect("clean dir");
        std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

        let args_log = temp.join("bridge-args.log");
        let script = temp.join("mock-bridge-ingest-no-retry.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
echo "bad arguments" >&2
exit 2
"#,
        )
        .expect("write no-retry script");
        let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod");

        std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
        std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
        std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "3");
        std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-ingest-no-retry",
            base_dir: Some(temp.as_path()),
            stages_raw: "ingest",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
        std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
        std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
        assert!(result.is_err(), "expected non-retryable failure");
        assert_eq!(
            count_lines(&args_log),
            1,
            "non-retryable errors should not retry"
        );

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let ingest = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("ingest"))
            .and_then(Value::as_object)
            .expect("ingest stage");
        assert_eq!(ingest.get("status").and_then(Value::as_str), Some("failed"));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_marks_preprocess_failed_for_index_alias_on_unreadable_split_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = temp_base("preprocess-index-alias-fail");
        let workspace_root = temp.join("rs-pipeline-preprocess-index-alias-fail");
        let split_dir = workspace_root.join("split");
        std::fs::create_dir_all(&split_dir).expect("split dir");
        let split_file = split_dir.join("2024-01.mbox");
        std::fs::write(
            &split_file,
            b"From a@example.com Mon Jan  1 01:02:03 +0000 2024\n\n",
        )
        .expect("split file");
        let mut perms = std::fs::metadata(&split_file).expect("meta").permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&split_file, perms).expect("chmod 000");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-preprocess-index-alias-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "index",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        let mut restore = std::fs::metadata(&split_file)
            .expect("meta restore")
            .permissions();
        restore.set_mode(0o644);
        let _ = std::fs::set_permissions(&split_file, restore);
        assert!(result.is_err(), "expected preprocess failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let preprocess = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("preprocess"))
            .and_then(Value::as_object)
            .expect("preprocess stage");
        assert_eq!(
            preprocess.get("status").and_then(Value::as_str),
            Some("failed")
        );
        let error = preprocess
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "preprocess failure should include error detail"
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_marks_preprocess_failed_for_clean_alias_on_unreadable_split_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = temp_base("preprocess-clean-alias-fail");
        let workspace_root = temp.join("rs-pipeline-preprocess-clean-alias-fail");
        let split_dir = workspace_root.join("split");
        std::fs::create_dir_all(&split_dir).expect("split dir");
        let split_file = split_dir.join("2024-01.mbox");
        std::fs::write(
            &split_file,
            b"From a@example.com Mon Jan  1 01:02:03 +0000 2024\n\n",
        )
        .expect("split file");
        let mut perms = std::fs::metadata(&split_file).expect("meta").permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&split_file, perms).expect("chmod 000");

        let options = PipelineRunOptions {
            input_mboxes: &[],
            workspace_name: "rs-pipeline-preprocess-clean-alias-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "clean",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
            clean_dir: None,
            embeddings_dir: None,
            db_path: None,
            ingest_batch_size: None,
            embedding_batch_size: None,
            chunk_size: None,
            chunk_overlap: None,
            compact_every: None,
            skip_exists_check: false,
            repair_embeddings: true,
        };
        let result = run_pipeline(&options);
        let mut restore = std::fs::metadata(&split_file)
            .expect("meta restore")
            .permissions();
        restore.set_mode(0o644);
        let _ = std::fs::set_permissions(&split_file, restore);
        assert!(result.is_err(), "expected preprocess failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let preprocess = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("preprocess"))
            .and_then(Value::as_object)
            .expect("preprocess stage");
        assert_eq!(
            preprocess.get("status").and_then(Value::as_str),
            Some("failed")
        );
        let error = preprocess
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "preprocess failure should include error detail"
        );
        let _ = std::fs::remove_dir_all(temp);
    }
}

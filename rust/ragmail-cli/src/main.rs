mod display;
mod file_ops;
mod logging;
mod python_bridge;
mod util;

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
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

use crate::display::{print_pipeline_header, print_pipeline_summary, StageDisplay};
use crate::file_ops::{
    collect_clean_files, collect_split_files, count_mbox_envelopes, count_non_empty_lines,
    count_non_empty_lines_for_paths, load_last_position, merge_index_parts, parse_stage_selection,
    split_checkpoint_path,
};
use crate::logging::{details_map, log_event, log_progress};
use crate::python_bridge::{
    bridge_error_total, bridge_skipped_total, run_python_bridge, run_python_passthrough,
};
use crate::util::is_interrupted_error;

const APP_VERSION: &str = env!("RAGMAIL_VERSION");
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
mod tests;

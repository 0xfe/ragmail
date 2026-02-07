use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use chrono::Local;
use clap::ArgAction;
use clap::{Parser, Subcommand};
use ragmail_clean::{clean_mbox_file, default_clean_outputs, default_summary_output, CleanOptions};
use ragmail_core::workspace::Workspace;
use ragmail_index::{build_index_for_file, BuildOptions};
use ragmail_mbox::{split_mbox_by_month, split_mbox_by_month_with_options};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
}

#[derive(Debug, Parser)]
#[command(
    name = "ragmail-rs",
    version = APP_VERSION,
    about = "Rust migration scaffold for ragmail"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Rust-native pipeline (stub in M1/M2).
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
        /// Comma-separated stage list (`split,index,clean`).
        #[arg(long, default_value = "split,index,clean")]
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
    /// Build a byte-offset index JSONL for an MBOX file.
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
    /// Clean an MBOX into JSONL outputs (M3 scaffold).
    Clean {
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
}

fn run_pipeline(options: &PipelineRunOptions<'_>) -> anyhow::Result<()> {
    let pipeline_started = Instant::now();
    let stages = parse_stage_selection(options.stages_raw)?;
    let wants_split = stages.contains("split");
    let wants_index = stages.contains("index");
    let wants_clean = stages.contains("clean");
    let wants_vectorize = stages.contains("vectorize");
    let wants_ingest = stages.contains("ingest");
    if wants_split && options.input_mboxes.is_empty() {
        bail!("split stage requires at least one input mbox");
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

    let existing_split_files = collect_split_files(split_dir.as_path())?;
    let split_outputs_exist = !existing_split_files.is_empty();
    let split_should_run = wants_split
        && (!resume_effective || !workspace.stage_done("split")? || !split_outputs_exist);
    if split_should_run {
        let split_started = Instant::now();
        log_event(logs_dir.as_path(), "split", "INFO", "start");
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
        let mut split_processed = 0_u64;
        let mut split_written = 0_u64;
        let mut split_skipped = 0_u64;
        let mut split_errors = 0_u64;
        let split_result = (|| -> anyhow::Result<()> {
            for input in options.input_mboxes {
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
                let stats = split_mbox_by_month_with_options(
                    input,
                    split_dir.as_path(),
                    start_offset,
                    year_filter.as_ref(),
                    Some(checkpoint_path.as_path()),
                    checkpoint_every,
                )
                .with_context(|| format!("split failed for {}", input.display()))?;
                split_processed += stats.processed;
                split_written += stats.written;
                split_skipped += stats.skipped;
                split_errors += stats.errors;
                log_progress(
                    logs_dir.as_path(),
                    "split",
                    split_processed,
                    None,
                    Some(split_skipped),
                    Some(split_errors),
                );
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
        println!(
            "pipeline split: processed={} written={} skipped={} errors={}",
            split_processed, split_written, split_skipped, split_errors
        );
        log_event(
            logs_dir.as_path(),
            "split",
            "INFO",
            format!(
                "done processed={} written={} skipped={} errors={} duration_s={:.3}",
                split_processed, split_written, split_skipped, split_errors, split_duration_s
            ),
        );
    } else if wants_split {
        println!("pipeline split: skipped");
        log_event(logs_dir.as_path(), "split", "INFO", "skipped");
    }

    let split_files = collect_split_files(split_dir.as_path())?;
    if (wants_index || wants_clean) && split_files.is_empty() {
        log_event(
            logs_dir.as_path(),
            "split",
            "ERROR",
            "no split outputs found",
        );
        bail!("no split outputs found in {}", split_dir.display());
    }

    let output_index = split_dir.join("mbox_index.jsonl");
    let index_outputs_exist = output_index.exists();
    let index_should_run = wants_index
        && (!resume_effective || !workspace.stage_done("index")? || !index_outputs_exist);
    if index_should_run {
        let index_started = Instant::now();
        log_event(logs_dir.as_path(), "index", "INFO", "start");
        workspace.update_stage(
            "index",
            "running",
            details_map(json!({"split_dir": split_dir.display().to_string()})),
        )?;
        let index_checkpoint_dir = checkpoints_dir.join("mbox_index-rs");
        let parts_dir = index_checkpoint_dir.join("parts");
        let state_dir = index_checkpoint_dir.join("state");
        if !resume_effective {
            if output_index.exists() {
                std::fs::remove_file(&output_index)?;
            }
            if index_checkpoint_dir.exists() {
                std::fs::remove_dir_all(&index_checkpoint_dir)?;
            }
        }
        std::fs::create_dir_all(&parts_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let mut indexed_total = 0_u64;
        let index_result = (|| -> anyhow::Result<()> {
            for split_file in &split_files {
                let mbox_name = split_file
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid split file name: {}", split_file.display())
                    })?
                    .to_string();
                let stem = split_file
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid split file stem: {}", split_file.display())
                    })?;
                let part_output = parts_dir.join(format!("{mbox_name}.jsonl"));
                let checkpoint_path = state_dir.join(format!("{stem}.checkpoint.json"));

                if resume_effective && part_output.exists() && !checkpoint_path.exists() {
                    indexed_total += count_non_empty_lines(&part_output)?;
                    log_progress(logs_dir.as_path(), "index", indexed_total, None, None, None);
                    continue;
                }

                let options = BuildOptions {
                    checkpoint_path: Some(checkpoint_path),
                    resume: resume_effective,
                    checkpoint_every,
                };
                build_index_for_file(split_file, &mbox_name, &part_output, &options).with_context(
                    || format!("index build failed for split file {}", split_file.display()),
                )?;
                indexed_total += count_non_empty_lines(&part_output)?;
                log_progress(logs_dir.as_path(), "index", indexed_total, None, None, None);
            }
            merge_index_parts(parts_dir.as_path(), &split_files, output_index.as_path())?;
            Ok(())
        })();
        if let Err(err) = index_result {
            log_event(
                logs_dir.as_path(),
                "index",
                "ERROR",
                format!("failed: {err}"),
            );
            workspace.update_stage(
                "index",
                "failed",
                details_map(json!({"error": err.to_string()})),
            )?;
            return Err(err);
        }
        let merged_count = count_non_empty_lines(&output_index)?;
        let index_duration_s = index_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "index",
            "done",
            details_map(json!({
                "output": output_index.display().to_string(),
                "indexed": merged_count,
                "parts_total": indexed_total,
                "duration_s": index_duration_s
            })),
        )?;
        println!("pipeline index: indexed={merged_count} parts_total={indexed_total}");
        log_event(
            logs_dir.as_path(),
            "index",
            "INFO",
            format!(
                "done indexed={merged_count} parts_total={indexed_total} duration_s={:.3}",
                index_duration_s
            ),
        );
    } else if wants_index {
        println!("pipeline index: skipped");
        log_event(logs_dir.as_path(), "index", "INFO", "skipped");
    }

    let expected_clean_missing = split_files.iter().any(|split_file| {
        let stem = split_file
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        !clean_dir.join(format!("{stem}.clean.jsonl")).exists()
    });
    let clean_should_run = wants_clean
        && (!resume_effective || !workspace.stage_done("clean")? || expected_clean_missing);
    if clean_should_run {
        let clean_started = Instant::now();
        log_event(logs_dir.as_path(), "clean", "INFO", "start");
        workspace.update_stage(
            "clean",
            "running",
            details_map(json!({"files": split_files.len()})),
        )?;
        let mut clean_processed = 0_u64;
        let mut clean_written = 0_u64;
        let mut clean_spam = 0_u64;
        let mut clean_errors = 0_u64;
        let clean_result = (|| -> anyhow::Result<()> {
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
                if resume_effective && clean_output.exists() {
                    log_event(
                        logs_dir.as_path(),
                        "clean",
                        "INFO",
                        format!("skip existing output={}", clean_output.display()),
                    );
                    continue;
                }
                let spam_output = spam_dir.join(format!("{stem}.spam.jsonl"));
                let summary_output = reports_dir.join(format!("{mbox_name}.summary"));
                let options = CleanOptions {
                    start_offset: 0,
                    append: false,
                    mbox_file_name: Some(mbox_name),
                    summary_output: Some(summary_output),
                    index_output: None,
                };
                let stats = clean_mbox_file(split_file, &clean_output, &spam_output, &options)
                    .with_context(|| {
                        format!("clean failed for split file {}", split_file.display())
                    })?;
                clean_processed += stats.processed;
                clean_written += stats.clean;
                clean_spam += stats.spam;
                clean_errors += stats.errors;
                log_progress(
                    logs_dir.as_path(),
                    "clean",
                    clean_processed,
                    None,
                    Some(clean_spam + clean_errors),
                    Some(clean_errors),
                );
            }
            Ok(())
        })();
        if let Err(err) = clean_result {
            log_event(
                logs_dir.as_path(),
                "clean",
                "ERROR",
                format!("failed: {err}"),
            );
            workspace.update_stage(
                "clean",
                "failed",
                details_map(json!({"error": err.to_string()})),
            )?;
            return Err(err);
        }
        let clean_duration_s = clean_started.elapsed().as_secs_f64();
        workspace.update_stage(
            "clean",
            "done",
            details_map(json!({
                "clean_files": split_files.len(),
                "processed": clean_processed,
                "written": clean_written,
                "skipped": clean_spam + clean_errors,
                "spam": clean_spam,
                "errors": clean_errors,
                "duration_s": clean_duration_s
            })),
        )?;
        println!(
            "pipeline clean: processed={} clean={} spam={} errors={}",
            clean_processed, clean_written, clean_spam, clean_errors
        );
        log_event(
            logs_dir.as_path(),
            "clean",
            "INFO",
            format!(
                "done processed={} clean={} spam={} errors={} duration_s={:.3}",
                clean_processed, clean_written, clean_spam, clean_errors, clean_duration_s
            ),
        );
    } else if wants_clean {
        println!("pipeline clean: skipped");
        log_event(logs_dir.as_path(), "clean", "INFO", "skipped");
    }

    let clean_files = collect_clean_files(clean_dir.as_path())?;
    if (wants_vectorize || wants_ingest) && clean_files.is_empty() {
        log_event(
            logs_dir.as_path(),
            "clean",
            "ERROR",
            "no clean outputs found",
        );
        bail!("no clean outputs found in {}", clean_dir.display());
    }

    let embeddings_dir = workspace.embeddings_dir();
    let db_path = workspace.db_dir().join("email_search.lancedb");

    let vectorize_should_run =
        wants_vectorize && (!resume_effective || !workspace.stage_done("vectorize")?);
    if vectorize_should_run {
        let vectorize_started = Instant::now();
        log_event(logs_dir.as_path(), "vectorize", "INFO", "start");
        let vectorize_total = count_non_empty_lines_for_paths(&clean_files)?;
        workspace.update_stage(
            "vectorize",
            "running",
            details_map(json!({"files": clean_files.len(), "total": vectorize_total})),
        )?;
        let mut args = vec![
            "--workspace".to_string(),
            options.workspace_name.to_string(),
            "--resume".to_string(),
            if resume_effective {
                "true".to_string()
            } else {
                "false".to_string()
            },
            "--checkpoint-interval".to_string(),
            options.checkpoint_interval.to_string(),
        ];
        if let Some(base) = options.base_dir {
            args.extend(["--base-dir".to_string(), base.display().to_string()]);
        }
        let result = run_python_bridge("vectorize", &args, logs_dir.as_path());
        let value = match result {
            Ok(payload) => payload,
            Err(err) => {
                log_event(
                    logs_dir.as_path(),
                    "vectorize",
                    "ERROR",
                    format!("failed: {err}"),
                );
                workspace.update_stage(
                    "vectorize",
                    "failed",
                    details_map(json!({"error": err.to_string()})),
                )?;
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
        println!(
            "pipeline vectorize: processed={processed} total={vectorize_total} files={files} skipped={skipped} errors={errors}"
        );
    } else if wants_vectorize {
        log_event(logs_dir.as_path(), "vectorize", "INFO", "skipped");
        println!("pipeline vectorize: skipped");
    }

    let ingest_should_run =
        wants_ingest && (!resume_effective || !workspace.stage_done("ingest")?);
    if ingest_should_run {
        let ingest_started = Instant::now();
        log_event(logs_dir.as_path(), "ingest", "INFO", "start");
        let ingest_total = count_non_empty_lines_for_paths(&clean_files)?;
        workspace.update_stage(
            "ingest",
            "running",
            details_map(json!({"files": clean_files.len(), "total": ingest_total})),
        )?;
        let mut args = vec![
            "--workspace".to_string(),
            options.workspace_name.to_string(),
            "--resume".to_string(),
            if resume_effective {
                "true".to_string()
            } else {
                "false".to_string()
            },
            "--db-path".to_string(),
            db_path.display().to_string(),
            "--embeddings-dir".to_string(),
            embeddings_dir.display().to_string(),
            "--checkpoint-interval".to_string(),
            options.checkpoint_interval.to_string(),
        ];
        if let Some(base) = options.base_dir {
            args.extend(["--base-dir".to_string(), base.display().to_string()]);
        }
        let result = run_python_bridge("ingest", &args, logs_dir.as_path());
        let value = match result {
            Ok(payload) => payload,
            Err(err) => {
                log_event(
                    logs_dir.as_path(),
                    "ingest",
                    "ERROR",
                    format!("failed: {err}"),
                );
                workspace.update_stage(
                    "ingest",
                    "failed",
                    details_map(json!({"error": err.to_string()})),
                )?;
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
        println!(
            "pipeline ingest: processed={processed} total={ingest_total} files={files} skipped={skipped} errors={errors}"
        );
    } else if wants_ingest {
        log_event(logs_dir.as_path(), "ingest", "INFO", "skipped");
        println!("pipeline ingest: skipped");
    }

    let pipeline_duration_s = pipeline_started.elapsed().as_secs_f64();
    println!(
        "pipeline complete: workspace={} duration_s={pipeline_duration_s:.3}",
        workspace_root.display()
    );
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
            "split" | "index" | "clean" | "vectorize" | "ingest" => {
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

fn run_python_bridge(stage: &str, args: &[String], logs_dir: &Path) -> anyhow::Result<Value> {
    let max_retries = python_bridge_max_retries();
    let total_attempts = max_retries + 1;
    let base_delay_ms = python_bridge_retry_delay_ms();
    for attempt in 1..=total_attempts {
        let override_bin = std::env::var("RAGMAIL_PY_BRIDGE_BIN").ok();
        let mut command = if let Some(bin) = override_bin {
            let mut cmd = ProcessCommand::new(bin);
            cmd.arg("py");
            cmd.arg(stage);
            cmd
        } else {
            let python =
                std::env::var("RAGMAIL_PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
            let mut cmd = ProcessCommand::new(python);
            cmd.arg("-m");
            cmd.arg("ragmail.cli");
            cmd.arg("py");
            cmd.arg(stage);
            cmd
        };
        command.args(args);
        command.current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."));
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

        let output = match command.output() {
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

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !output.status.success() {
            let code = output.status.code();
            let retryable = should_retry_bridge_failure(code, &stdout, &stderr);
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
                        stderr.trim()
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
                stdout.trim(),
                stderr.trim()
            );
        }

        let mut json_payload = None;
        for line in stdout.lines().rev() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
                json_payload = Some(value);
                break;
            }
        }
        let value = json_payload.with_context(|| {
            format!(
                "python bridge stage '{}' did not emit parseable JSON output: {}",
                stage,
                stdout.trim()
            )
        })?;
        if !stderr.trim().is_empty() {
            log_event(
                logs_dir,
                stage,
                "INFO",
                format!("python stderr: {}", stderr.trim()),
            );
        }
        return Ok(value);
    }

    bail!("python bridge stage '{stage}' failed unexpectedly without output")
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
        Some(Command::Clean {
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
                .with_context(|| format!("failed to clean input mbox: {}", input.display()))?;
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
            stages_raw: "split,index,clean",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
        };
        run_pipeline(&options).expect("pipeline run");

        let root = temp.join("rs-pipeline-state");
        assert!(root.join("workspace.json").exists());
        assert!(root.join("state.json").exists());
        assert!(root.join("split/mbox_index.jsonl").exists());
        assert!(root.join("logs/pipeline.log").exists());
        assert!(root.join("logs/split.log").exists());
        assert!(root.join("logs/index.log").exists());
        assert!(root.join("logs/clean.log").exists());
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
        for stage in ["split", "index", "clean"] {
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
    fn pipeline_marks_index_failed_on_unreadable_split_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = temp_base("index-fail");
        let workspace_root = temp.join("rs-pipeline-index-fail");
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
            workspace_name: "rs-pipeline-index-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "index",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
        };
        let result = run_pipeline(&options);
        let mut restore = std::fs::metadata(&split_file)
            .expect("meta restore")
            .permissions();
        restore.set_mode(0o644);
        let _ = std::fs::set_permissions(&split_file, restore);
        assert!(result.is_err(), "expected index failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let index = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("index"))
            .and_then(Value::as_object)
            .expect("index stage");
        assert_eq!(index.get("status").and_then(Value::as_str), Some("failed"));
        let error = index
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "index failure should include error detail"
        );
        let _ = std::fs::remove_dir_all(temp);
    }

    #[cfg(unix)]
    #[test]
    fn pipeline_marks_clean_failed_on_unreadable_split_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = temp_base("clean-fail");
        let workspace_root = temp.join("rs-pipeline-clean-fail");
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
            workspace_name: "rs-pipeline-clean-fail",
            base_dir: Some(temp.as_path()),
            stages_raw: "clean",
            resume: false,
            refresh: false,
            checkpoint_interval: 1,
            years: &[],
        };
        let result = run_pipeline(&options);
        let mut restore = std::fs::metadata(&split_file)
            .expect("meta restore")
            .permissions();
        restore.set_mode(0o644);
        let _ = std::fs::set_permissions(&split_file, restore);
        assert!(result.is_err(), "expected clean failure");

        let state_raw =
            std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
        let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
        let clean = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get("clean"))
            .and_then(Value::as_object)
            .expect("clean stage");
        assert_eq!(clean.get("status").and_then(Value::as_str), Some("failed"));
        let error = clean
            .get("details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("error"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            !error.is_empty(),
            "clean failure should include error detail"
        );
        let _ = std::fs::remove_dir_all(temp);
    }
}

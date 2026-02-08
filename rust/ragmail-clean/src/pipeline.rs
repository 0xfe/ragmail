use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ragmail_index::record_from_message;
use ragmail_mbox::MboxMessageStream;

use crate::header::{
    build_clean_headers, decode_header_value, normalize_date_value, parse_headers, spam_reason,
    split_csv,
};
use crate::mime::extract_content_and_attachments;
use crate::text::{clean_text, remove_signature};
use crate::types::{
    CleanEmailRecord, CleanError, CleanOptions, CleanOutcome, CleanStats, ContentBlock, MboxRef,
    SpamRecord,
};

/// Cleans an mbox file into clean + spam JSONL outputs.
pub fn clean_mbox_file(
    input_path: &Path,
    output_clean: &Path,
    output_spam: &Path,
    options: &CleanOptions,
) -> Result<CleanStats, CleanError> {
    clean_mbox_file_with_progress(
        input_path,
        output_clean,
        output_spam,
        options,
        Duration::from_millis(250),
        None,
    )
}

/// Cleans an mbox file into clean + spam JSONL outputs and emits periodic progress callbacks.
pub fn clean_mbox_file_with_progress(
    input_path: &Path,
    output_clean: &Path,
    output_spam: &Path,
    options: &CleanOptions,
    progress_every: Duration,
    mut progress_callback: Option<&mut dyn FnMut(&CleanStats)>,
) -> Result<CleanStats, CleanError> {
    if let Some(parent) = output_clean.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = output_spam.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let clean_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(options.append)
        .truncate(!options.append)
        .open(output_clean)?;
    let spam_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(options.append)
        .truncate(!options.append)
        .open(output_spam)?;
    let mut clean_writer = BufWriter::new(clean_file);
    let mut spam_writer = BufWriter::new(spam_file);
    let mut index_writer = if let Some(index_output) = options.index_output.as_ref() {
        if let Some(parent) = index_output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(options.append)
            .truncate(!options.append)
            .open(index_output)?;
        Some(BufWriter::new(index_file))
    } else {
        None
    };

    let mbox_file_name = options
        .mbox_file_name
        .clone()
        .unwrap_or_else(|| file_name_or_path(input_path));
    let mut stream = MboxMessageStream::from_path(input_path, options.start_offset)?;
    let mut stats = CleanStats::default();
    let mut last_progress = std::time::Instant::now();

    loop {
        let Some(message) = stream.next_message()? else {
            break;
        };
        stats.processed += 1;
        if let Some(writer) = index_writer.as_mut() {
            let record = record_from_message(&message.raw, &mbox_file_name, message.offset);
            serde_json::to_writer(&mut *writer, &record)?;
            writer.write_all(b"\n")?;
        }
        match clean_message(
            &message.raw,
            &mbox_file_name,
            message.offset,
            message.raw.len() as u64,
        ) {
            Ok(CleanOutcome::Clean(record)) => {
                serde_json::to_writer(&mut clean_writer, &record)?;
                clean_writer.write_all(b"\n")?;
                stats.clean += 1;
            }
            Ok(CleanOutcome::Spam(record)) => {
                let reason = record.reason.clone();
                serde_json::to_writer(&mut spam_writer, &record)?;
                spam_writer.write_all(b"\n")?;
                stats.spam += 1;
                *stats.spam_reasons.entry(reason).or_insert(0) += 1;
            }
            Err(_) => {
                stats.errors += 1;
            }
        }
        emit_clean_progress(
            &mut progress_callback,
            &mut last_progress,
            progress_every,
            &stats,
        );
    }

    clean_writer.flush()?;
    spam_writer.flush()?;
    if let Some(writer) = index_writer.as_mut() {
        writer.flush()?;
    }
    let summary_path = options
        .summary_output
        .clone()
        .unwrap_or_else(|| default_summary_output(input_path));
    write_summary_file(&summary_path, input_path, output_clean, output_spam, &stats)?;
    if let Some(callback) = progress_callback.as_mut() {
        callback(&stats);
    }
    Ok(stats)
}

fn emit_clean_progress(
    progress_callback: &mut Option<&mut dyn FnMut(&CleanStats)>,
    last_progress: &mut std::time::Instant,
    progress_every: Duration,
    stats: &CleanStats,
) {
    if let Some(callback) = progress_callback.as_mut() {
        let due = stats.processed == 1
            || progress_every.is_zero()
            || last_progress.elapsed() >= progress_every;
        if due {
            callback(stats);
            *last_progress = std::time::Instant::now();
        }
    }
}

pub(crate) fn clean_message(
    raw: &[u8],
    mbox_file_name: &str,
    offset: u64,
    length: u64,
) -> Result<CleanOutcome, CleanError> {
    let headers = parse_headers(raw);
    if let Some(reason) = spam_reason(&headers) {
        return Ok(CleanOutcome::Spam(SpamRecord {
            from: decode_header_value(headers.get("from").map(String::as_str).unwrap_or_default()),
            subject: headers
                .get("subject")
                .map(|value| decode_header_value(value))
                .unwrap_or_default(),
            date: headers
                .get("date")
                .map_or_else(String::new, |value| normalize_date_value(value)),
            reason,
        }));
    }

    let (body, attachments) = extract_content_and_attachments(raw, &headers);
    let body = clean_text(&body);
    let (body, _) = remove_signature(&body);
    let content = if body.is_empty() {
        vec![ContentBlock {
            kind: "text".to_string(),
            body: String::new(),
        }]
    } else {
        vec![ContentBlock {
            kind: "text".to_string(),
            body,
        }]
    };

    let tags = split_csv(headers.get("x-gmail-labels").map(String::as_str));
    let headers_out = build_clean_headers(&headers);

    Ok(CleanOutcome::Clean(
        CleanEmailRecord {
            headers: headers_out,
            tags,
            content,
            attachments,
            mbox: MboxRef {
                file: mbox_file_name.to_string(),
                offset,
                length,
            },
        }
        .into(),
    ))
}

fn file_name_or_path(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

/// Returns default output paths for `clean` and `spam` JSONL from an mbox path.
#[must_use]
pub fn default_clean_outputs(input_path: &Path) -> (PathBuf, PathBuf) {
    let base = input_path.with_extension("");
    let clean = PathBuf::from(format!("{}.clean.jsonl", base.display()));
    let spam = PathBuf::from(format!("{}.spam.jsonl", base.display()));
    (clean, spam)
}

/// Returns default summary output path (`<input>.summary`) for an mbox path.
#[must_use]
pub fn default_summary_output(input_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.summary", input_path.display()))
}

fn write_summary_file(
    summary_path: &Path,
    input_path: &Path,
    clean_path: &Path,
    spam_path: &Path,
    stats: &CleanStats,
) -> Result<(), CleanError> {
    if let Some(parent) = summary_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let original_size = std::fs::metadata(input_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let clean_size = std::fs::metadata(clean_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let spam_size = std::fs::metadata(spam_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let reduction = if original_size > 0 {
        (1.0_f64 - (clean_size as f64 / original_size as f64)) * 100.0
    } else {
        0.0
    };

    let mut output = String::new();
    output.push_str("Email Cleanup Summary\n");
    output.push_str("==================================================\n\n");
    output.push_str(&format!("Source: {}\n", input_path.display()));
    output.push_str("Statistics\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Total emails processed: {}\n", stats.processed));
    output.push_str(&format!("Clean emails written: {}\n", stats.clean));
    output.push_str(&format!("Spam/filtered: {}\n", stats.spam));
    output.push_str(&format!("Errors: {}\n\n", stats.errors));
    if !stats.spam_reasons.is_empty() {
        output.push_str("Spam/Filter Breakdown:\n");
        for (reason, count) in &stats.spam_reasons {
            output.push_str(&format!("  - {reason}: {count}\n"));
        }
        output.push('\n');
    }
    output.push_str("Size Analysis\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Original size: {original_size}\n"));
    output.push_str(&format!("Clean file size: {clean_size}\n"));
    output.push_str(&format!("Spam file size: {spam_size}\n"));
    output.push_str(&format!("Size reduction: {reduction:.1}%\n\n"));
    output.push_str("Output Files\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Clean: {}\n", clean_path.display()));
    output.push_str(&format!("Spam: {}\n", spam_path.display()));
    std::fs::write(summary_path, output)?;
    Ok(())
}

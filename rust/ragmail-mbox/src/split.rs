use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::stream::envelope_year_month;
use crate::{MboxError, MboxMessageStream, SplitStats};

const DEFAULT_MAX_OPEN_SPLIT_WRITERS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SplitCheckpoint {
    last_position: u64,
    processed: u64,
    written: u64,
    skipped: u64,
    errors: u64,
}

/// Splits an MBOX file into monthly output files (`YYYY-MM.mbox`).
///
/// The function appends to existing month files, enabling resume workflows where
/// callers restart with a non-zero `start_offset`.
pub fn split_mbox_by_month(
    input_path: &Path,
    output_dir: &Path,
    start_offset: u64,
    filter_years: Option<&std::collections::BTreeSet<u16>>,
) -> Result<SplitStats, MboxError> {
    split_mbox_by_month_with_options(
        input_path,
        output_dir,
        start_offset,
        filter_years,
        None,
        Duration::from_secs(30),
    )
}

/// Splits an MBOX file into monthly output files with optional checkpoint writes.
pub fn split_mbox_by_month_with_options(
    input_path: &Path,
    output_dir: &Path,
    start_offset: u64,
    filter_years: Option<&std::collections::BTreeSet<u16>>,
    checkpoint_path: Option<&Path>,
    checkpoint_every: Duration,
) -> Result<SplitStats, MboxError> {
    split_mbox_by_month_with_options_and_limit(
        input_path,
        output_dir,
        start_offset,
        filter_years,
        checkpoint_path,
        checkpoint_every,
        DEFAULT_MAX_OPEN_SPLIT_WRITERS,
        None,
        Duration::from_millis(250),
    )
}

/// Splits an MBOX file into monthly output files with optional checkpoint writes and
/// periodic progress callbacks.
#[allow(clippy::too_many_arguments)]
pub fn split_mbox_by_month_with_options_and_progress(
    input_path: &Path,
    output_dir: &Path,
    start_offset: u64,
    filter_years: Option<&std::collections::BTreeSet<u16>>,
    checkpoint_path: Option<&Path>,
    checkpoint_every: Duration,
    progress_every: Duration,
    progress_callback: &mut dyn FnMut(&SplitStats),
) -> Result<SplitStats, MboxError> {
    split_mbox_by_month_with_options_and_limit(
        input_path,
        output_dir,
        start_offset,
        filter_years,
        checkpoint_path,
        checkpoint_every,
        DEFAULT_MAX_OPEN_SPLIT_WRITERS,
        Some(progress_callback),
        progress_every,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn split_mbox_by_month_with_options_and_limit(
    input_path: &Path,
    output_dir: &Path,
    start_offset: u64,
    filter_years: Option<&std::collections::BTreeSet<u16>>,
    checkpoint_path: Option<&Path>,
    checkpoint_every: Duration,
    max_open_writers: usize,
    mut progress_callback: Option<&mut dyn FnMut(&SplitStats)>,
    progress_every: Duration,
) -> Result<SplitStats, MboxError> {
    std::fs::create_dir_all(output_dir)?;
    let mut stream = MboxMessageStream::from_path(input_path, start_offset)?;
    let mut writers: std::collections::BTreeMap<String, BufWriter<File>> =
        std::collections::BTreeMap::new();
    let mut writer_lru: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut stats = SplitStats::default();
    let mut last_checkpoint = Instant::now();
    let mut last_progress = Instant::now();
    let writer_limit = max_open_writers.max(1);

    loop {
        let next = stream.next_message()?;
        let Some(message) = next else {
            break;
        };
        stats.processed += 1;
        stats.last_position = stream.resume_offset();

        let Some((year, month)) = envelope_year_month(&message.raw) else {
            stats.errors += 1;
            stats.skipped += 1;
            emit_split_progress(
                &mut progress_callback,
                &mut last_progress,
                progress_every,
                &stats,
            );
            continue;
        };

        if let Some(years) = filter_years {
            if !years.contains(&year) {
                stats.skipped += 1;
                emit_split_progress(
                    &mut progress_callback,
                    &mut last_progress,
                    progress_every,
                    &stats,
                );
                continue;
            }
        }

        let period = format!("{year:04}-{month:02}");
        if !writers.contains_key(&period) {
            if writers.len() >= writer_limit {
                if let Some(evict_period) = writer_lru.pop_front() {
                    if let Some(mut evicted) = writers.remove(&evict_period) {
                        evicted.flush()?;
                    }
                }
            }
            let path = output_dir.join(format!("{period}.mbox"));
            let file = File::options().create(true).append(true).open(path)?;
            writers.insert(period.clone(), BufWriter::new(file));
        }
        touch_writer_lru(&mut writer_lru, &period);

        let writer = writers.get_mut(&period).expect("writer inserted above");
        writer.write_all(&message.raw)?;
        stats.written += 1;
        stats.last_position = stream.resume_offset();
        let month_stats = stats.by_month.entry(period).or_default();
        month_stats.emails += 1;
        month_stats.bytes += message.raw.len() as u64;

        if let Some(path) = checkpoint_path {
            let due = stats.processed == 1
                || checkpoint_every.is_zero()
                || last_checkpoint.elapsed() >= checkpoint_every;
            if due {
                flush_writers(&mut writers)?;
                write_split_checkpoint(path, &stats)?;
                last_checkpoint = Instant::now();
            }
        }
        emit_split_progress(
            &mut progress_callback,
            &mut last_progress,
            progress_every,
            &stats,
        );
    }

    flush_writers(&mut writers)?;

    stats.last_position = stream.resume_offset();
    if let Some(path) = checkpoint_path {
        write_split_checkpoint(path, &stats)?;
    }
    if let Some(callback) = progress_callback.as_mut() {
        callback(&stats);
    }
    Ok(stats)
}

fn emit_split_progress(
    progress_callback: &mut Option<&mut dyn FnMut(&SplitStats)>,
    last_progress: &mut Instant,
    progress_every: Duration,
    stats: &SplitStats,
) {
    if let Some(callback) = progress_callback.as_mut() {
        let due = stats.processed == 1
            || progress_every.is_zero()
            || last_progress.elapsed() >= progress_every;
        if due {
            callback(stats);
            *last_progress = Instant::now();
        }
    }
}

fn flush_writers(
    writers: &mut std::collections::BTreeMap<String, BufWriter<File>>,
) -> Result<(), MboxError> {
    for writer in writers.values_mut() {
        writer.flush()?;
    }
    Ok(())
}

fn touch_writer_lru(lru: &mut std::collections::VecDeque<String>, period: &str) {
    if let Some(position) = lru.iter().position(|value| value == period) {
        lru.remove(position);
    }
    lru.push_back(period.to_string());
}

fn write_split_checkpoint(path: &Path, stats: &SplitStats) -> Result<(), MboxError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let checkpoint = SplitCheckpoint {
        last_position: stats.last_position,
        processed: stats.processed,
        written: stats.written,
        skipped: stats.skipped,
        errors: stats.errors,
    };
    let bytes = serde_json::to_vec(&checkpoint)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    std::fs::write(path, bytes)?;
    Ok(())
}

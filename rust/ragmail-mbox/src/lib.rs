//! Streaming MBOX parsing primitives.

#![forbid(unsafe_code)]

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const DEFAULT_MAX_OPEN_SPLIT_WRITERS: usize = 64;

/// Stream parser errors.
#[derive(Debug, Error)]
pub enum MboxError {
    /// I/O failure while reading from stream.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Resume offset is outside the input stream length.
    #[error("start offset {start_offset} is beyond stream length {stream_len}")]
    StartOffsetOutOfRange { start_offset: u64, stream_len: u64 },
}

/// A single raw MBOX message and its byte offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MboxMessage {
    /// Byte offset (from file start) where the message envelope starts.
    pub offset: u64,
    /// Complete raw message bytes including envelope line.
    pub raw: Vec<u8>,
}

/// Per-month split output statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MonthSplitStats {
    pub emails: u64,
    pub bytes: u64,
}

/// Aggregate split stage statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SplitStats {
    pub processed: u64,
    pub written: u64,
    pub skipped: u64,
    pub errors: u64,
    pub last_position: u64,
    pub by_month: std::collections::BTreeMap<String, MonthSplitStats>,
}

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
fn split_mbox_by_month_with_options_and_limit(
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

/// Streaming parser for MBOX files with resume support.
#[derive(Debug)]
pub struct MboxMessageStream<R: Read + Seek> {
    reader: BufReader<R>,
    current_offset: u64,
    pending_from: Option<(u64, Vec<u8>)>,
}

impl<R: Read + Seek> MboxMessageStream<R> {
    /// Creates a new stream, optionally resuming from an approximate byte offset.
    ///
    /// When `start_offset > 0`, the stream seeks to that position and then syncs to
    /// the next valid MBOX envelope boundary.
    pub fn new(mut inner: R, start_offset: u64) -> Result<Self, MboxError> {
        let stream_len = inner.seek(SeekFrom::End(0))?;
        if start_offset > stream_len {
            return Err(MboxError::StartOffsetOutOfRange {
                start_offset,
                stream_len,
            });
        }
        inner.seek(SeekFrom::Start(start_offset))?;

        let mut stream = Self {
            reader: BufReader::new(inner),
            current_offset: start_offset,
            pending_from: None,
        };

        if start_offset > 0 {
            stream.sync_to_next_boundary()?;
        }

        Ok(stream)
    }

    /// Returns the current byte offset in the source stream.
    #[must_use]
    pub const fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the safest resume offset.
    ///
    /// If the stream has already read the next envelope into `pending_from`,
    /// this returns the start offset of that envelope so resume does not skip it.
    #[must_use]
    pub fn resume_offset(&self) -> u64 {
        self.pending_from
            .as_ref()
            .map(|(offset, _)| *offset)
            .unwrap_or(self.current_offset)
    }

    /// Returns the next message, or `None` at EOF.
    pub fn next_message(&mut self) -> Result<Option<MboxMessage>, MboxError> {
        let mut current = self.pending_from.take();
        let mut line = Vec::new();

        loop {
            line.clear();
            let line_offset = self.current_offset;
            let read = self.reader.read_until(b'\n', &mut line)?;
            if read == 0 {
                return Ok(current.map(|(offset, raw)| MboxMessage { offset, raw }));
            }
            self.current_offset += read as u64;

            if current.is_none() {
                if is_valid_from_line(&line) {
                    current = Some((line_offset, line.clone()));
                }
                continue;
            }

            if is_valid_from_line(&line) {
                self.pending_from = Some((line_offset, line.clone()));
                let (offset, raw) = current.expect("checked current.is_some()");
                return Ok(Some(MboxMessage { offset, raw }));
            }

            if let Some((_, ref mut raw)) = current {
                raw.extend_from_slice(&line);
            }
        }
    }

    fn sync_to_next_boundary(&mut self) -> Result<(), MboxError> {
        let mut line = Vec::new();
        loop {
            line.clear();
            let line_offset = self.current_offset;
            let read = self.reader.read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            self.current_offset += read as u64;
            if is_valid_from_line(&line) {
                self.pending_from = Some((line_offset, line.clone()));
                break;
            }
        }
        Ok(())
    }
}

impl MboxMessageStream<File> {
    /// Opens a stream from a file path.
    pub fn from_path(path: &Path, start_offset: u64) -> Result<Self, MboxError> {
        let file = File::open(path)?;
        MboxMessageStream::new(file, start_offset)
    }
}

/// Returns true when a line is a valid MBOX envelope (`From `) line.
#[must_use]
pub fn is_valid_from_line(line: &[u8]) -> bool {
    let trimmed = trim_line_end(line);
    if !trimmed.starts_with(b"From ") {
        return false;
    }

    let text = match std::str::from_utf8(trimmed) {
        Ok(text) => text,
        Err(_) => return false,
    };
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts[0] != "From" {
        return false;
    }
    match parts.len() {
        // From <sender> <weekday> <month> <day> <hh:mm:ss> <year>
        7 => {
            WEEKDAYS.contains(&parts[2])
                && MONTHS.contains(&parts[3])
                && is_day(parts[4])
                && is_hms(parts[5])
                && is_year(parts[6])
        }
        // From <sender> <weekday> <month> <day> <hh:mm:ss> <tz> <year>
        8 => {
            WEEKDAYS.contains(&parts[2])
                && MONTHS.contains(&parts[3])
                && is_day(parts[4])
                && is_hms(parts[5])
                && is_timezone(parts[6])
                && is_year(parts[7])
        }
        _ => false,
    }
}

fn trim_line_end(line: &[u8]) -> &[u8] {
    if let Some(line) = line.strip_suffix(b"\n") {
        return line.strip_suffix(b"\r").unwrap_or(line);
    }
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn is_day(day: &str) -> bool {
    match day.parse::<u8>() {
        Ok(value) => (1..=31).contains(&value),
        Err(_) => false,
    }
}

fn is_hms(value: &str) -> bool {
    let mut parts = value.split(':');
    let (Some(h), Some(m), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Ok(h) = h.parse::<u8>() else {
        return false;
    };
    let Ok(m) = m.parse::<u8>() else {
        return false;
    };
    let Ok(s) = s.parse::<u8>() else {
        return false;
    };
    h < 24 && m < 60 && s < 60
}

fn is_timezone(value: &str) -> bool {
    if value.len() == 5
        && (value.starts_with('+') || value.starts_with('-'))
        && value[1..].chars().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    (1..=5).contains(&value.len()) && value.chars().all(|c| c.is_ascii_alphabetic())
}

fn is_year(value: &str) -> bool {
    value.len() == 4 && value.chars().all(|c| c.is_ascii_digit())
}

fn envelope_year_month(raw: &[u8]) -> Option<(u16, u8)> {
    let line_end = raw
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(raw.len());
    let line = &raw[..line_end];
    let line = std::str::from_utf8(line).ok()?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.first().copied() != Some("From") {
        return None;
    }
    let year_idx = match parts.len() {
        7 => 6,
        8 => 7,
        _ => return None,
    };
    let year = parts[year_idx].parse::<u16>().ok()?;
    let month = month_number(parts[3])?;
    Some((year, month))
}

fn month_number(value: &str) -> Option<u8> {
    MONTHS
        .iter()
        .position(|name| *name == value)
        .map(|idx| (idx + 1) as u8)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        is_valid_from_line, split_mbox_by_month, split_mbox_by_month_with_options,
        split_mbox_by_month_with_options_and_limit, MboxMessageStream, MONTHS, WEEKDAYS,
    };

    fn sample_mbox() -> Vec<u8> {
        let sample = concat!(
            "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
            "Message-ID: <a@example>\n",
            "Subject: One\n",
            "\n",
            "line 1\n",
            ">From escaped\n",
            "From this is not a boundary\n",
            "\n",
            "From b@example Tue Jan  2 00:00:00 +0000 2024\n",
            "Subject: Two\n",
            "\n",
            "line 2\n",
            "From c@example Wed Jan  3 00:00:00 +0000 2024\n",
            "Subject: Three\n",
            "\n",
            "line 3\n",
        );
        sample.as_bytes().to_vec()
    }

    fn collect_all(mut stream: MboxMessageStream<Cursor<Vec<u8>>>) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        loop {
            let next = stream.next_message().expect("next_message failed");
            let Some(message) = next else {
                break;
            };
            out.push((message.offset, message.raw));
        }
        out
    }

    fn temp_path(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!("ragmail-{prefix}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn envelope_line_validation_matches_expected_shape() {
        assert!(is_valid_from_line(
            b"From a@example Mon Jan  1 00:00:00 2024\n"
        ));
        assert!(is_valid_from_line(
            b"From a@example Mon Jan  1 00:00:00 +0000 2024\n"
        ));
        assert!(is_valid_from_line(
            b"From a@example Tue Feb 10 12:59:59 UTC 2024\n"
        ));
        assert!(!is_valid_from_line(b"From missing fields\n"));
        assert!(!is_valid_from_line(
            b"From a@example Mon Jan 40 00:00:00 +0000 2024\n"
        ));
        assert!(!is_valid_from_line(
            b"From a@example Mon Jan  1 99:00:00 +0000 2024\n"
        ));
        assert!(!is_valid_from_line(b"From this is not a boundary\n"));
    }

    #[test]
    fn stream_reads_messages_and_preserves_offsets() {
        let data = sample_mbox();
        let stream = MboxMessageStream::new(Cursor::new(data.clone()), 0).expect("create stream");
        let messages = collect_all(stream);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].0, 0);

        let second_offset = data
            .windows(b"From b@example".len())
            .position(|w| w == b"From b@example")
            .expect("second offset") as u64;
        assert_eq!(messages[1].0, second_offset);
        assert!(std::str::from_utf8(&messages[0].1)
            .expect("utf8")
            .contains("From this is not a boundary"));
    }

    #[test]
    fn stream_resume_syncs_to_next_boundary() {
        let data = sample_mbox();
        let start_offset = data
            .windows(b"line 1\n".len())
            .position(|w| w == b"line 1\n")
            .expect("line 1 offset") as u64;
        let stream =
            MboxMessageStream::new(Cursor::new(data), start_offset).expect("create stream");
        let messages = collect_all(stream);
        assert_eq!(messages.len(), 2);
        let first = std::str::from_utf8(&messages[0].1).expect("utf8");
        assert!(first.starts_with("From b@example"));
    }

    #[test]
    fn split_by_month_writes_period_files() {
        let data = sample_mbox();
        let root = temp_path("split");
        let input = root.join("sample.mbox");
        let out = root.join("split");
        std::fs::write(&input, data).expect("write input");

        let stats = split_mbox_by_month(&input, &out, 0, None).expect("split");
        assert_eq!(stats.processed, 3);
        assert_eq!(stats.written, 3);
        assert_eq!(stats.skipped, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.by_month.len(), 1);
        assert!(out.join("2024-01.mbox").exists());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn split_by_month_respects_year_filter() {
        let sample = concat!(
            "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
            "Subject: One\n",
            "\n",
            "line 1\n",
            "From b@example Tue Jan  2 00:00:00 +0000 2025\n",
            "Subject: Two\n",
            "\n",
            "line 2\n",
        );
        let root = temp_path("split-filter");
        let input = root.join("sample.mbox");
        let out = root.join("split");
        std::fs::write(&input, sample.as_bytes()).expect("write input");

        let mut years = std::collections::BTreeSet::new();
        years.insert(2025);
        let stats = split_mbox_by_month(&input, &out, 0, Some(&years)).expect("split");
        assert_eq!(stats.processed, 2);
        assert_eq!(stats.written, 1);
        assert_eq!(stats.skipped, 1);
        assert!(out.join("2025-01.mbox").exists());
        assert!(!out.join("2024-01.mbox").exists());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn split_by_month_writes_checkpoint_file() {
        let data = sample_mbox();
        let root = temp_path("split-checkpoint");
        let input = root.join("sample.mbox");
        let out = root.join("split");
        let checkpoint = root.join("split.checkpoint.json");
        std::fs::write(&input, data).expect("write input");

        let stats = split_mbox_by_month_with_options(
            &input,
            &out,
            0,
            None,
            Some(&checkpoint),
            Duration::ZERO,
        )
        .expect("split");
        assert_eq!(stats.written, 3);
        assert!(checkpoint.exists());

        let checkpoint_text = std::fs::read_to_string(&checkpoint).expect("read checkpoint");
        assert!(checkpoint_text.contains("\"last_position\""));
        assert!(checkpoint_text.contains("\"written\":3"));

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn split_resume_from_offset_does_not_duplicate_messages() {
        let data = sample_mbox();
        let root = temp_path("split-resume-no-dup");
        let input = root.join("sample.mbox");
        let out = root.join("split");
        std::fs::write(&input, data).expect("write input");
        std::fs::create_dir_all(&out).expect("create out");

        let mut stream = MboxMessageStream::from_path(&input, 0).expect("stream");
        let first = stream.next_message().expect("next").expect("first");
        let resume_offset = stream.resume_offset();

        let month_path = out.join("2024-01.mbox");
        std::fs::write(&month_path, &first.raw).expect("write first");

        let stats = split_mbox_by_month(&input, &out, resume_offset, None).expect("resume split");
        assert_eq!(stats.written, 2);

        let merged = std::fs::read_to_string(&month_path).expect("read merged");
        let from_count = merged
            .lines()
            .filter(|line| is_valid_from_line(format!("{line}\n").as_bytes()))
            .count();
        assert_eq!(from_count, 3);

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn split_handles_many_months_with_low_writer_limit() {
        let root = temp_path("split-low-writer-limit");
        let input = root.join("sample-many-months.mbox");
        let out = root.join("split");
        let mut sample = String::new();
        for month in 1..=12 {
            let weekday = WEEKDAYS[(month as usize) % WEEKDAYS.len()];
            let month_name = MONTHS[(month - 1) as usize];
            sample.push_str(&format!(
                "From m{month}@example.com {weekday} {month_name}  1 00:00:00 +0000 2024\n"
            ));
            sample.push_str(&format!("Message-ID: <m{month}@example.com>\n"));
            sample.push_str(&format!("Subject: Month {month}\n"));
            sample.push('\n');
            sample.push_str("body\n");
        }
        std::fs::write(&input, sample.as_bytes()).expect("write input");

        let stats = split_mbox_by_month_with_options_and_limit(
            &input,
            &out,
            0,
            None,
            None,
            Duration::from_secs(30),
            2,
            None,
            Duration::from_millis(250),
        )
        .expect("split");

        assert_eq!(stats.processed, 12);
        assert_eq!(stats.written, 12);
        for month in 1..=12 {
            assert!(
                out.join(format!("2024-{month:02}.mbox")).exists(),
                "expected output for month {month:02}"
            );
        }

        std::fs::remove_dir_all(root).expect("cleanup");
    }
}

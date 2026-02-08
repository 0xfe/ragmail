use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::{MboxError, MboxMessage};

pub(crate) const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
pub(crate) const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

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

pub(crate) fn envelope_year_month(raw: &[u8]) -> Option<(u16, u8)> {
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

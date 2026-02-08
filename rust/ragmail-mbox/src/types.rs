use thiserror::Error;

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

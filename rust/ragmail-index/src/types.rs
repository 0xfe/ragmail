use std::path::PathBuf;
use std::time::Duration;

use ragmail_mbox::MboxError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable index row contract mirrored from Python `mbox_index.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRecord {
    pub email_id: String,
    pub message_id: Option<String>,
    pub message_id_lower: Option<String>,
    pub mbox_file: String,
    pub offset: u64,
    pub length: u64,
}

/// Checkpoint payload for resumable index runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub position: u64,
    pub indexed: u64,
    pub timestamp_epoch_s: u64,
}

/// Build-time settings for indexing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOptions {
    pub checkpoint_path: Option<PathBuf>,
    pub resume: bool,
    pub checkpoint_every: Duration,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            checkpoint_path: None,
            resume: true,
            checkpoint_every: Duration::from_secs(30),
        }
    }
}

/// Summary stats from an index build run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildStats {
    pub indexed: u64,
    pub last_position: u64,
}

/// Index builder errors.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Mbox(#[from] MboxError),
}

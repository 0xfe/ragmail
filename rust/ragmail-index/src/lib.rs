//! Byte-offset MBOX indexing stage implementation (Rust migration).

#![forbid(unsafe_code)]

mod build;
mod query;
mod record;
mod types;

pub use build::build_index_for_file;
pub use query::{find_in_index, read_message_bytes};
pub use record::record_from_message;
pub use types::{BuildOptions, BuildStats, IndexCheckpoint, IndexError, IndexRecord};

#[cfg(test)]
mod tests;

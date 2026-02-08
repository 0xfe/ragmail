//! Email cleaning stage implementation (Rust migration).

#![forbid(unsafe_code)]

mod codec;
mod header;
mod mime;
mod pipeline;
mod text;
mod types;

pub use pipeline::{
    clean_mbox_file, clean_mbox_file_with_progress, default_clean_outputs, default_summary_output,
};
pub use types::{CleanError, CleanOptions, CleanStats};

#[cfg(test)]
mod tests;

//! Streaming MBOX parsing primitives.

#![forbid(unsafe_code)]

mod split;
mod stream;
mod types;

pub use split::{
    split_mbox_by_month, split_mbox_by_month_with_options,
    split_mbox_by_month_with_options_and_progress,
};
pub use stream::{is_valid_from_line, MboxMessageStream};
pub use types::{MboxError, MboxMessage, MonthSplitStats, SplitStats};

#[cfg(test)]
mod tests;

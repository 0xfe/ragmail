//! Shared domain types for the Rust migration.

pub mod stage;
pub mod workspace;

pub use stage::Stage;

#[cfg(test)]
mod stage_tests;

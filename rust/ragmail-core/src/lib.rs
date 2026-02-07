//! Shared domain types for the Rust migration.

pub mod workspace;

/// Stable stage identifiers mirrored from the current Python pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Download,
    Split,
    Index,
    Clean,
    Vectorize,
    Ingest,
}

impl Stage {
    /// Returns the canonical stage name used in workspace state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Split => "split",
            Self::Index => "index",
            Self::Clean => "clean",
            Self::Vectorize => "vectorize",
            Self::Ingest => "ingest",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Stage;

    #[test]
    fn stage_names_are_stable() {
        assert_eq!(Stage::Download.as_str(), "download");
        assert_eq!(Stage::Split.as_str(), "split");
        assert_eq!(Stage::Index.as_str(), "index");
        assert_eq!(Stage::Clean.as_str(), "clean");
        assert_eq!(Stage::Vectorize.as_str(), "vectorize");
        assert_eq!(Stage::Ingest.as_str(), "ingest");
    }
}

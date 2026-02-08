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

# Risks and Findings

This file tracks known issues and risks discovered during refactor.

## Open

- None

## Closed

- **R1: Clean summary mismatch**
  - **Fix**: Ensure `ProgressDisplay` final summary uses `stats.total_emails` even for fast runs.
  - **Closed**: 2026-02-03

- **R2: HuggingFace cache permission warnings**
  - **Fix**: Workspace-managed cache env (`HF_HOME`, `HUGGINGFACE_HUB_CACHE`) and `--workspace` support on ragmail search/serve/stats.
  - **Closed**: 2026-02-03

- **R3: Hardcoded embedding dimension in stats**
  - **Fix**: Use `settings.embedding_dimension` in `stats` command.
  - **Closed**: 2026-02-03

- **R4: FTS index corruption caused empty search results**
  - **Fix**: Auto-rebuild FTS index on detection, and expand indexed fields to include sender/recipient metadata.
  - **Closed**: 2026-02-03

# Developer Guide

This guide is for contributors working on ragmail internals.
It covers setup, daily iteration, testing, debugging, and release prep.

## Repo Map

Top-level layout:
```text
ragmail/
├── python/
│   ├── lib/ragmail/           # Python package (CLI, vectorize, ingest, search, LLM)
│   ├── tests/                 # Python + bridge + tooling tests
│   ├── pyproject.toml
│   └── pytest.ini
├── rust/
│   ├── Cargo.toml             # Rust workspace manifest
│   ├── ragmail-cli/           # Rust CLI binary (ragmail)
│   ├── ragmail-core/          # Workspace/state contracts
│   ├── ragmail-mbox/          # Streaming MBOX + split
│   ├── ragmail-index/         # Byte-offset index builder
│   └── ragmail-clean/         # Cleaner
├── just.d/                    # just recipe groups + helper scripts
├── docs/
├── VERSION
└── workspaces/                # runtime outputs (gitignored)
```

## Toolchain Requirements

Required:
- Python 3.11+
- Rust 1.93.0+

Recommended:
- `uv`
- `just`

## First-Time Setup

```bash
# Clone and enter repo
git clone <your-repo-url>
cd ragmail

# Bootstrap Python deps + Rust fetch
just bootstrap

# Activate venv for direct Python invocations
source .venv/bin/activate
```

`just bootstrap` performs both:
- Python environment bootstrap into root `.venv`
- Rust workspace build (`cargo build --workspace`)
- Symlink `.venv/bin/ragmail` to `rust/target/debug/ragmail`

Implementation detail:
- Python bootstrap logic is in `just.d/scripts/bootstrap-python.sh`.
- It tries `uv` first and falls back to `python -m venv + pip` when needed.

No `just` fallback:
```bash
# Create venv
python3 -m venv .venv
source .venv/bin/activate

# Sync deps from python/
UV_PROJECT_ENVIRONMENT=.venv uv sync --project python --extra dev

# Fetch Rust crates
cargo fetch --manifest-path rust/Cargo.toml
```

## Daily Iteration Loop

```bash
# 1) Run Rust tests
just test-rust

# 2) Run Python tests
just test-python

# 3) Run strict Rust lint gates
just lint

# 4) Run everything for final confidence
just test-all
```

## Build Commands

```bash
# Rust debug build
just build-rust

# Rust release build (ragmail)
just build-rust-release

# Print current version from VERSION
just version
```

## Test Matrix

Default suites:
- Rust: `cargo test --manifest-path rust/Cargo.toml --workspace`
- Python: `./just.d/scripts/test-python.sh`

Targeted suites:
```bash
# Rust/Python pipeline bridge contracts
./just.d/scripts/test-python.sh -q python/tests/test_rust_pipeline_bridge.py python/tests/test_index_parity.py

# Release tooling tests
./just.d/scripts/test-python.sh -q python/tests/test_release_tooling.py

# Live integration tests (opt-in)
RAGMAIL_RUN_INTEGRATION_TESTS=1 ./just.d/scripts/test-python.sh -m integration
```

## Important File Locations

Pipeline orchestration:
- `python/lib/ragmail/cli.py`
- `python/lib/ragmail/pipeline.py`

Workspace and state model:
- `python/lib/ragmail/workspace.py`
- `rust/ragmail-core/src/workspace.rs`

Rust pipeline stages:
- `rust/ragmail-mbox/src/lib.rs` (`split`)
- `rust/ragmail-clean/src/lib.rs` (`preprocess`, including index-row emission)
- `rust/ragmail-index/src/lib.rs` (index row helpers used by preprocess)

Rust CLI orchestration:
- `rust/ragmail-cli/src/main.rs`

DB ingest and search:
- `python/lib/ragmail/ingest/`
- `python/lib/ragmail/storage/`
- `python/lib/ragmail/search/`

## Debugging Workflows

## Inspect workspace state
```bash
# Show workspace metadata
ragmail workspace info my-mail

# Inspect stage state JSON
cat workspaces/my-mail/state.json

# Review stage logs
ls -la workspaces/my-mail/logs
```

## Run single stages for isolation
```bash
# Isolate Rust preprocessing
ragmail pipeline private/gmail-2015.mbox --workspace debug --stages split,preprocess

# Isolate vectorize
ragmail pipeline --workspace debug --stages vectorize

# Isolate ingest
ragmail pipeline --workspace debug --stages ingest
```

## Reproduce with refresh
```bash
# Archive old outputs and rerun selected stages
ragmail pipeline --workspace debug --stages preprocess,vectorize,ingest --refresh
```

## Force Python to use a prebuilt Rust binary
```bash
# Avoid cargo run overhead during repeated tests
export RAGMAIL_BIN="$PWD/rust/target/debug/ragmail"

# Run pipeline normally; Python will call this binary
ragmail pipeline private/gmail-2015.mbox --workspace debug
```

## Rust-Python bridge tuning
Use these only for development and troubleshooting:
- `RAGMAIL_PY_BRIDGE_BIN`
- `RAGMAIL_PYTHON_BIN`
- `RAGMAIL_PY_BRIDGE_MAX_RETRIES`
- `RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS`

## Model and cache settings

Useful env vars:
- `RAGMAIL_CACHE_DIR`
- `HF_HOME`
- `HUGGINGFACE_HUB_CACHE`
- `EMAIL_SEARCH_OPENAI_BASE_URL`
- `EMAIL_SEARCH_OPENAI_API_KEY`
- `EMAIL_SEARCH_OPENAI_MODEL`

Example:
```bash
# Route LLM calls to a local OpenAI-compatible endpoint
export EMAIL_SEARCH_OPENAI_BASE_URL="http://localhost:11434/v1"
export EMAIL_SEARCH_OPENAI_API_KEY="dummy"
export EMAIL_SEARCH_OPENAI_MODEL="llama3.1"
```

## Release and Versioning

Version source of truth:
- `VERSION`

Common flow:
```bash
# Bump patch version
just bump-patch

# Run release checks
just release-check

# Build local artifacts
just release-artifacts

# Optional local release smoke
just release-smoke

# Create annotated version tag
just release
```

Additional release docs:
- [`release.md`](release.md)

## Writing New Tests

Guidelines:
- Put Python tests under `python/tests/`.
- Use fixture data from `python/tests/fixtures/` when possible.
- Keep slow or external tests behind markers or env gates.
- Prefer deterministic fixtures over network-bound dependencies.

## Documentation Update Rules

When behavior changes, update:
- `README.md` for user-facing workflows.
- `docs/pipeline.md` for stage behavior changes.
- `docs/DESIGN.md` for architecture/boundary changes.
- `docs/developers.md` for build/test/debug process changes.
- `ai-state.md` for AI-oriented repo state.

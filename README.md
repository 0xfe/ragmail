# RAGmail

## About

RAGmail lets you search and analyze your email with your favourite agent (Claude, Codex, etc.)

Typical questions you can answer:

- "What did we decide about the school trip budget?"
- "Where all did I travel to in in 2006?"
- "How many times did Bob email me in February 2026?"

## Quickstart
```bash
# 1) Clone
git clone <your-repo-url>
cd ragmail

# 2) Bootstrap Python + Rust deps
just bootstrap

# 3) Activate the venv
source .venv/bin/activate

# 4) Run full pipeline (model,split,preprocess,vectorize,ingest)
ragmail pipeline private/gmail-2015.mbox --workspace my-mail

# 5) Search your workspace
ragmail search "meeting tomorrow" --workspace my-mail
```

`just bootstrap` now does both:
- prepares a shared root `.venv` that includes the `ragmail` CLI
- builds the Rust workspace (so Rust stages are ready before first pipeline run)
## Quickstart (w/ Remote GPU)

Use this when embedding/vectorization is slow on your local machine. It performs most of the work
on the local machine, but lets you offload the GPU-heavy work to a remote machine.


```bash
# Local: run preprocessing only
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split,preprocess

# Local: send clean JSONL to remote GPU host
# (replace user@host and paths)
tar -czf my-mail-clean.tar.gz -C workspaces/my-mail clean
rsync -av my-mail-clean.tar.gz user@host:/data/ragmail/

# Remote: unpack + run vectorize only
ssh user@host '
  cd /data/ragmail && \
  tar -xzf my-mail-clean.tar.gz && \
  ragmail pipeline \
    --workspace my-mail \
    --base-dir /data/ragmail/workspaces \
    --stages vectorize \
    --clean-dir /data/ragmail/clean
'

# Remote: send embeddings back
tar -czf my-mail-embeddings.tar.gz -C /data/ragmail/workspaces/my-mail embeddings
rsync -av user@host:/data/ragmail/my-mail-embeddings.tar.gz ./

# Local: unpack embeddings + ingest
tar -xzf my-mail-embeddings.tar.gz -C workspaces/my-mail
ragmail pipeline --workspace my-mail --stages ingest
```

## Prerequisites
Required:
- `python` 3.11+
- Rust toolchain 1.93.0+ (`rustc`, `cargo`, `rustfmt`, `clippy`)

Recommended:
- `uv` for Python dependency and environment management
- `just` for common build/test/release commands

Optional for release maintainers:

- `dpkg-deb` for Linux `.deb` packaging

## Usage Instructions

Run `ragmail --help` for the full command list.

Common commands:
```bash
# Run full pipeline
ragmail pipeline private/gmail-2015.mbox --workspace my-mail

# Run specific stages
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split,preprocess
ragmail pipeline --workspace my-mail --stages vectorize
ragmail pipeline --workspace my-mail --stages ingest

# Resume is enabled by default
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --resume

# Re-run selected stages from scratch (archives old outputs)
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages preprocess --refresh

# Search
ragmail search "invoice" --workspace my-mail

# Search with RAG answer generation
ragmail search "what did we decide about the budget" --workspace my-mail --rag

# Show full raw message bytes by id
ragmail message --workspace my-mail --email-id <email_id>

# Workspace utilities
ragmail workspace init my-mail
ragmail workspace info my-mail
```

Note:
- A fresh workspace run needs at least one input MBOX when `split` is selected.
- If you run only `vectorize` and/or `ingest`, `input_mbox` is optional.

Useful pipeline flags:
- `--ingest-batch-size`: write batch size for DB inserts.
- `--embedding-batch-size`: batch size sent to embedding model.
- `--chunk-size` and `--chunk-overlap`: control chunk granularity.
- `--skip-exists-check`: faster ingest when safe.
- `--checkpoint-interval`: checkpoint frequency.
- `--compact-every`: periodic DB compaction during ingest.

## About Workspaces
Each workspace is an isolated processing run.
Use one workspace per dataset or experiment.

Default layout:
```text
workspaces/<name>/
├── inputs/                 # linked input mbox files
├── split/                  # monthly mbox files + mbox_index.jsonl
├── clean/                  # cleaned jsonl
├── spam/                   # filtered bulk/spam jsonl
├── reports/                # per-file summary reports
├── embeddings/             # embedding stores (*.embed.db)
├── db/
│   └── email_search.lancedb
├── logs/                   # stage logs
├── .checkpoints/           # resume checkpoints
├── workspace.json          # workspace config/paths
└── state.json              # stage state + durations
```

You can set a different root with `--base-dir`.

## Cost / Performance / Privacy Tradeoffs
### What runs locally by default
By default, all pipeline stages run on your machine.

- `model`, `split`, `preprocess`: local processing (`split`/`preprocess` are Rust-backed).
- `vectorize`: local embedding inference.
- `ingest`: local LanceDB writes.
- `search`: local retrieval.

No email data must leave your machine for these steps.

### What can call external models
LLM-assisted features (for example `ragmail search --rag`) can call an OpenAI-compatible API.

You control this with environment variables:
```bash
# Use a trusted hosted provider (default style)
export EMAIL_SEARCH_OPENAI_BASE_URL="https://api.openai.com/v1"
export EMAIL_SEARCH_OPENAI_API_KEY="<key>"
export EMAIL_SEARCH_OPENAI_MODEL="gpt-5.2"
```

You can also point to a local OpenAI-compatible server, such as Ollama or vLLM:
```bash
# Example: local OpenAI-compatible endpoint
export EMAIL_SEARCH_OPENAI_BASE_URL="http://localhost:11434/v1"
export EMAIL_SEARCH_OPENAI_API_KEY="dummy"
export EMAIL_SEARCH_OPENAI_MODEL="llama3.1"
```

### Moving compute to cloud GPUs
For faster embeddings, run only `vectorize` on a GPU machine.

For maximum throughput, run all stages on a GPU-backed cloud VM:
- Upload MBOX to a GCP or AWS VM.
- Run full `ragmail pipeline` there.
- Download only the workspace outputs you need.

This improves speed, but it changes your data boundary.
Use encryption and your organization’s data policy.

## More Docs
- Pipeline deep dive: [`docs/pipeline.md`](docs/pipeline.md)
- High-level design: [`docs/DESIGN.md`](docs/DESIGN.md)
- Developer guide: [`docs/developers.md`](docs/developers.md)
- Release process: [`docs/release.md`](docs/release.md)
- Usage examples: [`docs/examples.md`](docs/examples.md)
- Prompt details: [`docs/prompts.md`](docs/prompts.md)

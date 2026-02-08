# RAGmail

## About

I have about 22 years of e-mail in Gmail, and another 10 years of it my archives. I built this tool
so I could do some local analysis on my email.

RAGmail lets you search and analyze your email with your favourite agent (Claude, Codex, etc.)

Typical questions you can answer:

- "What did we decide about the school trip budget?"
- "Where all did I travel to in 2006?"
- "How many times did Bob email me in February 2026?"

## How it works

`RAGmail` performs all the heavy lifting around processing gigantic mail boxes, cleaning it up, and building
a database indexed for both full-text and semantic search.

Cleaning involves things like removing bulk mail, stripping headers, signatures, attachments, and other unnecessary
elements, normalizing and tagging common fields, and a host of other things.

After messages are cleaned, `RAGmail` generates embedding vectors for the subject and body of each message, and
then ingests everything into a vector database, with pointers to the original email for further analysis.




## Quickstart

### Download and Build `RAGmail`

```bash
# Clone this repo
git clone <this repo>
cd ragmail

# Build python and rust code
just bootstrap

# Activate the venv
source .venv/bin/activate
```

### Fetch your mailbox

`RAGmail` works with standard `.MBOX` files. You can download your entire Gmail mailbox
with Google Takeout.


### Ingest into `RAGmail`

```bash
# Run full pipeline (model,split,preprocess,vectorize,ingest)
ragmail pipeline private/gmail-2015.mbox --workspace my-mail

# Search your workspace
ragmail search "meeting tomorrow" --workspace my-mail
```

### Quickstart (with a remote GPU)

This is much much faster (10-100x) if you don't have a local GPU. Embedding calculations are
very compute-intensive, and if you have very large mailboxes it could take multiple days! A
single L4 GPU node on GCP can process a 15GB mailbox in less than an hour (for less than the
price of coffee.)

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

## Analyzing your email


## Prerequisites
Required:
- `python` 3.11+
- Rust toolchain 1.93.0+ (`rustc`, `cargo`, `rustfmt`, `clippy`)

Recommended:
- `uv` for Python dependency and environment management
- `just` for common build/test/release commands

Optional for release maintainers:

- `dpkg-deb` for Linux `.deb` packaging

Release artifact quickstart:
```bash
# Build a self-sufficient distribution for your host machine
just release host

# Build a specific platform (must run on matching OS/arch host)
just release linux/amd64
```

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

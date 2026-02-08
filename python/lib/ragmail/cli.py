"""Unified CLI wrapper for ragmail."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

import click

from ragmail import __version__
from ragmail.ignorelist import (
    apply_ignore_list_stream,
    load_ignore_list,
    write_ignore_list_template,
)
from ragmail.pipeline import run_pipeline, _path_size_bytes, _warmup_dependencies
from ragmail.mbox_index import read_message_bytes
from ragmail.workspace import default_cache_root, get_workspace

def _run_module(module: str, args: list[str]) -> None:
    cmd = [sys.executable, "-m", module, *args]
    result = subprocess.run(cmd)
    raise SystemExit(result.returncode)


def _args_has_flag(args: list[str], flag: str) -> bool:
    for arg in args:
        if arg == flag or arg.startswith(flag + "="):
            return True
    return False


def _apply_workspace_env(
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None = None,
    args: list[str] | None = None,
    set_db_env: bool = False,
) -> None:
    if not workspace_name:
        return
    ws = get_workspace(workspace_name, base_dir=base_dir)
    ws.ensure()
    ws.apply_env(cache_dir=cache_dir, base_dir=base_dir)

    if set_db_env:
        args_list = args or []
        if not _args_has_flag(args_list, "--db"):
            db_path = ws.db_dir / "email_search.lancedb"
            os.environ["EMAIL_SEARCH_DB_PATH"] = str(db_path)


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """ragmail - unified CLI for email cleaning, ingestion, and search."""


def _passthrough_command(help_text: str, *, hidden: bool = False):
    return click.command(
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        add_help_option=False,
        help=help_text,
        hidden=hidden,
    )


@_passthrough_command("Query the database (pass-through to email-search query).")
@click.option("--workspace", "workspace_name", help="Workspace name for cache/config")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.pass_context
def query(
    ctx: click.Context,
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None,
) -> None:
    _apply_workspace_env(
        workspace_name,
        base_dir,
        cache_dir=cache_dir,
        args=list(ctx.args),
        set_db_env=True,
    )
    _run_module("ragmail.search_cli", ["query", *ctx.args])


@_passthrough_command(
    "Backward-compatible alias for `query` (pass-through to email-search query).",
    hidden=True,
)
@click.option("--workspace", "workspace_name", help="Workspace name for cache/config")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.pass_context
def search(
    ctx: click.Context,
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None,
) -> None:
    _apply_workspace_env(
        workspace_name,
        base_dir,
        cache_dir=cache_dir,
        args=list(ctx.args),
        set_db_env=True,
    )
    _run_module("ragmail.search_cli", ["query", *ctx.args])


@_passthrough_command("Show database stats (pass-through to email-search stats).")
@click.option("--workspace", "workspace_name", help="Workspace name for cache/config")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.pass_context
def stats(
    ctx: click.Context,
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None,
) -> None:
    _apply_workspace_env(
        workspace_name,
        base_dir,
        cache_dir=cache_dir,
        args=list(ctx.args),
        set_db_env=True,
    )
    _run_module("ragmail.search_cli", ["stats", *ctx.args])


@_passthrough_command("Dedupe database tables (pass-through to email-search dedupe).")
@click.option("--workspace", "workspace_name", help="Workspace name for cache/config")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.pass_context
def dedupe(
    ctx: click.Context,
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None,
) -> None:
    _apply_workspace_env(
        workspace_name,
        base_dir,
        cache_dir=cache_dir,
        args=list(ctx.args),
        set_db_env=True,
    )
    _run_module("ragmail.search_cli", ["dedupe", *ctx.args])


@_passthrough_command("Run the API server (pass-through to email-search serve).")
@click.option("--workspace", "workspace_name", help="Workspace name for cache/config")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.pass_context
def serve(
    ctx: click.Context,
    workspace_name: str | None,
    base_dir: Path | None,
    cache_dir: Path | None,
) -> None:
    _apply_workspace_env(
        workspace_name,
        base_dir,
        cache_dir=cache_dir,
        args=list(ctx.args),
        set_db_env=True,
    )
    _run_module("ragmail.search_cli", ["serve", *ctx.args])


cli.add_command(query)
cli.add_command(search)
cli.add_command(stats)
cli.add_command(dedupe)
cli.add_command(serve)


@cli.command()
@click.argument("input_mbox", type=click.Path(exists=True, path_type=Path), nargs=-1)
@click.option("--workspace", "workspace_name", required=True, help="Workspace name")
@click.option("--years", type=int, multiple=True, help="Only process specific years")
@click.option("--resume/--no-resume", default=True, help="Resume pipeline if possible")
@click.option(
    "--refresh",
    is_flag=True,
    help="Restart selected stages from scratch (archive outputs and clear checkpoints)",
)
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.option("--clean-dir", type=click.Path(path_type=Path), help="Use clean JSONL files from this directory")
@click.option("--ingest-batch-size", type=int, help="Emails per write batch")
@click.option("--embedding-batch-size", type=int, help="Embedding model batch size")
@click.option("--chunk-size", type=int, help="Max characters per body chunk")
@click.option("--chunk-overlap", type=int, help="Chunk overlap in characters")
@click.option("--checkpoint-interval", type=int, help="Checkpoint interval in seconds")
@click.option("--preprocess-workers", type=int, help="Parallel workers for preprocess stage")
@click.option(
    "--compact-every",
    type=int,
    help="Run compaction every N ingested emails (0 disables periodic compaction)",
)
@click.option(
    "--no-repair-embeddings",
    is_flag=True,
    help="Disable automatic repair of missing embeddings during ingest",
)
@click.option(
    "--embeddings-dir",
    type=click.Path(path_type=Path),
    help="Use precomputed embeddings from this directory",
)
@click.option(
    "--stages",
    type=str,
    help="Comma-separated stages to run (model,split,preprocess,vectorize,ingest). Default: all",
)
@click.option(
    "--skip-exists-check",
    is_flag=True,
    help="Skip per-email existence check (auto-enabled on new/empty databases)",
)
@click.option(
    "--traceback/--no-traceback",
    default=False,
    help="Show full error traceback on failure",
)
def pipeline(
    input_mbox: tuple[Path, ...],
    workspace_name: str,
    years: tuple[int, ...],
    resume: bool,
    refresh: bool,
    base_dir: Path | None,
    cache_dir: Path | None,
    clean_dir: Path | None,
    ingest_batch_size: int | None,
    embedding_batch_size: int | None,
    chunk_size: int | None,
    chunk_overlap: int | None,
    checkpoint_interval: int | None,
    preprocess_workers: int | None,
    compact_every: int | None,
    no_repair_embeddings: bool,
    embeddings_dir: Path | None,
    stages: str | None,
    skip_exists_check: bool,
    traceback: bool,
) -> None:
    """Run model -> split -> preprocess -> vectorize -> ingest pipeline in a workspace."""
    repair_embeddings = not no_repair_embeddings
    stage_set: set[str] | None = None
    if stages:
        parts = [part.strip().lower() for part in stages.split(",") if part.strip()]
        if not parts:
            raise click.ClickException("Stages list is empty.")
        alias_map = {"download": "model", "clean": "preprocess", "index": "preprocess"}
        valid = {"model", "split", "preprocess", "vectorize", "ingest", *alias_map.keys()}
        invalid = [part for part in parts if part not in valid]
        if invalid:
            raise click.ClickException(
                f"Unknown stages: {', '.join(invalid)}. "
                "Use model,split,preprocess,vectorize,ingest."
            )
        stage_set = {alias_map.get(part, part) for part in parts}

    if clean_dir and (stage_set is None or "split" in stage_set or "preprocess" in stage_set):
        raise click.ClickException("--clean-dir can only be used for vectorize/ingest stages.")
    if not input_mbox and (stage_set is None or "split" in stage_set):
        raise click.ClickException("Provide at least one input MBOX file.")
    try:
        run_pipeline(
            list(input_mbox),
            workspace_name=workspace_name,
            years=years or None,
            resume=resume,
            refresh=refresh,
            base_dir=base_dir,
            cache_dir=cache_dir,
            clean_dir=clean_dir,
            ingest_batch_size=ingest_batch_size,
            embedding_batch_size=embedding_batch_size,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            checkpoint_interval=checkpoint_interval,
            preprocess_workers=preprocess_workers,
            compact_every=compact_every,
            repair_embeddings=repair_embeddings,
            embeddings_dir=embeddings_dir,
            stages=stage_set,
            skip_exists_check=skip_exists_check if skip_exists_check else None,
        )
    except KeyboardInterrupt:
        _print_interrupt_summary(
            workspace_name=workspace_name,
            base_dir=base_dir,
            cache_dir=cache_dir,
            clean_dir=clean_dir,
            embeddings_dir=embeddings_dir,
            input_mbox=list(input_mbox),
            years=list(years),
            stages=stage_set,
        )
        raise SystemExit(130)
    except click.ClickException:
        raise
    except Exception as exc:
        if traceback:
            raise
        message = str(exc).strip() or "Pipeline failed."
        raise click.ClickException(f"{message} (rerun with --traceback for details)") from None


def _print_interrupt_summary(
    *,
    workspace_name: str,
    base_dir: Path | None,
    cache_dir: Path | None,
    clean_dir: Path | None,
    embeddings_dir: Path | None,
    input_mbox: list[Path],
    years: list[int],
    stages: set[str] | None,
) -> None:
    ws = get_workspace(workspace_name, base_dir=base_dir)
    state = ws.load_state()
    selected = stages or {"model", "split", "preprocess", "vectorize", "ingest"}
    print()
    print("Interrupted. Checkpoints saved where available.")
    print("Checkpoint status:")
    for stage in ["model", "split", "preprocess", "vectorize", "ingest"]:
        if stage not in selected:
            continue
        status = state.get("stages", {}).get(stage, {}).get("status", "pending")
        print(f"  {stage:<9} {status}")
    print()
    print("Resumption:")
    print("  Completed stages are skipped; interrupted stages resume from checkpoints when possible.")
    print("Resume command:")
    print(
        "  "
        + _build_resume_command(
            input_mbox,
            workspace_name,
            base_dir,
            cache_dir,
            clean_dir,
            embeddings_dir,
            years,
            stages,
        )
    )


def _build_resume_command(
    inputs: list[Path],
    workspace_name: str,
    base_dir: Path | None,
    cache_dir: Path | None,
    clean_dir: Path | None,
    embeddings_dir: Path | None,
    years: list[int],
    stages: set[str] | None,
) -> str:
    args: list[str] = ["ragmail", "pipeline"]
    if inputs:
        args.extend(str(p) for p in inputs)
    args.extend(["--workspace", workspace_name, "--resume"])
    if base_dir is not None:
        args.extend(["--base-dir", str(base_dir)])
    if cache_dir is not None:
        args.extend(["--cache-dir", str(cache_dir)])
    if clean_dir is not None:
        args.extend(["--clean-dir", str(clean_dir)])
    if embeddings_dir is not None:
        args.extend(["--embeddings-dir", str(embeddings_dir)])
    if years:
        for year in years:
            args.extend(["--years", str(year)])
    if stages is not None:
        args.extend(["--stages", ",".join(sorted(stages))])
    return " ".join(shlex.quote(arg) for arg in args)


def _collect_clean_files(clean_root: Path) -> list[Path]:
    return sorted(clean_root.glob("*.clean.jsonl"))


def _bridge_emit(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


@cli.group("py")
def py_bridge() -> None:
    """Internal Python bridge commands used by Rust orchestration."""


@py_bridge.command("model")
@click.option("--workspace", "workspace_name", required=True, help="Workspace name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
def py_model(workspace_name: str, base_dir: Path | None, cache_dir: Path | None) -> None:
    """Contract command for Rust -> Python model warmup stage."""
    ws = get_workspace(workspace_name, base_dir=base_dir)
    ws.ensure()
    ws.apply_env(cache_dir=cache_dir, base_dir=base_dir)

    cache_root = cache_dir or default_cache_root(base_dir)
    before_bytes = _path_size_bytes(cache_root)
    started = time.monotonic()
    warmup_error: BaseException | None = None

    def _warmup_runner() -> None:
        nonlocal warmup_error
        try:
            _warmup_dependencies()
        except BaseException as exc:  # pragma: no cover - surfaced below
            warmup_error = exc

    warmup_thread = threading.Thread(target=_warmup_runner, daemon=True)
    warmup_thread.start()
    while warmup_thread.is_alive():
        current_bytes = _path_size_bytes(cache_root)
        _bridge_emit(
            {
                "event": "progress",
                "stage": "model",
                "downloaded_bytes": max(0, current_bytes - before_bytes),
                "cache_bytes": current_bytes,
                "elapsed_s": max(0.0, time.monotonic() - started),
            }
        )
        warmup_thread.join(timeout=0.5)
    warmup_thread.join()
    if warmup_error is not None:
        raise warmup_error

    after_bytes = _path_size_bytes(cache_root)
    _bridge_emit(
        {
            "event": "progress",
            "stage": "model",
            "downloaded_bytes": max(0, after_bytes - before_bytes),
            "cache_bytes": after_bytes,
            "elapsed_s": max(0.0, time.monotonic() - started),
        }
    )
    click.echo(
        json.dumps(
            {
                "status": "ok",
                "stage": "model",
                "workspace": workspace_name,
                "downloaded_bytes": max(0, after_bytes - before_bytes),
                "cache_bytes": after_bytes,
            }
        )
    )


@py_bridge.command("vectorize")
@click.option("--workspace", "workspace_name", required=True, help="Workspace name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.option("--clean-dir", type=click.Path(path_type=Path), help="Clean JSONL directory")
@click.option("--embeddings-dir", type=click.Path(path_type=Path), help="Embeddings output directory")
@click.option("--resume/--no-resume", default=True, help="Resume from checkpoints")
@click.option("--ingest-batch-size", type=int, help="Emails per batch for vectorization")
@click.option("--embedding-batch-size", type=int, help="Embedding model batch size")
@click.option("--chunk-size", type=int, help="Max characters per chunk")
@click.option("--chunk-overlap", type=int, help="Chunk overlap in characters")
@click.option("--checkpoint-interval", type=int, help="Checkpoint interval in seconds")
def py_vectorize(
    workspace_name: str,
    base_dir: Path | None,
    cache_dir: Path | None,
    clean_dir: Path | None,
    embeddings_dir: Path | None,
    resume: bool,
    ingest_batch_size: int | None,
    embedding_batch_size: int | None,
    chunk_size: int | None,
    chunk_overlap: int | None,
    checkpoint_interval: int | None,
) -> None:
    """Contract command for Rust -> Python vectorize stage."""
    ws = get_workspace(workspace_name, base_dir=base_dir)
    ws.ensure()
    ws.apply_env(cache_dir=cache_dir, base_dir=base_dir)

    clean_root = clean_dir or ws.clean_dir
    clean_files = _collect_clean_files(clean_root)
    if not clean_files:
        raise click.ClickException(f"No clean JSONL files found in {clean_root}.")

    embeddings_root = embeddings_dir or ws.embeddings_dir
    from ragmail.vectorize.run import vectorize_files

    def _progress(payload: dict) -> None:
        _bridge_emit({"event": "progress", "stage": "vectorize", **payload})

    _bridge_emit(
        {
            "event": "progress",
            "stage": "vectorize",
            "processed": 0,
            "startup_text": "initializing vectorization",
        }
    )
    processed = vectorize_files(
        clean_files,
        output_dir=embeddings_root,
        checkpoint_dir=ws.checkpoints_dir / "vectorize",
        errors_path=ws.logs_dir / "vectorize.errors.jsonl",
        resume=resume,
        progress_callback=_progress,
        quiet=True,
        vectorize_batch_size=ingest_batch_size,
        embedding_batch_size=embedding_batch_size,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        checkpoint_interval=checkpoint_interval,
    )

    click.echo(
        json.dumps(
            {
                "status": "ok",
                "stage": "vectorize",
                "workspace": workspace_name,
                "clean_dir": str(clean_root),
                "embeddings_dir": str(embeddings_root),
                "files": len(clean_files),
                "processed": processed,
                "resume": resume,
            }
        )
    )


@py_bridge.command("ingest")
@click.option("--workspace", "workspace_name", required=True, help="Workspace name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--cache-dir", type=click.Path(path_type=Path), help="Shared cache directory")
@click.option("--clean-dir", type=click.Path(path_type=Path), help="Clean JSONL directory")
@click.option("--embeddings-dir", type=click.Path(path_type=Path), help="Embeddings directory")
@click.option("--db-path", type=click.Path(path_type=Path), help="LanceDB path")
@click.option("--resume/--no-resume", default=True, help="Resume from checkpoints")
@click.option(
    "--skip-exists-check",
    type=click.Choice(["auto", "true", "false"]),
    default="auto",
    show_default=True,
    help="Email existence check mode",
)
@click.option("--ingest-batch-size", type=int, help="Emails per write batch")
@click.option("--embedding-batch-size", type=int, help="Embedding model batch size")
@click.option("--chunk-size", type=int, help="Max characters per body chunk")
@click.option("--chunk-overlap", type=int, help="Chunk overlap in characters")
@click.option("--checkpoint-interval", type=int, help="Checkpoint interval in seconds")
@click.option("--compact-every", type=int, help="Compaction interval in emails")
@click.option(
    "--repair-embeddings/--no-repair-embeddings",
    default=True,
    help="Repair missing embedding stores by generating them before ingest",
)
@click.option(
    "--bulk-import/--no-bulk-import",
    default=False,
    help="Enable bulk import mode for fresh DB loads",
)
def py_ingest(
    workspace_name: str,
    base_dir: Path | None,
    cache_dir: Path | None,
    clean_dir: Path | None,
    embeddings_dir: Path | None,
    db_path: Path | None,
    resume: bool,
    skip_exists_check: str,
    ingest_batch_size: int | None,
    embedding_batch_size: int | None,
    chunk_size: int | None,
    chunk_overlap: int | None,
    checkpoint_interval: int | None,
    compact_every: int | None,
    repair_embeddings: bool,
    bulk_import: bool,
) -> None:
    """Contract command for Rust -> Python ingest stage."""
    ws = get_workspace(workspace_name, base_dir=base_dir)
    ws.ensure()
    ws.apply_env(cache_dir=cache_dir, base_dir=base_dir)

    clean_root = clean_dir or ws.clean_dir
    clean_files = _collect_clean_files(clean_root)
    if not clean_files:
        raise click.ClickException(f"No clean JSONL files found in {clean_root}.")

    embeddings_root = embeddings_dir or ws.embeddings_dir
    if not embeddings_root.exists():
        raise click.ClickException(f"Embeddings directory not found: {embeddings_root}")

    from ragmail.ingest.run import ingest_files_from_embeddings
    from ragmail.vectorize.store import default_embedding_path

    if not repair_embeddings:
        missing = [
            default_embedding_path(path, embeddings_root)
            for path in clean_files
            if not default_embedding_path(path, embeddings_root).exists()
        ]
        if missing:
            preview = ", ".join(str(path) for path in missing[:3])
            if len(missing) > 3:
                preview += " ..."
            raise click.ClickException(f"Embeddings DBs missing for: {preview}")

    skip_exists_value: bool | None
    if skip_exists_check == "auto":
        skip_exists_value = None
    else:
        skip_exists_value = skip_exists_check == "true"

    db_target = db_path or (ws.db_dir / "email_search.lancedb")

    def _progress(payload: dict) -> None:
        _bridge_emit({"event": "progress", "stage": "ingest", **payload})

    def _compaction(payload: dict) -> None:
        _bridge_emit({"event": "compaction", "stage": "ingest", **payload})

    _bridge_emit(
        {
            "event": "progress",
            "stage": "ingest",
            "processed": 0,
            "startup_text": "initializing ingest",
        }
    )
    processed = ingest_files_from_embeddings(
        clean_files,
        embeddings_dir=embeddings_root,
        db_path=db_target,
        checkpoint_dir=ws.checkpoints_dir,
        errors_path=ws.logs_dir / "ingest.errors.jsonl",
        resume=resume,
        progress_callback=_progress,
        quiet=True,
        skip_exists_check=skip_exists_value,
        ingest_batch_size=ingest_batch_size,
        embedding_batch_size=embedding_batch_size,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        checkpoint_interval=checkpoint_interval,
        compact_every=compact_every,
        compaction_callback=_compaction,
        bulk_import=bulk_import,
        repair_missing_embeddings=repair_embeddings,
    )
    click.echo(
        json.dumps(
            {
                "status": "ok",
                "stage": "ingest",
                "workspace": workspace_name,
                "clean_dir": str(clean_root),
                "embeddings_dir": str(embeddings_root),
                "db_path": str(db_target),
                "files": len(clean_files),
                "processed": processed,
                "resume": resume,
                "repair_embeddings": repair_embeddings,
                "bulk_import": bulk_import,
            }
        )
    )


@cli.command("message")
@click.option("--workspace", "workspace_name", required=True, help="Workspace name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
@click.option("--email-id", "email_id", help="email_id from LanceDB/index")
@click.option("--message-id", "message_id", help="Message-ID header value")
@click.option("--output", type=click.Path(path_type=Path), help="Write raw message to a file")
def message(
    workspace_name: str,
    base_dir: Path | None,
    email_id: str | None,
    message_id: str | None,
    output: Path | None,
) -> None:
    """Dump the full raw message (including multipart attachments) using the MBOX index."""
    if bool(email_id) == bool(message_id):
        raise click.ClickException("Provide exactly one of --email-id or --message-id.")

    ws = get_workspace(workspace_name, base_dir=base_dir)
    ws.ensure()
    index_path = ws.split_dir / "mbox_index.jsonl"
    if not index_path.exists():
        raise click.ClickException(
            f"Index not found: {index_path}. Run `ragmail pipeline --stages preprocess --workspace {workspace_name}`."
        )

    raw_bytes, record, mbox_path = read_message_bytes(
        split_dir=ws.split_dir,
        index_path=index_path,
        message_id=message_id,
        email_id=email_id,
    )

    if output:
        output.write_bytes(raw_bytes)
        click.echo(f"Wrote {len(raw_bytes):,} bytes to {output}")
        return

    sys.stdout.buffer.write(raw_bytes)


@cli.group()
def workspace() -> None:
    """Manage ragmail workspaces."""


@workspace.command("init")
@click.argument("name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
def workspace_init(name: str, base_dir: Path | None) -> None:
    ws = get_workspace(name, base_dir=base_dir)
    ws.ensure()
    click.echo(f"Workspace created: {ws.root}")
    click.echo(f"Config: {ws.config_path}")


@workspace.command("info")
@click.argument("name")
@click.option("--base-dir", type=click.Path(path_type=Path), help="Workspace base directory")
def workspace_info(name: str, base_dir: Path | None) -> None:
    ws = get_workspace(name, base_dir=base_dir)
    if not ws.config_path.exists():
        raise click.ClickException(f"Workspace not found: {ws.root}")
    click.echo(f"Workspace: {ws.root}")
    click.echo(f"Inputs:    {ws.inputs_dir}")
    click.echo(f"Split:     {ws.split_dir}")
    click.echo(f"Clean:     {ws.clean_dir}")
    click.echo(f"Spam:      {ws.spam_dir}")
    click.echo(f"Embeddings: {ws.embeddings_dir}")
    click.echo(f"DB:        {ws.db_dir}")
    click.echo(f"Logs:      {ws.logs_dir}")
    click.echo(f"Reports:   {ws.reports_dir}")
    click.echo(f"State:     {ws.state_path}")
    click.echo(f"Config:    {ws.config_path}")


cli.add_command(pipeline)
cli.add_command(workspace)


@cli.group()
def ignore() -> None:
    """Manage ignore-list rules."""


@ignore.command("init")
@click.argument("path", type=click.Path(path_type=Path))
@click.option("--force", is_flag=True, help="Overwrite existing file")
def ignore_init(path: Path, force: bool) -> None:
    if path.exists() and not force:
        raise click.ClickException(f"File already exists: {path}")
    write_ignore_list_template(path)
    click.echo(f"Ignore list template written to: {path}")


@ignore.command("apply")
@click.argument("input_jsonl", type=click.Path(exists=True, path_type=Path))
@click.option("--ignore-list", "ignore_list_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--output", type=click.Path(path_type=Path), help="Filtered output JSONL")
@click.option("--ignored", type=click.Path(path_type=Path), help="Ignored output JSONL")
def ignore_apply(
    input_jsonl: Path,
    ignore_list_path: Path,
    output: Path | None,
    ignored: Path | None,
) -> None:
    ignore_list = load_ignore_list(ignore_list_path)
    if not ignore_list.rules:
        raise click.ClickException("Ignore list has no valid rules")

    output_path = output or input_jsonl.with_suffix(".filtered.jsonl")
    ignored_path = ignored or input_jsonl.with_suffix(".ignored.jsonl")

    stats = apply_ignore_list_stream(
        input_jsonl,
        ignore_list,
        output_path=output_path,
        ignored_path=ignored_path,
    )

    click.echo(
        f"Filtered {stats['total']} records: "
        f"{stats['kept']} kept, {stats['ignored']} ignored"
    )
    click.echo(f"Output:  {output_path}")
    click.echo(f"Ignored: {ignored_path}")


cli.add_command(ignore)
cli.add_command(py_bridge)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()

"""Unified CLI wrapper for ragmail."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import click

from ragmail.ignorelist import (
    apply_ignore_list_stream,
    load_ignore_list,
    write_ignore_list_template,
)
from ragmail.pipeline import run_pipeline
from ragmail.mbox_index import read_message_bytes
from ragmail.workspace import get_workspace

REPO_ROOT = Path(__file__).resolve().parents[1]


def _ensure_path(path: Path) -> None:
    if not path.exists():
        raise click.ClickException(
            f"Expected path not found: {path}. "
            "Run ragmail from the repo root with the expected layout."
        )


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
@click.version_option(version="0.1.0")
def cli() -> None:
    """ragmail - unified CLI for email cleaning, ingestion, and search."""


def _passthrough_command(help_text: str):
    return click.command(
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        add_help_option=False,
        help=help_text,
    )


@_passthrough_command("Search the database (pass-through to email-search search).")
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
    _run_module("ragmail.search_cli", ["search", *ctx.args])


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
    help="Comma-separated stages to run (download,split,index,clean,vectorize,ingest). Default: all",
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
    compact_every: int | None,
    no_repair_embeddings: bool,
    embeddings_dir: Path | None,
    stages: str | None,
    skip_exists_check: bool,
    traceback: bool,
) -> None:
    """Run split -> index -> clean -> ingest pipeline in a workspace."""
    repair_embeddings = not no_repair_embeddings
    stage_set: set[str] | None = None
    if stages:
        parts = [part.strip().lower() for part in stages.split(",") if part.strip()]
        if not parts:
            raise click.ClickException("Stages list is empty.")
        valid = {"download", "split", "index", "clean", "vectorize", "ingest"}
        invalid = [part for part in parts if part not in valid]
        if invalid:
            raise click.ClickException(
                f"Unknown stages: {', '.join(invalid)}. "
                "Use download,split,index,clean,vectorize,ingest."
            )
        stage_set = set(parts)

    if clean_dir and (stage_set is None or "split" in stage_set or "clean" in stage_set):
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
            compact_every=compact_every,
            repair_embeddings=repair_embeddings,
            embeddings_dir=embeddings_dir,
            stages=stage_set,
            skip_exists_check=skip_exists_check if skip_exists_check else None,
        )
    except click.ClickException:
        raise
    except Exception as exc:
        if traceback:
            raise
        message = str(exc).strip() or "Pipeline failed."
        raise click.ClickException(f"{message} (rerun with --traceback for details)") from None


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
            f"Index not found: {index_path}. Run `ragmail pipeline --stages index --workspace {workspace_name}`."
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


def main() -> None:
    cli()


if __name__ == "__main__":
    main()

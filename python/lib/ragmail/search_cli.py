"""Command-line interface for email-search."""

import os
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import get_settings
from .embedding import create_embedding_provider
from .llm import create_llm_backend
from .search import SearchEngine
from .storage import Database, EmailRepository

console = Console()
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


@click.group()
@click.version_option(version=__version__)
def cli():
    """Email Search - Ingest cleaned JSONL and query with natural language."""
    pass



def _run_query_command(
    query: str,
    db: Optional[Path],
    limit: int,
    rag: bool,
    plan: Optional[bool],
) -> None:
    """Execute a natural-language email query."""
    settings = get_settings()
    db_path = db or settings.db_path

    if not db_path.exists():
        console.print("[red]Error:[/red] Database not found. Run 'ingest' first.")
        raise SystemExit(1)

    if not console.is_terminal:
        console.print("[dim]Loading...[/dim]")

    def _load_engine() -> SearchEngine:
        database = Database(db_path)
        embedding_provider = create_embedding_provider(
            settings.embedding_provider,
            model_name=settings.embedding_model,
            model_revision=settings.embedding_model_revision,
        )
        repository = EmailRepository(database, dimension=embedding_provider.dimension)

        use_llm_planner = plan if plan is not None else rag

        # Create LLM backend if RAG or planner is requested
        llm_backend = None
        if rag or use_llm_planner:
            try:
                llm_backend = create_llm_backend(
                    model=settings.openai_model,
                    api_key=settings.openai_api_key,
                    base_url=settings.openai_base_url,
                )
            except Exception as exc:
                if rag:
                    console.print(
                        "[red]Error:[/red] Could not initialize LLM backend. "
                        "Set OpenAI-compatible env vars or rerun with [bold]--no-rag[/bold]."
                    )
                else:
                    console.print(
                        "[red]Error:[/red] Could not initialize LLM backend for query planner."
                    )
                raise SystemExit(1) from exc

        return SearchEngine(
            repository,
            embedding_provider,
            llm_backend=llm_backend,
            use_llm_planner=use_llm_planner,
        )

    if console.is_terminal:
        with console.status("Loading..."):
            engine = _load_engine()
    else:
        engine = _load_engine()

    if rag and engine.llm_backend:
        with console.status("[bold green]Generating answer..."):
            response = engine.search_with_rag(query, limit=limit)
    else:
        response = engine.search(query, limit=limit)

    if response.aggregations:
        if "top_senders" in response.aggregations:
            table = Table(title=f"Top Contacts ({response.query.year or 'All Time'})")
            table.add_column("Rank", style="dim")
            table.add_column("Email Address")
            table.add_column("Count", justify="right")

            for i, sender in enumerate(response.aggregations["top_senders"], 1):
                table.add_row(
                    str(i),
                    sender["address"],
                    str(sender["count"]),
                )
            console.print(table)
            return

        if "count" in response.aggregations:
            console.print(
                f"[bold]Count:[/bold] {response.aggregations['count']} emails"
            )
            return

    if not response.results:
        console.print("[yellow]No results found.[/yellow]")
        return

    table = Table(title=f"Query Results for: {query}")
    table.add_column("Date", style="dim", width=10)
    table.add_column("From", width=25)
    table.add_column("Subject", width=40)
    table.add_column("Score", justify="right", width=6)

    for result in response.results:
        date_str = result.date[:10] if result.date else "N/A"
        from_display = result.from_name or result.from_address
        table.add_row(
            date_str,
            from_display[:25],
            result.subject[:40],
            f"{result.score:.3f}",
        )

    console.print(table)
    console.print(f"\n[dim]Found {response.total_found} results[/dim]")

    # Display RAG answer if available
    if response.rag_answer:
        console.print("\n[bold green]Answer:[/bold green]")
        console.print(response.rag_answer)


@cli.command("query")
@click.argument("query")
@click.option(
    "--db", type=click.Path(exists=True, path_type=Path), help="Database path"
)
@click.option("--limit", type=int, default=10, help="Number of results")
@click.option(
    "--rag/--no-rag",
    default=True,
    help="Enable/disable RAG answer generation (default: enabled)",
)
@click.option("--plan/--no-plan", default=None, help="Use LLM query planner")
def query_command(
    query: str,
    db: Optional[Path],
    limit: int,
    rag: bool,
    plan: Optional[bool],
):
    """Query emails with natural language."""
    _run_query_command(query=query, db=db, limit=limit, rag=rag, plan=plan)


@cli.command("search", hidden=True)
@click.argument("query")
@click.option(
    "--db", type=click.Path(exists=True, path_type=Path), help="Database path"
)
@click.option("--limit", type=int, default=10, help="Number of results")
@click.option(
    "--rag/--no-rag",
    default=True,
    help="Enable/disable RAG answer generation (default: enabled)",
)
@click.option("--plan/--no-plan", default=None, help="Use LLM query planner")
def search_compat_command(
    query: str,
    db: Optional[Path],
    limit: int,
    rag: bool,
    plan: Optional[bool],
):
    """Backward-compatible alias for `query`."""
    _run_query_command(query=query, db=db, limit=limit, rag=rag, plan=plan)


@cli.command()
@click.option(
    "--db", type=click.Path(exists=True, path_type=Path), help="Database path"
)
@click.option("--dupes", is_flag=True, help="Report duplicate email/chunk IDs")
def stats(db: Optional[Path], dupes: bool):
    """Show database statistics."""
    settings = get_settings()
    db_path = db or settings.db_path

    if not db_path.exists():
        console.print("[red]Error:[/red] Database not found. Run 'ingest' first.")
        raise SystemExit(1)

    database = Database(db_path)
    repository = EmailRepository(database, dimension=settings.embedding_dimension)

    total = repository.count()
    by_year = repository.get_email_count_by_year()
    top_senders = repository.get_top_senders(limit=10)

    console.print(f"\n[bold]Total Emails:[/bold] {total:,}")

    if by_year:
        table = Table(title="Emails by Year")
        table.add_column("Year")
        table.add_column("Count", justify="right")
        for year, count in sorted(by_year.items()):
            table.add_row(str(year), f"{count:,}")
        console.print(table)

    if top_senders:
        table = Table(title="Top Senders")
        table.add_column("Rank", style="dim")
        table.add_column("Email Address")
        table.add_column("Count", justify="right")
        for i, (addr, count) in enumerate(top_senders, 1):
            table.add_row(str(i), addr, f"{count:,}")
        console.print(table)

    if dupes:
        _print_duplicate_stats(repository)


@cli.command()
@click.option("--db", type=click.Path(exists=True, path_type=Path), help="Database path")
@click.option(
    "--table",
    "table_choice",
    type=click.Choice(["emails", "chunks", "both"]),
    default="both",
    show_default=True,
    help="Which tables to dedupe",
)
@click.option("--dry-run", is_flag=True, help="Show duplicates without modifying the DB")
def dedupe(db: Optional[Path], table_choice: str, dry_run: bool):
    """Remove duplicate rows by ID (email_id, chunk_id)."""
    settings = get_settings()
    db_path = db or settings.db_path

    if not db_path.exists():
        console.print("[red]Error:[/red] Database not found. Run 'ingest' first.")
        raise SystemExit(1)

    database = Database(db_path)
    repository = EmailRepository(database, dimension=settings.embedding_dimension)

    if table_choice in ("emails", "both"):
        if not database.table_exists(repository.TABLE_NAME):
            console.print("[yellow]Emails table not found; skipping.[/yellow]")
        else:
            _dedupe_table(
                repository.table,
                id_column="email_id",
                label="emails",
                dry_run=dry_run,
            )
            if not dry_run:
                repository.create_fts_index(force=True)

    if table_choice in ("chunks", "both"):
        if not database.table_exists(repository.CHUNKS_TABLE_NAME):
            console.print("[yellow]Chunks table not found; skipping.[/yellow]")
        else:
            _dedupe_table(
                repository.chunks_table,
                id_column="chunk_id",
                label="chunks",
                dry_run=dry_run,
            )


def _table_to_arrow(table):
    if hasattr(table, "to_arrow"):
        return table.to_arrow()
    if hasattr(table, "to_pyarrow"):
        return table.to_pyarrow()
    if hasattr(table, "to_list"):
        import pyarrow as pa
        return pa.Table.from_pylist(table.to_list())
    raise click.ClickException("LanceDB table does not support Arrow export.")


def _compute_duplicate_stats(arrow_table, id_column: str) -> dict:
    import pyarrow.compute as pc

    total = arrow_table.num_rows
    if total == 0:
        return {
            "total": 0,
            "unique": 0,
            "duplicate_keys": 0,
            "duplicate_rows": 0,
            "top": [],
        }

    ids = arrow_table.column(id_column)
    counts = pc.value_counts(ids).to_pylist()
    unique = len(counts)
    duplicate_keys = 0
    duplicate_rows = 0
    for item in counts:
        count = item["counts"]
        if count > 1:
            duplicate_keys += 1
            duplicate_rows += (count - 1)

    top = sorted(
        (item for item in counts if item["counts"] > 1),
        key=lambda item: item["counts"],
        reverse=True,
    )[:5]

    return {
        "total": total,
        "unique": unique,
        "duplicate_keys": duplicate_keys,
        "duplicate_rows": duplicate_rows,
        "top": top,
    }


def _print_duplicate_stats(repository: EmailRepository) -> None:
    console.print("\n[bold]Duplicate Stats[/bold]")
    database = repository.database

    if database.table_exists(repository.TABLE_NAME):
        emails = _table_to_arrow(repository.table)
        stats = _compute_duplicate_stats(emails, "email_id")
        console.print(
            f"Emails: {stats['duplicate_keys']:,} duplicate IDs, "
            f"{stats['duplicate_rows']:,} extra rows "
            f"(total {stats['total']:,})"
        )
        _print_duplicate_top(stats, "email_id")
    else:
        console.print("Emails: table not found")

    if database.table_exists(repository.CHUNKS_TABLE_NAME):
        chunks = _table_to_arrow(repository.chunks_table)
        stats = _compute_duplicate_stats(chunks, "chunk_id")
        console.print(
            f"Chunks: {stats['duplicate_keys']:,} duplicate IDs, "
            f"{stats['duplicate_rows']:,} extra rows "
            f"(total {stats['total']:,})"
        )
        _print_duplicate_top(stats, "chunk_id")
    else:
        console.print("Chunks: table not found")


def _print_duplicate_top(stats: dict, id_column: str) -> None:
    if not stats["top"]:
        return
    table = Table(title=f"Top Duplicates ({id_column})")
    table.add_column(id_column)
    table.add_column("Count", justify="right")
    for item in stats["top"]:
        table.add_row(str(item["values"]), str(item["counts"]))
    console.print(table)


def _dedupe_table(table, id_column: str, label: str, dry_run: bool) -> None:
    import pyarrow as pa

    arrow_table = _table_to_arrow(table)
    stats = _compute_duplicate_stats(arrow_table, id_column)
    console.print(
        f"[bold]{label.capitalize()}[/bold]: "
        f"{stats['duplicate_keys']:,} duplicate IDs, "
        f"{stats['duplicate_rows']:,} extra rows "
        f"(total {stats['total']:,})"
    )
    _print_duplicate_top(stats, id_column)

    if dry_run or stats["duplicate_rows"] == 0:
        return

    ids = arrow_table.column(id_column).to_pylist()
    seen: set[str] = set()
    keep_mask: list[bool] = []
    for value in ids:
        if value in seen:
            keep_mask.append(False)
        else:
            seen.add(value)
            keep_mask.append(True)

    deduped = arrow_table.filter(pa.array(keep_mask))
    table.add(deduped, mode="overwrite")
    console.print(
        f"[green]✓[/green] {label.capitalize()} deduped: "
        f"{deduped.num_rows:,} rows remaining"
    )


@cli.command()
@click.option("--db", type=click.Path(path_type=Path), help="Database path")
@click.option("--host", default="0.0.0.0", help="Host to bind")
@click.option("--port", default=8000, help="Port to bind")
def serve(db: Optional[Path], host: str, port: int):
    """Start the REST API server."""
    import uvicorn

    settings = get_settings()
    if db:
        import os

        os.environ["EMAIL_SEARCH_DB_PATH"] = str(db)

    console.print(f"[bold]Starting server on {host}:{port}[/bold]")
    uvicorn.run(
        "ragmail.api:create_app",
        host=host,
        port=port,
        factory=True,
    )


if __name__ == "__main__":
    cli()

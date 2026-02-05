#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import lancedb

FTS_COLUMNS_EMAILS = (
    "subject",
    "body_plain",
    "from_name",
    "from_address",
    "to_addresses_str",
    "cc_addresses_str",
    "labels_str",
)

FTS_COLUMNS_CHUNKS = (
    "subject",
    "chunk_text",
    "from_name",
    "from_address",
    "to_addresses_str",
    "cc_addresses_str",
    "labels_str",
)

AMOUNT_RE = re.compile(
    r"(?P<currency>\$|USD\s*|CAD\s*)?\s*(?P<amount>[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)"
)


@dataclass
class Filters:
    where: str | None
    start: datetime | None
    end: datetime | None
    email_id: str | None
    from_address: str | None
    from_like: str | None
    to_like: str | None
    subject_like: str | None
    labels_like: str | None


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _parse_date(value: str, is_end: bool) -> datetime:
    # Accept YYYY-MM-DD or full ISO datetime
    if len(value) == 10:
        if is_end:
            return datetime.fromisoformat(value + "T23:59:59")
        return datetime.fromisoformat(value + "T00:00:00")
    return datetime.fromisoformat(value)


def _resolve_db_path(db: str | None, workspace: str | None) -> Path:
    if db:
        return Path(db)
    if workspace:
        ws_path = Path(workspace)
        if ws_path.is_dir() and (ws_path / "workspace.json").exists():
            root = ws_path
        else:
            root = Path("workspaces") / workspace
        ws_json = root / "workspace.json"
        if ws_json.exists():
            data = json.loads(ws_json.read_text())
            db_rel = data.get("paths", {}).get("db", "db")
            return root / db_rel / "email_search.lancedb"
        return root / "db" / "email_search.lancedb"
    default = Path("workspaces") / "2026" / "db" / "email_search.lancedb"
    if default.exists():
        return default
    raise SystemExit("Provide --workspace or --db (could not find default workspaces/2026)")


def _ensure_fts_index(table, table_name: str) -> None:
    try:
        indices = table.list_indices()
    except Exception:
        indices = []
    has_fts = False
    for idx in indices:
        name = str(getattr(idx, "name", ""))
        index_type = str(getattr(idx, "index_type", ""))
        if "fts" in (name + " " + index_type).lower():
            has_fts = True
            break
    if not has_fts:
        columns = (
            FTS_COLUMNS_CHUNKS
            if table_name == "email_chunks"
            else FTS_COLUMNS_EMAILS
        )
        table.create_fts_index(list(columns), use_tantivy=True, replace=True)


def _build_filters(args: argparse.Namespace) -> Filters:
    filters = []
    start = None
    end = None
    if args.year is not None:
        filters.append(f"year = {int(args.year)}")
    if args.month is not None:
        filters.append(f"month = {int(args.month)}")
    if args.start:
        start = _parse_date(args.start, is_end=False)
        filters.append(f"date >= '{start.isoformat()}'")
    if args.end:
        end = _parse_date(args.end, is_end=True)
        filters.append(f"date <= '{end.isoformat()}'")
    if args.email_id:
        filters.append(f"email_id = '{_escape(args.email_id)}'")
    if args.from_address:
        filters.append(f"from_address = '{_escape(args.from_address)}'")

    where = " AND ".join(filters) if filters else None
    return Filters(
        where=where,
        start=start,
        end=end,
        email_id=args.email_id,
        from_address=args.from_address,
        from_like=args.from_like,
        to_like=args.to_like,
        subject_like=args.subject_like,
        labels_like=args.labels_like,
    )


def _post_filter(rows: Iterable[dict], filters: Filters) -> list[dict]:
    out = []
    for row in rows:
        if filters.from_address:
            if row.get("from_address") != filters.from_address:
                continue
        if filters.from_like:
            needle = filters.from_like.lower()
            hay = f"{row.get('from_name','')} {row.get('from_address','')}".lower()
            if needle not in hay:
                continue
        if filters.to_like:
            needle = filters.to_like.lower()
            hay = f"{row.get('to_addresses_str','')} {row.get('cc_addresses_str','')}".lower()
            if needle not in hay:
                continue
        if filters.subject_like:
            needle = filters.subject_like.lower()
            hay = (row.get("subject") or "").lower()
            if needle not in hay:
                continue
        if filters.labels_like:
            needle = filters.labels_like.lower()
            hay = (row.get("labels_str") or "").lower()
            if needle not in hay:
                continue
        out.append(row)
    return out


def _snippet(text: str, terms: list[str], snip_len: int) -> str:
    if not text:
        return ""
    lower = text.lower()
    pos = -1
    for term in terms:
        if not term:
            continue
        idx = lower.find(term.lower())
        if idx != -1:
            pos = idx
            break
    if pos == -1:
        return " ".join(text.split())[:snip_len]
    start = max(0, pos - snip_len // 2)
    end = min(len(text), start + snip_len)
    return " ".join(text[start:end].split())


def _search_rows(
    table, query: str | None, filters: Filters, limit: int, table_name: str
) -> list[dict]:
    if query:
        _ensure_fts_index(table, table_name)
        search = table.search(query, query_type="fts").limit(limit)
    else:
        search = table.search().limit(limit)
    if filters.where:
        search = search.where(filters.where, prefilter=True)
    rows = search.to_list()
    return _post_filter(rows, filters)


def _print_rows(rows: list[dict], args: argparse.Namespace) -> None:
    terms = []
    if args.query:
        terms.extend([t for t in re.split(r"\s+", args.query) if t])
    if args.from_like:
        terms.append(args.from_like)
    if args.subject_like:
        terms.append(args.subject_like)
    if args.labels_like:
        terms.append(args.labels_like)

    fields = [f.strip() for f in args.fields.split(",") if f.strip()]
    for row in rows:
        output = {}
        for field in fields:
            if field == "snippet":
                text_field = "chunk_text" if args.table == "email_chunks" else "body_plain"
                text = row.get(text_field, "") or ""
                output[field] = _snippet(text, terms, args.snip_len)
            else:
                output[field] = row.get(field)
        if args.format == "json":
            print(json.dumps(output, default=str))
        else:
            parts = [f"{k}={output[k]}" for k in fields]
            print(" | ".join(parts))


def _sum_amounts(rows: list[dict], args: argparse.Namespace) -> None:
    totals = []
    details = []
    for row in rows:
        text_field = "chunk_text" if args.table == "email_chunks" else "body_plain"
        text = row.get(text_field, "") or ""
        for match in AMOUNT_RE.finditer(text):
            amount = match.group("amount").replace(",", "")
            try:
                value = float(amount)
            except ValueError:
                continue
            totals.append(value)
            if len(details) < args.examples:
                details.append(
                    {
                        "value": value,
                        "currency": (match.group("currency") or "").strip(),
                        "date": row.get("date"),
                        "from": row.get("from_address"),
                        "subject": row.get("subject"),
                        "email_id": row.get("email_id"),
                    }
                )

    result = {
        "amount_count": len(totals),
        "amount_sum": sum(totals),
        "amount_min": min(totals) if totals else None,
        "amount_max": max(totals) if totals else None,
        "examples": details,
    }
    if args.format == "json":
        print(json.dumps(result, default=str, indent=2))
    else:
        print(f"amount_count={result['amount_count']}")
        print(f"amount_sum={result['amount_sum']}")
        print(f"amount_min={result['amount_min']}")
        print(f"amount_max={result['amount_max']}")
        if details:
            print("examples:")
            for item in details:
                print(
                    "- "
                    + f"value={item['value']} currency={item['currency']} "
                    + f"date={item['date']} from={item['from']} "
                    + f"subject={item['subject']} email_id={item['email_id']}"
                )


def _connect_table(args: argparse.Namespace):
    db_path = _resolve_db_path(args.db, args.workspace)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    db = lancedb.connect(str(db_path))
    tables = db.list_tables()
    # lancedb list_tables() may return a response object with .tables + .page_token
    tables_list = list(getattr(tables, "tables", tables))
    if args.table not in tables_list:
        raise SystemExit(f"Table '{args.table}' not in {tables}")
    return db.open_table(args.table)


def main() -> int:
    parser = argparse.ArgumentParser(description="Query ragmail LanceDB workspaces")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(sub):
        sub.add_argument("--workspace", help="Workspace name or path")
        sub.add_argument("--db", help="Path to email_search.lancedb")
        sub.add_argument("--table", default="emails", help="emails or email_chunks")
        sub.add_argument("--year", type=int, help="Filter by year (YYYY)")
        sub.add_argument("--month", type=int, help="Filter by month (1-12)")
        sub.add_argument("--start", help="Start date YYYY-MM-DD or ISO datetime")
        sub.add_argument("--end", help="End date YYYY-MM-DD or ISO datetime")
        sub.add_argument("--email-id", dest="email_id", help="Match email_id exactly")
        sub.add_argument("--from-address", dest="from_address", help="Match sender address exactly")
        sub.add_argument("--from-like", dest="from_like", help="Match sender name/address")
        sub.add_argument("--to-like", dest="to_like", help="Match recipient name/address")
        sub.add_argument("--subject-like", dest="subject_like", help="Match subject")
        sub.add_argument("--labels-like", dest="labels_like", help="Match labels")
        sub.add_argument("--format", choices=["text", "json"], default="text")

    search = subparsers.add_parser("search", help="FTS search with filters")
    add_common(search)
    search.add_argument("--query", help="FTS query string")
    search.add_argument("--limit", type=int, default=50)
    search.add_argument(
        "--fields",
        default="date,from_address,from_name,subject,email_id,snippet",
        help="Comma-separated output fields (use 'snippet')",
    )
    search.add_argument("--snip-len", type=int, default=200)

    count = subparsers.add_parser("count", help="Count emails matching filters")
    add_common(count)
    count.add_argument("--query", help="FTS query string")
    count.add_argument("--max-scan", type=int, default=200000)

    sum_cmd = subparsers.add_parser("sum", help="Sum currency amounts in matches")
    add_common(sum_cmd)
    sum_cmd.add_argument("--query", help="FTS query string", required=True)
    sum_cmd.add_argument("--max-scan", type=int, default=200000)
    sum_cmd.add_argument("--examples", type=int, default=5)

    args = parser.parse_args()
    table = _connect_table(args)
    filters = _build_filters(args)

    if args.command == "search":
        rows = _search_rows(table, args.query, filters, args.limit, args.table)
        _print_rows(rows, args)
        return 0

    if args.command == "count":
        rows = _search_rows(table, args.query, filters, args.max_scan, args.table)
        truncated = len(rows) >= args.max_scan
        if args.format == "json":
            print(
                json.dumps(
                    {
                        "count": len(rows),
                        "truncated": truncated,
                        "max_scan": args.max_scan,
                    }
                )
            )
        else:
            print(f"count={len(rows)}")
            if truncated:
                print(f"note=truncated_at_max_scan ({args.max_scan})")
        return 0

    if args.command == "sum":
        rows = _search_rows(table, args.query, filters, args.max_scan, args.table)
        _sum_amounts(rows, args)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())

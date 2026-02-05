#!/usr/bin/env python3
from __future__ import annotations

import argparse
import email
import json
import sys
from email import policy
from pathlib import Path
from typing import Iterable


def parse_email_bytes(raw_bytes: bytes):
    try:
        lines = raw_bytes.split(b"\n", 1)
        if len(lines) > 1 and lines[0].startswith(b"From "):
            msg = email.message_from_bytes(lines[1], policy=policy.compat32)
            return msg
        return email.message_from_bytes(raw_bytes, policy=policy.compat32)
    except Exception:
        return None


def normalize_message_id(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    if v.startswith("<") and v.endswith(">"):
        v = v[1:-1]
    return v.strip()


def safe_filename(name: str, fallback: str) -> str:
    cleaned = name.replace("\\", "_").replace("/", "_").strip()
    return cleaned or fallback


def resolve_workspace_root(workspace: str | None) -> Path | None:
    if not workspace:
        return None
    ws_path = Path(workspace)
    if ws_path.is_dir() and (ws_path / "workspace.json").exists():
        return ws_path
    return Path("workspaces") / workspace


def resolve_split_dir(workspace: str | None) -> Path | None:
    root = resolve_workspace_root(workspace)
    if not root:
        return None
    ws_json = root / "workspace.json"
    if ws_json.exists():
        data = json.loads(ws_json.read_text())
        split_rel = data.get("paths", {}).get("split", "split")
        return root / split_rel
    return root / "split"


def resolve_index_path(workspace: str | None, index_path: str | None) -> Path | None:
    if index_path:
        return Path(index_path)
    if workspace:
        split_dir = resolve_split_dir(workspace)
        if split_dir:
            return split_dir / "mbox_index.jsonl"
    return None


def find_in_index(index_path: Path, message_id: str | None, email_id: str | None) -> dict | None:
    if not index_path.exists():
        return None
    msg_lower = message_id.lower() if message_id else None
    with index_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if email_id and rec.get("email_id") == email_id:
                return rec
            if message_id:
                if rec.get("message_id") == message_id:
                    return rec
                if msg_lower and rec.get("message_id_lower") == msg_lower:
                    return rec
    return None


def iter_attachments(msg) -> Iterable:
    for part in msg.walk():
        if part.is_multipart():
            continue
        filename = part.get_filename()
        disposition = part.get_content_disposition()
        if disposition == "attachment" or filename:
            yield part


def write_attachments(msg, out_dir: Path, overwrite: bool, list_only: bool) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for idx, part in enumerate(iter_attachments(msg), start=1):
        filename = part.get_filename()
        content_type = part.get_content_type()
        payload = part.get_payload(decode=True) or b""
        fallback = f"attachment-{idx}"
        if not filename:
            ext = content_type.split("/")[-1] if content_type else "bin"
            filename = f"{fallback}.{ext}"
        filename = safe_filename(filename, fallback)
        if list_only:
            print(f"{idx}: name={filename} type={content_type} size={len(payload)}")
            count += 1
            continue
        target = out_dir / filename
        if target.exists() and not overwrite:
            stem = target.stem
            suffix = target.suffix
            k = 1
            while True:
                candidate = out_dir / f"{stem}-{k}{suffix}"
                if not candidate.exists():
                    target = candidate
                    break
                k += 1
        target.write_bytes(payload)
        print(f"saved={target} size={len(payload)} type={content_type}")
        count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract attachments from split mbox files")
    parser.add_argument("--workspace", help="Workspace name or path")
    parser.add_argument("--message-id", dest="message_id", help="Message-ID header value")
    parser.add_argument("--email-id", dest="email_id", help="email_id from LanceDB")
    parser.add_argument("--index", help="Path to mbox_index.jsonl")
    parser.add_argument("--out-dir", default="attachments", help="Output directory")
    parser.add_argument("--list-only", action="store_true", help="List attachments without writing")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")

    args = parser.parse_args()

    if not args.message_id and not args.email_id:
        raise SystemExit("Provide --message-id or --email-id")

    if args.email_id and args.message_id:
        raise SystemExit("Provide only one of --message-id or --email-id")

    message_id = normalize_message_id(args.message_id) if args.message_id else None
    email_id = args.email_id

    index_path = resolve_index_path(args.workspace, args.index)
    if not index_path or not index_path.exists():
        raise SystemExit(
            "mbox_index.jsonl not found. Run `ragmail pipeline --stages index --workspace <name>`."
        )

    record = find_in_index(index_path, message_id, email_id)
    if not record:
        raise SystemExit("Message not found in index.")

    mbox_file = record.get("mbox_file")
    if not mbox_file:
        raise SystemExit("Index record missing mbox_file")
    mbox_path = Path(mbox_file)
    if not mbox_path.is_absolute():
        mbox_path = index_path.parent / mbox_file
    if not mbox_path.exists():
        raise SystemExit(f"MBOX not found: {mbox_path}")

    offset = int(record.get("offset", 0))
    length = int(record.get("length", 0))
    if length <= 0:
        raise SystemExit("Index record missing length")

    with open(mbox_path, "rb") as f:
        f.seek(offset)
        raw_bytes = f.read(length)
    msg = parse_email_bytes(raw_bytes)
    if not msg:
        raise SystemExit("Failed to parse message at indexed offset")
    print(f"found via index in {mbox_path} offset={offset} length={length}")
    count = write_attachments(
        msg,
        out_dir=Path(args.out_dir),
        overwrite=args.overwrite,
        list_only=args.list_only,
    )
    print(f"attachment_count={count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

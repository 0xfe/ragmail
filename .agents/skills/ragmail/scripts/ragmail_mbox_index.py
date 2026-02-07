#!/usr/bin/env python3
import sys

print(
    "Deprecated: use `ragmail pipeline --stages preprocess --workspace <name>`.",
    file=sys.stderr,
)
sys.exit(1)
from __future__ import annotations

import argparse
import email
import hashlib
import json
import re
import sys
import time
from email import policy
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path

FROM_LINE_PATTERN = re.compile(
    rb'^From \S+ (Mon|Tue|Wed|Thu|Fri|Sat|Sun) '
    rb'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
    rb'\s*(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{4}|\w+) (\d{4})$'
)


class MboxStreamParser:
    """Stream-based mbox parser for memory-efficient scanning."""

    def __init__(self, filepath: str, start_position: int = 0):
        self.filepath = filepath
        self.file = None
        self.start_position = max(0, int(start_position))
        self.current_position = self.start_position

    def __enter__(self):
        self.file = open(self.filepath, "rb")
        if self.start_position > 0:
            self.file.seek(self.start_position)
            self._sync_to_email_boundary()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def _is_from_line(self, line: bytes) -> bool:
        return line.startswith(b"From ") and FROM_LINE_PATTERN.match(line.rstrip())

    def _sync_to_email_boundary(self):
        while True:
            line = self.file.readline()
            if not line:
                break
            if self._is_from_line(line):
                self.file.seek(-len(line), 1)
                self.current_position = self.file.tell()
                break
            self.current_position = self.file.tell()

    def __iter__(self):
        return self

    def __next__(self):
        if not self.file:
            raise StopIteration

        email_lines = []
        email_start_pos = self.current_position

        while True:
            line = self.file.readline()
            if not line:
                if email_lines:
                    self.current_position = self.file.tell()
                    return email_start_pos, b"".join(email_lines)
                raise StopIteration

            self.current_position = self.file.tell()

            if self._is_from_line(line) and email_lines:
                self.file.seek(-len(line), 1)
                self.current_position = self.file.tell()
                return email_start_pos, b"".join(email_lines)

            email_lines.append(line)


def decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        parts = decode_header(value)
        decoded = []
        for part, encoding in parts:
            if isinstance(part, bytes):
                if encoding:
                    decoded.append(part.decode(encoding, errors="replace"))
                else:
                    decoded.append(part.decode("utf-8", errors="replace"))
            else:
                decoded.append(str(part))
        return " ".join(decoded)
    except Exception:
        return str(value)


def parse_date(value: str | None):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return None


def normalize_message_id(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    if v.startswith("<") and v.endswith(">"):
        v = v[1:-1]
    return v.strip()


def generate_email_id(message_id: str | None, from_address: str, date, subject: str) -> str:
    if message_id:
        return hashlib.sha256(message_id.encode()).hexdigest()[:16]
    components = [
        from_address,
        date.isoformat() if date else "",
        subject[:100],
    ]
    content = "|".join(components)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def parse_message_headers(raw_bytes: bytes):
    try:
        lines = raw_bytes.split(b"\n", 1)
        if len(lines) > 1 and lines[0].startswith(b"From "):
            msg = email.message_from_bytes(lines[1], policy=policy.compat32)
        else:
            msg = email.message_from_bytes(raw_bytes, policy=policy.compat32)
        return msg
    except Exception:
        return None


def resolve_workspace_root(workspace: str | None) -> Path | None:
    if not workspace:
        return None
    ws_path = Path(workspace)
    if ws_path.is_dir() and (ws_path / "workspace.json").exists():
        return ws_path
    return Path("workspaces") / workspace


def resolve_split_dir(workspace: str | None, split_dir: str | None) -> Path:
    if split_dir:
        return Path(split_dir)
    root = resolve_workspace_root(workspace)
    if not root:
        raise SystemExit("Provide --workspace or --split-dir")
    ws_json = root / "workspace.json"
    if ws_json.exists():
        data = json.loads(ws_json.read_text())
        split_rel = data.get("paths", {}).get("split", "split")
        return root / split_rel
    return root / "split"


def default_output_path(split_dir: Path) -> Path:
    return split_dir / "mbox_index.jsonl"


def checkpoint_path_for(output_path: Path) -> Path:
    return output_path.with_suffix(".checkpoint.json")


def load_checkpoint(path: Path):
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return data
    except Exception:
        return None


def save_checkpoint(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))


def iter_mbox_files(split_dir: Path):
    return sorted(split_dir.glob("gmail-*.mbox"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build mbox byte-offset index for attachment extraction")
    parser.add_argument("--workspace", help="Workspace name or path")
    parser.add_argument("--split-dir", help="Override split dir")
    parser.add_argument("--output", help="Output JSONL path (default: split/mbox_index.jsonl)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--checkpoint-every", type=int, default=30, help="Seconds between checkpoints")
    args = parser.parse_args()

    split_dir = resolve_split_dir(args.workspace, args.split_dir)
    if not split_dir.exists():
        raise SystemExit(f"Split dir not found: {split_dir}")

    output_path = Path(args.output) if args.output else default_output_path(split_dir)
    checkpoint_path = checkpoint_path_for(output_path)

    mbox_files = iter_mbox_files(split_dir)
    if not mbox_files:
        raise SystemExit(f"No mbox files found in {split_dir}")

    start_file = None
    start_pos = 0
    total_indexed = 0
    if args.resume:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            start_file = checkpoint.get("mbox_file")
            start_pos = int(checkpoint.get("position", 0))
            total_indexed = int(checkpoint.get("indexed", 0))

    mode = "a" if args.resume and output_path.exists() else "w"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    last_checkpoint = time.time()
    indexed = total_indexed

    with output_path.open(mode) as out_f:
        for mbox_path in mbox_files:
            if start_file and mbox_path.name < start_file:
                continue
            file_start_pos = start_pos if mbox_path.name == start_file else 0

            with MboxStreamParser(str(mbox_path), file_start_pos) as parser:
                for position, raw_bytes in parser:
                    msg = parse_message_headers(raw_bytes)
                    if not msg:
                        continue

                    message_id = normalize_message_id(msg.get("Message-ID"))
                    subject = decode_header_value(msg.get("Subject", ""))
                    from_name, from_address = parseaddr(msg.get("From", "") or "")
                    from_address = (from_address or "").lower()
                    date = parse_date(msg.get("Date"))

                    email_id = generate_email_id(message_id, from_address, date, subject)

                    record = {
                        "email_id": email_id,
                        "message_id": message_id,
                        "message_id_lower": message_id.lower() if message_id else None,
                        "mbox_file": mbox_path.name,
                        "offset": position,
                        "length": len(raw_bytes),
                    }
                    out_f.write(json.dumps(record, default=str) + "\n")
                    indexed += 1

                    now = time.time()
                    if now - last_checkpoint >= args.checkpoint_every:
                        save_checkpoint(
                            checkpoint_path,
                            {
                                "mbox_file": mbox_path.name,
                                "position": parser.current_position,
                                "indexed": indexed,
                                "output": str(output_path),
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            },
                        )
                        last_checkpoint = now

            start_pos = 0
            start_file = None

    if checkpoint_path.exists():
        checkpoint_path.unlink()
    print(f"indexed={indexed} output={output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

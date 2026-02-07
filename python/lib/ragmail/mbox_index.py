"""Read helpers for Rust-generated MBOX byte-offset indexes."""

from __future__ import annotations

import json
from pathlib import Path


def find_in_index(
    index_path: Path,
    *,
    message_id: str | None = None,
    email_id: str | None = None,
) -> dict | None:
    if not index_path.exists():
        return None

    msg_lower = message_id.lower() if message_id else None
    with index_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if email_id and record.get("email_id") == email_id:
                return record
            if message_id:
                if record.get("message_id") == message_id:
                    return record
                if msg_lower and record.get("message_id_lower") == msg_lower:
                    return record
    return None


def read_message_bytes(
    *,
    split_dir: Path,
    index_path: Path,
    message_id: str | None = None,
    email_id: str | None = None,
) -> tuple[bytes, dict, Path]:
    record = find_in_index(index_path, message_id=message_id, email_id=email_id)
    if not record:
        raise FileNotFoundError("Message not found in index.")

    mbox_file = record.get("mbox_file")
    if not mbox_file:
        raise FileNotFoundError("Index record missing mbox_file.")

    mbox_path = Path(mbox_file)
    if not mbox_path.is_absolute():
        mbox_path = split_dir / mbox_file
    if not mbox_path.exists():
        raise FileNotFoundError(f"MBOX not found: {mbox_path}")

    offset = int(record.get("offset", 0))
    length = int(record.get("length", 0))
    if length <= 0:
        raise FileNotFoundError("Index record missing length.")

    with mbox_path.open("rb") as handle:
        handle.seek(offset)
        raw_bytes = handle.read(length)
    return raw_bytes, record, mbox_path

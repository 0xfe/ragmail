"""Build a byte-offset index for split MBOX files."""

from __future__ import annotations

import email
import hashlib
import json
import re
import time
from dataclasses import dataclass
from email import policy
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Callable

FROM_LINE_PATTERN = re.compile(
    rb"^From \S+ (Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
    rb"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
    rb"\s*(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{4}|\w+) (\d{4})$"
)


@dataclass
class IndexStats:
    indexed: int
    output_path: Path


@dataclass
class IndexRecord:
    email_id: str
    message_id: str | None
    message_id_lower: str | None
    mbox_file: str
    offset: int
    length: int


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


def generate_email_id(
    message_id: str | None, from_address: str, date, subject: str
) -> str:
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


def record_from_message(
    *,
    msg,
    mbox_file: str,
    offset: int,
    length: int,
) -> IndexRecord:
    message_id = normalize_message_id(msg.get("Message-ID"))
    subject = decode_header_value(msg.get("Subject", ""))
    _, from_address = parseaddr(msg.get("From", "") or "")
    from_address = (from_address or "").lower()
    date = parse_date(msg.get("Date"))
    email_id = generate_email_id(message_id, from_address, date, subject)
    return IndexRecord(
        email_id=email_id,
        message_id=message_id,
        message_id_lower=message_id.lower() if message_id else None,
        mbox_file=mbox_file,
        offset=offset,
        length=length,
    )


class MboxIndexWriter:
    def __init__(self, output_path: Path, mode: str = "a"):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.output_path.open(mode)
        self.count = 0

    def write_record(self, record: IndexRecord) -> None:
        self._handle.write(json.dumps(record.__dict__, default=str) + "\n")
        self.count += 1

    def write_from_message(
        self,
        *,
        msg,
        mbox_file: str,
        offset: int,
        length: int,
    ) -> None:
        record = record_from_message(
            msg=msg,
            mbox_file=mbox_file,
            offset=offset,
            length=length,
        )
        self.write_record(record)

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()


def _load_checkpoint(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _save_checkpoint(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2))


def build_mbox_index(
    *,
    split_dir: Path,
    output_path: Path,
    checkpoint_path: Path,
    resume: bool = True,
    checkpoint_every: int = 30,
    progress_callback: Callable[[dict], None] | None = None,
) -> IndexStats:
    split_dir = Path(split_dir)
    output_path = Path(output_path)
    checkpoint_path = Path(checkpoint_path)

    mbox_files = sorted(split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox"))
    if not mbox_files:
        raise FileNotFoundError(f"No split MBOX files found in {split_dir}")

    start_file = None
    start_pos = 0
    indexed = 0
    if resume:
        checkpoint = _load_checkpoint(checkpoint_path)
        if checkpoint:
            start_file = checkpoint.get("mbox_file")
            start_pos = int(checkpoint.get("position", 0))
            indexed = int(checkpoint.get("indexed", 0))
    else:
        if output_path.exists():
            output_path.unlink()
        if checkpoint_path.exists():
            checkpoint_path.unlink()

    mode = "a" if resume and output_path.exists() else "w"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    last_checkpoint = time.time()
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

                    record = record_from_message(
                        msg=msg,
                        mbox_file=mbox_path.name,
                        offset=position,
                        length=len(raw_bytes),
                    )
                    out_f.write(json.dumps(record.__dict__, default=str) + "\n")
                    indexed += 1

                    if progress_callback:
                        progress_callback(
                            {
                                "processed": indexed,
                                "mbox_file": mbox_path.name,
                            }
                        )

                    now = time.time()
                    if now - last_checkpoint >= checkpoint_every:
                        _save_checkpoint(
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

    return IndexStats(indexed=indexed, output_path=output_path)


def find_in_index(
    index_path: Path,
    *,
    message_id: str | None = None,
    email_id: str | None = None,
) -> dict | None:
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

    with open(mbox_path, "rb") as handle:
        handle.seek(offset)
        raw_bytes = handle.read(length)
    return raw_bytes, record, mbox_path

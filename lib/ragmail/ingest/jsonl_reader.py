"""Streaming JSONL reader for cleaned email exports."""

import json
from collections.abc import Iterator
from pathlib import Path


class JsonlReader:
    """Memory-efficient streaming reader for JSON Lines files."""

    def __init__(self, path: Path | str):
        """Initialize reader with path to JSONL file.

        Args:
            path: Path to the JSON Lines file
        """
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"JSONL file not found: {self.path}")

    def __iter__(self) -> Iterator[dict]:
        """Iterate over JSON objects in the JSONL file."""
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    yield {
                        "__ragmail_error__": "json_decode",
                        "__line__": line_number,
                        "__error__": str(exc),
                        "__raw__": line,
                    }

    def count(self) -> int:
        """Count total non-empty lines in JSONL file."""
        count = 0
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    count += 1
        return count

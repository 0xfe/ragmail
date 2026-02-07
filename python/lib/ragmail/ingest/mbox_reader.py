"""Streaming MBOX file reader for multi-GB files."""

import mailbox
from collections.abc import Iterator
from email.message import Message
from pathlib import Path


class MboxReader:
    """Memory-efficient streaming reader for MBOX files."""

    def __init__(self, path: Path | str):
        """Initialize reader with path to MBOX file.

        Args:
            path: Path to the MBOX file
        """
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"MBOX file not found: {self.path}")

    def __iter__(self) -> Iterator[Message]:
        """Iterate over messages in the MBOX file.

        Yields:
            Email messages one at a time for memory efficiency
        """
        mbox = mailbox.mbox(str(self.path))
        try:
            for message in mbox:
                yield message
        finally:
            mbox.close()

    def count(self) -> int:
        """Count total messages in MBOX file.

        Note: This requires iterating through the entire file.

        Returns:
            Number of messages in the file
        """
        count = 0
        mbox = mailbox.mbox(str(self.path))
        try:
            for _ in mbox:
                count += 1
        finally:
            mbox.close()
        return count

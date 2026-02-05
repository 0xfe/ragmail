"""Email repository for CRUD operations."""

import shutil
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..ingest.email_parser import ParsedEmail
from .database import Database
from .schema import create_email_chunk_schema, create_email_schema_flat


class EmailRepository:
    """Repository for email storage and retrieval."""

    TABLE_NAME = "emails"
    CHUNKS_TABLE_NAME = "email_chunks"
    FTS_COLUMNS = (
        "subject",
        "body_plain",
        "from_name",
        "from_address",
        "to_addresses_str",
        "cc_addresses_str",
        "labels_str",
    )

    def __init__(self, database: Database, dimension: int = 384):
        """Initialize repository.

        Args:
            database: Database connection
            dimension: Vector dimension for the configured embedding model
        """
        self.database = database
        self.dimension = dimension
        self._table = None
        self._chunks_table = None
        self._fts_ready: bool | None = None

    @property
    def table(self):
        """Get or create the emails table."""
        if self._table is None:
            if self.database.table_exists(self.TABLE_NAME):
                self._table = self.database.db.open_table(self.TABLE_NAME)
            else:
                EmailRecordFlat = create_email_schema_flat(self.dimension)
                self._table = self.database.db.create_table(
                    self.TABLE_NAME,
                    schema=EmailRecordFlat,
                )
        return self._table

    @property
    def chunks_table(self):
        """Get or create the email chunks table."""
        if self._chunks_table is None:
            if self.database.table_exists(self.CHUNKS_TABLE_NAME):
                self._chunks_table = self.database.db.open_table(
                    self.CHUNKS_TABLE_NAME
                )
            else:
                EmailChunkRecord = create_email_chunk_schema(self.dimension)
                self._chunks_table = self.database.db.create_table(
                    self.CHUNKS_TABLE_NAME,
                    schema=EmailChunkRecord,
                )
        return self._chunks_table

    def add(
        self,
        email: ParsedEmail,
        subject_vector: NDArray[np.float32],
        chunk_texts: Sequence[str] | None = None,
        chunk_vectors: NDArray[np.float32] | None = None,
    ) -> None:
        """Add a single email to the database.

        Args:
            email: Parsed email to store
            subject_vector: Subject embedding
            chunk_texts: Optional body chunks
            chunk_vectors: Optional body chunk embeddings
        """
        chunk_texts = list(chunk_texts or [])
        if chunk_vectors is None:
            chunk_vectors = np.array([], dtype=np.float32).reshape(0, self.dimension)

        self.add_batch(
            [email],
            np.array([subject_vector], dtype=np.float32),
            chunk_texts,
            chunk_vectors,
            [0 for _ in chunk_texts],
            list(range(len(chunk_texts))),
        )

    def add_batch(
        self,
        emails: Sequence[ParsedEmail],
        subject_vectors: NDArray[np.float32],
        chunk_texts: Sequence[str] | None = None,
        chunk_vectors: NDArray[np.float32] | None = None,
        chunk_email_indices: Sequence[int] | None = None,
        chunk_indices: Sequence[int] | None = None,
    ) -> None:
        """Add multiple emails to the database.

        Args:
            emails: List of parsed emails
            subject_vectors: Array of subject embeddings
            chunk_texts: Optional list of body chunk texts
            chunk_vectors: Optional array of body chunk embeddings
            chunk_email_indices: Email indices for each chunk
            chunk_indices: Chunk index per chunk
        """
        records = [
            self._to_record(email, subject_vectors[i])
            for i, email in enumerate(emails)
        ]
        self.table.add(records)

        chunk_texts = list(chunk_texts or [])
        if not chunk_texts:
            return
        if chunk_vectors is None:
            raise ValueError("chunk_vectors are required when chunk_texts are provided")
        if len(chunk_texts) != len(chunk_vectors):
            raise ValueError("chunk_texts and chunk_vectors must be the same length")

        if chunk_email_indices is None or chunk_indices is None:
            raise ValueError("chunk_email_indices and chunk_indices are required")
        if len(chunk_email_indices) != len(chunk_texts):
            raise ValueError("chunk_email_indices length must match chunk_texts")
        if len(chunk_indices) != len(chunk_texts):
            raise ValueError("chunk_indices length must match chunk_texts")

        chunk_records = [
            self._to_chunk_record(
                emails[chunk_email_indices[i]],
                chunk_texts[i],
                chunk_indices[i],
                chunk_vectors[i],
            )
            for i in range(len(chunk_texts))
        ]
        if chunk_records:
            self.chunks_table.add(chunk_records)

    def get(self, email_id: str) -> dict[str, Any] | None:
        """Get an email by ID.

        Args:
            email_id: The email's unique identifier

        Returns:
            Email record or None if not found
        """
        results = (
            self.table.search()
            .where(f"email_id = '{email_id}'", prefilter=True)
            .limit(1)
            .to_list()
        )
        return results[0] if results else None

    def exists(self, email_id: str) -> bool:
        """Check if an email exists in the database.

        Args:
            email_id: The email's unique identifier

        Returns:
            True if exists
        """
        return self.get(email_id) is not None

    def count(self, where: str | None = None) -> int:
        """Get total number of emails in the database."""
        if where:
            return self.table.count_rows(filter=where)
        return self.table.count_rows()

    def is_empty(self) -> bool:
        """Return True if the emails table is missing or empty."""
        if not self.database.table_exists(self.TABLE_NAME):
            return True
        try:
            table = self._table or self.database.db.open_table(self.TABLE_NAME)
            return table.count_rows() == 0
        except Exception:
            return False

    def search_vector(
        self,
        query_vector: NDArray[np.float32],
        limit: int = 20,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search emails by subject vector similarity."""
        return self.search_subject_vectors(query_vector, limit, where)

    def search_subject_vectors(
        self,
        query_vector: NDArray[np.float32],
        limit: int = 20,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search emails by subject vector similarity."""
        search = self.table.search(query_vector).limit(limit)
        if where:
            search = search.where(where, prefilter=True)
        return search.to_list()

    def search_body_chunks(
        self,
        query_vector: NDArray[np.float32],
        limit: int = 20,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search body chunks by vector similarity."""
        try:
            if not self.database.table_exists(self.CHUNKS_TABLE_NAME):
                return []
            if self.chunks_table.count_rows() == 0:
                return []
        except Exception:
            return []
        search = self.chunks_table.search(query_vector).limit(limit)
        if where:
            search = search.where(where, prefilter=True)
        return search.to_list()

    def search_fts(
        self,
        query: str,
        limit: int = 20,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        """Full-text search on emails.

        Args:
            query: Search query
            limit: Maximum results
            where: Optional filter

        Returns:
            List of matching records
        """
        self.ensure_fts_index()
        return self._search_fts(query, limit=limit, where=where)

    def get_by_sender(
        self,
        sender: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get emails from a specific sender.

        Args:
            sender: Sender email address
            limit: Maximum results

        Returns:
            List of emails from the sender
        """
        return (
            self.table.search()
            .where(f"from_address = '{sender}'", prefilter=True)
            .limit(limit)
            .to_list()
        )

    def get_by_date_range(
        self,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get emails within a date range.

        Args:
            start: Start date
            end: End date
            limit: Maximum results

        Returns:
            List of emails in the date range
        """
        start_str = start.isoformat()
        end_str = end.isoformat()
        return (
            self.table.search()
            .where(f"date >= '{start_str}' AND date <= '{end_str}'", prefilter=True)
            .limit(limit)
            .to_list()
        )

    def get_top_senders(
        self, year: int | None = None, limit: int = 10
    ) -> list[tuple[str, int]]:
        """Get top email senders by count.

        Args:
            year: Optional year filter
            limit: Number of top senders to return

        Returns:
            List of (sender, count) tuples
        """
        if year:
            results = (
                self.table.search()
                .where(f"year = {year}", prefilter=True)
                .limit(10000)
                .to_list()
            )
        else:
            results = self.table.search().limit(10000).to_list()

        sender_counts: dict[str, int] = {}
        for row in results:
            sender = row.get("from_address", "")
            sender_counts[sender] = sender_counts.get(sender, 0) + 1

        sorted_senders = sorted(
            sender_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return sorted_senders[:limit]

    def get_email_count_by_year(self) -> dict[int, int]:
        """Get email count grouped by year.

        Returns:
            Dictionary mapping year to email count
        """
        results = self.table.search().limit(100000).to_list()
        year_counts: dict[int, int] = {}
        for row in results:
            year = row.get("year")
            if year:
                year_counts[year] = year_counts.get(year, 0) + 1
        return dict(sorted(year_counts.items()))

    def _cleanup_fts_index(self) -> None:
        """Remove any existing corrupted FTS index directories."""
        try:
            # Get the database path from the database object
            table_path = self.database.path / f"{self.TABLE_NAME}.lance"
            fts_index_path = table_path / "_indices" / "fts"
            if fts_index_path.exists():
                shutil.rmtree(fts_index_path, ignore_errors=True)
        except Exception:
            # Ignore cleanup errors - we'll try to create the index anyway
            pass

    def _has_fts_index(self) -> bool:
        """Check if any FTS index exists for the table."""
        try:
            indices = self.table.list_indices()
            for idx in indices:
                if hasattr(idx, "index_type") and "fts" in str(idx.index_type).lower():
                    return True
                if hasattr(idx, "name") and "fts" in str(idx.name).lower():
                    return True
            return False
        except Exception:
            return False

    def _fts_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return "fts" in message and (
            "does not exist" in message
            or "filedoesnotexist" in message
            or "no such file" in message
            or "index not found" in message
        )

    def ensure_fts_index(self) -> None:
        """Ensure a usable FTS index exists; rebuild if corrupted."""
        if self._fts_ready is True:
            return

        if not self._has_fts_index():
            self.create_fts_index(force=True)
            self._fts_ready = True
            return

        try:
            self.table.search("fts", query_type="fts").limit(1).to_list()
            self._fts_ready = True
        except Exception as exc:
            if self._fts_error(exc):
                self.create_fts_index(force=True)
                self._fts_ready = True
            else:
                self._fts_ready = False
                raise

    def _search_fts(
        self,
        query: str,
        limit: int = 20,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        search = self.table.search(query, query_type="fts").limit(limit)
        if where:
            search = search.where(where, prefilter=True)
        return search.to_list()

    def create_fts_index(self, force: bool = False) -> None:
        """Create full-text search index on core text fields."""
        if force:
            self._cleanup_fts_index()

        if force or not self._has_fts_index():
            try:
                self.table.create_fts_index(
                    list(self.FTS_COLUMNS),
                    use_tantivy=True,
                    replace=True,
                )
            except (FileNotFoundError, ValueError) as exc:
                if "already exists" not in str(exc).lower():
                    raise

    def _to_record(
        self,
        email: ParsedEmail,
        subject_vector: NDArray[np.float32],
    ) -> dict[str, Any]:
        """Convert parsed email to database record."""
        attachment_names = [a.filename for a in email.attachments]
        attachment_types = [a.content_type for a in email.attachments]
        return {
            "email_id": email.email_id,
            "message_id": email.message_id,
            "subject": email.subject,
            "from_address": email.from_address,
            "from_name": email.from_name,
            "to_addresses_str": ",".join(email.to_addresses),
            "cc_addresses_str": ",".join(email.cc_addresses),
            "date": email.date,
            "body_plain": email.body_plain[:10000],
            "has_attachment": email.has_attachment,
            "attachment_names": attachment_names,
            "attachment_types": attachment_types,
            "labels_str": ",".join(email.labels),
            "in_reply_to": email.in_reply_to,
            "thread_id": email.thread_id,
            "year": email.date.year if email.date else None,
            "month": email.date.month if email.date else None,
            "mbox_file": email.mbox_file,
            "mbox_offset": email.mbox_offset,
            "mbox_length": email.mbox_length,
            "subject_vector": subject_vector.tolist(),
        }

    def _to_chunk_record(
        self,
        email: ParsedEmail,
        chunk_text: str,
        chunk_index: int,
        body_vector: NDArray[np.float32],
    ) -> dict[str, Any]:
        attachment_names = [a.filename for a in email.attachments]
        attachment_types = [a.content_type for a in email.attachments]
        return {
            "chunk_id": f"{email.email_id}:{chunk_index}",
            "email_id": email.email_id,
            "message_id": email.message_id,
            "thread_id": email.thread_id,
            "chunk_index": chunk_index,
            "chunk_text": chunk_text[:5000],
            "subject": email.subject,
            "from_address": email.from_address,
            "from_name": email.from_name,
            "to_addresses_str": ",".join(email.to_addresses),
            "cc_addresses_str": ",".join(email.cc_addresses),
            "date": email.date,
            "has_attachment": email.has_attachment,
            "attachment_names": attachment_names,
            "attachment_types": attachment_types,
            "labels_str": ",".join(email.labels),
            "in_reply_to": email.in_reply_to,
            "year": email.date.year if email.date else None,
            "month": email.date.month if email.date else None,
            "mbox_file": email.mbox_file,
            "mbox_offset": email.mbox_offset,
            "mbox_length": email.mbox_length,
            "body_vector": body_vector.tolist(),
        }

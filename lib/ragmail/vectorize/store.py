"""Embedding store for precomputed vectors (SQLite)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
from numpy.typing import NDArray


@dataclass(frozen=True)
class EmbeddingMeta:
    embedding_model: str
    embedding_model_revision: str | None
    embedding_dimension: int
    chunk_size: int
    chunk_overlap: int
    created_at: str
    source: str
    format_version: int = 1

    def to_dict(self) -> dict[str, object]:
        return {
            "embedding_model": self.embedding_model,
            "embedding_model_revision": self.embedding_model_revision,
            "embedding_dimension": self.embedding_dimension,
            "chunk_size": self.chunk_size,
            "chunk_overlap": self.chunk_overlap,
            "created_at": self.created_at,
            "source": self.source,
            "format_version": self.format_version,
        }


def default_embedding_path(input_file: Path, output_dir: Path) -> Path:
    """Return default embedding DB path for an input file."""
    name = input_file.name
    if name.endswith(".jsonl"):
        name = name[: -len(".jsonl")]
    elif name.endswith(".json"):
        name = name[: -len(".json")]
    return output_dir / f"{name}.embed.db"


def _vector_to_blob(vector: NDArray[np.float32]) -> bytes:
    return np.asarray(vector, dtype=np.float32).tobytes()


def _blob_to_vector(blob: bytes, dimension: int) -> NDArray[np.float32]:
    return np.frombuffer(blob, dtype=np.float32, count=dimension)


def _chunked(seq: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


class EmbeddingStore:
    """SQLite-backed store for precomputed embeddings."""

    def __init__(self, path: Path):
        self.path = path
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._meta_cache: dict | None = None
        self._setup()

    def close(self) -> None:
        self._conn.close()

    def _setup(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS email_vectors (
                email_id TEXT PRIMARY KEY,
                subject_vector BLOB NOT NULL,
                chunk_count INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chunk_vectors (
                email_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_vector BLOB NOT NULL,
                PRIMARY KEY (email_id, chunk_index)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunk_email ON chunk_vectors(email_id)"
        )
        self._conn.commit()

    def set_meta(self, meta: EmbeddingMeta) -> None:
        payload = json.dumps(meta.to_dict())
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                ("config", payload),
            )
        self._meta_cache = meta.to_dict()

    def get_meta(self) -> dict | None:
        if self._meta_cache is not None:
            return dict(self._meta_cache)
        cur = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            ("config",),
        )
        row = cur.fetchone()
        if not row:
            return None
        payload = json.loads(row["value"])
        self._meta_cache = payload
        return dict(payload)

    def ensure_meta(self, meta: EmbeddingMeta) -> None:
        existing = self.get_meta()
        if not existing:
            self.set_meta(meta)
            return
        mismatch = {}
        for key in [
            "embedding_model",
            "embedding_model_revision",
            "embedding_dimension",
            "chunk_size",
            "chunk_overlap",
            "format_version",
        ]:
            if existing.get(key) != meta.to_dict().get(key):
                mismatch[key] = {"existing": existing.get(key), "new": meta.to_dict().get(key)}
        if mismatch:
            raise ValueError(
                "Embedding store metadata mismatch: "
                + ", ".join(
                    f"{key} (existing={value['existing']} new={value['new']})"
                    for key, value in mismatch.items()
                )
            )

    @property
    def dimension(self) -> int:
        meta = self.get_meta()
        if not meta:
            raise ValueError("Embedding store metadata missing.")
        return int(meta["embedding_dimension"])

    @property
    def chunk_size(self) -> int:
        meta = self.get_meta()
        if not meta:
            raise ValueError("Embedding store metadata missing.")
        return int(meta["chunk_size"])

    @property
    def chunk_overlap(self) -> int:
        meta = self.get_meta()
        if not meta:
            raise ValueError("Embedding store metadata missing.")
        return int(meta["chunk_overlap"])

    def add_batch(
        self,
        email_ids: Sequence[str],
        subject_vectors: NDArray[np.float32],
        chunk_counts: Sequence[int],
        chunk_email_indices: Sequence[int],
        chunk_indices: Sequence[int],
        chunk_vectors: NDArray[np.float32] | None,
    ) -> None:
        if len(email_ids) != len(subject_vectors):
            raise ValueError("email_ids and subject_vectors must be the same length")
        if len(email_ids) != len(chunk_counts):
            raise ValueError("email_ids and chunk_counts must be the same length")

        email_rows = [
            (email_ids[i], _vector_to_blob(subject_vectors[i]), int(chunk_counts[i]))
            for i in range(len(email_ids))
        ]

        chunk_rows: list[tuple[str, int, bytes]] = []
        if chunk_vectors is not None and len(chunk_vectors) > 0:
            if len(chunk_email_indices) != len(chunk_vectors):
                raise ValueError("chunk_email_indices must match chunk_vectors length")
            if len(chunk_indices) != len(chunk_vectors):
                raise ValueError("chunk_indices must match chunk_vectors length")
            for i, vector in enumerate(chunk_vectors):
                email_id = email_ids[int(chunk_email_indices[i])]
                chunk_rows.append((email_id, int(chunk_indices[i]), _vector_to_blob(vector)))

        with self._conn:
            self._conn.executemany(
                "INSERT OR REPLACE INTO email_vectors (email_id, subject_vector, chunk_count) "
                "VALUES (?, ?, ?)",
                email_rows,
            )
            if chunk_rows:
                self._conn.executemany(
                    "INSERT OR REPLACE INTO chunk_vectors (email_id, chunk_index, chunk_vector) "
                    "VALUES (?, ?, ?)",
                    chunk_rows,
                )

    def fetch_subject_vectors(
        self, email_ids: Sequence[str], *, batch_size: int = 900
    ) -> dict[str, NDArray[np.float32]]:
        if not email_ids:
            return {}
        result: dict[str, NDArray[np.float32]] = {}
        dimension = self.dimension
        for chunk in _chunked(list(email_ids), batch_size):
            placeholders = ",".join("?" for _ in chunk)
            cur = self._conn.execute(
                f"SELECT email_id, subject_vector FROM email_vectors "
                f"WHERE email_id IN ({placeholders})",
                list(chunk),
            )
            for row in cur.fetchall():
                result[row["email_id"]] = _blob_to_vector(row["subject_vector"], dimension)
        return result

    def fetch_chunk_vectors(
        self, email_ids: Sequence[str], *, batch_size: int = 900
    ) -> dict[str, list[NDArray[np.float32]]]:
        if not email_ids:
            return {}
        result: dict[str, list[NDArray[np.float32]]] = {}
        dimension = self.dimension
        for chunk in _chunked(list(email_ids), batch_size):
            placeholders = ",".join("?" for _ in chunk)
            cur = self._conn.execute(
                f"SELECT email_id, chunk_index, chunk_vector FROM chunk_vectors "
                f"WHERE email_id IN ({placeholders}) "
                f"ORDER BY email_id, chunk_index",
                list(chunk),
            )
            for row in cur.fetchall():
                email_id = row["email_id"]
                vectors = result.setdefault(email_id, [])
                vectors.append(_blob_to_vector(row["chunk_vector"], dimension))
        return result

    def fetch_chunk_counts(
        self, email_ids: Sequence[str], *, batch_size: int = 900
    ) -> dict[str, int]:
        if not email_ids:
            return {}
        result: dict[str, int] = {}
        for chunk in _chunked(list(email_ids), batch_size):
            placeholders = ",".join("?" for _ in chunk)
            cur = self._conn.execute(
                f"SELECT email_id, chunk_count FROM email_vectors "
                f"WHERE email_id IN ({placeholders})",
                list(chunk),
            )
            for row in cur.fetchall():
                result[row["email_id"]] = int(row["chunk_count"])
        return result

    @staticmethod
    def build_meta(
        *,
        embedding_model: str,
        embedding_model_revision: str | None,
        embedding_dimension: int,
        chunk_size: int,
        chunk_overlap: int,
        source: str,
    ) -> EmbeddingMeta:
        return EmbeddingMeta(
            embedding_model=embedding_model,
            embedding_model_revision=embedding_model_revision,
            embedding_dimension=embedding_dimension,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            created_at=datetime.now().isoformat(),
            source=source,
        )

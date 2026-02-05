from __future__ import annotations

from pathlib import Path

import numpy as np

from ragmail.ingest import IngestPipeline
from ragmail.ingest.run import ingest_files_from_embeddings
from ragmail.ingest.text_processing import clean_body_for_embedding, chunk_text
from ragmail.storage import Database, EmailRepository
from ragmail.vectorize.store import EmbeddingStore, default_embedding_path


def _build_embeddings(
    input_file: Path,
    embeddings_dir: Path,
    *,
    dimension: int,
    chunk_size: int,
    chunk_overlap: int,
) -> tuple[list[str], int]:
    embeddings_dir.mkdir(parents=True, exist_ok=True)
    store_path = default_embedding_path(input_file, embeddings_dir)
    store = EmbeddingStore(store_path)
    meta = store.build_meta(
        embedding_model="test-model",
        embedding_model_revision=None,
        embedding_dimension=dimension,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        source=str(input_file),
    )
    store.ensure_meta(meta)

    pipeline = IngestPipeline()
    emails = list(pipeline.ingest(input_file, resume=False, validate=True))
    email_ids = [email.email_id for email in emails]

    subject_vectors = []
    chunk_vectors = []
    chunk_email_indices = []
    chunk_indices = []
    chunk_counts = []
    total_chunks = 0

    for idx, email in enumerate(emails):
        subject_vectors.append(np.full(dimension, idx + 1, dtype=np.float32))
        cleaned_body = clean_body_for_embedding(email.body_plain)
        chunks = chunk_text(cleaned_body, max_chars=chunk_size, overlap=chunk_overlap)
        chunk_counts.append(len(chunks))
        total_chunks += len(chunks)
        for chunk_index, _chunk in enumerate(chunks):
            chunk_vectors.append(
                np.full(dimension, (idx + 1) * 10 + chunk_index, dtype=np.float32)
            )
            chunk_email_indices.append(idx)
            chunk_indices.append(chunk_index)

    subject_vectors_arr = np.stack(subject_vectors, axis=0)
    chunk_vectors_arr = (
        np.stack(chunk_vectors, axis=0) if chunk_vectors else np.zeros((0, dimension))
    )

    store.add_batch(
        email_ids,
        subject_vectors_arr,
        chunk_counts,
        chunk_email_indices,
        chunk_indices,
        chunk_vectors_arr,
    )
    store.close()
    return email_ids, total_chunks


def test_ingest_from_embeddings_roundtrip(tmp_path: Path, sample_jsonl_path: Path) -> None:
    embeddings_dir = tmp_path / "embeddings"
    db_path = tmp_path / "email_search.lancedb"
    checkpoint_dir = tmp_path / "checkpoints"
    dimension = 8
    chunk_size = 40
    chunk_overlap = 5

    email_ids, total_chunks = _build_embeddings(
        sample_jsonl_path,
        embeddings_dir,
        dimension=dimension,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    processed = ingest_files_from_embeddings(
        [sample_jsonl_path],
        embeddings_dir=embeddings_dir,
        db_path=db_path,
        checkpoint_dir=checkpoint_dir,
        resume=False,
        skip_exists_check=True,
        ingest_batch_size=2,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    assert processed == len(email_ids)

    repository = EmailRepository(Database(db_path), dimension=dimension)
    assert repository.count() == len(email_ids)
    if repository.database.table_exists(repository.CHUNKS_TABLE_NAME):
        assert repository.chunks_table.count_rows() == total_chunks

    if hasattr(repository.table, "to_arrow"):
        arrow = repository.table.to_arrow()
    elif hasattr(repository.table, "to_pyarrow"):
        arrow = repository.table.to_pyarrow()
    else:
        raise AssertionError("LanceTable does not support Arrow export")
    rows = arrow.to_pylist()
    stored = {row["email_id"]: row["subject_vector"] for row in rows}
    for idx, email_id in enumerate(email_ids):
        expected = np.full(dimension, idx + 1, dtype=np.float32)
        np.testing.assert_allclose(stored[email_id], expected)


def test_ingest_repairs_missing_embeddings(tmp_path: Path, sample_jsonl_path: Path) -> None:
    embeddings_dir = tmp_path / "embeddings"
    db_path = tmp_path / "email_search.lancedb"
    checkpoint_dir = tmp_path / "checkpoints"
    dimension = 8
    chunk_size = 40
    chunk_overlap = 5

    email_ids, _total_chunks = _build_embeddings(
        sample_jsonl_path,
        embeddings_dir,
        dimension=dimension,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    store_path = default_embedding_path(sample_jsonl_path, embeddings_dir)
    store = EmbeddingStore(store_path)
    with store._conn:
        store._conn.execute(
            "DELETE FROM email_vectors WHERE email_id = ?",
            (email_ids[0],),
        )
        store._conn.execute(
            "DELETE FROM chunk_vectors WHERE email_id = ?",
            (email_ids[0],),
        )
    store.close()

    class DummyEmbeddingProvider:
        def __init__(self, dimension: int) -> None:
            self.dimension = dimension

        def encode(self, texts, batch_size=None, show_progress=False):
            return np.stack(
                [np.full(self.dimension, idx + 7, dtype=np.float32) for idx in range(len(texts))],
                axis=0,
            )

    processed = ingest_files_from_embeddings(
        [sample_jsonl_path],
        embeddings_dir=embeddings_dir,
        db_path=db_path,
        checkpoint_dir=checkpoint_dir,
        resume=False,
        skip_exists_check=True,
        ingest_batch_size=2,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        repair_missing_embeddings=True,
        embedding_provider=DummyEmbeddingProvider(dimension),
        embedding_batch_size=2,
    )

    assert processed == len(email_ids)

    store = EmbeddingStore(store_path)
    subject_map = store.fetch_subject_vectors([email_ids[0]])
    store.close()
    assert email_ids[0] in subject_map


def test_bulk_ingest_from_embeddings(tmp_path: Path, sample_jsonl_path: Path) -> None:
    embeddings_dir = tmp_path / "embeddings"
    db_path = tmp_path / "email_search.lancedb"
    checkpoint_dir = tmp_path / "checkpoints"
    dimension = 8
    chunk_size = 40
    chunk_overlap = 5

    email_ids, total_chunks = _build_embeddings(
        sample_jsonl_path,
        embeddings_dir,
        dimension=dimension,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    processed = ingest_files_from_embeddings(
        [sample_jsonl_path],
        embeddings_dir=embeddings_dir,
        db_path=db_path,
        checkpoint_dir=checkpoint_dir,
        resume=False,
        skip_exists_check=True,
        ingest_batch_size=2,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        bulk_import=True,
    )

    assert processed == len(email_ids)

    repository = EmailRepository(Database(db_path), dimension=dimension)
    assert repository.count() == len(email_ids)
    if repository.database.table_exists(repository.CHUNKS_TABLE_NAME):
        assert repository.chunks_table.count_rows() == total_chunks

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from ragmail.vectorize.store import EmbeddingStore, default_embedding_path


def test_embedding_store_meta_roundtrip(tmp_path: Path) -> None:
    store_path = tmp_path / "test.embed.db"
    store = EmbeddingStore(store_path)
    meta = store.build_meta(
        embedding_model="test-model",
        embedding_model_revision="rev1",
        embedding_dimension=8,
        chunk_size=100,
        chunk_overlap=10,
        source="sample.jsonl",
    )
    store.ensure_meta(meta)
    loaded = store.get_meta()
    store.close()

    assert loaded is not None
    assert loaded["embedding_model"] == "test-model"
    assert loaded["embedding_model_revision"] == "rev1"
    assert loaded["embedding_dimension"] == 8
    assert loaded["chunk_size"] == 100
    assert loaded["chunk_overlap"] == 10


def test_embedding_store_meta_mismatch(tmp_path: Path) -> None:
    store_path = tmp_path / "test.embed.db"
    store = EmbeddingStore(store_path)
    meta = store.build_meta(
        embedding_model="test-model",
        embedding_model_revision="rev1",
        embedding_dimension=8,
        chunk_size=100,
        chunk_overlap=10,
        source="sample.jsonl",
    )
    store.ensure_meta(meta)

    mismatched = store.build_meta(
        embedding_model="test-model",
        embedding_model_revision="rev1",
        embedding_dimension=12,
        chunk_size=100,
        chunk_overlap=10,
        source="sample.jsonl",
    )
    with pytest.raises(ValueError, match="metadata mismatch"):
        store.ensure_meta(mismatched)
    store.close()


def test_embedding_store_roundtrip_vectors(tmp_path: Path) -> None:
    store_path = tmp_path / "test.embed.db"
    store = EmbeddingStore(store_path)
    meta = store.build_meta(
        embedding_model="test-model",
        embedding_model_revision=None,
        embedding_dimension=4,
        chunk_size=50,
        chunk_overlap=5,
        source="sample.jsonl",
    )
    store.ensure_meta(meta)

    email_ids = ["a", "b"]
    subject_vectors = np.array(
        [[1.0, 1.0, 1.0, 1.0], [2.0, 2.0, 2.0, 2.0]], dtype=np.float32
    )
    chunk_counts = [2, 1]
    chunk_email_indices = [0, 0, 1]
    chunk_indices = [0, 1, 0]
    chunk_vectors = np.array(
        [
            [10.0, 10.0, 10.0, 10.0],
            [11.0, 11.0, 11.0, 11.0],
            [20.0, 20.0, 20.0, 20.0],
        ],
        dtype=np.float32,
    )

    store.add_batch(
        email_ids,
        subject_vectors,
        chunk_counts,
        chunk_email_indices,
        chunk_indices,
        chunk_vectors,
    )

    subjects = store.fetch_subject_vectors(email_ids)
    chunks = store.fetch_chunk_vectors(email_ids)
    counts = store.fetch_chunk_counts(email_ids)
    store.close()

    assert set(subjects.keys()) == {"a", "b"}
    np.testing.assert_allclose(subjects["a"], subject_vectors[0])
    np.testing.assert_allclose(subjects["b"], subject_vectors[1])
    assert counts == {"a": 2, "b": 1}
    assert len(chunks["a"]) == 2
    assert len(chunks["b"]) == 1
    np.testing.assert_allclose(chunks["a"][0], chunk_vectors[0])
    np.testing.assert_allclose(chunks["a"][1], chunk_vectors[1])
    np.testing.assert_allclose(chunks["b"][0], chunk_vectors[2])


def test_default_embedding_path(tmp_path: Path) -> None:
    input_file = tmp_path / "gmail-2015.clean.jsonl"
    out = default_embedding_path(input_file, tmp_path)
    assert out.name == "gmail-2015.clean.embed.db"

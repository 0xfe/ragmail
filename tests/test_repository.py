"""Tests for EmailRepository."""

import numpy as np
import pytest

from ragmail.ingest import ParsedEmail
from ragmail.storage import Database, EmailRepository


def test_repository_add_and_get(temp_db: Database, sample_emails: list[ParsedEmail]):
    """Test adding and retrieving emails."""
    repository = EmailRepository(temp_db)
    email = sample_emails[0]
    vector = np.random.randn(384).astype(np.float32)

    repository.add(email, vector)

    retrieved = repository.get(email.email_id)
    assert retrieved is not None
    assert retrieved["email_id"] == email.email_id
    assert retrieved["subject"] == email.subject
    assert retrieved["attachment_names"] == ["agenda.pdf"]
    assert retrieved["attachment_types"] == ["application/pdf"]


def test_repository_add_batch(temp_db: Database, sample_emails: list[ParsedEmail]):
    """Test batch adding emails."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)

    repository.add_batch(sample_emails, vectors)

    assert repository.count() == len(sample_emails)


def test_repository_exists(temp_db: Database, sample_emails: list[ParsedEmail]):
    """Test existence check."""
    repository = EmailRepository(temp_db)
    email = sample_emails[0]
    vector = np.random.randn(384).astype(np.float32)

    assert not repository.exists(email.email_id)

    repository.add(email, vector)

    assert repository.exists(email.email_id)


def test_repository_vector_search(temp_db: Database, sample_emails: list[ParsedEmail]):
    """Test vector similarity search."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)
    repository.add_batch(sample_emails, vectors)

    query_vector = vectors[0]
    results = repository.search_vector(query_vector, limit=2)

    assert len(results) <= 2
    assert results[0]["email_id"] == sample_emails[0].email_id


def test_repository_body_chunk_search(
    temp_db: Database,
    sample_emails: list[ParsedEmail],
):
    """Test chunk-level vector search."""
    repository = EmailRepository(temp_db)
    subject_vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)

    chunk_texts = ["Chunk one", "Chunk two"]
    chunk_vectors = np.random.randn(2, 384).astype(np.float32)
    chunk_email_indices = [0, 1]
    chunk_indices = [0, 0]

    repository.add_batch(
        sample_emails,
        subject_vectors,
        chunk_texts,
        chunk_vectors,
        chunk_email_indices,
        chunk_indices,
    )

    results = repository.search_body_chunks(chunk_vectors[0], limit=1)

    assert len(results) == 1
    assert results[0]["email_id"] == sample_emails[0].email_id


def test_repository_get_top_senders(temp_db: Database, sample_emails: list[ParsedEmail]):
    """Test getting top senders."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)
    repository.add_batch(sample_emails, vectors)

    top = repository.get_top_senders(limit=5)

    assert len(top) > 0
    assert all(isinstance(addr, str) and isinstance(count, int) for addr, count in top)

"""Tests for SearchEngine."""

import numpy as np

from ragmail.ingest import ParsedEmail
from ragmail.search import SearchEngine
from ragmail.storage import Database, EmailRepository


def test_search_engine_basic_search(
    temp_db: Database,
    sample_emails: list[ParsedEmail],
    mock_embedding_provider,
):
    """Test basic search functionality."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)
    repository.add_batch(sample_emails, vectors)

    engine = SearchEngine(repository, mock_embedding_provider)
    response = engine.search("meeting tomorrow")

    assert response.query.raw_query == "meeting tomorrow"
    assert response.total_found >= 0


def test_search_engine_aggregation_query(
    temp_db: Database,
    sample_emails: list[ParsedEmail],
    mock_embedding_provider,
):
    """Test aggregation query handling."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)
    repository.add_batch(sample_emails, vectors)

    engine = SearchEngine(repository, mock_embedding_provider)
    response = engine.search("who did I email most in 2022")

    assert response.query.query_type == "aggregation"
    assert response.aggregations is not None
    assert "top_senders" in response.aggregations


def test_search_engine_count_query(
    temp_db: Database,
    sample_emails: list[ParsedEmail],
    mock_embedding_provider,
):
    """Test count query handling."""
    repository = EmailRepository(temp_db)
    vectors = np.random.randn(len(sample_emails), 384).astype(np.float32)
    repository.add_batch(sample_emails, vectors)

    engine = SearchEngine(repository, mock_embedding_provider)
    response = engine.search("how many emails")

    assert response.query.query_type == "count"
    assert response.aggregations is not None
    assert response.aggregations["count"] == len(sample_emails)

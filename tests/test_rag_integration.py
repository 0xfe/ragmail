"""Tests for RAG (Retrieval-Augmented Generation) integration."""

import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

from ragmail.llm.base import Message
from ragmail.search.engine import SearchEngine, SearchResponse
from ragmail.search.query_parser import ParsedQuery
from ragmail.storage import Database, EmailRepository


class TestRAGIntegration:
    """Test RAG integration with mocked components."""

    @pytest.fixture
    def mock_search_results(self):
        """Create mock search results."""
        from ragmail.search.hybrid_search import SearchResult

        return [
            SearchResult(
                email_id="test-1",
                subject="Project Update",
                from_address="john@example.com",
                from_name="John Smith",
                date="2024-01-15",
                body_snippet="The project is on track for completion next month.",
                score=0.95,
            ),
            SearchResult(
                email_id="test-2",
                subject="Re: Project Update",
                from_address="jane@example.com",
                from_name="Jane Doe",
                date="2024-01-16",
                body_snippet="Great news! Let me know if you need any help.",
                score=0.87,
            ),
        ]

    @pytest.fixture
    def mock_repository(self):
        """Create mock email repository."""
        repo = Mock(spec=EmailRepository)
        repo.dimension = 1024

        # Mock the get method to return email records as dicts
        def mock_get(email_id):
            email_records = {
                "test-1": {
                    "email_id": "test-1",
                    "subject": "Project Update",
                    "from_name": "John Smith",
                    "from_address": "john@example.com",
                    "date": datetime(2024, 1, 15),
                    "body_plain": "The project is on track for completion next month.",
                },
                "test-2": {
                    "email_id": "test-2",
                    "subject": "Re: Project Update",
                    "from_name": "Jane Doe",
                    "from_address": "jane@example.com",
                    "date": datetime(2024, 1, 16),
                    "body_plain": "Great news! Let me know if you need any help.",
                },
            }
            return email_records.get(email_id)

        repo.get = mock_get
        return repo

    @pytest.fixture
    def mock_embedding_provider(self):
        """Create mock embedding provider."""
        provider = Mock()
        provider.dimension = 1024
        provider.encode_query.return_value = [0.1] * 1024
        return provider

    @pytest.fixture
    def mock_llm_backend(self):
        """Create mock LLM backend."""
        backend = Mock()

        # Mock LLM response
        response = Mock()
        response.content = "Based on the emails, John reported that the project is on track for completion next month. Jane responded positively and offered help."
        response.model = "gpt-5.2"
        response.usage = {"prompt_tokens": 100, "completion_tokens": 25}
        response.finish_reason = "stop"

        backend.complete.return_value = response
        return backend

    @pytest.fixture
    def search_engine(
        self,
        mock_repository,
        mock_embedding_provider,
        mock_llm_backend,
        mock_search_results,
    ):
        """Create search engine with mocked hybrid searcher."""
        engine = SearchEngine(
            repository=mock_repository,
            embedding_provider=mock_embedding_provider,
            llm_backend=mock_llm_backend,
        )

        # Mock the hybrid searcher
        engine.hybrid_searcher = Mock()
        engine.hybrid_searcher.search.return_value = mock_search_results

        return engine

    def test_search_without_rag(self, search_engine):
        """Test regular search without RAG."""
        response = search_engine.search("project status", limit=10)

        assert isinstance(response, SearchResponse)
        assert len(response.results) == 2
        assert response.rag_answer is None  # No RAG answer in regular search

    def test_search_with_rag(self, search_engine, mock_llm_backend):
        """Test search with RAG enabled."""
        response = search_engine.search_with_rag(
            "What did John say about the project?", limit=10
        )

        assert isinstance(response, SearchResponse)
        assert len(response.results) == 2
        assert response.rag_answer is not None
        assert "John" in response.rag_answer or "project" in response.rag_answer.lower()

        # Verify LLM was called
        mock_llm_backend.complete.assert_called_once()

    def test_rag_context_construction(self, search_engine, mock_llm_backend):
        """Test that RAG properly constructs context from emails."""
        search_engine.search_with_rag("project status", limit=10)

        # Get the messages passed to LLM
        call_args = mock_llm_backend.complete.call_args
        messages = call_args[0][0]

        # Should have at least 2 messages (system and user)
        assert len(messages) >= 2

        # Check that email context is in a user message
        user_messages = [m for m in messages if m.role == "user"]
        assert len(user_messages) > 0

        user_content = user_messages[0].content
        assert "Project Update" in user_content
        assert "John Smith" in user_content or "john@example.com" in user_content

    def test_rag_with_no_results(self, search_engine, mock_llm_backend):
        """Test RAG when no emails are found."""
        # Override to return no results
        search_engine.hybrid_searcher.search.return_value = []

        response = search_engine.search_with_rag("unknown topic", limit=10)

        assert len(response.results) == 0
        # When no results, RAG returns early without calling LLM
        mock_llm_backend.complete.assert_not_called()

    def test_rag_response_format(self, search_engine):
        """Test that RAG response has correct format."""
        response = search_engine.search_with_rag("project status", limit=10)

        assert hasattr(response, "query")
        assert hasattr(response, "results")
        assert hasattr(response, "total_found")
        assert hasattr(response, "rag_answer")
        assert hasattr(response, "aggregations")


class TestRAGPromptIntegration:
    """Test RAG prompt integration with LLM."""

    def test_rag_prompt_formatting(self):
        """Test that RAG prompt is formatted correctly."""
        from ragmail.prompts import RAG_PROMPT

        # Pass email records as dicts (as the prompts expect)
        email_records = [
            {
                "email_id": "test-1",
                "subject": "Test Email",
                "from_name": "Test User",
                "from_address": "test@example.com",
                "date": "2024-01-15",
                "body_plain": "This is test content.",
            }
        ]

        question = "What is this about?"
        messages = RAG_PROMPT.format(question, email_records)

        # Should have system and user messages
        assert len(messages) == 2
        assert messages[0].role == "system"
        assert messages[1].role == "user"

        # User message should contain context
        assert "Test Email" in messages[1].content
        assert "What is this about?" in messages[1].content


class TestQueryExpansionIntegration:
    """Test query expansion with LLM."""

    @pytest.fixture
    def mock_repository(self):
        """Create mock repository."""
        repo = Mock(spec=EmailRepository)
        repo.dimension = 1024
        return repo

    @pytest.fixture
    def mock_embedding_provider(self):
        """Create mock embedding provider."""
        provider = Mock()
        provider.dimension = 1024
        provider.encode_query.return_value = [0.1] * 1024
        return provider

    @pytest.fixture
    def mock_llm_backend(self):
        """Create mock LLM backend for query expansion."""
        backend = Mock()

        response = Mock()
        response.content = """{
            "intent": "search",
            "entities": {
                "people": ["John"],
                "dates": ["2024"],
                "topics": ["project"]
            },
            "sub_queries": ["John project 2024"],
            "search_terms": ["John project", "2024 project"]
        }"""

        backend.complete.return_value = response
        return backend

    @pytest.fixture
    def search_engine_with_expansion(
        self, mock_repository, mock_embedding_provider, mock_llm_backend
    ):
        """Create search engine with mocked components."""
        from ragmail.search.hybrid_search import SearchResult

        engine = SearchEngine(
            repository=mock_repository,
            embedding_provider=mock_embedding_provider,
            llm_backend=mock_llm_backend,
        )

        # Mock the hybrid searcher
        engine.hybrid_searcher = Mock()
        engine.hybrid_searcher.search.return_value = [
            SearchResult(
                email_id="test-1",
                subject="Project Update",
                from_address="john@example.com",
                from_name="John Smith",
                date="2024-01-15",
                body_snippet="Project content",
                score=0.95,
            )
        ]

        return engine

    def test_query_expansion(self, search_engine_with_expansion, mock_llm_backend):
        """Test search with query expansion."""
        response = search_engine_with_expansion.search_with_expansion(
            "emails from John in 2024", limit=10
        )

        # Should return results
        assert response is not None
        assert isinstance(response, SearchResponse)
        # LLM should be called for expansion
        mock_llm_backend.complete.assert_called()


@pytest.mark.integration
class TestRAGEndToEnd:
    """End-to-end RAG tests requiring real components."""

    @pytest.fixture(autouse=True)
    def check_api_key(self):
        """Skip if no OpenAI API key."""
        import os

        api_key = os.environ.get("EMAIL_SEARCH_OPENAI_API_KEY") or os.environ.get(
            "OPENAI_API_KEY"
        )
        if not api_key:
            pytest.skip("OpenAI API key not set")

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create temporary database."""
        db_path = tmp_path / "test.lancedb"
        return Database(db_path)

    @pytest.fixture
    def real_repository(self, temp_db, real_embedding_provider):
        """Create real email repository."""
        return EmailRepository(temp_db, dimension=real_embedding_provider.dimension)

    @pytest.fixture
    def real_embedding_provider(self):
        """Create real embedding provider."""
        from ragmail.embedding import create_embedding_provider

        return create_embedding_provider("sentence_transformer")

    @pytest.fixture
    def real_llm_backend(self):
        """Create real LLM backend."""
        from ragmail.llm import create_llm_backend

        return create_llm_backend()

    def test_e2e_rag_search(
        self, real_repository, real_embedding_provider, real_llm_backend
    ):
        """Test end-to-end RAG search with real components."""
        import numpy as np

        # Add test emails
        from ragmail.ingest.email_parser import ParsedEmail

        emails = [
            ParsedEmail(
                email_id="test-1",
                message_id="<msg1@test.com>",
                subject="Project Update",
                from_address="john@example.com",
                from_name="John Smith",
                to_addresses=["me@example.com"],
                cc_addresses=[],
                date=datetime(2024, 1, 15),
                body_plain="The project is progressing well and will be completed by March.",
                body_html="",
                has_attachment=False,
                labels=[],
                in_reply_to=None,
                thread_id="thread-1",
            ),
            ParsedEmail(
                email_id="test-2",
                message_id="<msg2@test.com>",
                subject="Budget Review",
                from_address="jane@example.com",
                from_name="Jane Doe",
                to_addresses=["me@example.com"],
                cc_addresses=[],
                date=datetime(2024, 1, 16),
                body_plain="We need to review the Q1 budget before the end of the month.",
                body_html="",
                has_attachment=False,
                labels=[],
                in_reply_to=None,
                thread_id="thread-2",
            ),
        ]

        # Encode and add emails
        texts = [f"{e.subject}\n\n{e.body_plain}" for e in emails]
        vectors = real_embedding_provider.encode(texts)
        real_repository.add_batch(emails, vectors)

        # Create search engine
        engine = SearchEngine(
            repository=real_repository,
            embedding_provider=real_embedding_provider,
            llm_backend=real_llm_backend,
        )

        # Search with RAG
        response = engine.search_with_rag("What is the status of the project?", limit=5)

        assert len(response.results) > 0
        assert response.rag_answer is not None
        assert len(response.rag_answer) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

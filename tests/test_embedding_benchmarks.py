"""Benchmark tests comparing embedding providers."""

import pytest
import numpy as np
from datetime import datetime
from pathlib import Path

from ragmail.embedding.sentence_transformer import SentenceTransformerProvider
from ragmail.storage import Database, EmailRepository
from ragmail.search.engine import SearchEngine


class TestEmbeddingBenchmark:
    """Benchmark embedding quality with synthetic data."""

    @pytest.fixture
    def test_emails(self):
        """Create test emails for benchmarking."""
        return [
            {
                "email_id": f"email-{i}",
                "subject": subject,
                "body_plain": body,
                "from_name": sender,
                "date": datetime(2024, 1, 1),
            }
            for i, (subject, body, sender) in enumerate(
                [
                    # Project-related emails (group 1)
                    (
                        "Project Kickoff",
                        "We are starting the new project tomorrow. Please review the requirements.",
                        "John",
                    ),
                    (
                        "Re: Project Kickoff",
                        "Thanks for the update. I'll prepare the documentation.",
                        "Sarah",
                    ),
                    (
                        "Project Status",
                        "The project is 50% complete and on schedule.",
                        "John",
                    ),
                    (
                        "Project Deadline",
                        "Reminder: project deliverables are due next Friday.",
                        "Manager",
                    ),
                    # Meeting-related emails (group 2)
                    (
                        "Meeting Tomorrow",
                        "Let's meet at 2pm to discuss the roadmap.",
                        "Sarah",
                    ),
                    (
                        "Re: Meeting Tomorrow",
                        "I can make it. Should I bring the slides?",
                        "John",
                    ),
                    (
                        "Meeting Notes",
                        "Here are the notes from today's standup.",
                        "Sarah",
                    ),
                    # Budget-related emails (group 3)
                    (
                        "Q1 Budget Review",
                        "Please submit your budget proposals by Friday.",
                        "Finance",
                    ),
                    (
                        "Budget Approved",
                        "Your budget request has been approved for $50K.",
                        "Finance",
                    ),
                    # Personal/unrelated (group 4)
                    ("Lunch Today?", "Want to grab lunch at the new place?", "Friend"),
                    (
                        "Weekend Plans",
                        "Are you free for hiking this weekend?",
                        "Friend",
                    ),
                ]
            )
        ]

    @pytest.fixture
    def test_queries(self):
        """Test queries for retrieval."""
        return {
            "project": [
                "email-0",
                "email-1",
                "email-2",
                "email-3",
            ],  # Should retrieve project emails
            "meeting": [
                "email-4",
                "email-5",
                "email-6",
            ],  # Should retrieve meeting emails
            "budget": ["email-7", "email-8"],  # Should retrieve budget emails
        }

    def test_nomic_provider(self, test_emails, test_queries):
        """Test Nomic embedding quality."""
        provider = SentenceTransformerProvider()

        # Encode emails
        texts = [f"{e['subject']}\n\n{e['body_plain']}" for e in test_emails]
        email_embeddings = provider.encode(texts, show_progress=False)

        results = {}
        for query_name, expected_ids in test_queries.items():
            # Encode query
            query_embedding = provider.encode_query(query_name)

            # Calculate similarities
            similarities = np.dot(email_embeddings, query_embedding)

            # Get top 4 results
            top_indices = np.argsort(similarities)[-4:][::-1]
            retrieved_ids = [test_emails[i]["email_id"] for i in top_indices]

            # Calculate precision
            correct = sum(1 for rid in retrieved_ids if rid in expected_ids)
            precision = correct / len(retrieved_ids)

            results[query_name] = {
                "precision": precision,
                "retrieved": retrieved_ids,
                "expected": expected_ids,
            }

        # Assert reasonable precision
        for query_name, result in results.items():
            print(f"\nNomic - {query_name}: {result['precision']:.2f} precision")
            assert result["precision"] >= 0.5, f"Low precision for {query_name}"

class TestEmbeddingProviderComparison:
    """Compare embedding providers on various metrics."""

    @pytest.mark.parametrize(
        "provider_name,expected_dim",
        [
            ("sentence_transformer", 768),
        ],
    )
    def test_provider_dimensions(self, provider_name, expected_dim):
        """Test that providers return correct dimensions."""
        from ragmail.embedding import create_embedding_provider
        provider = create_embedding_provider(provider_name)

        assert provider.dimension == expected_dim

    def test_nomic_embedding_speed(self):
        """Test Nomic embedding speed."""
        import time

        provider = SentenceTransformerProvider()
        texts = [f"Test email content {i} about various topics" for i in range(10)]

        start = time.time()
        result = provider.encode(texts, show_progress=False)
        elapsed = time.time() - start

        assert result.shape == (10, 768)
        print(f"\nNomic encoding time: {elapsed:.3f}s for {len(texts)} texts")

    # Legacy benchmarks removed.


class TestRetrievalQuality:
    """Test retrieval quality metrics."""

    def test_retrieval_precision_at_k(self):
        """Test precision@k metric for search results."""
        # Simulate search results
        results = [
            {"email_id": "1", "relevant": True},
            {"email_id": "2", "relevant": True},
            {"email_id": "3", "relevant": False},
            {"email_id": "4", "relevant": True},
            {"email_id": "5", "relevant": False},
        ]

        # Calculate precision@k for different k values
        for k in [1, 3, 5]:
            relevant_in_top_k = sum(1 for r in results[:k] if r["relevant"])
            precision = relevant_in_top_k / k
            print(f"\nPrecision@{k}: {precision:.2f}")

            # Basic sanity check
            assert 0 <= precision <= 1

    def test_mean_reciprocal_rank(self):
        """Test Mean Reciprocal Rank (MRR) metric."""
        # Simulate multiple queries with results
        queries_results = [
            # First relevant item at position 1
            [{"relevant": True}, {"relevant": False}, {"relevant": False}],
            # First relevant item at position 3
            [{"relevant": False}, {"relevant": False}, {"relevant": True}],
            # First relevant item at position 2
            [{"relevant": False}, {"relevant": True}, {"relevant": True}],
        ]

        # Calculate MRR
        reciprocal_ranks = []
        for results in queries_results:
            for i, result in enumerate(results, 1):
                if result["relevant"]:
                    reciprocal_ranks.append(1.0 / i)
                    break

        mrr = sum(reciprocal_ranks) / len(reciprocal_ranks)
        print(f"\nMean Reciprocal Rank: {mrr:.2f}")

        assert 0 < mrr <= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

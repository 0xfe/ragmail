"""Hybrid search combining vector and full-text search with RRF."""

from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..storage.repository import EmailRepository


@dataclass
class SearchResult:
    """A single search result with relevance score."""

    email_id: str
    subject: str
    from_address: str
    from_name: str
    date: str | None
    body_snippet: str
    score: float
    vector_rank: int | None = None
    fts_rank: int | None = None

    @classmethod
    def from_record(
        cls,
        record: dict[str, Any],
        score: float,
        vector_rank: int | None = None,
        fts_rank: int | None = None,
    ) -> "SearchResult":
        """Create SearchResult from database record."""
        body = record.get("chunk_text") or record.get("body_plain", "")
        snippet = body[:200] + "..." if len(body) > 200 else body

        date_val = record.get("date")
        date_str = None
        if date_val:
            if hasattr(date_val, "isoformat"):
                date_str = date_val.isoformat()
            else:
                date_str = str(date_val)

        return cls(
            email_id=record.get("email_id", ""),
            subject=record.get("subject", ""),
            from_address=record.get("from_address", ""),
            from_name=record.get("from_name", ""),
            date=date_str,
            body_snippet=snippet,
            score=score,
            vector_rank=vector_rank,
            fts_rank=fts_rank,
        )


class HybridSearcher:
    """Hybrid search using Reciprocal Rank Fusion (RRF)."""

    def __init__(
        self,
        repository: EmailRepository,
        rrf_k: int = 60,
        subject_weight: float = 1.2,
        body_weight: float = 1.0,
        fts_weight: float = 0.4,
    ):
        """Initialize hybrid searcher.

        Args:
            repository: Email repository for database access
            rrf_k: RRF constant (higher = less weight to top results)
            subject_weight: Weight for subject vector matches
            body_weight: Weight for body chunk matches
            fts_weight: Weight for full-text search results
        """
        self.repository = repository
        self.rrf_k = rrf_k
        self.subject_weight = subject_weight
        self.body_weight = body_weight
        self.fts_weight = fts_weight

    def search(
        self,
        query_vector: NDArray[np.float32] | None,
        query_text: str,
        limit: int = 20,
        where: str | None = None,
        use_vector: bool = True,
        use_fts: bool = True,
    ) -> list[SearchResult]:
        """Perform hybrid search combining vector and FTS.

        Args:
            query_vector: Query embedding
            query_text: Query text for FTS
            limit: Maximum results to return
            where: Optional SQL filter

        Returns:
            Ranked list of search results
        """
        subject_fetch = max(limit * 2, 20)
        body_fetch = max(limit * 5, 80)

        subject_results = []
        body_results = []
        fts_results = []

        if use_vector and query_vector is not None:
            subject_results = self.repository.search_subject_vectors(
                query_vector, subject_fetch, where
            )
            body_results = self.repository.search_body_chunks(
                query_vector, body_fetch, where
            )

        if use_fts and query_text:
            try:
                fts_results = self.repository.search_fts(
                    query_text, max(limit * 3, 30), where
                )
            except Exception:
                fts_results = []

        return self._fuse_results(subject_results, body_results, fts_results, limit)

    def _fuse_results(
        self,
        subject_results: list[dict[str, Any]],
        body_results: list[dict[str, Any]],
        fts_results: list[dict[str, Any]],
        limit: int,
    ) -> list[SearchResult]:
        """Fuse vector and FTS results using RRF.

        Args:
            subject_results: Results from subject vector search
            body_results: Results from body chunk search
            fts_results: Results from full-text search
            limit: Maximum results to return

        Returns:
            Fused and ranked results
        """
        scores: dict[str, float] = {}
        subject_records: dict[str, dict[str, Any]] = {}
        body_records: dict[str, dict[str, Any]] = {}
        fts_records: dict[str, dict[str, Any]] = {}
        subject_scores: dict[str, float] = {}
        body_scores: dict[str, float] = {}
        vector_ranks: dict[str, int] = {}
        fts_ranks: dict[str, int] = {}

        for rank, record in enumerate(subject_results, start=1):
            email_id = record.get("email_id", "")
            rrf_score = self.subject_weight / (self.rrf_k + rank)
            subject_scores[email_id] = max(subject_scores.get(email_id, 0.0), rrf_score)
            subject_records.setdefault(email_id, record)
            vector_ranks[email_id] = min(vector_ranks.get(email_id, rank), rank)

        for rank, record in enumerate(body_results, start=1):
            email_id = record.get("email_id", "")
            rrf_score = self.body_weight / (self.rrf_k + rank)
            body_scores[email_id] = max(body_scores.get(email_id, 0.0), rrf_score)
            body_records.setdefault(email_id, record)
            vector_ranks[email_id] = min(vector_ranks.get(email_id, rank), rank)

        for rank, record in enumerate(fts_results, start=1):
            email_id = record.get("email_id", "")
            rrf_score = self.fts_weight / (self.rrf_k + rank)
            scores[email_id] = scores.get(email_id, 0) + rrf_score
            fts_records.setdefault(email_id, record)
            fts_ranks[email_id] = rank

        for email_id in set(subject_scores) | set(body_scores):
            vector_score = max(subject_scores.get(email_id, 0.0), body_scores.get(email_id, 0.0))
            scores[email_id] = scores.get(email_id, 0) + vector_score

        sorted_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)

        results = []
        for email_id in sorted_ids[:limit]:
            subject_score = subject_scores.get(email_id, 0.0)
            body_score = body_scores.get(email_id, 0.0)
            if body_score >= subject_score and email_id in body_records:
                record = body_records[email_id]
            elif email_id in subject_records:
                record = subject_records[email_id]
            else:
                record = fts_records[email_id]
            result = SearchResult.from_record(
                record,
                scores[email_id],
                vector_ranks.get(email_id),
                fts_ranks.get(email_id),
            )
            results.append(result)

        return results

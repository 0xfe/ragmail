"""Search engine facade combining all search components."""

from dataclasses import dataclass
from typing import Any

from ..embedding.base import EmbeddingProvider
from ..llm.base import LLMBackend
from ..prompts import QUERY_EXPANSION_PROMPT, RAG_PROMPT
from ..storage.repository import EmailRepository
from .hybrid_search import HybridSearcher, SearchResult
from .query_parser import ParsedQuery, QueryParser
from .query_planner import QueryPlan, QueryPlanner


@dataclass
class SearchResponse:
    """Complete search response with results and metadata."""

    query: ParsedQuery
    results: list[SearchResult]
    total_found: int
    aggregations: dict[str, Any] | None = None
    rag_answer: str | None = None
    query_plan: QueryPlan | None = None


class SearchEngine:
    """Main search engine combining query parsing, embedding, and hybrid search."""

    def __init__(
        self,
        repository: EmailRepository,
        embedding_provider: EmbeddingProvider,
        llm_backend: LLMBackend | None = None,
        rrf_k: int = 60,
        use_llm_planner: bool = False,
    ):
        """Initialize search engine.

        Args:
            repository: Email repository
            embedding_provider: Embedding provider for query encoding
            llm_backend: Optional LLM backend for RAG responses
            rrf_k: RRF constant for hybrid search
        """
        self.repository = repository
        self.embedding_provider = embedding_provider
        self.llm_backend = llm_backend
        self.query_parser = QueryParser()
        self.query_planner = QueryPlanner(
            llm_backend if use_llm_planner else None
        )
        self.hybrid_searcher = HybridSearcher(repository, rrf_k=rrf_k)

    def search(self, query: str, limit: int = 20) -> SearchResponse:
        """Search emails with natural language query.

        Args:
            query: Natural language search query
            limit: Maximum results to return

        Returns:
            SearchResponse with results and metadata
        """
        parsed = self.query_parser.parse(query)
        plan = self.query_planner.plan(query, parsed)

        if plan.intent == "aggregation":
            return self._handle_aggregation(parsed, plan)
        if plan.intent == "count":
            return self._handle_count(parsed, plan)

        vector_query = plan.vector_query or parsed.semantic_query or query
        fts_query = plan.fts_query or parsed.semantic_query or query
        where_clause = plan.to_where_clause() or parsed.to_where_clause()

        query_vector = None
        if plan.use_vector:
            query_vector = self.embedding_provider.encode_query(vector_query)
        results = self.hybrid_searcher.search(
            query_vector,
            fts_query,
            limit=limit,
            where=where_clause,
            use_vector=plan.use_vector,
            use_fts=plan.use_fts,
        )

        return SearchResponse(
            query=parsed,
            results=results,
            total_found=len(results),
            query_plan=plan,
        )

    def search_with_rag(self, query: str, limit: int = 20) -> SearchResponse:
        """Search emails and generate RAG response.

        Args:
            query: Natural language search query
            limit: Maximum results to retrieve for context

        Returns:
            SearchResponse with RAG answer
        """
        # First do regular search
        response = self.search(query, limit=limit)

        # If no LLM backend or no results, return regular response
        if not self.llm_backend or not response.results:
            return response

        # Get email records for RAG context
        email_records = []
        for result in response.results[:10]:  # Use top 10 for context
            email = self.repository.get(result.email_id)
            if email:
                email_records.append(email)

        # Generate RAG response
        messages = RAG_PROMPT.format(
            question=query,
            emails=email_records,
        )

        llm_response = self.llm_backend.complete(
            messages, max_tokens=1024, temperature=0.3
        )

        return SearchResponse(
            query=response.query,
            results=response.results,
            total_found=response.total_found,
            aggregations=response.aggregations,
            rag_answer=llm_response.content,
            query_plan=response.query_plan,
        )

    def _handle_aggregation(
        self, parsed: ParsedQuery, plan: QueryPlan | None = None
    ) -> SearchResponse:
        """Handle aggregation queries like 'who did I email most'.

        Args:
            parsed: Parsed query with aggregation intent

        Returns:
            SearchResponse with aggregation results
        """
        if (plan and plan.aggregation_field) == "from_address" or (
            not plan and parsed.aggregation_field == "from_address"
        ):
            top_senders = self.repository.get_top_senders(
                year=parsed.year,
                limit=10,
            )
            aggregations = {
                "top_senders": [
                    {"address": addr, "count": count} for addr, count in top_senders
                ],
            }
            return SearchResponse(
                query=parsed,
                results=[],
                total_found=0,
                aggregations=aggregations,
                query_plan=plan,
            )

        return SearchResponse(
            query=parsed,
            results=[],
            total_found=0,
            query_plan=plan,
        )

    def _handle_count(self, parsed: ParsedQuery, plan: QueryPlan | None = None) -> SearchResponse:
        """Handle count queries.

        Args:
            parsed: Parsed query with count intent

        Returns:
            SearchResponse with count
        """
        where_clause = plan.to_where_clause() if plan else parsed.to_where_clause()
        count = self.repository.count(where=where_clause)

        return SearchResponse(
            query=parsed,
            results=[],
            total_found=count,
            aggregations={"count": count},
            query_plan=plan,
        )

    def search_with_expansion(
        self, query: str, limit: int = 20, expand: bool = True
    ) -> SearchResponse:
        """Search emails with optional query expansion.

        Args:
            query: Natural language search query
            limit: Maximum results to return
            expand: Whether to use LLM for query expansion

        Returns:
            SearchResponse with results
        """
        # If no LLM backend or expansion disabled, do regular search
        if not self.llm_backend or not expand:
            return self.search(query, limit=limit)

        # Use LLM to expand the query
        try:
            messages = QUERY_EXPANSION_PROMPT.format(query=query)
            llm_response = self.llm_backend.complete(
                messages, max_tokens=1024, temperature=0.3
            )

            # Parse the expanded query suggestions
            # For now, just use the semantic query from the expansion
            # TODO: Parse JSON response and use sub-queries
            expanded_query = llm_response.content

            # Use the expanded understanding to improve search
            parsed = self.query_parser.parse(query)

            # If we have search terms from expansion, use them
            if parsed.semantic_query:
                search_text = parsed.semantic_query
            else:
                search_text = query

        except Exception:
            # Fallback to regular search on error
            search_text = query

        query_vector = self.embedding_provider.encode_query(search_text)
        where_clause = self.query_parser.parse(query).to_where_clause()

        results = self.hybrid_searcher.search(
            query_vector,
            search_text,
            limit=limit,
            where=where_clause,
        )

        return SearchResponse(
            query=self.query_parser.parse(query),
            results=results,
            total_found=len(results),
        )

    def search_similar(self, email_id: str, limit: int = 10) -> list[SearchResult]:
        """Find emails similar to a given email.

        Args:
            email_id: ID of the reference email
            limit: Maximum results

        Returns:
            List of similar emails
        """
        email = self.repository.get(email_id)
        if not email:
            return []

        vector = email.get("subject_vector")
        if vector is None:
            return []

        import numpy as np

        query_vector = np.array(vector, dtype=np.float32)

        results = self.repository.search_subject_vectors(
            query_vector,
            limit=limit + 1,
        )

        filtered = [r for r in results if r.get("email_id") != email_id]

        return [SearchResult.from_record(r, 0.0) for r in filtered[:limit]]

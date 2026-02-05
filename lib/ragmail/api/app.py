"""FastAPI application."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from ..config import Settings, get_settings
from ..embedding import SentenceTransformerProvider
from ..search import SearchEngine
from ..storage import Database, EmailRepository


class SearchRequest(BaseModel):
    """Search request body."""

    query: str
    limit: int = 20


class SearchResultResponse(BaseModel):
    """Single search result."""

    email_id: str
    subject: str
    from_address: str
    from_name: str
    date: str | None
    body_snippet: str
    score: float


class SearchResponse(BaseModel):
    """Search response."""

    query: str
    results: list[SearchResultResponse]
    total_found: int
    aggregations: dict[str, Any] | None = None


class EmailResponse(BaseModel):
    """Full email response."""

    email_id: str
    subject: str
    from_address: str
    from_name: str
    to_addresses: list[str]
    date: str | None
    body: str
    has_attachment: bool
    labels: list[str]


class StatsResponse(BaseModel):
    """Database statistics response."""

    total_emails: int
    emails_by_year: dict[int, int]
    top_senders: list[dict[str, Any]]


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str


app_state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    settings = get_settings()

    if settings.db_path.exists():
        database = Database(settings.db_path)
        repository = EmailRepository(database)
        embedding_provider = SentenceTransformerProvider(settings.embedding_model)
        search_engine = SearchEngine(
            repository,
            embedding_provider,
            rrf_k=settings.search_rrf_k,
        )

        app_state["database"] = database
        app_state["repository"] = repository
        app_state["search_engine"] = search_engine
        app_state["settings"] = settings

    yield

    if "database" in app_state:
        app_state["database"].close()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create FastAPI application.

    Args:
        settings: Optional settings override

    Returns:
        Configured FastAPI app
    """
    app = FastAPI(
        title="Email Search API",
        description="Search and analyze email archives",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        return HealthResponse(status="healthy", version="0.1.0")

    @app.post("/search", response_model=SearchResponse)
    async def search(request: SearchRequest):
        """Search emails with natural language query."""
        if "search_engine" not in app_state:
            raise HTTPException(status_code=503, detail="Database not initialized")

        search_engine = app_state["search_engine"]
        response = search_engine.search(request.query, limit=request.limit)

        return SearchResponse(
            query=request.query,
            results=[
                SearchResultResponse(
                    email_id=r.email_id,
                    subject=r.subject,
                    from_address=r.from_address,
                    from_name=r.from_name,
                    date=r.date,
                    body_snippet=r.body_snippet,
                    score=r.score,
                )
                for r in response.results
            ],
            total_found=response.total_found,
            aggregations=response.aggregations,
        )

    @app.get("/search", response_model=SearchResponse)
    async def search_get(
        q: str = Query(..., description="Search query"),
        limit: int = Query(20, description="Maximum results"),
    ):
        """Search emails with GET request."""
        return await search(SearchRequest(query=q, limit=limit))

    @app.get("/emails/{email_id}", response_model=EmailResponse)
    async def get_email(email_id: str):
        """Get a specific email by ID."""
        if "repository" not in app_state:
            raise HTTPException(status_code=503, detail="Database not initialized")

        repository = app_state["repository"]
        email = repository.get(email_id)

        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        to_addresses = email.get("to_addresses_str", "")
        labels = email.get("labels_str", "")

        date_val = email.get("date")
        date_str = None
        if date_val:
            if hasattr(date_val, "isoformat"):
                date_str = date_val.isoformat()
            else:
                date_str = str(date_val)

        return EmailResponse(
            email_id=email["email_id"],
            subject=email.get("subject", ""),
            from_address=email.get("from_address", ""),
            from_name=email.get("from_name", ""),
            to_addresses=to_addresses.split(",") if to_addresses else [],
            date=date_str,
            body=email.get("body_plain", ""),
            has_attachment=email.get("has_attachment", False),
            labels=labels.split(",") if labels else [],
        )

    @app.get("/stats", response_model=StatsResponse)
    async def get_stats():
        """Get database statistics."""
        if "repository" not in app_state:
            raise HTTPException(status_code=503, detail="Database not initialized")

        repository = app_state["repository"]

        total = repository.count()
        by_year = repository.get_email_count_by_year()
        top_senders = repository.get_top_senders(limit=10)

        return StatsResponse(
            total_emails=total,
            emails_by_year=by_year,
            top_senders=[
                {"address": addr, "count": count}
                for addr, count in top_senders
            ],
        )

    @app.get("/emails/{email_id}/similar", response_model=list[SearchResultResponse])
    async def get_similar(email_id: str, limit: int = Query(10)):
        """Find emails similar to a given email."""
        if "search_engine" not in app_state:
            raise HTTPException(status_code=503, detail="Database not initialized")

        search_engine = app_state["search_engine"]
        results = search_engine.search_similar(email_id, limit=limit)

        return [
            SearchResultResponse(
                email_id=r.email_id,
                subject=r.subject,
                from_address=r.from_address,
                from_name=r.from_name,
                date=r.date,
                body_snippet=r.body_snippet,
                score=r.score,
            )
            for r in results
        ]

    return app

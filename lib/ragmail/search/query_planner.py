"""Query planning for hybrid search."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import re
from typing import Any, Literal

from ..llm.base import LLMBackend
from ..prompts import QUERY_PLAN_PROMPT
from .query_parser import ParsedQuery, QueryParser


@dataclass
class QueryPlan:
    """Structured query plan for hybrid search."""

    intent: Literal["search", "count", "aggregation"] = "search"
    vector_query: str = ""
    fts_query: str = ""
    use_vector: bool = True
    use_fts: bool = True
    filters: dict[str, Any] = field(default_factory=dict)
    aggregation_field: str | None = None

    def to_where_clause(self) -> str | None:
        """Build SQL WHERE clause from filters."""
        conditions: list[str] = []

        def escape(value: str) -> str:
            return value.replace("'", "''")

        def like(value: str) -> str:
            return f"%{escape(value)}%"

        def from_clause(value: str) -> str:
            if "@" in value:
                return f"from_address = '{escape(value)}'"
            if "." in value and " " not in value:
                return f"from_address LIKE '{like('@' + value)}'"
            return (
                f"(from_name LIKE '{like(value)}' OR from_address LIKE '{like(value)}')"
            )

        def to_clause(value: str) -> str:
            if "@" in value:
                return f"to_addresses_str LIKE '{like(value)}'"
            if "." in value and " " not in value:
                return f"to_addresses_str LIKE '{like('@' + value)}'"
            return f"to_addresses_str LIKE '{like(value)}'"

        from_value = _normalize_str(self.filters.get("from"))
        if from_value:
            conditions.append(from_clause(from_value))

        to_value = _normalize_str(self.filters.get("to"))
        if to_value:
            conditions.append(to_clause(to_value))

        from_domain = _normalize_str(self.filters.get("from_domain"))
        if from_domain:
            conditions.append(f"from_address LIKE '{like('@' + from_domain)}'")

        year = _normalize_int(self.filters.get("year"))
        if year:
            conditions.append(f"year = {year}")

        month = _normalize_int(self.filters.get("month"))
        if month:
            conditions.append(f"month = {month}")

        has_attachment = _normalize_bool(self.filters.get("has_attachment"))
        if has_attachment is not None:
            conditions.append(f"has_attachment = {has_attachment}")

        start_date = _parse_date(self.filters.get("start_date"))
        if start_date:
            conditions.append(f"date >= '{start_date.isoformat()}'")

        end_date = _parse_date(self.filters.get("end_date"))
        if end_date:
            conditions.append(f"date <= '{end_date.isoformat()}'")

        labels = self.filters.get("labels")
        if isinstance(labels, str):
            labels = [labels]
        if labels:
            for label in labels:
                label_str = _normalize_str(label)
                if label_str:
                    conditions.append(f"labels_str LIKE '{like(label_str)}'")

        return " AND ".join(conditions) if conditions else None

    @classmethod
    def from_parsed(cls, parsed: ParsedQuery) -> "QueryPlan":
        """Create a query plan from a parsed query."""
        vector_query = parsed.semantic_query or parsed.raw_query
        fts_query = " ".join(parsed.keywords) if parsed.keywords else vector_query
        filters: dict[str, Any] = {}

        if parsed.from_address:
            filters["from"] = parsed.from_address
        if parsed.to_address:
            filters["to"] = parsed.to_address
        if parsed.year:
            filters["year"] = parsed.year
        if parsed.month:
            filters["month"] = parsed.month
        if parsed.start_date:
            filters["start_date"] = parsed.start_date.date().isoformat()
        if parsed.end_date:
            filters["end_date"] = parsed.end_date.date().isoformat()
        if parsed.has_attachment is not None:
            filters["has_attachment"] = parsed.has_attachment
        if parsed.labels:
            filters["labels"] = parsed.labels

        return cls(
            intent=parsed.query_type,
            vector_query=vector_query,
            fts_query=fts_query,
            filters=filters,
            aggregation_field=parsed.aggregation_field,
        )

    @classmethod
    def from_llm(cls, data: dict[str, Any], fallback: "QueryPlan") -> "QueryPlan":
        """Create a query plan from LLM JSON with fallback."""
        if not isinstance(data, dict):
            return fallback

        intent = _normalize_str(data.get("intent")) or fallback.intent
        if intent not in {"search", "count", "aggregation"}:
            intent = fallback.intent

        vector_query = _normalize_str(data.get("vector_query")) or fallback.vector_query
        fts_query = _normalize_str(data.get("fts_query")) or fallback.fts_query

        filters = dict(fallback.filters)
        raw_filters = data.get("filters")
        if isinstance(raw_filters, dict):
            for key in [
                "from",
                "to",
                "from_domain",
                "year",
                "month",
                "start_date",
                "end_date",
                "has_attachment",
                "labels",
            ]:
                if key in raw_filters:
                    filters[key] = raw_filters[key]

        aggregation_field = fallback.aggregation_field
        raw_agg = data.get("aggregation")
        if isinstance(raw_agg, dict):
            aggregation_field = _normalize_str(raw_agg.get("field")) or aggregation_field
        aggregation_field = _normalize_str(data.get("aggregation_field")) or aggregation_field

        use_vector = _normalize_bool(data.get("use_vector"))
        if use_vector is None:
            use_vector = fallback.use_vector

        use_fts = _normalize_bool(data.get("use_fts"))
        if use_fts is None:
            use_fts = fallback.use_fts

        return cls(
            intent=intent,
            vector_query=vector_query,
            fts_query=fts_query,
            filters=filters,
            aggregation_field=aggregation_field,
            use_vector=use_vector,
            use_fts=use_fts,
        )


class QueryPlanner:
    """Generate query plans using heuristics or LLMs."""

    def __init__(self, llm_backend: LLMBackend | None = None):
        self.llm_backend = llm_backend
        self.parser = QueryParser()

    def plan(self, query: str, parsed: ParsedQuery | None = None) -> QueryPlan:
        parsed = parsed or self.parser.parse(query)
        fallback = QueryPlan.from_parsed(parsed)
        if not self.llm_backend:
            return fallback

        try:
            messages = QUERY_PLAN_PROMPT.format(query=query)
            response = self.llm_backend.complete(
                messages, max_tokens=512, temperature=0.2
            )
            data = _parse_json_block(response.content)
            return QueryPlan.from_llm(data, fallback)
        except Exception:
            return fallback


def _parse_json_block(text: str) -> dict[str, Any]:
    if not text:
        raise ValueError("Empty response")
    fenced = re.search(r"```(?:json)?\\s*(\\{.*?\\})\\s*```", text, re.S)
    if fenced:
        return json.loads(fenced.group(1))
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("No JSON object found")


def _normalize_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return str(value).strip() or None


def _normalize_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _parse_date(value: Any) -> datetime | None:
    value_str = _normalize_str(value)
    if not value_str:
        return None
    try:
        return datetime.fromisoformat(value_str)
    except ValueError:
        return None

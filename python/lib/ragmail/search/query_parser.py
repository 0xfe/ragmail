"""Query parsing for natural language search queries."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


@dataclass
class ParsedQuery:
    """Parsed search query with extracted components."""

    raw_query: str
    keywords: list[str] = field(default_factory=list)
    semantic_query: str = ""
    from_address: str | None = None
    to_address: str | None = None
    year: int | None = None
    month: int | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    has_attachment: bool | None = None
    labels: list[str] = field(default_factory=list)
    query_type: Literal["search", "aggregation", "count"] = "search"
    aggregation_field: str | None = None

    def to_where_clause(self) -> str | None:
        """Build SQL WHERE clause from parsed query."""
        conditions = []

        if self.from_address:
            conditions.append(f"from_address = '{self.from_address}'")
        if self.to_address:
            conditions.append(f"to_addresses_str LIKE '%{self.to_address}%'")
        if self.year:
            conditions.append(f"year = {self.year}")
        if self.month:
            conditions.append(f"month = {self.month}")
        if self.has_attachment is not None:
            conditions.append(f"has_attachment = {self.has_attachment}")
        if self.start_date:
            conditions.append(f"date >= '{self.start_date.isoformat()}'")
        if self.end_date:
            conditions.append(f"date <= '{self.end_date.isoformat()}'")

        return " AND ".join(conditions) if conditions else None


class QueryParser:
    """Parse natural language queries into structured components."""

    YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
    MONTH_NAMES = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    EMAIL_PATTERN = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
    AGGREGATION_PATTERNS = [
        (re.compile(r"who.*(?:email|talk|convers|correspond).*most", re.I), "from_address"),
        (re.compile(r"top.*(?:sender|contact|people)", re.I), "from_address"),
        (re.compile(r"most.*(?:email|message).*from", re.I), "from_address"),
        (re.compile(r"how many.*(?:email|message)", re.I), "count"),
    ]
    FROM_PATTERNS = [
        re.compile(r"from\s+(\S+@\S+)", re.I),
        re.compile(r"from\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)", re.I),
    ]
    TO_PATTERNS = [
        re.compile(r"to\s+(\S+@\S+)", re.I),
    ]

    def parse(self, query: str) -> ParsedQuery:
        """Parse a natural language query.

        Args:
            query: Natural language search query

        Returns:
            ParsedQuery with extracted components
        """
        parsed = ParsedQuery(raw_query=query)

        self._extract_aggregation(query, parsed)
        self._extract_dates(query, parsed)
        self._extract_addresses(query, parsed)
        self._extract_attachment(query, parsed)

        parsed.semantic_query = self._build_semantic_query(query, parsed)
        parsed.keywords = self._extract_keywords(query)

        return parsed

    def _extract_aggregation(self, query: str, parsed: ParsedQuery) -> None:
        """Extract aggregation intent from query."""
        for pattern, field in self.AGGREGATION_PATTERNS:
            if pattern.search(query):
                if field == "count":
                    parsed.query_type = "count"
                else:
                    parsed.query_type = "aggregation"
                    parsed.aggregation_field = field
                return

    def _extract_dates(self, query: str, parsed: ParsedQuery) -> None:
        """Extract date information from query."""
        year_match = self.YEAR_PATTERN.search(query)
        if year_match:
            parsed.year = int(year_match.group())

        query_lower = query.lower()
        for month_name, month_num in self.MONTH_NAMES.items():
            if month_name in query_lower:
                parsed.month = month_num
                break

    def _extract_addresses(self, query: str, parsed: ParsedQuery) -> None:
        """Extract email addresses from query."""
        for pattern in self.FROM_PATTERNS:
            match = pattern.search(query)
            if match:
                addr = match.group(1)
                if "@" in addr or not addr.lower() in ["me", "i", "my"]:
                    parsed.from_address = addr.lower()
                break

        for pattern in self.TO_PATTERNS:
            match = pattern.search(query)
            if match:
                parsed.to_address = match.group(1).lower()
                break

    def _extract_attachment(self, query: str, parsed: ParsedQuery) -> None:
        """Extract attachment filter from query."""
        query_lower = query.lower()
        if "with attachment" in query_lower or "has attachment" in query_lower:
            parsed.has_attachment = True
        elif "without attachment" in query_lower or "no attachment" in query_lower:
            parsed.has_attachment = False

    def _build_semantic_query(self, query: str, parsed: ParsedQuery) -> str:
        """Build semantic search query by removing structured parts."""
        semantic = query

        if parsed.year:
            semantic = self.YEAR_PATTERN.sub("", semantic)

        for month in self.MONTH_NAMES:
            semantic = re.sub(rf"\b{month}\b", "", semantic, flags=re.I)

        for pattern in self.FROM_PATTERNS + self.TO_PATTERNS:
            semantic = pattern.sub("", semantic)

        semantic = re.sub(r"with(out)?\s+attachment", "", semantic, flags=re.I)

        semantic = re.sub(r"\s+", " ", semantic).strip()

        return semantic

    def _extract_keywords(self, query: str) -> list[str]:
        """Extract significant keywords from query."""
        stop_words = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "of", "with", "by", "from", "as", "is", "was", "are",
            "were", "been", "be", "have", "has", "had", "do", "does", "did",
            "will", "would", "could", "should", "may", "might", "must",
            "my", "your", "his", "her", "its", "our", "their", "this", "that",
            "these", "those", "i", "you", "he", "she", "it", "we", "they",
            "who", "what", "where", "when", "why", "how", "which", "whom",
            "email", "emails", "message", "messages", "mail", "mails",
        }

        words = re.findall(r"\b\w+\b", query.lower())
        keywords = [
            word for word in words
            if word not in stop_words
            and len(word) > 2
            and not word.isdigit()
        ]

        return keywords

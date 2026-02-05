"""Tests for QueryParser."""

from ragmail.search import QueryParser


def test_query_parser_extracts_year():
    """Test year extraction from query."""
    parser = QueryParser()

    result = parser.parse("emails from 2017")
    assert result.year == 2017

    result = parser.parse("what happened in 2020")
    assert result.year == 2020


def test_query_parser_extracts_month():
    """Test month extraction from query."""
    parser = QueryParser()

    result = parser.parse("emails from January")
    assert result.month == 1

    result = parser.parse("what happened in Dec")
    assert result.month == 12


def test_query_parser_extracts_email_address():
    """Test email address extraction."""
    parser = QueryParser()

    result = parser.parse("emails from john@example.com")
    assert result.from_address == "john@example.com"


def test_query_parser_detects_aggregation():
    """Test aggregation query detection."""
    parser = QueryParser()

    result = parser.parse("who did I email most in 2017")
    assert result.query_type == "aggregation"
    assert result.aggregation_field == "from_address"
    assert result.year == 2017


def test_query_parser_detects_count():
    """Test count query detection."""
    parser = QueryParser()

    result = parser.parse("how many emails in 2020")
    assert result.query_type == "count"
    assert result.year == 2020


def test_query_parser_builds_where_clause():
    """Test WHERE clause generation."""
    parser = QueryParser()

    result = parser.parse("emails from 2017")
    where = result.to_where_clause()
    assert "year = 2017" in where


def test_query_parser_extracts_keywords():
    """Test keyword extraction."""
    parser = QueryParser()

    result = parser.parse("emails about taxes and refund")
    assert "taxes" in result.keywords
    assert "refund" in result.keywords


def test_query_parser_builds_semantic_query():
    """Test semantic query construction."""
    parser = QueryParser()

    result = parser.parse("emails about project deadline in 2020")
    assert "2020" not in result.semantic_query
    assert "project" in result.semantic_query.lower() or "deadline" in result.semantic_query.lower()

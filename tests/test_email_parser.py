"""Tests for JsonEmailParser."""

from pathlib import Path

from ragmail.ingest import JsonEmailParser, JsonlReader


def test_email_parser_parses_basic_fields(sample_jsonl_path: Path):
    """Test parsing of basic email fields."""
    reader = JsonlReader(sample_jsonl_path)
    parser = JsonEmailParser()

    records = list(reader)
    email = parser.parse(records[0])

    assert email.subject == "Meeting Tomorrow"
    assert email.from_address == "john@example.com"
    assert email.from_name == "John Doe"
    assert "jane@example.com" in email.to_addresses
    assert email.date is not None
    assert email.date.year == 2022


def test_email_parser_extracts_body(sample_jsonl_path: Path):
    """Test body extraction."""
    reader = JsonlReader(sample_jsonl_path)
    parser = JsonEmailParser()

    records = list(reader)
    email = parser.parse(records[0])

    assert "meet tomorrow" in email.body_plain.lower()
    assert "2pm" in email.body_plain


def test_email_parser_extracts_labels(sample_jsonl_path: Path):
    """Test tag extraction."""
    reader = JsonlReader(sample_jsonl_path)
    parser = JsonEmailParser()

    records = list(reader)
    email = parser.parse(records[2])

    assert "Important" in email.labels
    assert "Work" in email.labels


def test_email_parser_generates_unique_ids(sample_jsonl_path: Path):
    """Test that each email gets a unique ID."""
    reader = JsonlReader(sample_jsonl_path)
    parser = JsonEmailParser()

    ids = set()
    for record in reader:
        email = parser.parse(record)
        assert email.email_id not in ids
        ids.add(email.email_id)


def test_email_parser_handles_reply(sample_jsonl_path: Path):
    """Test parsing of reply emails."""
    reader = JsonlReader(sample_jsonl_path)
    parser = JsonEmailParser()

    records = list(reader)
    reply = parser.parse(records[1])

    assert reply.in_reply_to == "msg001@example.com"
    assert reply.thread_id is not None

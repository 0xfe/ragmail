"""Tests for JsonlReader."""

from pathlib import Path

import pytest

from ragmail.ingest import JsonlReader


def test_jsonl_reader_iterates_messages(sample_jsonl_path: Path):
    """Test that JsonlReader iterates through all records."""
    reader = JsonlReader(sample_jsonl_path)
    records = list(reader)

    assert len(records) == 3


def test_jsonl_reader_count(sample_jsonl_path: Path):
    """Test record counting."""
    reader = JsonlReader(sample_jsonl_path)
    count = reader.count()

    assert count == 3


def test_jsonl_reader_file_not_found():
    """Test error handling for missing file."""
    with pytest.raises(FileNotFoundError):
        JsonlReader(Path("/nonexistent/file.jsonl"))


def test_jsonl_reader_record_fields(sample_jsonl_path: Path):
    """Test that record fields are accessible."""
    reader = JsonlReader(sample_jsonl_path)
    records = list(reader)

    first_record = records[0]
    headers = first_record["headers"]
    assert headers["from"]["email"] == "john@example.com"
    assert headers["subject"] == "Meeting Tomorrow"
    assert headers["to"][0]["email"] == "jane@example.com"


def test_jsonl_reader_invalid_json(tmp_path: Path):
    """Invalid JSON lines should yield an error record."""
    path = tmp_path / "bad.jsonl"
    path.write_text("{bad json}\\n", encoding="utf-8")

    reader = JsonlReader(path)
    records = list(reader)

    assert len(records) == 1
    assert records[0]["__ragmail_error__"] == "json_decode"

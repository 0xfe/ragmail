"""Tests for JSONL validation."""

from pathlib import Path

from ragmail.ingest import IngestPipeline
from ragmail.ingest.validation import JsonEmailValidator


def test_validator_accepts_valid_record(sample_jsonl_path: Path):
    record = next(iter(open(sample_jsonl_path, "r", encoding="utf-8")))
    import json

    data = json.loads(record)
    validator = JsonEmailValidator()
    issues = validator.validate(data)
    assert issues == []


def test_validator_flags_missing_content():
    validator = JsonEmailValidator()
    record = {"headers": {"subject": "Hello"}, "tags": []}
    issues = validator.validate(record)
    codes = {issue.code for issue in issues}
    assert "content.type" in codes
    assert "content.empty" in codes


def test_validator_allows_attachments_only():
    validator = JsonEmailValidator()
    record = {
        "headers": {"subject": "Attachment only"},
        "tags": [],
        "content": [],
        "attachments": [{"filename": "file.txt", "content_type": "text/plain", "size": 10}],
    }
    issues = validator.validate(record)
    codes = {issue.code for issue in issues}
    assert "content.empty" not in codes
    assert "content.text" not in codes


def test_pipeline_validate_logs_errors(tmp_path: Path):
    bad_jsonl = tmp_path / "bad.jsonl"
    bad_jsonl.write_text("{bad json}\n", encoding="utf-8")

    pipeline = IngestPipeline(errors_path=tmp_path / "errors.jsonl")
    results = pipeline.validate(bad_jsonl)

    assert results["errors"] == 1
    assert (tmp_path / "errors.jsonl").exists()

"""Historical-format fixture tests for Rust clean stage."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragmail import pipeline as pipeline_module


FIXTURE = Path(__file__).parent / "fixtures" / "historical_edge_cases.mbox"


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _by_message_id(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for row in rows:
        headers = row.get("headers", {})
        message_id = headers.get("message_id")
        if isinstance(message_id, str):
            out[message_id] = row
    return out


def _normalized_body(value: str) -> str:
    return "\n".join(line.strip() for line in value.splitlines() if line.strip())


def test_rust_clean_historical_fixture(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_mbox = tmp_path / "historical_edge_cases.mbox"
    shutil.copyfile(FIXTURE, input_mbox)

    rust_clean = tmp_path / "historical_edge_cases.rust.clean.jsonl"
    rust_spam = tmp_path / "historical_edge_cases.rust.spam.jsonl"
    rust_summary = tmp_path / "historical_edge_cases.rust.summary"
    rust_stats = pipeline_module._run_rust_clean(
        input_mbox=input_mbox,
        output_clean=rust_clean,
        output_spam=rust_spam,
        summary_output=rust_summary,
        stage="preprocess",
        ws=SimpleNamespace(logs_dir=tmp_path / "logs"),
    )
    rust_clean_rows = _read_jsonl(rust_clean)
    rust_spam_rows = _read_jsonl(rust_spam)

    assert rust_stats == {"processed": 5, "clean": 4, "spam": 1, "errors": 0}
    assert len(rust_clean_rows) == 4
    assert len(rust_spam_rows) == 1
    assert rust_summary.exists()

    rust_clean_by_id = _by_message_id(rust_clean_rows)

    assert set(rust_clean_by_id) == {
        "msg-2005@example.com",
        "msg-2012@example.com",
        "msg-2016@example.com",
        "msg-2024@example.com",
    }

    rust_2005 = rust_clean_by_id["msg-2005@example.com"]
    assert rust_2005["headers"]["subject"] == "Réunion Notes"
    assert rust_2005["headers"]["date"] == "2005-01-03T08:10:00+00:00"
    assert "Alice" not in _normalized_body(rust_2005["content"][0]["body"])

    rust_2012 = rust_clean_by_id["msg-2012@example.com"]
    assert "Project Update" in _normalized_body(rust_2012["content"][0]["body"])
    assert "Rendered from HTML only." in _normalized_body(rust_2012["content"][0]["body"])

    rust_2016 = rust_clean_by_id["msg-2016@example.com"]
    assert rust_2016["headers"]["list_id"] == "engineering.list.example"

    rust_2024 = rust_clean_by_id["msg-2024@example.com"]
    assert rust_2024["attachments"] == [
        {"filename": "q1-report.pdf", "content_type": "application/pdf", "size": 5}
    ]

    assert rust_spam_rows[0]["reason"] == "mailer:hubspot"

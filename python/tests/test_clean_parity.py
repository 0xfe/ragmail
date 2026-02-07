"""Rust clean stage contract tests."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragmail import pipeline as pipeline_module


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def test_rust_clean_contract_sample_fixture(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_mbox = tmp_path / "2024-01.mbox"
    input_mbox.write_text(
        (
            "From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n"
            "Message-ID: <a@example.com>\n"
            "From: Alpha <alpha@example.com>\n"
            "To: Beta <beta@example.com>\n"
            "Date: Mon, 1 Jan 2024 01:02:03 +0000\n"
            "Subject: =?UTF-8?B?SGVsbG8g4piD?=\n"
            "Content-Type: multipart/mixed; boundary=\"mix-1\"\n"
            "\n"
            "--mix-1\n"
            "Content-Type: text/plain; charset=utf-8\n"
            "\n"
            "Hello world.\n"
            "-- \n"
            "Alpha\n"
            "--mix-1\n"
            "Content-Type: application/pdf; name=\"report.pdf\"\n"
            "Content-Disposition: attachment; filename=\"report.pdf\"\n"
            "Content-Transfer-Encoding: base64\n"
            "\n"
            "SGVsbG8=\n"
            "--mix-1--\n"
            "\n"
            "From promo@example.com Tue Jan  2 04:05:06 +0000 2024\n"
            "Message-ID: <s@example.com>\n"
            "From: Promo <promo@example.com>\n"
            "Date: Tue, 2 Jan 2024 04:05:06 +0000\n"
            "Subject: Sale now\n"
            "X-Mailer: MailChimp Delivery\n"
            "\n"
            "Buy now.\n"
        ),
        encoding="utf-8",
    )

    rust_clean = tmp_path / "2024-01.rust.clean.jsonl"
    rust_spam = tmp_path / "2024-01.rust.spam.jsonl"
    rust_summary = tmp_path / "2024-01.rust.summary"
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

    assert rust_stats == {"processed": 2, "clean": 1, "spam": 1, "errors": 0}
    assert len(rust_clean_rows) == 1
    assert len(rust_spam_rows) == 1

    rust_row = rust_clean_rows[0]
    assert set(rust_row.keys()) == {"headers", "tags", "content", "attachments", "mbox"}
    assert rust_row["headers"]["subject"] == "Hello ☃"
    assert rust_row["headers"]["date"] == "2024-01-01T01:02:03+00:00"
    assert rust_row["content"][0]["body"] == "Hello world."
    assert rust_row["attachments"] == [
        {"filename": "report.pdf", "content_type": "application/pdf", "size": 5}
    ]

    assert rust_spam_rows[0]["reason"] == "mailer:mailchimp"
    assert rust_summary.exists()

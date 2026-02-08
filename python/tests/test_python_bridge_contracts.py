"""Tests for Rust->Python bridge contract commands."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

import ragmail.cli as cli_module
from ragmail.workspace import get_workspace


def test_py_vectorize_contract_outputs_json(tmp_path: Path, monkeypatch):
    ws = get_workspace("py-bridge-vectorize", base_dir=tmp_path)
    ws.ensure()
    clean_file = ws.clean_dir / "2024-01.clean.jsonl"
    clean_file.write_text("{}\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_vectorize_files(input_files, **kwargs):
        captured["input_files"] = list(input_files)
        captured["kwargs"] = kwargs
        return 7

    monkeypatch.setattr("ragmail.vectorize.run.vectorize_files", _fake_vectorize_files)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "py",
            "vectorize",
            "--workspace",
            ws.name,
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    payload = lines[-1]
    assert payload["status"] == "ok"
    assert payload["stage"] == "vectorize"
    assert payload["workspace"] == ws.name
    assert payload["processed"] == 7
    assert payload["files"] == 1
    assert captured["input_files"] == [clean_file]
    assert captured["kwargs"]["checkpoint_dir"] == ws.checkpoints_dir / "vectorize"
    assert captured["kwargs"]["errors_path"] == ws.logs_dir / "vectorize.errors.jsonl"


def test_py_vectorize_contract_emits_progress_events(tmp_path: Path, monkeypatch):
    ws = get_workspace("py-bridge-vectorize-progress", base_dir=tmp_path)
    ws.ensure()
    clean_file = ws.clean_dir / "2024-01.clean.jsonl"
    clean_file.write_text("{}\n", encoding="utf-8")

    def _fake_vectorize_files(input_files, **kwargs):
        progress = kwargs["progress_callback"]
        progress({"processed": 1})
        progress({"processed": 2})
        return 2

    monkeypatch.setattr("ragmail.vectorize.run.vectorize_files", _fake_vectorize_files)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "py",
            "vectorize",
            "--workspace",
            ws.name,
            "--base-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    progress_events = [line for line in lines if line.get("event") == "progress"]
    assert len(progress_events) >= 2
    assert progress_events[-1]["processed"] == 2
    assert lines[-1]["status"] == "ok"
    assert lines[-1]["processed"] == 2


def test_py_ingest_contract_outputs_json(tmp_path: Path, monkeypatch):
    ws = get_workspace("py-bridge-ingest", base_dir=tmp_path)
    ws.ensure()
    clean_file = ws.clean_dir / "2024-01.clean.jsonl"
    clean_file.write_text("{}\n", encoding="utf-8")
    embedding_file = ws.embeddings_dir / "2024-01.clean.embed.db"
    embedding_file.write_bytes(b"placeholder")

    captured: dict[str, object] = {}

    def _fake_ingest_files_from_embeddings(input_files, **kwargs):
        captured["input_files"] = list(input_files)
        captured["kwargs"] = kwargs
        return 5

    monkeypatch.setattr(
        "ragmail.ingest.run.ingest_files_from_embeddings",
        _fake_ingest_files_from_embeddings,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "py",
            "ingest",
            "--workspace",
            ws.name,
            "--base-dir",
            str(tmp_path),
            "--skip-exists-check",
            "false",
            "--no-repair-embeddings",
        ],
    )

    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    payload = lines[-1]
    assert payload["status"] == "ok"
    assert payload["stage"] == "ingest"
    assert payload["workspace"] == ws.name
    assert payload["processed"] == 5
    assert payload["files"] == 1
    assert payload["repair_embeddings"] is False
    assert captured["input_files"] == [clean_file]
    assert captured["kwargs"]["skip_exists_check"] is False
    assert captured["kwargs"]["checkpoint_dir"] == ws.checkpoints_dir
    assert captured["kwargs"]["errors_path"] == ws.logs_dir / "ingest.errors.jsonl"


def test_py_ingest_contract_emits_progress_and_compaction_events(tmp_path: Path, monkeypatch):
    ws = get_workspace("py-bridge-ingest-progress", base_dir=tmp_path)
    ws.ensure()
    clean_file = ws.clean_dir / "2024-01.clean.jsonl"
    clean_file.write_text("{}\n", encoding="utf-8")
    embedding_file = ws.embeddings_dir / "2024-01.clean.embed.db"
    embedding_file.write_bytes(b"placeholder")

    def _fake_ingest_files_from_embeddings(input_files, **kwargs):
        progress = kwargs["progress_callback"]
        compaction = kwargs["compaction_callback"]
        progress({"processed": 3, "skipped": 1, "skipped_exists": 1, "skipped_errors": 0})
        compaction({"phase": "start", "reason": "periodic"})
        compaction({"phase": "done", "reason": "periodic"})
        progress({"processed": 5, "skipped": 1, "skipped_exists": 1, "skipped_errors": 0})
        return 5

    monkeypatch.setattr(
        "ragmail.ingest.run.ingest_files_from_embeddings",
        _fake_ingest_files_from_embeddings,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "py",
            "ingest",
            "--workspace",
            ws.name,
            "--base-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    progress_events = [line for line in lines if line.get("event") == "progress"]
    compaction_events = [line for line in lines if line.get("event") == "compaction"]
    assert len(progress_events) >= 2
    assert len(compaction_events) >= 2
    assert progress_events[-1]["processed"] == 5
    assert lines[-1]["status"] == "ok"
    assert lines[-1]["processed"] == 5

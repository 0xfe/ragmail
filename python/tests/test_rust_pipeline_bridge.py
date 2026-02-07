"""Tests for the Rust split/preprocess bridges in the Python pipeline."""

from __future__ import annotations

import json
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

import ragmail.cli as cli_module
import ragmail.pipeline as pipeline_module


def _write_synthetic_mbox(path: Path, *, total: int, month: str) -> None:
    lines: list[str] = []
    for i in range(1, total + 1):
        day = (i % 28) + 1
        lines.append(
            f"From user{i}@example.com Mon {month} {day:2d} 01:02:03 +0000 2024\n"
        )
        lines.append(f"Message-ID: <bridge-{month}-{i}@example.com>\n")
        lines.append(f"From: User {i} <user{i}@example.com>\n")
        lines.append(f"Date: Mon, {day} {month} 2024 01:02:03 +0000\n")
        lines.append(f"Subject: Bridge {i}\n")
        lines.append("\n")
        lines.append(f"Body {i}\n")
    path.write_text("".join(lines), encoding="utf-8")


def _write_rotating_month_synthetic_mbox(path: Path, *, total: int) -> None:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    lines: list[str] = []
    for i in range(1, total + 1):
        month = months[(i - 1) % len(months)]
        day = (i % 28) + 1
        lines.append(
            f"From user{i}@example.com Mon {month} {day:2d} 01:02:03 +0000 2024\n"
        )
        lines.append(f"Message-ID: <interrupt-{i}@example.com>\n")
        lines.append(f"From: User {i} <user{i}@example.com>\n")
        lines.append(f"Date: Mon, {day} {month} 2024 01:02:03 +0000\n")
        lines.append(f"Subject: Interrupt {i}\n")
        lines.append("\n")
        lines.append(f"Body {i}\n")
    path.write_text("".join(lines), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_pipeline_cli_invokes_pipeline_without_legacy_flags(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_run_pipeline(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return None

    monkeypatch.setattr(cli_module, "run_pipeline", _fake_run_pipeline)
    sample = (Path(__file__).parent / "fixtures" / "sample.mbox").resolve()
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["pipeline", str(sample), "--workspace", "test-ws", "--stages", "split"],
    )
    assert result.exit_code == 0
    kwargs = captured["kwargs"]
    assert kwargs["workspace_name"] == "test-ws"
    assert kwargs["stages"] == {"split"}
    assert "rust_split_index" not in kwargs
    assert "rust_clean" not in kwargs


def test_pipeline_cli_rejects_legacy_rust_flags():
    sample = (Path(__file__).parent / "fixtures" / "sample.mbox").resolve()
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "pipeline",
            str(sample),
            "--workspace",
            "test-ws",
            "--rust-split-index",
        ],
    )
    assert result.exit_code != 0
    assert "No such option: --rust-split-index" in result.output
    clean_result = runner.invoke(
        cli_module.cli,
        [
            "pipeline",
            str(sample),
            "--workspace",
            "test-ws",
            "--rust-clean",
        ],
    )
    assert clean_result.exit_code != 0
    assert "No such option: --rust-clean" in clean_result.output


def test_build_resume_command_excludes_legacy_flags():
    command = cli_module._build_resume_command(
        inputs=[Path("/tmp/inbox.mbox")],
        workspace_name="ws1",
        base_dir=None,
        cache_dir=None,
        clean_dir=None,
        embeddings_dir=None,
        years=[2025],
        stages={"split", "index"},
    )
    assert "--rust-split-index" not in command
    assert "--rust-clean" not in command


def test_parse_rust_split_stats():
    output = "split complete: processed=12 written=9 skipped=2 errors=1"
    stats = pipeline_module._parse_rust_split_stats(output)
    assert stats == {
        "processed": 12,
        "written": 9,
        "skipped": 2,
        "errors": 1,
        "last_position": 0,
    }


def test_parse_rust_split_stats_with_last_position():
    output = "split complete: processed=12 written=9 skipped=2 errors=1 last_position=987"
    stats = pipeline_module._parse_rust_split_stats(output)
    assert stats["last_position"] == 987


def test_run_rust_split_uses_checkpoint_offset(tmp_path: Path, monkeypatch):
    input_mbox = tmp_path / "sample.mbox"
    input_mbox.write_text("", encoding="utf-8")
    checkpoint_path = tmp_path / "split-rs" / "sample.checkpoint.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text('{"last_position": 123}', encoding="utf-8")

    calls: list[list[str]] = []

    def _fake_run_rust_cli(args: list[str], *, stage: str, ws):
        calls.append(args)
        return "split complete: processed=1 written=1 skipped=0 errors=0 last_position=456"

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", _fake_run_rust_cli)
    stats = pipeline_module._run_rust_split(
        input_mbox=input_mbox,
        output_dir=tmp_path / "split",
        years=None,
        checkpoint_path=checkpoint_path,
        resume=True,
        checkpoint_interval=30,
        stage="split",
        ws=SimpleNamespace(logs_dir=tmp_path / "logs"),
    )

    assert stats["last_position"] == 456
    assert calls, "expected rust split bridge call"
    args = calls[0]
    assert "--start-offset" in args
    assert args[args.index("--start-offset") + 1] == "123"
    assert "--checkpoint" in args
    assert args[args.index("--checkpoint") + 1] == str(checkpoint_path)
    assert "--resume" in args
    assert args[args.index("--resume") + 1] == "true"
    assert not checkpoint_path.exists()


def test_rust_split_resume_restarts_when_checkpoint_missing(tmp_path: Path, monkeypatch):
    ws = pipeline_module.get_workspace("rust-split-ws", base_dir=tmp_path)
    ws.ensure()
    input_mbox = tmp_path / "source.mbox"
    input_mbox.write_text(
        (
            "From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n"
            "Subject: One\n\nBody one.\n"
        ),
        encoding="utf-8",
    )
    existing_split = ws.split_dir / "2024-01.mbox"
    existing_split.write_text("stale split output\n", encoding="utf-8")

    calls: list[dict[str, object]] = []

    def _fake_run_rust_split(**kwargs):
        calls.append(kwargs)
        return {"processed": 1, "written": 1, "skipped": 0, "errors": 0, "last_position": 10}

    monkeypatch.setattr(pipeline_module, "_run_rust_split", _fake_run_rust_split)

    pipeline_module.run_pipeline(
        input_mboxes=[input_mbox],
        workspace_name="rust-split-ws",
        base_dir=tmp_path,
        stages={"split"},
        resume=True,
    )

    assert len(calls) == 1
    assert calls[0]["resume"] is False
    assert not existing_split.exists()


def test_parse_rust_clean_stats():
    output = "clean complete: processed=12 clean=9 spam=2 errors=1"
    stats = pipeline_module._parse_rust_clean_stats(output)
    assert stats == {"processed": 12, "clean": 9, "spam": 2, "errors": 1}


def test_run_pipeline_uses_rust_clean_by_default(tmp_path: Path, monkeypatch):
    ws = pipeline_module.get_workspace("rust-clean-ws", base_dir=tmp_path)
    ws.ensure()
    split_mbox = ws.split_dir / "2024-01.mbox"
    split_mbox.write_text(
        "From alpha@example.com Mon Jan  1 00:00:00 +0000 2024\n"
        "Message-ID: <a@example.com>\n"
        "From: Alpha <alpha@example.com>\n"
        "Date: Mon, 1 Jan 2024 00:00:00 +0000\n"
        "Subject: One\n"
        "\n"
        "Body one.\n",
        encoding="utf-8",
    )

    rust_calls: list[tuple[Path, Path, Path]] = []

    def _fake_run_rust_clean(
        *,
        input_mbox: Path,
        output_clean: Path,
        output_spam: Path,
        summary_output: Path,
        index_output: Path | None = None,
        stage: str,
        ws,
    ):
        rust_calls.append((input_mbox, output_clean, output_spam))
        output_clean.parent.mkdir(parents=True, exist_ok=True)
        output_spam.parent.mkdir(parents=True, exist_ok=True)
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        output_clean.write_text(
            json.dumps(
                {
                    "headers": {"subject": "One"},
                    "tags": [],
                    "content": [{"type": "text", "body": "Body one."}],
                    "mbox": {"file": "2024-01.mbox", "offset": 0, "length": 42},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_spam.write_text("", encoding="utf-8")
        summary_output.write_text("summary", encoding="utf-8")
        if index_output is not None:
            index_output.parent.mkdir(parents=True, exist_ok=True)
            index_output.write_text(
                json.dumps(
                    {
                        "email_id": "2024-01:0",
                        "message_id": "a@example.com",
                        "message_id_lower": "a@example.com",
                        "mbox_file": "2024-01.mbox",
                        "offset": 0,
                        "length": 42,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        return {"processed": 1, "clean": 1, "spam": 0, "errors": 0}

    monkeypatch.setattr(pipeline_module, "_run_rust_clean", _fake_run_rust_clean)

    pipeline_module.run_pipeline(
        input_mboxes=[],
        workspace_name="rust-clean-ws",
        base_dir=tmp_path,
        stages={"preprocess"},
        resume=True,
    )

    assert len(rust_calls) == 1
    assert rust_calls[0][0] == split_mbox
    assert (ws.clean_dir / "2024-01.clean.jsonl").exists()
    assert (ws.spam_dir / "2024-01.spam.jsonl").exists()
    assert (ws.reports_dir / "2024-01.mbox.summary").exists()


def test_model_stage_only_completes_without_inputs(tmp_path: Path, monkeypatch):
    ws_name = "model-only"
    ws = pipeline_module.get_workspace(ws_name, base_dir=tmp_path)
    ws.ensure()

    monkeypatch.setattr(pipeline_module, "_warmup_dependencies", lambda: None)

    pipeline_module.run_pipeline(
        input_mboxes=[],
        workspace_name=ws_name,
        base_dir=tmp_path,
        stages={"model"},
        resume=True,
    )

    state = ws.load_state()
    assert state.get("stages", {}).get("model", {}).get("status") == "done"


def test_build_rust_mbox_index_merges_parts(tmp_path: Path, monkeypatch):
    split_dir = tmp_path / "split"
    split_dir.mkdir()
    split_files = [split_dir / "2024-01.mbox", split_dir / "2024-02.mbox"]
    for path in split_files:
        path.write_text("From a@example.com Thu Jan  1 00:00:00 2024\n\n", encoding="utf-8")

    calls: list[list[str]] = []

    def _fake_run_rust_cli(args: list[str], *, stage: str, ws):
        calls.append(args)
        output_idx = args.index("--output") + 1
        mbox_idx = args.index("--mbox-file") + 1
        output_path = Path(args[output_idx])
        mbox_file = args[mbox_idx]
        record = {
            "email_id": mbox_file.replace(".mbox", ""),
            "message_id": None,
            "message_id_lower": None,
            "mbox_file": mbox_file,
            "offset": 0,
            "length": 10,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
        return "index complete: indexed=1 last_position=42"

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", _fake_run_rust_cli)
    progress: list[int] = []
    total = pipeline_module._build_rust_mbox_index(
        split_files=split_files,
        output_path=split_dir / "mbox_index.jsonl",
        checkpoint_dir=tmp_path / "checkpoints" / "mbox_index-rs",
        resume=False,
        checkpoint_interval=30,
        progress_callback=lambda payload: progress.append(payload["processed"]),
        ws=SimpleNamespace(),
    )
    assert total == 2
    assert len(calls) == 2
    merged_lines = (split_dir / "mbox_index.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(merged_lines) == 2
    assert progress[-1] == 2


def test_rust_split_resume_after_interrupt_soak(tmp_path: Path, monkeypatch):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_a = tmp_path / "input-a.mbox"
    input_b = tmp_path / "input-b.mbox"
    _write_synthetic_mbox(input_a, total=120, month="Jan")
    _write_synthetic_mbox(input_b, total=120, month="Feb")

    original_run_rust_cli = pipeline_module._run_rust_cli
    split_call_count = 0

    def _interrupting_run_rust_cli(args: list[str], *, stage: str, ws):
        nonlocal split_call_count
        if stage == "split":
            split_call_count += 1
            if split_call_count == 2:
                raise KeyboardInterrupt()
        return original_run_rust_cli(args, stage=stage, ws=ws)

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", _interrupting_run_rust_cli)
    with pytest.raises(KeyboardInterrupt):
        pipeline_module.run_pipeline(
            input_mboxes=[input_a, input_b],
            workspace_name="rust-split-interrupt",
            base_dir=tmp_path,
            stages={"split"},
            resume=False,
        )

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", original_run_rust_cli)
    pipeline_module.run_pipeline(
        input_mboxes=[input_a, input_b],
        workspace_name="rust-split-interrupt",
        base_dir=tmp_path,
        stages={"split"},
        resume=True,
    )

    ws = pipeline_module.get_workspace("rust-split-interrupt", base_dir=tmp_path)
    split_files = sorted(ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox"))
    assert len(split_files) == 2
    total = pipeline_module._count_mbox_messages(split_files)
    assert total == 240


def test_rust_index_resume_after_interrupt_soak(tmp_path: Path, monkeypatch):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_file = tmp_path / "input-multi-month.mbox"
    _write_synthetic_mbox(input_file, total=150, month="Jan")
    with input_file.open("a", encoding="utf-8") as handle:
        temp_file = tmp_path / "feb.mbox"
        _write_synthetic_mbox(temp_file, total=150, month="Feb")
        handle.write(temp_file.read_text(encoding="utf-8"))
    ws_name = "rust-index-interrupt"

    pipeline_module.run_pipeline(
        input_mboxes=[input_file],
        workspace_name=ws_name,
        base_dir=tmp_path,
        stages={"split"},
        resume=False,
    )

    original_run_rust_cli = pipeline_module._run_rust_cli
    index_call_count = 0

    def _interrupting_index_run_rust_cli(args: list[str], *, stage: str, ws):
        nonlocal index_call_count
        if stage == "preprocess":
            index_call_count += 1
            if index_call_count == 2:
                raise KeyboardInterrupt()
        return original_run_rust_cli(args, stage=stage, ws=ws)

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", _interrupting_index_run_rust_cli)
    with pytest.raises(KeyboardInterrupt):
        pipeline_module.run_pipeline(
            input_mboxes=[input_file],
            workspace_name=ws_name,
            base_dir=tmp_path,
            stages={"preprocess"},
            resume=True,
        )

    monkeypatch.setattr(pipeline_module, "_run_rust_cli", original_run_rust_cli)
    pipeline_module.run_pipeline(
        input_mboxes=[input_file],
        workspace_name=ws_name,
        base_dir=tmp_path,
        stages={"preprocess"},
        resume=True,
    )

    ws = pipeline_module.get_workspace(ws_name, base_dir=tmp_path)
    index_rows = (ws.split_dir / "mbox_index.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(index_rows) == 300


def test_rust_split_index_process_interrupt_resume_soak(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_mbox = tmp_path / "process-interrupt.mbox"
    total_messages = 12000
    _write_rotating_month_synthetic_mbox(input_mbox, total=total_messages)

    ws_name = "rust-process-interrupt"
    ws = pipeline_module.get_workspace(ws_name, base_dir=tmp_path)
    ws.ensure()
    command = [
        sys.executable,
        "-m",
        "ragmail.cli",
        "pipeline",
        str(input_mbox),
        "--workspace",
        ws_name,
        "--base-dir",
        str(tmp_path),
        "--stages",
        "split,preprocess",
        "--checkpoint-interval",
        "1",
        "--no-resume",
    ]

    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=Path.cwd(),
        env={**os.environ, "UV_CACHE_DIR": str((Path.cwd() / ".uv-cache").resolve())},
    )

    interrupted = False
    deadline = time.monotonic() + 90
    loop_start = time.monotonic()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        split_outputs = list(ws.split_dir.glob("*.mbox"))
        split_status = None
        preprocess_status = None
        if ws.state_path.exists():
            try:
                state = json.loads(ws.state_path.read_text(encoding="utf-8"))
                split_status = state.get("stages", {}).get("split", {}).get("status")
                preprocess_status = state.get("stages", {}).get("preprocess", {}).get("status")
            except json.JSONDecodeError:
                # state.json can be observed between write/flush cycles.
                pass
        if split_outputs or split_status == "running" or preprocess_status == "running":
            process.send_signal(signal.SIGINT)
            interrupted = True
            break
        # Fallback to keep this test deterministic on very fast machines.
        if time.monotonic() - loop_start >= 0.02:
            process.send_signal(signal.SIGINT)
            interrupted = True
            break
        time.sleep(0.002)

    if process.poll() is None:
        process.wait(timeout=30)

    assert interrupted, "failed to interrupt process before completion"
    assert process.returncode not in (None, 0), "interrupted run should not exit cleanly"

    pipeline_module.run_pipeline(
        input_mboxes=[input_mbox],
        workspace_name=ws_name,
        base_dir=tmp_path,
        stages={"split", "preprocess"},
        resume=True,
    )

    split_files = sorted(ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox"))
    assert pipeline_module._count_mbox_messages(split_files) == total_messages
    rows = _read_jsonl(ws.split_dir / "mbox_index.jsonl")
    assert len(rows) == total_messages
    offsets = {(row["mbox_file"], row["offset"]) for row in rows}
    assert len(offsets) == total_messages

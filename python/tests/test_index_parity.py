"""Rust index-output contract and robustness tests (via preprocess)."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragmail import pipeline as pipeline_module


REQUIRED_INDEX_KEYS = {
    "email_id",
    "message_id",
    "message_id_lower",
    "mbox_file",
    "offset",
    "length",
}


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _write_synthetic_multi_month_mbox(path: Path, total: int = 600) -> None:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    lines: list[str] = []
    for i in range(1, total + 1):
        month = months[(i - 1) % len(months)]
        day = (i % 28) + 1
        envelope = f"From user{i}@example.com Mon {month} {day:2d} 01:02:03 +0000 2024\n"
        lines.append(envelope)
        if i % 5 != 0:
            lines.append(f"Message-ID: <soak-{i}@example.com>\n")
        lines.append(f"From: User {i} <user{i}@example.com>\n")
        lines.append(f"Date: Mon, {day} {month} 2024 01:02:03 +0000\n")
        lines.append(f"Subject: Soak {i}\n")
        lines.append("\n")
        lines.append(f"Body {i}\n")
        if i % 17 == 0:
            lines.append("From this line is in the body and not a boundary\n")
    path.write_text("".join(lines), encoding="utf-8")


def test_rust_index_output_contract_sample_fixture(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    split_dir = tmp_path / "split"
    split_dir.mkdir(parents=True, exist_ok=True)
    split_file = split_dir / "2022-01.mbox"
    split_file.write_text(
        (
            "From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n"
            "Message-ID: <a@example.com>\n"
            "From: Alpha <alpha@example.com>\n"
            "Date: Mon, 1 Jan 2024 01:02:03 +0000\n"
            "Subject: One\n"
            "\n"
            "Body one.\n"
            "\n"
            "From beta@example.com Tue Jan  2 04:05:06 +0000 2024\n"
            "Message-ID: <b@example.com>\n"
            "From: Beta <beta@example.com>\n"
            "Date: Tue, 2 Jan 2024 04:05:06 +0000\n"
            "Subject: Two\n"
            "\n"
            "Body two.\n"
        ),
        encoding="utf-8",
    )

    rust_output = split_dir / "mbox_index.rust.jsonl"
    indexed = pipeline_module._build_rust_mbox_index(
        split_files=[split_file],
        output_path=rust_output,
        checkpoint_dir=tmp_path / ".checkpoints" / "mbox_index-rs",
        resume=False,
        checkpoint_interval=30,
        progress_callback=lambda payload: payload,
        ws=SimpleNamespace(logs_dir=tmp_path / "logs"),
    )

    rows = _read_jsonl(rust_output)
    assert indexed == 2
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == REQUIRED_INDEX_KEYS
    locations = [(row["mbox_file"], row["offset"], row["length"]) for row in rows]
    assert locations[0][0] == "2022-01.mbox"
    assert locations[1][0] == "2022-01.mbox"
    assert locations[0][1] < locations[1][1]


def test_rust_index_larger_synthetic_fixture(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    split_dir = tmp_path / "split"
    split_dir.mkdir(parents=True, exist_ok=True)
    split_file = split_dir / "2024-01.mbox"

    lines: list[str] = []
    for i in range(1, 121):
        day = (i % 28) + 1
        envelope = f"From user{i}@example.com Mon Jan {day:2d} 01:02:03 +0000 2024\n"
        message_id = f"<msg-{i}@example.com>" if i % 3 != 0 else ""
        lines.append(envelope)
        if message_id:
            lines.append(f"Message-ID: {message_id}\n")
        lines.append(f"From: User {i} <user{i}@example.com>\n")
        lines.append(f"Date: Mon, {day} Jan 2024 01:02:03 +0000\n")
        lines.append(f"Subject: Synthetic {i}\n")
        lines.append("\n")
        lines.append(f"Body {i}\n")
        if i % 10 == 0:
            lines.append("From this is not a boundary\n")
    split_file.write_text("".join(lines), encoding="utf-8")

    rust_output = split_dir / "mbox_index.rust.large.jsonl"
    pipeline_module._build_rust_mbox_index(
        split_files=[split_file],
        output_path=rust_output,
        checkpoint_dir=tmp_path / ".checkpoints" / "mbox_index-rs",
        resume=False,
        checkpoint_interval=30,
        progress_callback=lambda payload: payload,
        ws=SimpleNamespace(logs_dir=tmp_path / "logs"),
    )

    rows = _read_jsonl(rust_output)
    assert len(rows) == 120
    assert len({(row["offset"], row["length"]) for row in rows}) == 120
    assert [row["offset"] for row in rows] == sorted(row["offset"] for row in rows)


def test_split_index_large_corpus_rust_pipeline(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_mbox = tmp_path / "synthetic-large.mbox"
    _write_synthetic_multi_month_mbox(input_mbox, total=800)

    pipeline_module.run_pipeline(
        input_mboxes=[input_mbox],
        workspace_name="rs-large",
        base_dir=tmp_path,
        stages={"split", "preprocess"},
        resume=False,
    )

    ws = pipeline_module.get_workspace("rs-large", base_dir=tmp_path)

    rows = _read_jsonl(ws.split_dir / "mbox_index.jsonl")
    assert len(rows) == 800

    split_names = sorted(path.name for path in ws.split_dir.glob("*.mbox"))
    assert split_names == [
        "2024-01.mbox",
        "2024-02.mbox",
        "2024-03.mbox",
        "2024-04.mbox",
        "2024-05.mbox",
        "2024-06.mbox",
    ]


def test_rust_split_index_resume_idempotent_large_corpus(tmp_path: Path):
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")

    input_mbox = tmp_path / "synthetic-resume.mbox"
    _write_synthetic_multi_month_mbox(input_mbox, total=650)

    pipeline_module.run_pipeline(
        input_mboxes=[input_mbox],
        workspace_name="rs-resume",
        base_dir=tmp_path,
        stages={"split", "preprocess"},
        resume=False,
    )
    ws = pipeline_module.get_workspace("rs-resume", base_dir=tmp_path)
    before_index = (ws.split_dir / "mbox_index.jsonl").read_text(encoding="utf-8")
    before_split_sizes = {
        path.name: path.stat().st_size
        for path in ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox")
    }

    pipeline_module.run_pipeline(
        input_mboxes=[input_mbox],
        workspace_name="rs-resume",
        base_dir=tmp_path,
        stages={"split", "preprocess"},
        resume=True,
    )

    after_index = (ws.split_dir / "mbox_index.jsonl").read_text(encoding="utf-8")
    after_split_sizes = {
        path.name: path.stat().st_size
        for path in ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox")
    }
    assert before_index == after_index
    assert before_split_sizes == after_split_sizes

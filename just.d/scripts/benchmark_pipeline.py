#!/usr/bin/env python3
"""Benchmark Rust pipeline split/index/clean throughput."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun")


@dataclass
class IterationResult:
    iteration: int
    elapsed_s: float
    messages: int
    input_bytes: int
    msg_per_s: float
    bytes_per_s: float
    stage_durations: dict[str, float]
    workspace: str


def write_rotating_month_synthetic_mbox(path: Path, *, total: int) -> None:
    lines: list[str] = []
    for i in range(1, total + 1):
        month = MONTHS[(i - 1) % len(MONTHS)]
        day = (i % 28) + 1
        lines.append(
            f"From user{i}@example.com Mon {month} {day:2d} 01:02:03 +0000 2024\n"
        )
        lines.append(f"Message-ID: <bench-{i}@example.com>\n")
        lines.append(f"From: User {i} <user{i}@example.com>\n")
        lines.append(f"Date: Mon, {day} {month} 2024 01:02:03 +0000\n")
        lines.append(f"Subject: Benchmark {i}\n")
        lines.append("\n")
        lines.append(f"Body {i}\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(lines), encoding="utf-8")


def read_stage_durations(state_path: Path) -> dict[str, float]:
    if not state_path.exists():
        return {}
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    out: dict[str, float] = {}
    stages = payload.get("stages", {})
    if not isinstance(stages, dict):
        return out
    for stage in ("split", "index", "clean"):
        entry = stages.get(stage, {})
        if not isinstance(entry, dict):
            continue
        details = entry.get("details", {})
        if not isinstance(details, dict):
            continue
        duration = details.get("duration_s")
        if isinstance(duration, (int, float)):
            out[stage] = float(duration)
    return out


def human_bytes_per_second(value: float) -> str:
    units = ("B/s", "KB/s", "MB/s", "GB/s")
    size = value
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.2f} {units[idx]}"


def run_iteration(
    *,
    repo_root: Path,
    python_bin: Path,
    input_mbox: Path,
    messages: int,
    input_bytes: int,
    base_dir: Path,
    workspace_name: str,
    checkpoint_interval: int,
    stages: str,
    rust_bin: Path | None,
) -> IterationResult:
    env = os.environ.copy()
    lib_path = str((repo_root / "python" / "lib").resolve())
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = (
        lib_path if not existing_pythonpath else f"{lib_path}{os.pathsep}{existing_pythonpath}"
    )
    if rust_bin is not None:
        env["RAGMAIL_BIN"] = str(rust_bin)
        env["RAGMAIL_RS_BIN"] = str(rust_bin)

    cmd = [
        str(python_bin),
        "-m",
        "ragmail.cli",
        "pipeline",
        str(input_mbox),
        "--workspace",
        workspace_name,
        "--base-dir",
        str(base_dir),
        "--stages",
        stages,
        "--checkpoint-interval",
        str(checkpoint_interval),
        "--no-resume",
    ]

    started = time.perf_counter()
    result = subprocess.run(
        cmd,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = time.perf_counter() - started
    if result.returncode != 0:
        raise RuntimeError(
            f"benchmark iteration failed workspace={workspace_name}\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}\n"
        )

    state_path = base_dir / workspace_name / "state.json"
    stage_durations = read_stage_durations(state_path)
    return IterationResult(
        iteration=0,
        elapsed_s=elapsed,
        messages=messages,
        input_bytes=input_bytes,
        msg_per_s=(messages / elapsed) if elapsed > 0 else 0.0,
        bytes_per_s=(input_bytes / elapsed) if elapsed > 0 else 0.0,
        stage_durations=stage_durations,
        workspace=workspace_name,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--messages",
        type=int,
        default=20000,
        help="Total synthetic emails in generated benchmark input (default: 20000)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Iterations to run (default: 3)",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("bench-workspaces"),
        help="Benchmark workspace root (default: bench-workspaces)",
    )
    parser.add_argument(
        "--stages",
        default="split,preprocess",
        help="Pipeline stages to benchmark (default: split,preprocess)",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=5,
        help="Pipeline checkpoint interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--rust-bin",
        type=Path,
        default=None,
        help="Optional path to ragmail binary (sets RAGMAIL_RS_BIN)",
    )
    parser.add_argument(
        "--build-rust-bin",
        action="store_true",
        help="Build debug ragmail binary before running benchmark",
    )
    parser.add_argument(
        "--keep-workspaces",
        action="store_true",
        help="Keep per-iteration workspaces instead of deleting them after each run",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional JSON report path (default: <base-dir>/reports/pipeline-bench-<ts>.json)",
    )
    return parser


def summarize(rows: list[IterationResult]) -> dict[str, Any]:
    elapsed = [row.elapsed_s for row in rows]
    msg_rate = [row.msg_per_s for row in rows]
    byte_rate = [row.bytes_per_s for row in rows]
    out: dict[str, Any] = {
        "iterations": len(rows),
        "elapsed_s": {
            "mean": statistics.fmean(elapsed),
            "min": min(elapsed),
            "max": max(elapsed),
        },
        "messages_per_s": {
            "mean": statistics.fmean(msg_rate),
            "min": min(msg_rate),
            "max": max(msg_rate),
        },
        "bytes_per_s": {
            "mean": statistics.fmean(byte_rate),
            "min": min(byte_rate),
            "max": max(byte_rate),
        },
    }
    if len(rows) > 1:
        out["elapsed_s"]["stdev"] = statistics.stdev(elapsed)
        out["messages_per_s"]["stdev"] = statistics.stdev(msg_rate)
        out["bytes_per_s"]["stdev"] = statistics.stdev(byte_rate)

    stage_summary: dict[str, dict[str, float]] = {}
    for stage in ("split", "preprocess"):
        stage_values = [
            value for value in (row.stage_durations.get(stage) for row in rows) if value is not None
        ]
        if not stage_values:
            continue
        stats = {
            "mean": statistics.fmean(stage_values),
            "min": min(stage_values),
            "max": max(stage_values),
        }
        if len(stage_values) > 1:
            stats["stdev"] = statistics.stdev(stage_values)
        stage_summary[stage] = stats
    out["stage_duration_s"] = stage_summary
    return out


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.messages < 1:
        parser.error("--messages must be >= 1")
    if args.iterations < 1:
        parser.error("--iterations must be >= 1")

    repo_root = Path(__file__).resolve().parents[2]
    python_bin = Path(sys.executable)

    base_dir = args.base_dir
    if not base_dir.is_absolute():
        base_dir = (repo_root / base_dir).resolve()
    base_dir.mkdir(parents=True, exist_ok=True)

    rust_bin: Path | None = args.rust_bin
    if rust_bin is not None and not rust_bin.is_absolute():
        rust_bin = (repo_root / rust_bin).resolve()

    if args.build_rust_bin:
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(repo_root / "rust/Cargo.toml"),
                "-p",
                "ragmail-cli",
            ],
            cwd=repo_root,
            check=True,
        )
        default_bin = repo_root / "rust/target/debug/ragmail"
        if default_bin.exists():
            rust_bin = default_bin

    if rust_bin is None:
        candidate = repo_root / "rust/target/debug/ragmail"
        if candidate.exists():
            rust_bin = candidate

    input_mbox = base_dir / f"bench-input-{args.messages}.mbox"
    if not input_mbox.exists():
        print(f"[bench] generating synthetic corpus: {input_mbox} ({args.messages} emails)")
        write_rotating_month_synthetic_mbox(input_mbox, total=args.messages)
    input_bytes = input_mbox.stat().st_size

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    reports_dir = base_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = (
        args.report_out.resolve()
        if args.report_out is not None
        else reports_dir / f"pipeline-bench-{timestamp}.json"
    )

    rows: list[IterationResult] = []
    print(f"[bench] mode=rust iterations={args.iterations}")
    for iteration in range(1, args.iterations + 1):
        workspace_name = f"bench-rust-{timestamp}-{iteration:02d}"
        print(f"[bench]   iteration={iteration} workspace={workspace_name}")
        row = run_iteration(
            repo_root=repo_root,
            python_bin=python_bin,
            input_mbox=input_mbox,
            messages=args.messages,
            input_bytes=input_bytes,
            base_dir=base_dir,
            workspace_name=workspace_name,
            checkpoint_interval=args.checkpoint_interval,
            stages=args.stages,
            rust_bin=rust_bin,
        )
        row.iteration = iteration
        rows.append(row)
        print(
            "[bench]   "
            f"elapsed={row.elapsed_s:.3f}s "
            f"msg/s={row.msg_per_s:.1f} "
            f"throughput={human_bytes_per_second(row.bytes_per_s)}"
        )
        if not args.keep_workspaces:
            shutil.rmtree(base_dir / workspace_name, ignore_errors=True)

    summary = summarize(rows)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(repo_root),
        "python_bin": str(python_bin),
        "rust_bin": str(rust_bin) if rust_bin is not None else None,
        "config": {
            "mode": "rust",
            "messages": args.messages,
            "iterations": args.iterations,
            "stages": args.stages,
            "checkpoint_interval": args.checkpoint_interval,
            "base_dir": str(base_dir),
            "keep_workspaces": args.keep_workspaces,
        },
        "input": {
            "mbox": str(input_mbox),
            "bytes": input_bytes,
        },
        "results": [
            {
                "mode": "rust",
                "iteration": row.iteration,
                "elapsed_s": row.elapsed_s,
                "messages": row.messages,
                "input_bytes": row.input_bytes,
                "messages_per_s": row.msg_per_s,
                "bytes_per_s": row.bytes_per_s,
                "stage_durations_s": row.stage_durations,
                "workspace": row.workspace,
            }
            for row in rows
        ],
        "summary": {"rust": summary},
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("")
    print("Mode Summary")
    print(
        "- rust: "
        f"elapsed_mean={summary['elapsed_s']['mean']:.3f}s "
        f"msg/s_mean={summary['messages_per_s']['mean']:.1f} "
        f"throughput_mean={human_bytes_per_second(summary['bytes_per_s']['mean'])}"
    )
    stage_stats = summary.get("stage_duration_s", {})
    if stage_stats:
        formatted = ", ".join(
            f"{name}={values['mean']:.3f}s" for name, values in stage_stats.items()
        )
        print(f"  stage_mean: {formatted}")

    print(f"[bench] report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

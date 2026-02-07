#!/usr/bin/env python3
"""Run benchmark harness and enforce a minimum Rust throughput floor."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--min-msg-per-s", type=float, default=1.0)
    parser.add_argument("--checkpoint-interval", type=int, default=5)
    parser.add_argument("--base-dir", type=Path, default=Path("bench-workspaces"))
    parser.add_argument("--report-out", type=Path, default=None)
    parser.add_argument("--build-rust-bin", action="store_true")
    parser.add_argument(
        "--keep-workspaces",
        action="store_true",
        help="Keep benchmark workspaces and reports for inspection",
    )
    return parser


def _extract_msg_per_s(report: dict) -> float | None:
    summary = report.get("summary", {})
    if not isinstance(summary, dict):
        return None

    rust_summary = summary.get("rust")
    if isinstance(rust_summary, dict):
        messages = rust_summary.get("messages_per_s")
        if isinstance(messages, dict):
            value = messages.get("mean")
            if isinstance(value, (int, float)):
                return float(value)

    messages = summary.get("messages_per_s")
    if isinstance(messages, dict):
        value = messages.get("mean")
        if isinstance(value, (int, float)):
            return float(value)

    return None


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.messages < 1:
        parser.error("--messages must be >= 1")
    if args.iterations < 1:
        parser.error("--iterations must be >= 1")
    if args.min_msg_per_s <= 0:
        parser.error("--min-msg-per-s must be > 0")

    script = Path(__file__).resolve().parent / "benchmark_pipeline.py"
    cmd = [
        sys.executable,
        str(script),
        "--messages",
        str(args.messages),
        "--iterations",
        str(args.iterations),
        "--checkpoint-interval",
        str(args.checkpoint_interval),
        "--base-dir",
        str(args.base_dir),
    ]
    if args.report_out is not None:
        cmd.extend(["--report-out", str(args.report_out)])
    if args.build_rust_bin:
        cmd.append("--build-rust-bin")
    if args.keep_workspaces:
        cmd.append("--keep-workspaces")

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        return result.returncode

    report_path: Path
    if args.report_out is not None:
        report_path = args.report_out
    else:
        reports_dir = args.base_dir / "reports"
        reports = sorted(reports_dir.glob("pipeline-bench-*.json"))
        if not reports:
            print("benchmark threshold check failed: no report generated", file=sys.stderr)
            return 1
        report_path = reports[-1]

    report = json.loads(report_path.read_text(encoding="utf-8"))
    msg_per_s = _extract_msg_per_s(report)
    if msg_per_s is None:
        print(
            f"benchmark threshold check failed: missing message throughput in {report_path}",
            file=sys.stderr,
        )
        return 1

    print(
        f"[bench-threshold] rust msg/s={msg_per_s:.1f} "
        f"(min required {args.min_msg_per_s:.1f})"
    )
    if msg_per_s < args.min_msg_per_s:
        print(
            f"benchmark threshold check failed: {msg_per_s:.1f} < {args.min_msg_per_s:.1f}",
            file=sys.stderr,
        )
        return 1

    print(f"[bench-threshold] PASS ({report_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

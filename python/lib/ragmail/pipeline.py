"""Pipeline orchestration for ragmail."""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import signal
import shlex
import subprocess
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Iterable
import re
import threading
import time
from datetime import datetime

from ragmail.common import signals
from ragmail.common.terminal import Colors, format_time, format_bytes
from ragmail.workspace import Workspace, default_cache_root, get_workspace
from ragmail.config import get_settings


def _resolve_repo_root() -> Path:
    explicit_repo_root = os.environ.get("RAGMAIL_RS_REPO_ROOT")
    if explicit_repo_root:
        return Path(explicit_repo_root).expanduser().resolve()
    if os.environ.get("RAGMAIL_RS_BIN"):
        return Path.cwd()

    # Search upward for the Rust workspace to keep this path stable if package
    # layout changes (e.g., `lib/` moving under `python/`).
    for parent in Path(__file__).resolve().parents:
        if (parent / "rust/Cargo.toml").exists():
            return parent
    return Path.cwd()


REPO_ROOT = _resolve_repo_root()
_RUST_SPLIT_COMPLETE_PATTERN = re.compile(
    r"split complete: processed=(\d+) written=(\d+) skipped=(\d+) errors=(\d+)(?: last_position=(\d+))?"
)
_RUST_INDEX_COMPLETE_PATTERN = re.compile(
    r"index complete: indexed=(\d+) last_position=(\d+)"
)
_RUST_CLEAN_COMPLETE_PATTERN = re.compile(
    r"clean complete: processed=(\d+) clean=(\d+) spam=(\d+) errors=(\d+)"
)


def run_pipeline(
    input_mboxes: Iterable[Path],
    workspace_name: str,
    years: Iterable[int] | None = None,
    resume: bool = True,
    refresh: bool = False,
    base_dir: Path | None = None,
    cache_dir: Path | None = None,
    clean_dir: Path | None = None,
    ingest_batch_size: int | None = None,
    embedding_batch_size: int | None = None,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
    skip_exists_check: bool | None = None,
    checkpoint_interval: int | None = None,
    preprocess_workers: int | None = None,
    compact_every: int | None = None,
    repair_embeddings: bool = True,
    embeddings_dir: Path | None = None,
    stages: set[str] | None = None,
) -> Workspace:
    ws = get_workspace(workspace_name, base_dir=base_dir)
    workspace_exists = ws.root.exists() and any(ws.root.iterdir())
    if workspace_exists and not resume:
        raise RuntimeError(
            f"Workspace already exists at {ws.root}. "
            "Use --resume to continue or choose a new workspace."
        )
    ws.ensure()
    ws.apply_env(cache_dir=cache_dir, base_dir=base_dir)
    cache_root = cache_dir or default_cache_root(base_dir)

    inputs = [Path(p).resolve() for p in input_mboxes]

    if stages is None:
        # Default pipeline skips the explicit warmup stage; vectorize handles model/cache warmup.
        stages = {"split", "preprocess", "vectorize", "ingest"}
    else:
        stage_aliases = {"download": "model", "clean": "preprocess", "index": "preprocess"}
        stages = {stage_aliases.get(stage, stage) for stage in stages}

    def _selected(stage: str) -> bool:
        return stage in stages

    def _refresh_selected() -> set[str]:
        return set(stages)

    if not inputs and _selected("split"):
        raise ValueError("Split stage requires at least one input MBOX.")

    if refresh:
        _apply_refresh(ws, _refresh_selected())

    resume_effective = resume and not refresh
    skip_exists_effective = skip_exists_check
    if refresh and _selected("ingest"):
        skip_exists_effective = True

    signals.reset_interrupt()

    pipeline_start = time.monotonic()
    _print_header(
        ws=ws,
        inputs=inputs,
        years=years,
        resume=resume_effective,
        refresh=refresh,
        cache_root=cache_root,
    )
    ordered_stages = ["model", "split", "preprocess", "vectorize", "ingest"]
    stage_display = _StageDisplay([stage for stage in ordered_stages if stage in stages])
    stage_display.render(force=True)
    stop_spinner = threading.Event()
    spinner_thread = threading.Thread(
        target=stage_display.spin,
        args=(stop_spinner,),
        daemon=True,
    )
    spinner_thread.start()

    interrupt_note_printed = False

    def _on_interrupt(signum: int) -> None:
        nonlocal interrupt_note_printed
        if interrupt_note_printed:
            return
        interrupt_note_printed = True
        try:
            sig_name = signal.Signals(signum).name
        except Exception:
            sig_name = str(signum)
        stage_display.note("Interrupt received. Finishing current batch and saving checkpoints...")
        stop_spinner.set()
        _log_event(ws, "pipeline", "WARN", f"interrupt received ({sig_name})")

    signals.install_signal_handlers(_on_interrupt)

    if not resume_effective and not refresh:
        ws.reset_state()

    for input_mbox in inputs:
        _ensure_link_unique(ws.inputs_dir, input_mbox)

    if _selected("model"):
        stage_display.update("model", "running")
        ws.update_stage("model", "running")
        _log_event(ws, "model", "INFO", "start")
        model_start = time.monotonic()
        cache_start_bytes = _path_size_bytes(cache_root)
        warmup_error: BaseException | None = None

        def _warmup_runner() -> None:
            nonlocal warmup_error
            try:
                _warmup_dependencies()
            except BaseException as exc:  # pragma: no cover - surfaced below
                warmup_error = exc

        warmup_thread = threading.Thread(target=_warmup_runner, daemon=True)
        try:
            with _stage_log(ws, "model"):
                warmup_thread.start()
                while warmup_thread.is_alive():
                    current_bytes = _path_size_bytes(cache_root)
                    stage_display.update_progress(
                        "model",
                        processed=0,
                        meta={
                            "downloaded_bytes": max(0, current_bytes - cache_start_bytes),
                            "cache_bytes": current_bytes,
                        },
                    )
                    time.sleep(0.5)
                warmup_thread.join()
                if warmup_error is not None:
                    raise warmup_error
        except KeyboardInterrupt:
            stage_display.update("model", "interrupted")
            ws.update_stage("model", "interrupted")
            _log_event(ws, "model", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("model", "failed")
            ws.update_stage("model", "failed")
            _log_event(ws, "model", "ERROR", "failed")
            raise
        else:
            model_duration = time.monotonic() - model_start
            cache_final_bytes = _path_size_bytes(cache_root)
            stage_display.update_progress(
                "model",
                processed=0,
                meta={
                    "downloaded_bytes": max(0, cache_final_bytes - cache_start_bytes),
                    "cache_bytes": cache_final_bytes,
                },
            )
            stage_display.update("model", "done", duration_s=model_duration)
            ws.update_stage(
                "model",
                "done",
                {
                    "duration_s": model_duration,
                    "downloaded_bytes": max(0, cache_final_bytes - cache_start_bytes),
                },
            )
            _log_event(ws, "model", "INFO", f"done in {model_duration:.2f}s")
    else:
        stage_display.update("model", "skipped")
        _log_event(ws, "model", "INFO", "skipped (not selected)")

    existing_split_files = sorted(
        ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox")
    )
    split_outputs_exist = bool(existing_split_files)
    if resume_effective and _selected("split") and ws.stage_done("split") and not split_outputs_exist:
        _log_event(
            ws,
            "split",
            "WARN",
            "stage marked done but outputs missing; rerunning split",
        )
    split_total = 0
    split_errors = 0
    split_should_run = _selected("split") and (
        (not resume_effective)
        or (not ws.stage_done("split"))
        or (not split_outputs_exist)
    )
    if split_should_run:
        split_start = time.monotonic()
        _log_event(ws, "split", "INFO", "start")
        stage_display.update("split", "running")
        split_total_bytes = sum(p.stat().st_size for p in inputs)
        split_existing = 0
        if resume_effective and split_outputs_exist:
            split_existing = _count_mbox_messages(existing_split_files)
        split_resume_mode = resume_effective and split_outputs_exist
        if split_resume_mode:
            split_checkpoint_dir = ws.checkpoints_dir / "split-rs"
            missing_split_checkpoint = any(
                not _split_checkpoint_path(split_checkpoint_dir, input_mbox).exists()
                for input_mbox in inputs
            )
            if missing_split_checkpoint:
                _log_event(
                    ws,
                    "split",
                    "WARN",
                    "rust split resume checkpoint missing; restarting split stage to avoid duplicate appends",
                )
                for existing in existing_split_files:
                    existing.unlink()
                if split_checkpoint_dir.exists():
                    shutil.rmtree(split_checkpoint_dir)
                existing_split_files = []
                split_outputs_exist = False
                split_existing = 0
                split_resume_mode = False
        split_bytes_processed = 0
        ws.update_stage("split", "running", {"inputs": [str(p) for p in inputs]})
        stage_display.update_progress(
            "split",
            processed=split_existing,
            meta={
                "bytes_processed": split_bytes_processed,
                "bytes_total": split_total_bytes,
            },
        )
        split_processed = 0
        split_skipped = 0
        split_written = 0
        try:
            with _stage_log(ws, "split"):
                split_errors = 0
                split_checkpoint_dir = ws.checkpoints_dir / "split-rs"
                if not split_resume_mode and split_checkpoint_dir.exists():
                    shutil.rmtree(split_checkpoint_dir)
                for input_mbox in inputs:
                    input_size = input_mbox.stat().st_size
                    checkpoint_path = _split_checkpoint_path(split_checkpoint_dir, input_mbox)
                    split_stats = _run_rust_split(
                        input_mbox=input_mbox,
                        output_dir=ws.split_dir,
                        years=years,
                        checkpoint_path=checkpoint_path,
                        resume=split_resume_mode,
                        checkpoint_interval=checkpoint_interval or 30,
                        stage="split",
                        ws=ws,
                    )
                    split_processed += split_stats["processed"]
                    split_written += split_stats["written"]
                    split_skipped += split_stats["skipped"]
                    split_errors += split_stats["errors"]
                    split_bytes_processed += input_size
                    stage_display.update_progress(
                        "split",
                        processed=split_existing + split_processed,
                        skipped=split_skipped,
                        meta={
                            "bytes_processed": split_bytes_processed,
                            "bytes_total": split_total_bytes,
                        },
                    )
                    _log_progress(
                        ws,
                        "split",
                        split_existing + split_processed,
                        skipped=split_skipped,
                    )

                stage_display.update_progress(
                    "split",
                    processed=split_existing + split_processed,
                    skipped=split_skipped,
                    meta={
                        "bytes_processed": split_bytes_processed,
                        "bytes_total": split_total_bytes,
                    },
                )
        except KeyboardInterrupt:
            stage_display.update("split", "interrupted")
            ws.update_stage("split", "interrupted")
            _log_event(ws, "split", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("split", "failed")
            ws.update_stage("split", "failed")
            _log_event(ws, "split", "ERROR", "failed")
            raise
        else:
            split_total = split_existing + split_processed
            ws.update_stage(
                "split",
                "done",
                {
                    "output_dir": str(ws.split_dir),
                    "processed": split_total,
                    "written": split_written,
                    "skipped": split_skipped,
                    "duration_s": time.monotonic() - split_start,
                },
            )
            stage_display.update("split", "done", duration_s=time.monotonic() - split_start)
            _log_event(
                ws,
                "split",
                "INFO",
                f"done in {time.monotonic() - split_start:.2f}s",
            )
    else:
        if _selected("split"):
            stage_display.update("split", "skipped")
            _log_event(ws, "split", "INFO", "skipped (already done)")
        else:
            stage_display.update("split", "skipped")
            _log_event(ws, "split", "INFO", "skipped (not selected)")

    split_files = sorted(ws.split_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].mbox"))
    if _selected("preprocess") and not split_files:
        _log_event(ws, "split", "ERROR", "no split outputs found")
        raise RuntimeError(
            f"No split MBOX files found in {ws.split_dir}. "
            "Provide input MBOX files or run `ragmail pipeline <mbox> --workspace <name>`. "
            "If the workspace was moved or split outputs were deleted, rerun "
            "`ragmail pipeline <mbox> --workspace <name> --stages split` (or `--refresh`)."
        )

    configured_preprocess_workers = preprocess_workers
    if configured_preprocess_workers is None:
        configured_preprocess_workers = get_settings().preprocess_workers
    preprocess_workers_effective = max(1, int(configured_preprocess_workers))

    expected_clean = {ws.clean_dir / f"{mbox.stem}.clean.jsonl" for mbox in split_files}
    expected_spam = {ws.spam_dir / f"{mbox.stem}.spam.jsonl" for mbox in split_files}
    expected_summary = {ws.reports_dir / f"{mbox.name}.summary" for mbox in split_files}
    preprocess_index_parts_dir = ws.checkpoints_dir / "preprocess-rs" / "index-parts"
    expected_index_parts = {preprocess_index_parts_dir / f"{mbox.name}.jsonl" for mbox in split_files}
    existing_clean = set(ws.clean_dir.glob("*.clean.jsonl"))
    existing_spam = set(ws.spam_dir.glob("*.spam.jsonl"))
    existing_summary = set(ws.reports_dir.glob("*.summary"))
    existing_index_parts = set(preprocess_index_parts_dir.glob("*.jsonl"))
    missing_preprocess_outputs = bool(
        split_files
        and (
            not expected_clean.issubset(existing_clean)
            or not expected_spam.issubset(existing_spam)
            or not expected_summary.issubset(existing_summary)
            or not expected_index_parts.issubset(existing_index_parts)
        )
    )
    if (
        resume_effective
        and _selected("preprocess")
        and ws.stage_done("preprocess")
        and missing_preprocess_outputs
    ):
        _log_event(
            ws,
            "preprocess",
            "WARN",
            "stage marked done but outputs missing; rerunning preprocess (resume)",
        )
    clean_should_run = _selected("preprocess") and (
        (not resume_effective) or (not ws.stage_done("preprocess")) or missing_preprocess_outputs
    )
    clean_total = 0
    clean_written = 0
    clean_spam = 0
    clean_errors = 0
    if clean_should_run:
        clean_start = time.monotonic()
        _log_event(ws, "preprocess", "INFO", "start")
        stage_display.update("preprocess", "running")
        if clean_total == 0:
            clean_total = _count_mbox_messages(split_files)
        stage_display.set_total("preprocess", clean_total)
        ws.update_stage(
            "preprocess",
            "running",
            {"files": len(split_files), "workers": preprocess_workers_effective},
        )
        clean_outputs: list[Path] = []
        try:
            with _stage_log(ws, "preprocess"):
                clean_processed = 0
                clean_skipped = 0
                preprocess_index_parts_dir.mkdir(parents=True, exist_ok=True)
                preprocess_tasks: list[tuple[Path, Path, Path, Path, Path]] = []
                for mbox in split_files:
                    clean_jsonl = ws.clean_dir / f"{mbox.stem}.clean.jsonl"
                    spam_jsonl = ws.spam_dir / f"{mbox.stem}.spam.jsonl"
                    summary_output = ws.reports_dir / f"{mbox.name}.summary"
                    index_part_output = preprocess_index_parts_dir / f"{mbox.name}.jsonl"
                    if (
                        resume_effective
                        and clean_jsonl.exists()
                        and spam_jsonl.exists()
                        and summary_output.exists()
                        and index_part_output.exists()
                    ):
                        clean_outputs.append(clean_jsonl)
                        continue

                    preprocess_tasks.append(
                        (mbox, clean_jsonl, spam_jsonl, summary_output, index_part_output)
                    )

                def _record_preprocess_stats(*, stats: dict[str, int], clean_jsonl: Path) -> None:
                    nonlocal clean_processed, clean_skipped, clean_written, clean_spam, clean_errors
                    if clean_jsonl.exists():
                        clean_outputs.append(clean_jsonl)
                    clean_written += stats["clean"]
                    clean_spam += stats["spam"]
                    clean_errors += stats["errors"]
                    clean_processed += stats["processed"]
                    clean_skipped += stats["spam"] + stats["errors"]
                    stage_display.update_progress(
                        "preprocess",
                        processed=clean_processed,
                        skipped=clean_skipped,
                        meta={"spam": clean_spam, "errors": clean_errors},
                    )
                    _log_progress(
                        ws,
                        "preprocess",
                        clean_processed,
                        total=clean_total,
                        skipped=clean_skipped,
                        spam=clean_spam,
                        errors=clean_errors,
                    )

                worker_count = min(preprocess_workers_effective, max(1, len(preprocess_tasks)))
                if worker_count > 1:
                    stage_display.note(f"Preprocess parallelism: {worker_count} workers")

                if worker_count <= 1:
                    for (
                        mbox,
                        clean_jsonl,
                        spam_jsonl,
                        summary_output,
                        index_part_output,
                    ) in preprocess_tasks:
                        stats = _run_rust_clean(
                            input_mbox=mbox,
                            output_clean=clean_jsonl,
                            output_spam=spam_jsonl,
                            summary_output=summary_output,
                            index_output=index_part_output,
                            stage="preprocess",
                            ws=ws,
                        )
                        _record_preprocess_stats(stats=stats, clean_jsonl=clean_jsonl)
                else:
                    with ThreadPoolExecutor(max_workers=worker_count) as pool:
                        futures = {
                            pool.submit(
                                _run_rust_clean,
                                input_mbox=mbox,
                                output_clean=clean_jsonl,
                                output_spam=spam_jsonl,
                                summary_output=summary_output,
                                index_output=index_part_output,
                                stage="preprocess",
                                ws=ws,
                            ): clean_jsonl
                            for (
                                mbox,
                                clean_jsonl,
                                spam_jsonl,
                                summary_output,
                                index_part_output,
                            ) in preprocess_tasks
                        }
                        for future in as_completed(futures):
                            clean_jsonl = futures[future]
                            stats = future.result()
                            _record_preprocess_stats(stats=stats, clean_jsonl=clean_jsonl)

                _merge_index_parts(
                    parts_dir=preprocess_index_parts_dir,
                    split_files=split_files,
                    output_path=ws.split_dir / "mbox_index.jsonl",
                )
        except KeyboardInterrupt:
            stage_display.update("preprocess", "interrupted")
            ws.update_stage("preprocess", "interrupted")
            _log_event(ws, "preprocess", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("preprocess", "failed")
            ws.update_stage("preprocess", "failed")
            _log_event(ws, "preprocess", "ERROR", "failed")
            raise
        else:
            ws.update_stage(
                "preprocess",
                "done",
                {
                    "clean_files": len(clean_outputs),
                    "processed": clean_total,
                    "written": clean_written,
                    "skipped": clean_spam + clean_errors,
                    "index_output": str(ws.split_dir / "mbox_index.jsonl"),
                    "duration_s": time.monotonic() - clean_start,
                },
            )
            stage_display.update("preprocess", "done", duration_s=time.monotonic() - clean_start)
            _log_event(
                ws,
                "preprocess",
                "INFO",
                f"done in {time.monotonic() - clean_start:.2f}s",
            )
    else:
        if _selected("preprocess"):
            stage_display.update("preprocess", "skipped")
            _log_event(ws, "preprocess", "INFO", "skipped (already done)")
        else:
            stage_display.update("preprocess", "skipped")
            _log_event(ws, "preprocess", "INFO", "skipped (not selected)")

    clean_root = clean_dir or ws.clean_dir
    if split_files and clean_root == ws.clean_dir:
        expected_clean = {clean_root / f"{mbox.stem}.clean.jsonl" for mbox in split_files}
        clean_files = sorted(path for path in expected_clean if path.exists())
    else:
        clean_files = sorted(clean_root.glob("*.clean.jsonl"))
    if (_selected("vectorize") or _selected("ingest")) and not clean_files:
        _log_event(ws, "preprocess", "ERROR", "no clean outputs found")
        raise RuntimeError(
            f"No clean JSONL files found in {clean_root}. "
            "Run `ragmail pipeline <mbox> --workspace <name> --stages preprocess` "
            "or rerun from split+preprocess with the original inputs."
        )

    if embeddings_dir is not None:
        embeddings_root = embeddings_dir
    elif clean_dir is not None:
        embeddings_root = clean_dir.parent / "embeddings"
    else:
        embeddings_root = ws.embeddings_dir
    if not _selected("vectorize"):
        stage_display.update("vectorize", "skipped")
        _log_event(ws, "vectorize", "INFO", "skipped (not selected)")

    vectorize_count = 0
    vectorize_total = 0
    if _selected("vectorize") and (not resume_effective or not ws.stage_done("vectorize")):
        vectorize_start = time.monotonic()
        _log_event(ws, "vectorize", "INFO", "start")
        stage_display.update("vectorize", "running")
        vectorize_total = _count_jsonl_lines(clean_files)
        stage_display.set_total("vectorize", vectorize_total)
        ws.update_stage("vectorize", "running", {"files": len(clean_files)})
        from ragmail.vectorize.run import vectorize_files
        try:
            with _stage_log(ws, "vectorize"):
                vectorize_seen = 0

                def _vectorize_progress(payload):
                    nonlocal vectorize_seen
                    vectorize_seen = payload["processed"]
                    stage_display.update_progress(
                        "vectorize",
                        processed=vectorize_seen,
                    )
                    _log_progress(
                        ws,
                        "vectorize",
                        vectorize_seen,
                        total=vectorize_total,
                    )

                vectorize_count = vectorize_files(
                    clean_files,
                    output_dir=embeddings_root,
                    checkpoint_dir=ws.checkpoints_dir / "vectorize",
                    errors_path=ws.logs_dir / "vectorize.errors.jsonl",
                    resume=resume_effective,
                    progress_callback=_vectorize_progress,
                    quiet=True,
                    vectorize_batch_size=ingest_batch_size,
                    embedding_batch_size=embedding_batch_size,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                    checkpoint_interval=checkpoint_interval,
                )
        except KeyboardInterrupt:
            stage_display.update("vectorize", "interrupted")
            ws.update_stage("vectorize", "interrupted")
            _log_event(ws, "vectorize", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("vectorize", "failed")
            ws.update_stage("vectorize", "failed")
            _log_event(ws, "vectorize", "ERROR", "failed")
            raise
        else:
            ws.update_stage(
                "vectorize",
                "done",
                {
                    "output_dir": str(embeddings_root),
                    "processed": vectorize_total,
                    "written": vectorize_count,
                    "duration_s": time.monotonic() - vectorize_start,
                },
            )
            stage_display.update("vectorize", "done", duration_s=time.monotonic() - vectorize_start)
            _log_event(
                ws,
                "vectorize",
                "INFO",
                f"done in {time.monotonic() - vectorize_start:.2f}s",
            )
    elif _selected("vectorize"):
        stage_display.update("vectorize", "skipped")
        _log_event(ws, "vectorize", "INFO", "skipped (already done)")

    ingest_count = 0
    ingest_total = 0
    ingest_skipped = 0
    if not _selected("ingest"):
        stage_display.update("ingest", "skipped")
        _log_event(ws, "ingest", "INFO", "skipped (not selected)")
    elif not resume_effective or not ws.stage_done("ingest"):
        ingest_start = time.monotonic()
        _log_event(ws, "ingest", "INFO", "start")
        stage_display.update("ingest", "running")
        ingest_total = _count_jsonl_lines(clean_files)
        stage_display.set_total("ingest", ingest_total)
        ws.update_stage("ingest", "running", {"files": len(clean_files)})
        db_path = ws.db_dir / "email_search.lancedb"
        from ragmail.ingest.run import ingest_files_from_embeddings
        try:
            with _stage_log(ws, "ingest"):
                ingest_seen = 0
                ingest_skipped_existing = 0

                def _ingest_progress(payload):
                    nonlocal ingest_seen, ingest_skipped_existing
                    ingest_seen = payload["processed"]
                    ingest_skipped_existing = payload.get("skipped_exists", payload.get("skipped", 0))
                    ingest_skipped_errors = payload.get("skipped_errors", 0)
                    ingest_skipped_total = ingest_skipped_existing + ingest_skipped_errors
                    stage_display.update_progress(
                        "ingest",
                        processed=ingest_seen,
                        skipped=ingest_skipped_total,
                        meta={
                            "skipped_exists": ingest_skipped_existing,
                            "skipped_errors": ingest_skipped_errors,
                        },
                    )
                    _log_progress(
                        ws,
                        "ingest",
                        ingest_seen,
                        total=ingest_total,
                        skipped=ingest_skipped_total,
                        errors=ingest_skipped_errors,
                    )

                def _ingest_compaction(payload):
                    phase = payload.get("phase")
                    reason = payload.get("reason", "periodic")
                    processed = payload.get("processed")
                    if phase == "start":
                        suffix = f" at {processed:,}" if processed is not None else ""
                        stage_display.note(f"Compacting tables ({reason}){suffix}...")
                        _log_event(
                            ws,
                            "ingest",
                            "INFO",
                            f"compaction start ({reason}){suffix}",
                        )
                    elif phase == "done":
                        duration = payload.get("duration_s")
                        suffix = f" at {processed:,}" if processed is not None else ""
                        if duration is None:
                            _log_event(
                                ws,
                                "ingest",
                                "INFO",
                                f"compaction done ({reason}){suffix}",
                            )
                        else:
                            _log_event(
                                ws,
                                "ingest",
                                "INFO",
                                f"compaction done ({reason}){suffix} in {duration:.2f}s",
                            )
                        stage_display.note("")
                    elif phase == "fts_start":
                        stage_display.note("Building FTS index...")
                        _log_event(ws, "ingest", "INFO", "fts index start")
                    elif phase == "fts_done":
                        stage_display.note("")
                        _log_event(ws, "ingest", "INFO", "fts index done")

                from ragmail.vectorize.store import default_embedding_path

                embeddings_available = False
                if embeddings_root.exists():
                    for path in clean_files:
                        if default_embedding_path(path, embeddings_root).exists():
                            embeddings_available = True
                            break

                if not embeddings_root.exists():
                    raise RuntimeError(
                        f"Embeddings directory not found: {embeddings_root}"
                    )
                if not embeddings_available:
                    raise RuntimeError(
                        f"No embeddings found in {embeddings_root}. "
                        "Run the vectorize stage first."
                    )

                if skip_exists_effective is None:
                    skip_exists_effective = True
                missing = [
                    str(default_embedding_path(path, embeddings_root))
                    for path in clean_files
                    if not default_embedding_path(path, embeddings_root).exists()
                ]
                if missing and not repair_embeddings:
                    raise RuntimeError(
                        "Embeddings DBs missing for: "
                        + ", ".join(missing[:3])
                        + (" ..." if len(missing) > 3 else "")
                    )
                if missing and repair_embeddings:
                    _log_event(
                        ws,
                        "ingest",
                        "INFO",
                        f"repairing missing embeddings for {len(missing)} files",
                    )

                ingest_count = ingest_files_from_embeddings(
                    clean_files,
                    embeddings_dir=embeddings_root,
                    db_path=db_path,
                    checkpoint_dir=ws.checkpoints_dir,
                    errors_path=ws.logs_dir / "ingest.errors.jsonl",
                    resume=resume_effective,
                    progress_callback=_ingest_progress,
                    compaction_callback=_ingest_compaction,
                    quiet=True,
                    ingest_batch_size=ingest_batch_size,
                    embedding_batch_size=embedding_batch_size,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                    skip_exists_check=skip_exists_effective,
                    checkpoint_interval=checkpoint_interval,
                    compact_every=compact_every,
                    bulk_import=refresh,
                    repair_missing_embeddings=repair_embeddings,
                )
                ingest_skipped = max(0, ingest_total - ingest_count)
        except KeyboardInterrupt:
            stage_display.update("ingest", "interrupted")
            ws.update_stage("ingest", "interrupted")
            _log_event(ws, "ingest", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("ingest", "failed")
            ws.update_stage("ingest", "failed")
            _log_event(ws, "ingest", "ERROR", "failed")
            raise
        else:
            ws.update_stage(
                "ingest",
                "done",
                {
                    "db": str(db_path),
                    "processed": ingest_total,
                    "written": ingest_count,
                    "skipped": ingest_skipped,
                    "duration_s": time.monotonic() - ingest_start,
                },
            )
            stage_display.update("ingest", "done", duration_s=time.monotonic() - ingest_start)
            _log_event(
                ws,
                "ingest",
                "INFO",
                f"done in {time.monotonic() - ingest_start:.2f}s",
            )
    else:
        stage_display.update("ingest", "skipped")
        _log_event(ws, "ingest", "INFO", "skipped (already done)")

    stop_spinner.set()
    spinner_thread.join(timeout=1)
    stage_display.finish()
    split_written = _count_mbox_messages(split_files) if split_files else 0
    emails_found = split_total or split_written
    clean_total = clean_total or emails_found
    clean_written = _count_jsonl_lines(clean_files)
    clean_spam = _count_jsonl_lines(list(ws.spam_dir.glob("*.spam.jsonl")))
    ingest_total = ingest_total or clean_written
    ingest_skipped = ingest_skipped or max(0, ingest_total - ingest_count)
    ingest_errors = _count_jsonl_lines([ws.logs_dir / "ingest.errors.jsonl"])
    _print_summary(
        ws=ws,
        mailbox_files=len(inputs),
        emails_found=emails_found,
        split_total=split_total,
        split_written=split_written,
        split_errors=split_errors,
        clean_total=clean_total,
        clean_written=clean_written,
        clean_spam=clean_spam,
        clean_errors=clean_errors,
        vectorize_count=vectorize_count,
        ingest_total=ingest_total,
        ingest_count=ingest_count,
        ingest_errors=ingest_errors,
        total_duration_s=time.monotonic() - pipeline_start,
    )

    return ws


def _ensure_link_unique(target_dir: Path, source: Path) -> Path:
    target = target_dir / source.name
    if target.exists():
        try:
            if target.resolve() == source.resolve():
                return target
        except OSError:
            pass
        stem = source.stem
        suffix = source.suffix
        idx = 1
        while True:
            candidate = target_dir / f"{stem}-{idx}{suffix}"
            if not candidate.exists():
                target = candidate
                break
            idx += 1
    try:
        target.symlink_to(source)
    except OSError:
        shutil.copy2(source, target)
    return target


def _apply_refresh(ws: Workspace, stages: set[str]) -> None:
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    archive_root = ws.root / "old" / timestamp

    def _archive_dir(path: Path, name: str) -> None:
        if not path.exists():
            return
        try:
            has_entries = path.is_dir() and any(path.iterdir())
        except OSError:
            has_entries = False
        if not has_entries and path.is_dir():
            return
        archive_root.mkdir(parents=True, exist_ok=True)
        target = archive_root / name
        shutil.move(str(path), str(target))
        path.mkdir(parents=True, exist_ok=True)

    def _clear_checkpoints(root: Path) -> None:
        if not root.exists():
            return
        for checkpoint in root.glob("*.checkpoint.json"):
            checkpoint.unlink()

    if "split" in stages:
        _archive_dir(ws.split_dir, "split")
        rust_split_checkpoint_dir = ws.checkpoints_dir / "split-rs"
        if rust_split_checkpoint_dir.exists():
            shutil.rmtree(rust_split_checkpoint_dir)
    if "preprocess" in stages:
        _archive_dir(ws.clean_dir, "clean")
        _archive_dir(ws.spam_dir, "spam")
        _archive_dir(ws.reports_dir, "reports")
        index_path = ws.split_dir / "mbox_index.jsonl"
        if index_path.exists():
            index_path.unlink()
        preprocess_checkpoint_dir = ws.checkpoints_dir / "preprocess-rs"
        if preprocess_checkpoint_dir.exists():
            shutil.rmtree(preprocess_checkpoint_dir)
    if "vectorize" in stages:
        _archive_dir(ws.embeddings_dir, "embeddings")
        _archive_dir(ws.checkpoints_dir / "vectorize", "checkpoints-vectorize")
    if "ingest" in stages:
        _archive_dir(ws.db_dir, "db")
        _clear_checkpoints(ws.checkpoints_dir)

    state = ws.load_state()
    stages_state = state.get("stages", {})
    for stage in stages:
        stages_state.pop(stage, None)
    state["stages"] = stages_state
    ws.save_state(state)


def _print_header(
    *,
    ws: Workspace,
    inputs: list[Path],
    years: Iterable[int] | None,
    resume: bool,
    refresh: bool,
    cache_root: Path,
) -> None:
    from ragmail import __version__

    settings = get_settings()
    years_list = list(years) if years else []
    years_str = ", ".join(str(y) for y in years_list) if years_list else "all"
    if not inputs:
        input_list = "none (using workspace outputs)"
    else:
        input_list = ", ".join(str(p) for p in inputs)
        if len(input_list) > 120:
            input_list = (
                f"{inputs[0]} (+{len(inputs) - 1} more)" if len(inputs) > 1 else str(inputs[0])
            )

    print(f"{Colors.CYAN}{Colors.BOLD}RAGMail v{__version__} - running pipeline{Colors.RESET}")
    print(f"Workspace: {ws.root}")
    print(f"Inputs:    {input_list}")
    print(f"Years:     {years_str}")
    if resume:
        print("Resume:    True")
    elif refresh:
        print("Resume:    False (refresh)")
    else:
        print("Resume:    False (state reset)")
    print(f"Cache:     {cache_root}")
    print(f"Embedding: {settings.embedding_model}")
    print()


def _stage_color(status: str) -> str:
    if status == "done":
        return Colors.GREEN
    if status == "running":
        return Colors.YELLOW
    if status == "failed":
        return Colors.RED
    if status == "interrupted":
        return Colors.YELLOW
    if status == "skipped":
        return Colors.BLUE
    return Colors.DIM


class _StageDisplay:
    def __init__(self, stages: list[str]):
        self._stages = stages
        self._status = {stage: "pending" for stage in stages}
        self._progress = {
            stage: {"processed": 0, "total": None, "skipped": 0, "meta": {}}
            for stage in stages
        }
        self._durations: dict[str, float] = {}
        self._stage_width = max((len(stage) for stage in stages), default=8)
        self._status_width = max((len(status) for status in self._status.values()), default=7)
        self._duration_width = 0
        self._lines_printed = 0
        self._note = ""
        self._spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self._spinner_idx = {stage: 0 for stage in stages}
        self._lock = threading.Lock()
        self._min_render_interval = 0.2
        self._spinner_interval = 0.2
        self._last_render = 0.0
        self._dirty = False
        import sys
        self._out = sys.__stdout__ or sys.stdout

    def update(self, stage: str, status: str, duration_s: float | None = None) -> None:
        with self._lock:
            self._status[stage] = status
            self._status_width = max(self._status_width, len(status))
            if duration_s is not None:
                self._durations[stage] = duration_s
                self._duration_width = max(self._duration_width, len(format_time(duration_s)))
        self.render(force=True)

    def note(self, message: str) -> None:
        with self._lock:
            self._note = message
        self.render(force=True)

    def set_total(self, stage: str, total: int) -> None:
        with self._lock:
            self._progress[stage]["total"] = total
        self.render(force=True)

    def update_progress(
        self,
        stage: str,
        *,
        processed: int | None = None,
        skipped: int | None = None,
        meta: dict | None = None,
    ) -> None:
        with self._lock:
            progress = self._progress[stage]
            if processed is not None:
                progress["processed"] = processed
            if skipped is not None:
                progress["skipped"] = skipped
            if meta:
                merged = dict(progress.get("meta", {}))
                merged.update(meta)
                progress["meta"] = merged
            self._progress[stage] = progress
        self.render()

    def render(self, *, force: bool = False) -> None:
        with self._lock:
            now = time.monotonic()
            if not force and (now - self._last_render) < self._min_render_interval:
                self._dirty = True
                return
            self._dirty = False
            if self._lines_printed > 0:
                self._clear_lines(self._lines_printed)

            lines = [f"{Colors.BOLD}Stages:{Colors.RESET}"]
            for stage in self._stages:
                status = self._status.get(stage, "pending")
                color = _stage_color(status)
                progress = self._progress.get(stage, {})
                processed = progress.get("processed", 0)
                total = progress.get("total")
                skipped = progress.get("skipped", 0)
                meta = progress.get("meta", {})
                spinner = " " if status != "running" else self._spinner[self._spinner_idx[stage]]
                if total is not None and total > 0:
                    pct = processed / total * 100
                    progress_text = f"{processed:,}/{total:,} ({pct:5.1f}%)"
                elif total == 0:
                    progress_text = "0/0 (100.0%)"
                else:
                    progress_text = f"{processed:,}"
                if stage == "model" and (
                    meta.get("downloaded_bytes") is not None or meta.get("cache_bytes") is not None
                ):
                    downloaded_bytes = int(meta.get("downloaded_bytes", 0) or 0)
                    cache_bytes = int(meta.get("cache_bytes", 0) or 0)
                    progress_text = (
                        f"{format_bytes(downloaded_bytes)} downloaded  "
                        f"cache: {format_bytes(cache_bytes)}"
                    )
                if stage == "split" and meta.get("bytes_total") is not None:
                    bytes_total = max(0, int(meta.get("bytes_total", 0)))
                    bytes_processed = max(0, int(meta.get("bytes_processed", 0)))
                    if bytes_total:
                        bytes_pct = bytes_processed / bytes_total * 100
                        bytes_text = (
                            f"{format_bytes(bytes_processed)}/{format_bytes(bytes_total)} "
                            f"({bytes_pct:5.1f}%)"
                        )
                    else:
                        bytes_text = format_bytes(bytes_processed)
                    progress_text = f"{processed:,} emails  {bytes_text}"
                if stage == "preprocess" and (
                    meta.get("spam") is not None or meta.get("errors") is not None
                ):
                    bulk = meta.get("spam", 0)
                    errors = meta.get("errors", 0)
                    progress_text = f"{progress_text}  skipped: {bulk:,} bulk, {errors:,} errors"
                elif meta.get("skipped_exists") is not None or meta.get("skipped_errors") is not None:
                    exists = meta.get("skipped_exists", 0)
                    errors = meta.get("skipped_errors", 0)
                    if exists or errors:
                        progress_text = (
                            f"{progress_text}  skipped (exists: {exists:,}, errors: {errors:,})"
                        )
                elif skipped:
                    progress_text = f"{progress_text}  skipped {skipped:,}"
                duration = self._durations.get(stage)
                duration_text = ""
                if duration is not None:
                    duration_text = f"  {Colors.DIM}{format_time(duration):>{self._duration_width}}{Colors.RESET}"
                lines.append(
                    f"  {spinner} {color}{stage:<{self._stage_width}}{Colors.RESET} "
                    f"{color}{status:<{self._status_width}}{Colors.RESET}  {progress_text}{duration_text}"
                )

            if self._note:
                lines.append(f"{Colors.DIM}{self._note}{Colors.RESET}")

            print("\n".join(lines), file=self._out, flush=True)
            self._lines_printed = len(lines)
            self._last_render = now

    def finish(self) -> None:
        self._note = ""
        self.render(force=True)

    def spin(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            any_running = False
            with self._lock:
                for stage in self._stages:
                    if self._status.get(stage) == "running":
                        any_running = True
                        self._spinner_idx[stage] = (self._spinner_idx[stage] + 1) % len(self._spinner)
                dirty = self._dirty
            if any_running or dirty:
                self.render()
            time.sleep(self._spinner_interval)

    @staticmethod
    def _clear_lines(n: int) -> None:
        for _ in range(n):
            import sys
            out = sys.__stdout__ or sys.stdout
            out.write("\033[1A\r\033[2K")
            out.flush()


def _print_summary(
    *,
    ws: Workspace,
    mailbox_files: int,
    emails_found: int,
    split_total: int,
    split_written: int,
    split_errors: int,
    clean_total: int,
    clean_written: int,
    clean_spam: int,
    clean_errors: int,
    vectorize_count: int,
    ingest_total: int,
    ingest_count: int,
    ingest_errors: int,
    total_duration_s: float | None = None,
) -> None:
    print()
    print(f"{Colors.BOLD}Outputs:{Colors.RESET}")
    print(f"  Mailbox files: {mailbox_files}")
    print(f"  Emails found: {emails_found:,}")
    split_error_text = "no errors" if split_errors == 0 else f"{split_errors:,} errors"
    print(f"  Split: {split_written:,} ({split_error_text})")
    print(
        f"  Preprocessed: {clean_total:,} (ignoring: {clean_spam:,} bulk, {clean_errors:,} errors)"
    )
    print(f"  Vectorized: {vectorize_count:,}")
    print(f"  Ingested: {ingest_count:,} ({ingest_errors:,} errors)")
    print(f"  Embeddings: {ws.embeddings_dir}")
    print(f"  Database: {ws.db_dir / 'email_search.lancedb'}")
    if total_duration_s is not None:
        print(f"  Total time: {format_time(total_duration_s)}")
    print(f"  Logs: {ws.logs_dir}")
    print()


_FROM_LINE_PATTERN = re.compile(
    br"^From \S+ (Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
    br"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
    br"\s*(\d{1,2}) (\d{2}:\d{2}:\d{2})(?: ([+-]\d{4}|\w+))? (\d{4})$"
)


def _count_mbox_messages(paths: Iterable[Path]) -> int:
    total = 0
    for path in paths:
        with open(path, "rb") as handle:
            for line in handle:
                if line.startswith(b"From ") and _FROM_LINE_PATTERN.match(line.rstrip()):
                    total += 1
    return total


def _count_jsonl_lines(paths: Iterable[Path]) -> int:
    total = 0
    for path in paths:
        if not path.exists():
            continue
        with open(path, "rb") as handle:
            for _ in handle:
                total += 1
    return total


def _path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0

    total = 0
    for root, _, files in os.walk(path):
        root_path = Path(root)
        for filename in files:
            try:
                total += int((root_path / filename).stat().st_size)
            except OSError:
                continue
    return total


def _rust_cli_base_command() -> list[str]:
    override = os.environ.get("RAGMAIL_BIN") or os.environ.get("RAGMAIL_RS_BIN")
    if override:
        return shlex.split(override)
    prebuilt = REPO_ROOT / "rust/target/debug/ragmail"
    legacy_prebuilt = REPO_ROOT / "rust/target/debug/ragmail-rs"
    if prebuilt.exists():
        return [str(prebuilt)]
    if legacy_prebuilt.exists():
        return [str(legacy_prebuilt)]
    manifest = REPO_ROOT / "rust/Cargo.toml"
    if not manifest.exists():
        raise RuntimeError(
            "Rust stage runner not found (missing rust/Cargo.toml). "
            "Set RAGMAIL_BIN (or RAGMAIL_RS_BIN) to a prebuilt ragmail binary, or set "
            "RAGMAIL_RS_REPO_ROOT to a repository checkout."
        )
    return [
        "cargo",
        "run",
        "--manifest-path",
        str(manifest),
        "-p",
        "ragmail-cli",
        "--quiet",
        "--",
    ]


def _run_rust_cli(args: list[str], *, stage: str, ws: Workspace) -> str:
    cmd = [*_rust_cli_base_command(), *args]
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Rust stage '{stage}' unavailable ({exc}). "
            "Install Rust toolchain or set RAGMAIL_BIN to a ragmail binary."
        ) from exc

    output = f"{result.stdout}\n{result.stderr}".strip()
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n")
    if result.returncode != 0:
        command = " ".join(shlex.quote(part) for part in cmd)
        raise RuntimeError(
            f"Rust stage '{stage}' failed (exit {result.returncode}): {command}"
        )
    _log_event(ws, stage, "INFO", f"rust command: {' '.join(shlex.quote(part) for part in cmd)}")
    return output


def _parse_rust_split_stats(output: str) -> dict[str, int]:
    match = _RUST_SPLIT_COMPLETE_PATTERN.search(output)
    if not match:
        raise RuntimeError(f"Could not parse rust split output: {output[:2000]}")
    return {
        "processed": int(match.group(1)),
        "written": int(match.group(2)),
        "skipped": int(match.group(3)),
        "errors": int(match.group(4)),
        "last_position": int(match.group(5) or 0),
    }


def _parse_rust_index_stats(output: str) -> dict[str, int]:
    match = _RUST_INDEX_COMPLETE_PATTERN.search(output)
    if not match:
        raise RuntimeError(f"Could not parse rust index output: {output[:2000]}")
    return {
        "indexed": int(match.group(1)),
        "last_position": int(match.group(2)),
    }


def _parse_rust_clean_stats(output: str) -> dict[str, int]:
    match = _RUST_CLEAN_COMPLETE_PATTERN.search(output)
    if not match:
        raise RuntimeError(f"Could not parse rust clean output: {output[:2000]}")
    return {
        "processed": int(match.group(1)),
        "clean": int(match.group(2)),
        "spam": int(match.group(3)),
        "errors": int(match.group(4)),
    }


def _run_rust_split(
    *,
    input_mbox: Path,
    output_dir: Path,
    years: Iterable[int] | None,
    checkpoint_path: Path,
    resume: bool,
    checkpoint_interval: int,
    stage: str,
    ws: Workspace,
) -> dict[str, int]:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    if not resume and checkpoint_path.exists():
        checkpoint_path.unlink()
    start_offset = _load_split_checkpoint_offset(checkpoint_path) if resume else 0
    args = [
        "split",
        str(input_mbox),
        "--output-dir",
        str(output_dir),
        "--start-offset",
        str(start_offset),
        "--checkpoint",
        str(checkpoint_path),
        "--resume",
        "true" if resume else "false",
        "--checkpoint-interval",
        str(checkpoint_interval),
    ]
    if years:
        for year in sorted(set(years)):
            args.extend(["--years", str(year)])
    output = _run_rust_cli(args, stage=stage, ws=ws)
    stats = _parse_rust_split_stats(output)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    return stats


def _split_checkpoint_path(checkpoint_dir: Path, input_mbox: Path) -> Path:
    digest = hashlib.sha1(str(input_mbox.resolve()).encode("utf-8")).hexdigest()[:12]
    return checkpoint_dir / f"{input_mbox.stem}-{digest}.checkpoint.json"


def _load_split_checkpoint_offset(checkpoint_path: Path) -> int:
    if not checkpoint_path.exists():
        return 0
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        value = int(payload.get("last_position", 0))
        return max(0, value)
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return 0


def _run_rust_clean(
    *,
    input_mbox: Path,
    output_clean: Path,
    output_spam: Path,
    summary_output: Path,
    index_output: Path | None = None,
    stage: str,
    ws: Workspace,
) -> dict[str, int]:
    args = [
        "clean",
        str(input_mbox),
        "--output-clean",
        str(output_clean),
        "--output-spam",
        str(output_spam),
        "--summary-output",
        str(summary_output),
        "--mbox-file",
        input_mbox.name,
    ]
    if index_output is not None:
        args.extend(["--index-output", str(index_output)])
    output = _run_rust_cli(args, stage=stage, ws=ws)
    return _parse_rust_clean_stats(output)


def _build_rust_mbox_index(
    *,
    split_files: list[Path],
    output_path: Path,
    checkpoint_dir: Path,
    resume: bool,
    checkpoint_interval: int,
    progress_callback,
    ws: Workspace,
) -> int:
    parts_dir = checkpoint_dir / "parts"
    state_dir = checkpoint_dir / "state"
    if not resume:
        if output_path.exists():
            output_path.unlink()
        if checkpoint_dir.exists():
            shutil.rmtree(checkpoint_dir)

    parts_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    indexed_total = 0
    for mbox in split_files:
        part_output = parts_dir / f"{mbox.name}.jsonl"
        checkpoint_path = state_dir / f"{mbox.stem}.checkpoint.json"

        if resume and part_output.exists() and not checkpoint_path.exists():
            part_count = _count_jsonl_lines([part_output])
            indexed_total += part_count
            progress_callback({"processed": indexed_total})
            continue

        args = [
            "index",
            str(mbox),
            "--mbox-file",
            mbox.name,
            "--output",
            str(part_output),
            "--checkpoint",
            str(checkpoint_path),
            "--resume",
            "true" if resume else "false",
            "--checkpoint-interval",
            str(checkpoint_interval),
        ]
        output = _run_rust_cli(args, stage="index", ws=ws)
        _ = _parse_rust_index_stats(output)
        part_count = _count_jsonl_lines([part_output])
        indexed_total += part_count
        progress_callback({"processed": indexed_total})

    _merge_index_parts(parts_dir=parts_dir, split_files=split_files, output_path=output_path)
    final_count = _count_jsonl_lines([output_path])
    progress_callback({"processed": final_count})
    return final_count


def _merge_index_parts(*, parts_dir: Path, split_files: list[Path], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as out:
        for mbox in split_files:
            part_path = parts_dir / f"{mbox.name}.jsonl"
            if not part_path.exists():
                raise RuntimeError(f"Missing rust index part output: {part_path}")
            with open(part_path, "rb") as handle:
                shutil.copyfileobj(handle, out)


def _warmup_dependencies() -> None:
    import logging
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="A new version of the following files was downloaded*",
    )
    warnings.filterwarnings(
        "ignore",
        message="Could not cache non-existence of file*",
    )
    logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
    logging.getLogger("huggingface_hub.file_download").setLevel(logging.ERROR)
    logging.getLogger("transformers").setLevel(logging.ERROR)
    logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

    from ragmail.embedding import create_embedding_provider

    settings = get_settings()
    provider = create_embedding_provider(
        settings.embedding_provider,
        model_name=settings.embedding_model,
        model_revision=settings.embedding_model_revision,
    )
    _ = provider.dimension


@contextmanager
def _stage_log(ws: Workspace, stage: str):
    log_path = ws.logs_dir / f"{stage}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8", buffering=1) as handle:
        with redirect_stdout(handle), redirect_stderr(handle):
            yield log_path


_LOG_LOCK = threading.Lock()
_PROGRESS_LOG_STATE: dict[str, int] = {}


def _log_event(ws: Workspace, stage: str, level: str, message: str) -> None:
    log_path = ws.logs_dir / f"{stage}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} | {level:<5} | {message}\n"
    with _LOG_LOCK:
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(line)


def _log_progress(
    ws: Workspace,
    stage: str,
    processed: int,
    *,
    total: int | None = None,
    skipped: int | None = None,
    spam: int | None = None,
    errors: int | None = None,
) -> None:
    step = 300
    last = _PROGRESS_LOG_STATE.get(stage, 0)
    should_log = processed - last >= step or (total is not None and processed >= total)
    if not should_log:
        return

    _PROGRESS_LOG_STATE[stage] = processed
    parts = [f"progress {processed:,}"]
    if total is not None and total > 0:
        pct = processed / total * 100
        parts.append(f"of {total:,} ({pct:0.1f}%)")
    if skipped:
        parts.append(f"skipped {skipped:,}")
    if spam:
        parts.append(f"bulk {spam:,}")
    if errors:
        parts.append(f"errors {errors:,}")
    _log_event(ws, stage, "INFO", " ".join(parts))

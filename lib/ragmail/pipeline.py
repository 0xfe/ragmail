"""Pipeline orchestration for ragmail."""

from __future__ import annotations

import shutil
import signal
import shlex
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Iterable
import re
import threading
import time
from datetime import datetime

from ragmail.clean.cleaner import process_mbox
from ragmail.common import signals
from ragmail.common.terminal import Colors, format_time, format_bytes
from ragmail.split.splitter import MboxSplitter
from ragmail.workspace import Workspace, default_cache_root, get_workspace
from ragmail.mbox_index import build_mbox_index, MboxIndexWriter
from ragmail.config import get_settings


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

    def _selected(stage: str) -> bool:
        return stages is None or stage in stages

    def _refresh_selected() -> set[str]:
        if stages is None:
            return {"download", "split", "index", "clean", "vectorize", "ingest"}
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
    stage_display = _StageDisplay(["download", "split", "index", "clean", "vectorize", "ingest"])
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

    if _selected("download"):
        stage_display.set_total("download", 1)
        stage_display.update("download", "running")
        ws.update_stage("download", "running")
        _log_event(ws, "download", "INFO", "start")
        download_start = time.monotonic()
        try:
            with _stage_log(ws, "download"):
                _warmup_dependencies()
        except KeyboardInterrupt:
            stage_display.update("download", "interrupted")
            ws.update_stage("download", "interrupted")
            _log_event(ws, "download", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("download", "failed")
            ws.update_stage("download", "failed")
            _log_event(ws, "download", "ERROR", "failed")
            raise
        else:
            stage_display.update_progress("download", processed=1)
            stage_display.update("download", "done", duration_s=time.monotonic() - download_start)
            ws.update_stage("download", "done", {"duration_s": time.monotonic() - download_start})
            _log_event(
                ws,
                "download",
                "INFO",
                f"done in {time.monotonic() - download_start:.2f}s",
            )
    else:
        stage_display.update("download", "skipped")
        _log_event(ws, "download", "INFO", "skipped (not selected)")

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
        try:
            with _stage_log(ws, "split"):
                split_processed = 0
                split_skipped = 0
                split_written = 0
                split_errors = 0

                for input_mbox in inputs:
                    base_processed = split_processed
                    base_skipped = split_skipped

                    input_size = input_mbox.stat().st_size
                    base_bytes = split_bytes_processed

                    def _split_progress(
                        payload,
                        *,
                        _base=base_processed,
                        _base_skip=base_skipped,
                        _base_bytes=base_bytes,
                    ):
                        nonlocal split_processed, split_skipped, split_bytes_processed
                        split_processed = split_existing + _base + payload["processed"]
                        split_skipped = _base_skip + payload["skipped"]
                        split_bytes_processed = _base_bytes + payload.get("position", 0)
                        stage_display.update_progress(
                            "split",
                            processed=split_processed,
                            skipped=split_skipped,
                            meta={
                                "bytes_processed": split_bytes_processed,
                                "bytes_total": split_total_bytes,
                            },
                        )
                        _log_progress(
                            ws,
                            "split",
                            split_processed,
                            skipped=split_skipped,
                        )

                    splitter = MboxSplitter(
                        input_file=str(input_mbox),
                        output_dir=str(ws.split_dir),
                        filter_years=list(years) if years else None,
                        progress_callback=_split_progress,
                        show_progress=False,
                    )
                    split_resume = resume_effective and split_outputs_exist
                    splitter.run(resume=split_resume)
                    split_processed = base_processed + splitter.processed_emails
                    split_skipped = base_skipped + splitter.skipped_emails
                    split_written += splitter.written_emails
                    split_errors += splitter.error_emails
                    split_bytes_processed = base_bytes + input_size

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
            ws.update_stage(
                "split",
                "done",
                {
                    "output_dir": str(ws.split_dir),
                    "processed": split_processed,
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
    if (_selected("index") or _selected("clean")) and not split_files:
        _log_event(ws, "split", "ERROR", "no split outputs found")
        raise RuntimeError(
            f"No split MBOX files found in {ws.split_dir}. "
            "Provide input MBOX files or run `ragmail pipeline <mbox> --workspace <name>`. "
            "If the workspace was moved or split outputs were deleted, rerun "
            "`ragmail pipeline <mbox> --workspace <name> --stages split` (or `--refresh`)."
        )

    expected_clean = {ws.clean_dir / f"{mbox.stem}.clean.jsonl" for mbox in split_files}
    existing_clean = set(ws.clean_dir.glob("*.clean.jsonl"))
    missing_clean = bool(expected_clean and not expected_clean.issubset(existing_clean))
    if resume_effective and _selected("clean") and ws.stage_done("clean") and missing_clean:
        _log_event(
            ws,
            "clean",
            "WARN",
            "stage marked done but outputs missing; rerunning clean (resume)",
        )
    clean_should_run = _selected("clean") and (
        (not resume_effective) or (not ws.stage_done("clean")) or missing_clean
    )
    index_in_clean = clean_should_run

    clean_total = 0
    if clean_should_run:
        clean_total = _count_mbox_messages(split_files)
        stage_display.set_total("index", clean_total)
        stage_display.update("index", "running")
        ws.update_stage("index", "running", {"split_dir": str(ws.split_dir), "via": "clean"})
        _log_event(ws, "index", "INFO", "start (via clean)")

    index_outputs_exist = (ws.split_dir / "mbox_index.jsonl").exists()
    if resume_effective and _selected("index") and ws.stage_done("index") and not index_outputs_exist:
        _log_event(
            ws,
            "index",
            "WARN",
            "stage marked done but outputs missing; rerunning index",
        )
    index_should_run = (
        _selected("index")
        and not index_in_clean
        and ((not resume_effective) or (not ws.stage_done("index")) or (not index_outputs_exist))
    )

    if index_should_run:
        index_start = time.monotonic()
        _log_event(ws, "index", "INFO", "start")
        stage_display.update("index", "running")
        index_total = clean_total or _count_mbox_messages(split_files)
        if index_total:
            stage_display.set_total("index", index_total)
        ws.update_stage("index", "running", {"split_dir": str(ws.split_dir)})
        try:
            with _stage_log(ws, "index"):
                index_count = 0

                def _index_progress(payload):
                    nonlocal index_count
                    index_count = payload.get("processed", index_count)
                    stage_display.update_progress("index", processed=index_count)
                    _log_progress(ws, "index", index_count, total=index_total or None)

                index_stats = build_mbox_index(
                    split_dir=ws.split_dir,
                    output_path=ws.split_dir / "mbox_index.jsonl",
                    checkpoint_path=ws.checkpoints_dir / "mbox_index.checkpoint.json",
                    resume=resume_effective,
                    checkpoint_every=checkpoint_interval or 30,
                    progress_callback=_index_progress,
                )
                index_count = index_stats.indexed
                stage_display.update_progress("index", processed=index_count)
        except KeyboardInterrupt:
            stage_display.update("index", "interrupted")
            ws.update_stage("index", "interrupted")
            _log_event(ws, "index", "WARN", "interrupted")
            raise
        except Exception:
            stage_display.update("index", "failed")
            ws.update_stage("index", "failed")
            _log_event(ws, "index", "ERROR", "failed")
            raise
        else:
            ws.update_stage(
                "index",
                "done",
                {
                    "output": str(ws.split_dir / "mbox_index.jsonl"),
                    "indexed": index_count,
                    "duration_s": time.monotonic() - index_start,
                },
            )
            stage_display.update("index", "done", duration_s=time.monotonic() - index_start)
            _log_event(
                ws,
                "index",
                "INFO",
                f"done in {time.monotonic() - index_start:.2f}s",
            )
    elif not index_in_clean:
        stage_display.update("index", "skipped")
        _log_event(ws, "index", "INFO", "skipped (already done or not selected)")

    clean_total = clean_total or 0
    clean_written = 0
    clean_spam = 0
    clean_errors = 0
    if clean_should_run:
        clean_start = time.monotonic()
        _log_event(ws, "clean", "INFO", "start")
        stage_display.update("clean", "running")
        if clean_total == 0:
            clean_total = _count_mbox_messages(split_files)
        stage_display.set_total("clean", clean_total)
        ws.update_stage("clean", "running", {"files": len(split_files)})
        clean_outputs: list[Path] = []
        index_writer = None
        if index_in_clean:
            index_path = ws.split_dir / "mbox_index.jsonl"
            if resume_effective and not index_path.exists():
                existing_clean = list(ws.clean_dir.glob("*.clean.jsonl"))
                if existing_clean:
                    raise RuntimeError(
                        f"Index missing at {index_path}. "
                        "Run `ragmail pipeline --stages clean --workspace <name>` to rebuild "
                        "(or `--stages index` for index-only)."
                    )
            index_mode = "a" if (resume_effective and index_path.exists()) else "w"
            index_writer = MboxIndexWriter(index_path, mode=index_mode)
        try:
            with _stage_log(ws, "clean"):
                clean_processed = 0
                clean_skipped = 0
                for mbox in split_files:
                    clean_jsonl = ws.clean_dir / f"{mbox.stem}.clean.jsonl"
                    if resume and clean_jsonl.exists():
                        clean_outputs.append(clean_jsonl)
                        continue

                    link_path = _ensure_link_unique(ws.clean_dir, mbox)
                    base_processed = clean_processed
                    base_skipped = clean_skipped
                    base_spam = clean_spam
                    base_errors = clean_errors

                    def _clean_progress(
                        payload,
                        *,
                        _base=base_processed,
                        _base_skip=base_skipped,
                        _base_spam=base_spam,
                        _base_errors=base_errors,
                    ):
                        nonlocal clean_processed, clean_skipped
                        spam_total = _base_spam + payload.get("spam", 0)
                        error_total = _base_errors + payload.get("errors", 0)
                        clean_processed = _base + payload["processed"]
                        clean_skipped = _base_skip + payload["skipped"]
                        stage_display.update_progress(
                            "clean",
                            processed=clean_processed,
                            skipped=clean_skipped,
                            meta={
                                "spam": spam_total,
                                "errors": error_total,
                            },
                        )
                        if index_in_clean:
                            stage_display.update_progress("index", processed=clean_processed)
                        _log_progress(
                            ws,
                            "clean",
                            clean_processed,
                            total=clean_total,
                            skipped=clean_skipped,
                            spam=spam_total,
                            errors=error_total,
                        )

                    stats = process_mbox(
                        str(link_path),
                        resume=resume,
                        verbose=False,
                        progress_callback=_clean_progress,
                        show_progress=False,
                        index_writer=index_writer,
                    )

                    spam_path = ws.clean_dir / f"{mbox.stem}.spam.jsonl"
                    summary_path = ws.clean_dir / f"{mbox.name}.summary"

                    if spam_path.exists():
                        shutil.move(spam_path, ws.spam_dir / spam_path.name)
                    if summary_path.exists():
                        shutil.move(summary_path, ws.reports_dir / summary_path.name)

                    if link_path.exists():
                        try:
                            link_path.unlink()
                        except OSError:
                            pass

                    if clean_jsonl.exists():
                        clean_outputs.append(clean_jsonl)
                    if stats:
                        clean_written += stats.clean_emails
                        clean_spam += stats.spam_emails
                        clean_errors += stats.error_emails
                        clean_processed = base_processed + stats.total_emails
                        clean_skipped = base_skipped + stats.spam_emails + stats.error_emails
                        stage_display.update_progress(
                            "clean",
                            processed=clean_processed,
                            skipped=clean_skipped,
                            meta={"spam": clean_spam, "errors": clean_errors},
                        )
        except KeyboardInterrupt:
            if index_in_clean:
                stage_display.update("index", "interrupted")
                ws.update_stage("index", "interrupted")
                _log_event(ws, "index", "WARN", "interrupted")
            stage_display.update("clean", "interrupted")
            ws.update_stage("clean", "interrupted")
            _log_event(ws, "clean", "WARN", "interrupted")
            raise
        except Exception:
            if index_in_clean:
                stage_display.update("index", "failed")
                ws.update_stage("index", "failed")
                _log_event(ws, "index", "ERROR", "failed")
            stage_display.update("clean", "failed")
            ws.update_stage("clean", "failed")
            _log_event(ws, "clean", "ERROR", "failed")
            raise
        else:
            ws.update_stage(
                "clean",
                "done",
                {
                    "clean_files": len(clean_outputs),
                    "processed": clean_total,
                    "written": clean_written,
                    "skipped": clean_spam + clean_errors,
                    "duration_s": time.monotonic() - clean_start,
                },
            )
            stage_display.update("clean", "done", duration_s=time.monotonic() - clean_start)
            _log_event(
                ws,
                "clean",
                "INFO",
                f"done in {time.monotonic() - clean_start:.2f}s",
            )
            if index_in_clean:
                ws.update_stage(
                    "index",
                    "done",
                    {
                        "output": str(ws.split_dir / "mbox_index.jsonl"),
                        "indexed": clean_processed,
                        "duration_s": time.monotonic() - clean_start,
                        "via": "clean",
                    },
                )
                stage_display.update("index", "done", duration_s=time.monotonic() - clean_start)
        finally:
            if index_writer is not None:
                index_writer.close()
    else:
        if _selected("clean"):
            stage_display.update("clean", "skipped")
            _log_event(ws, "clean", "INFO", "skipped (already done)")
        else:
            stage_display.update("clean", "skipped")
            _log_event(ws, "clean", "INFO", "skipped (not selected)")

    clean_root = clean_dir or ws.clean_dir
    if split_files and clean_root == ws.clean_dir:
        expected_clean = {clean_root / f"{mbox.stem}.clean.jsonl" for mbox in split_files}
        clean_files = sorted(path for path in expected_clean if path.exists())
    else:
        clean_files = sorted(clean_root.glob("*.clean.jsonl"))
    if (_selected("vectorize") or _selected("ingest")) and not clean_files:
        _log_event(ws, "clean", "ERROR", "no clean outputs found")
        raise RuntimeError(
            f"No clean JSONL files found in {clean_root}. "
            "Run `ragmail pipeline <mbox> --workspace <name> --stages clean` "
            "or rerun from split+clean with the original inputs."
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
    if "index" in stages:
        index_path = ws.split_dir / "mbox_index.jsonl"
        checkpoint_path = ws.checkpoints_dir / "mbox_index.checkpoint.json"
        if index_path.exists():
            index_path.unlink()
        if checkpoint_path.exists():
            checkpoint_path.unlink()
    if "clean" in stages:
        _archive_dir(ws.clean_dir, "clean")
        _archive_dir(ws.spam_dir, "spam")
        _archive_dir(ws.reports_dir, "reports")
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
                if stage == "clean" and (meta.get("spam") is not None or meta.get("errors") is not None):
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
        f"  Cleaned: {clean_total:,} (ignoring: {clean_spam:,} bulk, {clean_errors:,} errors)"
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
    br"\s*(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{4}|\w+) (\d{4})$"
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

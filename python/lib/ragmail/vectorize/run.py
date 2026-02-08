"""Vectorization runner for ragmail pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Callable
import threading
import time

from ragmail.common import signals
from ragmail.config import get_settings
from ragmail.embedding import create_embedding_provider
from ragmail.ingest import IngestPipeline
from ragmail.ingest.text_processing import clean_body_for_embedding, chunk_text
from ragmail.vectorize.store import EmbeddingStore, default_embedding_path

def vectorize_files(
    input_files: Iterable[Path],
    output_dir: Path,
    checkpoint_dir: Path,
    resume: bool = True,
    errors_path: Path | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    quiet: bool = False,
    vectorize_batch_size: int | None = None,
    embedding_batch_size: int | None = None,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
    checkpoint_interval: int | None = None,
    validate: bool = True,
    strict: bool = False,
    max_errors: int | None = None,
    limit: int | None = None,
) -> int:
    signals.install_signal_handlers()

    settings = get_settings()
    effective_vectorize_batch_size = (
        vectorize_batch_size
        if vectorize_batch_size is not None
        else settings.ingest_batch_size
    )
    effective_embedding_batch_size = (
        embedding_batch_size
        if embedding_batch_size is not None
        else settings.embedding_batch_size
    )
    effective_chunk_size = (
        chunk_size if chunk_size is not None else settings.ingest_chunk_size
    )
    effective_chunk_overlap = (
        chunk_overlap
        if chunk_overlap is not None
        else settings.ingest_chunk_overlap
    )
    effective_checkpoint_interval = (
        checkpoint_interval
        if checkpoint_interval is not None
        else settings.ingest_checkpoint_interval
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    last_progress_time = time.monotonic()
    progress_interval = 0.1
    progress_step = 25
    startup_interval = 0.5

    # Emit startup heartbeat while embedding provider initialization blocks.
    startup_text = "loading embedding model"
    startup_stop = threading.Event()
    startup_thread: threading.Thread | None = None
    startup_started = time.monotonic()
    if progress_callback:
        progress_callback({"processed": processed, "startup_text": startup_text})

        def _startup_heartbeat() -> None:
            while not startup_stop.wait(startup_interval):
                progress_callback(
                    {
                        "processed": processed,
                        "startup_text": startup_text,
                        "elapsed_s": max(0.0, time.monotonic() - startup_started),
                    }
                )

        startup_thread = threading.Thread(target=_startup_heartbeat, daemon=True)
        startup_thread.start()

    try:
        embedding_provider = create_embedding_provider(
            settings.embedding_provider,
            model_name=settings.embedding_model,
            model_revision=settings.embedding_model_revision,
        )
    finally:
        if startup_thread is not None:
            startup_stop.set()
            startup_thread.join(timeout=1.0)

    if progress_callback:
        progress_callback(
            {"processed": processed, "startup_text": "preparing vectorization pipeline"}
        )

    pipeline = IngestPipeline(
        checkpoint_dir=checkpoint_dir,
        checkpoint_interval=effective_checkpoint_interval,
        errors_path=errors_path,
    )

    def _maybe_emit_progress(force: bool = False):
        nonlocal last_progress_time
        if not progress_callback:
            return
        now = time.monotonic()
        if force or (now - last_progress_time) >= progress_interval or (processed % progress_step == 0):
            progress_callback({"processed": processed, "startup_text": ""})
            last_progress_time = now

    def _check_interrupt():
        signals.raise_if_interrupted()

    for input_file in input_files:
        _check_interrupt()
        output_path = default_embedding_path(input_file, output_dir)
        store = EmbeddingStore(output_path)
        meta = store.build_meta(
            embedding_model=settings.embedding_model,
            embedding_model_revision=settings.embedding_model_revision,
            embedding_dimension=embedding_provider.dimension,
            chunk_size=effective_chunk_size,
            chunk_overlap=effective_chunk_overlap,
            source=str(input_file),
        )
        store.ensure_meta(meta)

        if not quiet:
            print(f"Vectorizing: {input_file} -> {output_path}")

        batch: list = []
        for email in pipeline.ingest(
            input_file,
            resume=resume,
            validate=validate,
            strict=strict,
            max_errors=max_errors,
        ):
            _check_interrupt()
            batch.append(email)
            if progress_callback and processed == 0:
                now = time.monotonic()
                if (now - last_progress_time) >= startup_interval:
                    progress_callback({"processed": 0, "startup_text": "building first batch"})
                    last_progress_time = now

            if len(batch) >= effective_vectorize_batch_size:
                _check_interrupt()
                _vectorize_batch(
                    batch,
                    embedding_provider,
                    store,
                    batch_size=effective_embedding_batch_size,
                    chunk_size=effective_chunk_size,
                    chunk_overlap=effective_chunk_overlap,
                )
                processed += len(batch)
                batch = []
                _maybe_emit_progress()

            if limit and processed >= limit:
                break

        if batch and (not limit or processed < limit):
            _check_interrupt()
            _vectorize_batch(
                batch,
                embedding_provider,
                store,
                batch_size=effective_embedding_batch_size,
                chunk_size=effective_chunk_size,
                chunk_overlap=effective_chunk_overlap,
            )
            processed += len(batch)
            batch = []
            _maybe_emit_progress()

        store.close()

        if limit and processed >= limit:
            break

    if progress_callback:
        _maybe_emit_progress(force=True)

    if not quiet:
        print(f"Vectorized {processed} emails")

    return processed


def _vectorize_batch(
    batch,
    embedding_provider,
    store: EmbeddingStore,
    batch_size: int,
    chunk_size: int,
    chunk_overlap: int,
) -> None:
    signals.raise_if_interrupted()
    subject_texts = [f"Subject: {email.subject}".strip() for email in batch]
    subject_vectors = embedding_provider.encode(
        subject_texts,
        batch_size=batch_size,
        show_progress=False,
    )

    chunk_texts: list[str] = []
    chunk_email_indices: list[int] = []
    chunk_indices: list[int] = []
    chunk_counts: list[int] = [0 for _ in batch]

    for idx, email in enumerate(batch):
        cleaned_body = clean_body_for_embedding(email.body_plain)
        chunks = chunk_text(
            cleaned_body,
            max_chars=chunk_size,
            overlap=chunk_overlap,
        )
        chunk_counts[idx] = len(chunks)
        for chunk_index, chunk in enumerate(chunks):
            chunk_texts.append(chunk)
            chunk_email_indices.append(idx)
            chunk_indices.append(chunk_index)

    chunk_vectors = None
    if chunk_texts:
        signals.raise_if_interrupted()
        chunk_vectors = embedding_provider.encode(
            chunk_texts,
            batch_size=batch_size,
            show_progress=False,
        )

    email_ids = [email.email_id for email in batch]
    store.add_batch(
        email_ids,
        subject_vectors,
        chunk_counts,
        chunk_email_indices,
        chunk_indices,
        chunk_vectors,
    )

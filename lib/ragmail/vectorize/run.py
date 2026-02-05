"""Vectorization runner for ragmail pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Callable
import signal
import time

from ragmail.config import get_settings
from ragmail.embedding import create_embedding_provider
from ragmail.ingest import IngestPipeline
from ragmail.ingest.text_processing import clean_body_for_embedding, chunk_text
from ragmail.vectorize.store import EmbeddingStore, default_embedding_path

_VECTORIZER_INTERRUPTED = False


def _vectorize_signal_handler(signum, frame):
    global _VECTORIZER_INTERRUPTED
    _VECTORIZER_INTERRUPTED = True


signal.signal(signal.SIGINT, _vectorize_signal_handler)
signal.signal(signal.SIGTERM, _vectorize_signal_handler)


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
    global _VECTORIZER_INTERRUPTED
    _VECTORIZER_INTERRUPTED = False

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

    embedding_provider = create_embedding_provider(
        settings.embedding_provider,
        model_name=settings.embedding_model,
        model_revision=settings.embedding_model_revision,
    )

    pipeline = IngestPipeline(
        checkpoint_dir=checkpoint_dir,
        checkpoint_interval=effective_checkpoint_interval,
        errors_path=errors_path,
    )

    processed = 0
    last_progress_time = time.monotonic()
    progress_interval = 0.1
    progress_step = 25

    def _maybe_emit_progress(force: bool = False):
        nonlocal last_progress_time
        if not progress_callback:
            return
        now = time.monotonic()
        if force or (now - last_progress_time) >= progress_interval or (processed % progress_step == 0):
            progress_callback({"processed": processed})
            last_progress_time = now

    def _check_interrupt():
        if _VECTORIZER_INTERRUPTED:
            raise KeyboardInterrupt

    for input_file in input_files:
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

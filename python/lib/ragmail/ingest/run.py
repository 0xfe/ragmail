"""Ingestion runner for ragmail pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Callable
import time

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

from ragmail.common import signals
from ragmail.config import get_settings
from ragmail.embedding import create_embedding_provider
from ragmail.ingest import IngestPipeline
from ragmail.ingest.text_processing import clean_body_for_embedding, chunk_text
from ragmail.storage import Database, EmailRepository
from ragmail.storage.schema import create_email_chunk_schema, create_email_schema_flat
from ragmail.vectorize.store import EmbeddingMeta, EmbeddingStore, default_embedding_path

def ingest_files(
    input_files: Iterable[Path],
    db_path: Path,
    checkpoint_dir: Path,
    resume: bool = True,
    errors_path: Path | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    quiet: bool = False,
    skip_exists_check: bool | None = None,
    ingest_batch_size: int | None = None,
    embedding_batch_size: int | None = None,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
    checkpoint_interval: int | None = None,
    compact_every: int | None = None,
    compaction_callback: Callable[[dict], None] | None = None,
) -> int:
    signals.install_signal_handlers()
    settings = get_settings()
    db_path_existed = db_path.exists()
    effective_ingest_batch_size = (
        ingest_batch_size
        if ingest_batch_size is not None
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
    effective_compact_every = (
        compact_every if compact_every is not None else settings.ingest_compact_every
    )
    if effective_compact_every is not None and effective_compact_every <= 0:
        effective_compact_every = None

    embedding_provider = create_embedding_provider(
        settings.embedding_provider,
        model_name=settings.embedding_model,
        model_revision=settings.embedding_model_revision,
    )

    database = Database(db_path)
    repository = EmailRepository(database, dimension=embedding_provider.dimension)
    if skip_exists_check is None:
        if not db_path_existed or repository.is_empty():
            skip_exists_check = True
        else:
            skip_exists_check = False

    pipeline = IngestPipeline(
        checkpoint_dir=checkpoint_dir,
        checkpoint_interval=effective_checkpoint_interval,
        errors_path=errors_path,
    )

    batch_size = effective_ingest_batch_size
    processed = 0
    seen = 0
    skipped_existing = 0
    skipped_errors = 0
    last_progress_time = time.monotonic()
    progress_interval = 0.1
    progress_step = 25
    flush_size = batch_size if progress_callback is None else min(batch_size, progress_step)
    batch: list = []
    next_compact = (
        processed + effective_compact_every
        if effective_compact_every
        else None
    )

    def _maybe_emit_progress(force: bool = False):
        nonlocal last_progress_time
        if not progress_callback:
            return
        now = time.monotonic()
        done = processed + skipped_existing + skipped_errors
        if force or (now - last_progress_time) >= progress_interval or (done % progress_step == 0):
            progress_callback(
                {
                    "processed": done,
                    "ingested": processed,
                    "skipped": skipped_existing + skipped_errors,
                    "skipped_exists": skipped_existing,
                    "skipped_errors": skipped_errors,
                }
            )
            last_progress_time = now

    def _check_interrupt():
        signals.raise_if_interrupted()

    def _maybe_compact(reason: str = "periodic") -> None:
        nonlocal next_compact
        if effective_compact_every is None or next_compact is None:
            return
        if processed < next_compact:
            return
        while next_compact is not None and processed >= next_compact:
            _compact_repository(
                repository,
                compaction_callback=compaction_callback,
                processed=processed,
                reason=reason,
            )
            next_compact = next_compact + effective_compact_every

    for input_file in input_files:
        if not quiet:
            print(f"Ingesting: {input_file}")
        def _error_callback(_payload):
            nonlocal skipped_errors
            skipped_errors += 1
            _maybe_emit_progress()

        for email in pipeline.ingest(
            input_file,
            resume=resume,
            error_callback=_error_callback,
        ):
            _check_interrupt()
            seen += 1
            if not skip_exists_check:
                if repository.exists(email.email_id):
                    skipped_existing += 1
                    _maybe_emit_progress()
                    continue

            batch.append(email)

            if len(batch) >= flush_size:
                _check_interrupt()
                _process_batch(
                    batch,
                    embedding_provider,
                    repository,
                    batch_size=effective_embedding_batch_size,
                    chunk_size=effective_chunk_size,
                    chunk_overlap=effective_chunk_overlap,
                )
                processed += len(batch)
                batch = []
                _maybe_compact()
                _maybe_emit_progress()

        if batch:
            _check_interrupt()
            _process_batch(
                batch,
                embedding_provider,
                repository,
                batch_size=effective_embedding_batch_size,
                chunk_size=effective_chunk_size,
                chunk_overlap=effective_chunk_overlap,
            )
            processed += len(batch)
            batch = []
            _maybe_compact()
            _maybe_emit_progress()

    if progress_callback:
        _maybe_emit_progress(force=True)

    if not quiet:
        print(f"Ingested {processed} emails")
        print(f"Total in database: {repository.count()}")

    if processed > 0:
        _compact_repository(
            repository,
            compaction_callback=compaction_callback,
            processed=processed,
            reason="final",
        )
        if compaction_callback:
            compaction_callback({"phase": "fts_start", "reason": "final"})
        if not quiet:
            print("Creating FTS index...")
        repository.create_fts_index()
        if not quiet:
            print("FTS index created")
        if compaction_callback:
            compaction_callback({"phase": "fts_done", "reason": "final"})
    else:
        if not quiet:
            print("No new emails; skipping FTS index.")

    return processed


def ingest_files_from_embeddings(
    input_files: Iterable[Path],
    embeddings_dir: Path,
    db_path: Path,
    checkpoint_dir: Path,
    resume: bool = True,
    errors_path: Path | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    quiet: bool = False,
    skip_exists_check: bool | None = None,
    ingest_batch_size: int | None = None,
    embedding_batch_size: int | None = None,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
    checkpoint_interval: int | None = None,
    compact_every: int | None = None,
    compaction_callback: Callable[[dict], None] | None = None,
    validate: bool = True,
    strict: bool = False,
    max_errors: int | None = None,
    limit: int | None = None,
    embeddings_map: dict[Path, Path] | None = None,
    repair_missing_embeddings: bool = False,
    embedding_provider=None,
    bulk_import: bool = False,
) -> int:
    signals.install_signal_handlers()

    settings = get_settings()
    effective_ingest_batch_size = (
        ingest_batch_size
        if ingest_batch_size is not None
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
    effective_compact_every = (
        compact_every if compact_every is not None else settings.ingest_compact_every
    )
    if effective_compact_every is not None and effective_compact_every <= 0:
        effective_compact_every = None

    embeddings_dir.mkdir(parents=True, exist_ok=True)
    input_files_list = list(input_files)
    if not input_files_list:
        return 0

    meta_source = None
    for input_file in input_files_list:
        candidate = _resolve_embedding_path(input_file, embeddings_dir, embeddings_map)
        if not candidate.exists():
            continue
        store = EmbeddingStore(candidate)
        meta = store.get_meta()
        store.close()
        if meta:
            meta_source = meta
            break
    if not meta_source:
        raise ValueError(
            f"No embeddings found in {embeddings_dir}. Run the vectorize stage first."
        )

    embedding_dimension = int(meta_source["embedding_dimension"])
    store_chunk_size = int(meta_source["chunk_size"])
    store_chunk_overlap = int(meta_source["chunk_overlap"])
    meta_template = EmbeddingMeta(
        embedding_model=str(meta_source.get("embedding_model", "unknown")),
        embedding_model_revision=meta_source.get("embedding_model_revision"),
        embedding_dimension=embedding_dimension,
        chunk_size=store_chunk_size,
        chunk_overlap=store_chunk_overlap,
        created_at=str(meta_source.get("created_at", "unknown")),
        source=str(meta_source.get("source", "unknown")),
        format_version=int(meta_source.get("format_version", 1)),
    )

    if chunk_size is not None and chunk_size != store_chunk_size:
        raise ValueError(
            f"chunk_size mismatch: embeddings use {store_chunk_size}, got {chunk_size}"
        )
    if chunk_overlap is not None and chunk_overlap != store_chunk_overlap:
        raise ValueError(
            f"chunk_overlap mismatch: embeddings use {store_chunk_overlap}, got {chunk_overlap}"
        )

    effective_chunk_size = store_chunk_size
    effective_chunk_overlap = store_chunk_overlap

    if repair_missing_embeddings and embedding_provider is None:
        embedding_provider = create_embedding_provider(
            settings.embedding_provider,
            model_name=settings.embedding_model,
            model_revision=settings.embedding_model_revision,
        )

    database = Database(db_path)
    repository = EmailRepository(database, dimension=embedding_dimension)
    if skip_exists_check is None:
        if not db_path.exists() or repository.is_empty():
            skip_exists_check = True
        else:
            skip_exists_check = False

    pipeline = IngestPipeline(
        checkpoint_dir=checkpoint_dir,
        checkpoint_interval=effective_checkpoint_interval,
        errors_path=errors_path,
    )

    if bulk_import:
        return _bulk_ingest_from_embeddings(
            input_files_list,
            embeddings_dir=embeddings_dir,
            repository=repository,
            embedding_dimension=embedding_dimension,
            chunk_size=effective_chunk_size,
            chunk_overlap=effective_chunk_overlap,
            embedding_batch_size=effective_embedding_batch_size,
            ingest_batch_size=effective_ingest_batch_size,
            resume=False,
            validate=validate,
            strict=strict,
            max_errors=max_errors,
            limit=limit,
            repair_missing_embeddings=repair_missing_embeddings,
            embedding_provider=embedding_provider,
            progress_callback=progress_callback,
            compaction_callback=compaction_callback,
            errors_path=errors_path,
        )

    processed = 0
    skipped_existing = 0
    skipped_errors = 0
    last_progress_time = time.monotonic()
    progress_interval = 0.1
    progress_step = 25
    batch: list = []
    next_compact = (
        processed + effective_compact_every
        if effective_compact_every
        else None
    )

    def _maybe_emit_progress(force: bool = False):
        nonlocal last_progress_time
        if not progress_callback:
            return
        now = time.monotonic()
        done = processed + skipped_existing + skipped_errors
        if force or (now - last_progress_time) >= progress_interval or (done % progress_step == 0):
            progress_callback(
                {
                    "processed": done,
                    "ingested": processed,
                    "skipped": skipped_existing + skipped_errors,
                    "skipped_exists": skipped_existing,
                    "skipped_errors": skipped_errors,
                }
            )
            last_progress_time = now

    def _check_interrupt():
        signals.raise_if_interrupted()

    def _maybe_compact(reason: str = "periodic") -> None:
        nonlocal next_compact
        if effective_compact_every is None or next_compact is None:
            return
        if processed < next_compact:
            return
        while next_compact is not None and processed >= next_compact:
            _compact_repository(
                repository,
                compaction_callback=compaction_callback,
                processed=processed,
                reason=reason,
            )
            next_compact = next_compact + effective_compact_every

    for input_file in input_files_list:
        embed_path = _resolve_embedding_path(input_file, embeddings_dir, embeddings_map)
        store = EmbeddingStore(embed_path)
        store_meta = store.get_meta()
        if not store_meta:
            if repair_missing_embeddings:
                store.ensure_meta(meta_template)
                store_meta = store.get_meta()
            else:
                store.close()
                raise ValueError(f"Missing metadata in embedding store: {embed_path}")
        if int(store_meta["embedding_dimension"]) != embedding_dimension:
            store.close()
            raise ValueError(
                "Embedding dimension mismatch across embedding files "
                f"({embed_path})"
            )
        if int(store_meta["chunk_size"]) != effective_chunk_size:
            store.close()
            raise ValueError(
                "Chunk size mismatch across embedding files "
                f"({embed_path})"
            )
        if int(store_meta["chunk_overlap"]) != effective_chunk_overlap:
            store.close()
            raise ValueError(
                "Chunk overlap mismatch across embedding files "
                f"({embed_path})"
            )

        if not quiet:
            print(f"Ingesting: {input_file} (embeddings: {embed_path})")

        def _error_callback(_payload):
            nonlocal skipped_errors
            skipped_errors += 1
            _maybe_emit_progress()

        for email in pipeline.ingest(
            input_file,
            resume=resume,
            validate=validate,
            strict=strict,
            max_errors=max_errors,
            error_callback=_error_callback,
        ):
            _check_interrupt()
            if not skip_exists_check:
                if repository.exists(email.email_id):
                    skipped_existing += 1
                    _maybe_emit_progress()
                    continue

            batch.append(email)

            if len(batch) >= effective_ingest_batch_size:
                if limit and processed + len(batch) > limit:
                    batch = batch[: max(0, limit - processed)]
                if batch:
                    _process_batch_from_embeddings(
                        batch,
                        store,
                        repository,
                        chunk_size=effective_chunk_size,
                        chunk_overlap=effective_chunk_overlap,
                        repair_missing_embeddings=repair_missing_embeddings,
                        embedding_provider=embedding_provider,
                        embedding_batch_size=effective_embedding_batch_size,
                    )
                    processed += len(batch)
                    batch = []
                    _maybe_compact()
                    _maybe_emit_progress()

            if limit and processed >= limit:
                break

        if batch and (not limit or processed < limit):
            if limit and processed + len(batch) > limit:
                batch = batch[: max(0, limit - processed)]
            if batch:
                _process_batch_from_embeddings(
                    batch,
                    store,
                    repository,
                    chunk_size=effective_chunk_size,
                    chunk_overlap=effective_chunk_overlap,
                    repair_missing_embeddings=repair_missing_embeddings,
                    embedding_provider=embedding_provider,
                    embedding_batch_size=effective_embedding_batch_size,
                )
                processed += len(batch)
                batch = []
                _maybe_compact()
                _maybe_emit_progress()

        store.close()

        if limit and processed >= limit:
            break

    if progress_callback:
        _maybe_emit_progress(force=True)

    if not quiet:
        print(f"Ingested {processed} emails")
        print(f"Total in database: {repository.count()}")

    if processed > 0:
        _compact_repository(
            repository,
            compaction_callback=compaction_callback,
            processed=processed,
            reason="final",
        )
        if compaction_callback:
            compaction_callback({"phase": "fts_start", "reason": "final"})
        if not quiet:
            print("Creating FTS index...")
        repository.create_fts_index()
        if not quiet:
            print("FTS index created")
        if compaction_callback:
            compaction_callback({"phase": "fts_done", "reason": "final"})
    else:
        if not quiet:
            print("No new emails; skipping FTS index.")

    return processed


def _resolve_embedding_path(
    input_file: Path,
    embeddings_dir: Path,
    embeddings_map: dict[Path, Path] | None,
) -> Path:
    if embeddings_map and input_file in embeddings_map:
        return embeddings_map[input_file]
    return default_embedding_path(input_file, embeddings_dir)


def _compact_repository(
    repository: EmailRepository,
    *,
    compaction_callback: Callable[[dict], None] | None = None,
    processed: int | None = None,
    reason: str = "periodic",
) -> None:
    if compaction_callback:
        compaction_callback({"phase": "start", "processed": processed, "reason": reason})
    start = time.monotonic()
    try:
        if hasattr(repository.table, "optimize"):
            repository.table.optimize()
        elif hasattr(repository.table, "compact_files"):
            repository.table.compact_files()
        if repository.database.table_exists(repository.CHUNKS_TABLE_NAME):
            if hasattr(repository.chunks_table, "optimize"):
                repository.chunks_table.optimize()
            elif hasattr(repository.chunks_table, "compact_files"):
                repository.chunks_table.compact_files()
    except Exception:
        if compaction_callback:
            compaction_callback(
                {
                    "phase": "done",
                    "processed": processed,
                    "reason": reason,
                }
            )
        return
    if compaction_callback:
        compaction_callback(
            {
                "phase": "done",
                "processed": processed,
                "reason": reason,
                "duration_s": time.monotonic() - start,
            }
        )


def _resolve_embeddings_for_batch(
    batch,
    store: EmbeddingStore,
    *,
    chunk_size: int,
    chunk_overlap: int,
    repair_missing_embeddings: bool,
    embedding_provider=None,
    embedding_batch_size: int | None = None,
) -> tuple[dict[str, NDArray[np.float32]], dict[str, list[NDArray[np.float32]]], dict[str, list[str]]]:
    email_ids = [email.email_id for email in batch]
    subject_map = store.fetch_subject_vectors(email_ids)
    chunk_vectors_by_email = store.fetch_chunk_vectors(email_ids)

    chunk_texts_by_email: dict[str, list[str]] = {}
    for email in batch:
        cleaned_body = clean_body_for_embedding(email.body_plain)
        chunks = chunk_text(
            cleaned_body,
            max_chars=chunk_size,
            overlap=chunk_overlap,
        )
        chunk_texts_by_email[email.email_id] = chunks

    missing_subject = [email_id for email_id in email_ids if email_id not in subject_map]
    mismatch_chunks: list[str] = []
    for email_id in email_ids:
        expected = len(chunk_texts_by_email.get(email_id, []))
        actual = len(chunk_vectors_by_email.get(email_id, []))
        if expected != actual:
            mismatch_chunks.append(email_id)

    if (missing_subject or mismatch_chunks) and not repair_missing_embeddings:
        if missing_subject:
            raise ValueError(
                "Missing subject embeddings for "
                f"{len(missing_subject)} emails (e.g. {missing_subject[:3]})"
            )
        raise ValueError(
            "Chunk count mismatch for "
            f"{len(mismatch_chunks)} emails (e.g. {mismatch_chunks[:3]})"
        )

    if (missing_subject or mismatch_chunks) and repair_missing_embeddings:
        if embedding_provider is None:
            raise ValueError("Embedding provider required to repair missing embeddings.")
        repair_ids = set(missing_subject) | set(mismatch_chunks)
        repair_emails = [email for email in batch if email.email_id in repair_ids]
        subject_texts = [f"Subject: {email.subject}".strip() for email in repair_emails]
        subject_vectors = embedding_provider.encode(
            subject_texts,
            batch_size=embedding_batch_size,
            show_progress=False,
        )
        subject_vectors = np.asarray(subject_vectors, dtype=np.float32)

        chunk_texts: list[str] = []
        chunk_email_indices: list[int] = []
        chunk_indices: list[int] = []
        chunk_counts: list[int] = []

        for idx, email in enumerate(repair_emails):
            chunks = chunk_texts_by_email.get(email.email_id, [])
            chunk_counts.append(len(chunks))
            for chunk_index, chunk in enumerate(chunks):
                chunk_texts.append(chunk)
                chunk_email_indices.append(idx)
                chunk_indices.append(chunk_index)

        chunk_vectors = None
        if chunk_texts:
            chunk_vectors = embedding_provider.encode(
                chunk_texts,
                batch_size=embedding_batch_size,
                show_progress=False,
            )
            chunk_vectors = np.asarray(chunk_vectors, dtype=np.float32)

        store.add_batch(
            [email.email_id for email in repair_emails],
            subject_vectors,
            chunk_counts,
            chunk_email_indices,
            chunk_indices,
            chunk_vectors,
        )

        for idx, email in enumerate(repair_emails):
            subject_map[email.email_id] = subject_vectors[idx]

        offset = 0
        for idx, email in enumerate(repair_emails):
            count = chunk_counts[idx]
            if count == 0:
                chunk_vectors_by_email[email.email_id] = []
            else:
                chunk_vectors_by_email[email.email_id] = [
                    chunk_vectors[offset + i] for i in range(count)
                ]
                offset += count

    return subject_map, chunk_vectors_by_email, chunk_texts_by_email


def _resolve_subject_vectors_for_batch(
    batch,
    store: EmbeddingStore,
    *,
    chunk_size: int,
    chunk_overlap: int,
    repair_missing_embeddings: bool,
    embedding_provider=None,
    embedding_batch_size: int | None = None,
) -> dict[str, NDArray[np.float32]]:
    email_ids = [email.email_id for email in batch]
    subject_map = store.fetch_subject_vectors(email_ids)
    missing_subject = [email_id for email_id in email_ids if email_id not in subject_map]

    if missing_subject and not repair_missing_embeddings:
        raise ValueError(
            "Missing subject embeddings for "
            f"{len(missing_subject)} emails (e.g. {missing_subject[:3]})"
        )

    if missing_subject and repair_missing_embeddings:
        if embedding_provider is None:
            raise ValueError("Embedding provider required to repair missing embeddings.")
        repair_emails = [email for email in batch if email.email_id in missing_subject]
        subject_texts = [f"Subject: {email.subject}".strip() for email in repair_emails]
        subject_vectors = embedding_provider.encode(
            subject_texts,
            batch_size=embedding_batch_size,
            show_progress=False,
        )
        subject_vectors = np.asarray(subject_vectors, dtype=np.float32)
        chunk_counts = []
        for email in repair_emails:
            cleaned_body = clean_body_for_embedding(email.body_plain)
            chunks = chunk_text(
                cleaned_body,
                max_chars=chunk_size,
                overlap=chunk_overlap,
            )
            chunk_counts.append(len(chunks))

        store.add_batch(
            [email.email_id for email in repair_emails],
            subject_vectors,
            chunk_counts,
            [],
            [],
            None,
        )

        for idx, email in enumerate(repair_emails):
            subject_map[email.email_id] = subject_vectors[idx]

    return subject_map


def _process_batch_from_embeddings(
    batch,
    store: EmbeddingStore,
    repository: EmailRepository,
    chunk_size: int,
    chunk_overlap: int,
    repair_missing_embeddings: bool = False,
    embedding_provider=None,
    embedding_batch_size: int | None = None,
) -> None:
    signals.raise_if_interrupted()
    email_ids = [email.email_id for email in batch]
    subject_map, chunk_vectors_by_email, chunk_texts_by_email = _resolve_embeddings_for_batch(
        batch,
        store,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        repair_missing_embeddings=repair_missing_embeddings,
        embedding_provider=embedding_provider,
        embedding_batch_size=embedding_batch_size,
    )

    subject_vectors = np.array(
        [subject_map[email_id] for email_id in email_ids], dtype=np.float32
    )

    chunk_texts: list[str] = []
    chunk_email_indices: list[int] = []
    chunk_indices: list[int] = []
    chunk_vectors: list[NDArray[np.float32]] = []

    for idx, email in enumerate(batch):
        chunks = chunk_texts_by_email.get(email.email_id, [])
        vectors = chunk_vectors_by_email.get(email.email_id, [])
        if len(chunks) != len(vectors):
            raise ValueError(
                f"Chunk count mismatch for {email.email_id}: "
                f"{len(chunks)} text chunks vs {len(vectors)} vectors"
            )
        for chunk_index, chunk in enumerate(chunks):
            chunk_texts.append(chunk)
            chunk_email_indices.append(idx)
            chunk_indices.append(chunk_index)
            if vectors:
                chunk_vectors.append(vectors[chunk_index])

    chunk_vectors_array = None
    if chunk_vectors:
        chunk_vectors_array = np.array(chunk_vectors, dtype=np.float32)

    repository.add_batch(
        batch,
        subject_vectors,
        chunk_texts,
        chunk_vectors_array,
        chunk_email_indices,
        chunk_indices,
    )


def _process_batch(
    batch,
    embedding_provider,
    repository,
    batch_size: int,
    chunk_size: int,
    chunk_overlap: int,
):
    """Process a batch of emails."""
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

    for idx, email in enumerate(batch):
        cleaned_body = clean_body_for_embedding(email.body_plain)
        chunks = chunk_text(
            cleaned_body,
            max_chars=chunk_size,
            overlap=chunk_overlap,
        )
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

    repository.add_batch(
        batch,
        subject_vectors,
        chunk_texts,
        chunk_vectors,
        chunk_email_indices,
        chunk_indices,
    )


def _bulk_ingest_from_embeddings(
    input_files: list[Path],
    *,
    embeddings_dir: Path,
    repository: EmailRepository,
    embedding_dimension: int,
    chunk_size: int,
    chunk_overlap: int,
    embedding_batch_size: int,
    ingest_batch_size: int,
    resume: bool,
    validate: bool,
    strict: bool,
    max_errors: int | None,
    limit: int | None,
    repair_missing_embeddings: bool,
    embedding_provider,
    progress_callback: Callable[[dict], None] | None,
    compaction_callback: Callable[[dict], None] | None,
    errors_path: Path | None,
) -> int:
    processed = 0
    skipped_errors = 0
    last_progress_time = time.monotonic()
    progress_interval = 0.1
    progress_step = 25

    EmailRecordFlat = create_email_schema_flat(embedding_dimension)
    EmailChunkRecord = create_email_chunk_schema(embedding_dimension)
    email_schema = EmailRecordFlat.to_arrow_schema()
    chunk_schema = EmailChunkRecord.to_arrow_schema()

    def _check_interrupt():
        signals.raise_if_interrupted()

    def _maybe_emit_progress(force: bool = False):
        nonlocal last_progress_time
        if not progress_callback:
            return
        now = time.monotonic()
        done = processed + skipped_errors
        if force or (now - last_progress_time) >= progress_interval or (done % progress_step == 0):
            progress_callback(
                {
                    "processed": done,
                    "ingested": processed,
                    "skipped": skipped_errors,
                    "skipped_exists": 0,
                    "skipped_errors": skipped_errors,
                }
            )
            last_progress_time = now

    def _error_callback(_payload):
        nonlocal skipped_errors
        skipped_errors += 1
        _maybe_emit_progress()

    def _iter_email_batches():
        nonlocal processed
        for input_file in input_files:
            _check_interrupt()
            store = EmbeddingStore(_resolve_embedding_path(input_file, embeddings_dir, None))
            pipeline = IngestPipeline(
                checkpoint_dir=None,
                checkpoint_interval=0,
                errors_path=errors_path,
            )
            batch: list = []
            for email in pipeline.ingest(
                input_file,
                resume=resume,
                validate=validate,
                strict=strict,
                max_errors=max_errors,
                error_callback=_error_callback,
            ):
                _check_interrupt()
                batch.append(email)
                if len(batch) >= ingest_batch_size:
                    _check_interrupt()
                    if limit and processed + len(batch) > limit:
                        batch = batch[: max(0, limit - processed)]
                    if not batch:
                        break
                    subject_map = _resolve_subject_vectors_for_batch(
                        batch,
                        store,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        repair_missing_embeddings=repair_missing_embeddings,
                        embedding_provider=embedding_provider,
                        embedding_batch_size=embedding_batch_size,
                    )
                    records = [
                        repository._to_record(email, subject_map[email.email_id])
                        for email in batch
                    ]
                    if records:
                        yield pa.RecordBatch.from_pylist(records, schema=email_schema)
                    processed += len(batch)
                    batch = []
                    _maybe_emit_progress()
                if limit and processed >= limit:
                    break

            if batch and (not limit or processed < limit):
                _check_interrupt()
                if limit and processed + len(batch) > limit:
                    batch = batch[: max(0, limit - processed)]
                if not batch:
                    batch = []
                else:
                    subject_map = _resolve_subject_vectors_for_batch(
                        batch,
                        store,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        repair_missing_embeddings=repair_missing_embeddings,
                        embedding_provider=embedding_provider,
                        embedding_batch_size=embedding_batch_size,
                    )
                    records = [
                        repository._to_record(email, subject_map[email.email_id])
                        for email in batch
                    ]
                    if records:
                        yield pa.RecordBatch.from_pylist(records, schema=email_schema)
                    processed += len(batch)
                    batch = []
                    _maybe_emit_progress()

            store.close()

            if limit and processed >= limit:
                break

    def _iter_chunk_batches():
        chunk_processed = 0
        for input_file in input_files:
            _check_interrupt()
            store = EmbeddingStore(_resolve_embedding_path(input_file, embeddings_dir, None))
            pipeline = IngestPipeline(
                checkpoint_dir=None,
                checkpoint_interval=0,
                errors_path=None,
            )
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
                if len(batch) >= ingest_batch_size:
                    _check_interrupt()
                    if limit and chunk_processed + len(batch) > limit:
                        batch = batch[: max(0, limit - chunk_processed)]
                    if not batch:
                        break
                    chunk_records = _build_chunk_records(
                        batch,
                        store,
                        repository,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        repair_missing_embeddings=repair_missing_embeddings,
                        embedding_provider=embedding_provider,
                        embedding_batch_size=embedding_batch_size,
                    )
                    if chunk_records:
                        yield pa.RecordBatch.from_pylist(chunk_records, schema=chunk_schema)
                    chunk_processed += len(batch)
                    batch = []
                if limit and chunk_processed >= limit:
                    break

            if batch and (not limit or chunk_processed < limit):
                _check_interrupt()
                if limit and chunk_processed + len(batch) > limit:
                    batch = batch[: max(0, limit - chunk_processed)]
                if not batch:
                    batch = []
                else:
                    chunk_records = _build_chunk_records(
                        batch,
                        store,
                        repository,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        repair_missing_embeddings=repair_missing_embeddings,
                        embedding_provider=embedding_provider,
                        embedding_batch_size=embedding_batch_size,
                    )
                    if chunk_records:
                        yield pa.RecordBatch.from_pylist(chunk_records, schema=chunk_schema)
                    chunk_processed += len(batch)
                    batch = []

            store.close()

            if limit and chunk_processed >= limit:
                break

    def _email_chain():
        yield pa.RecordBatch.from_pylist([], schema=email_schema)
        yield from _iter_email_batches()

    repository.database.db.create_table(
        repository.TABLE_NAME,
        data=_email_chain(),
        schema=email_schema,
    )

    def _chunk_chain():
        yield pa.RecordBatch.from_pylist([], schema=chunk_schema)
        yield from _iter_chunk_batches()

    repository.database.db.create_table(
        repository.CHUNKS_TABLE_NAME,
        data=_chunk_chain(),
        schema=chunk_schema,
    )

    if progress_callback:
        _maybe_emit_progress(force=True)

    if processed > 0:
        _compact_repository(
            repository,
            compaction_callback=compaction_callback,
            processed=processed,
            reason="final",
        )
        if compaction_callback:
            compaction_callback({"phase": "fts_start", "reason": "final"})
        repository.create_fts_index()
        if compaction_callback:
            compaction_callback({"phase": "fts_done", "reason": "final"})

    return processed


def _build_chunk_records(
    batch,
    store: EmbeddingStore,
    repository: EmailRepository,
    *,
    chunk_size: int,
    chunk_overlap: int,
    repair_missing_embeddings: bool,
    embedding_provider,
    embedding_batch_size: int | None,
) -> list[dict]:
    subject_map, chunk_vectors_by_email, chunk_texts_by_email = _resolve_embeddings_for_batch(
        batch,
        store,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        repair_missing_embeddings=repair_missing_embeddings,
        embedding_provider=embedding_provider,
        embedding_batch_size=embedding_batch_size,
    )

    chunk_records: list[dict] = []
    for idx, email in enumerate(batch):
        chunks = chunk_texts_by_email.get(email.email_id, [])
        vectors = chunk_vectors_by_email.get(email.email_id, [])
        if len(chunks) != len(vectors):
            raise ValueError(
                f"Chunk count mismatch for {email.email_id}: "
                f"{len(chunks)} text chunks vs {len(vectors)} vectors"
            )
        for chunk_index, chunk in enumerate(chunks):
            chunk_records.append(
                repository._to_chunk_record(
                    email,
                    chunk,
                    chunk_index,
                    vectors[chunk_index],
                )
            )
    return chunk_records

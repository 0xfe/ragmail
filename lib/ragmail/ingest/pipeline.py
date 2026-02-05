"""Ingestion pipeline orchestration."""

import json
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Callable

from rich.progress import Progress, TaskID

from .email_parser import EmailParser, ParsedEmail
from .json_email_parser import JsonEmailParser
from .jsonl_reader import JsonlReader
from .mbox_reader import MboxReader
from .validation import JsonEmailValidator, ValidationIssue


class IngestStrictError(Exception):
    """Raised to stop ingestion when strict validation is enabled."""


class IngestPipeline:
    """Orchestrates ingestion with progress tracking and checkpointing."""

    def __init__(
        self,
        checkpoint_dir: Path | None = None,
        checkpoint_interval: int = 120,
        errors_path: Path | None = None,
    ):
        """Initialize the ingestion pipeline.

        Args:
            checkpoint_dir: Directory for checkpoint files
            checkpoint_interval: How often to save checkpoints (seconds)
            errors_path: Path for structured error logs (JSONL)
        """
        self.mbox_parser = EmailParser()
        self.json_parser = JsonEmailParser()
        self.validator = JsonEmailValidator()
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_interval = checkpoint_interval
        self.errors_path = errors_path

    def reader_for(self, input_path: Path):
        """Select a reader based on the input file extension."""
        suffix = input_path.suffix.lower()
        if suffix in {".jsonl", ".json"}:
            return JsonlReader(input_path)
        return MboxReader(input_path)

    def _parser_for(self, input_path: Path):
        """Select a parser based on the input file extension."""
        suffix = input_path.suffix.lower()
        if suffix in {".jsonl", ".json"}:
            return self.json_parser
        return self.mbox_parser

    def ingest(
        self,
        input_path: Path,
        callback: Callable[[ParsedEmail], None] | None = None,
        progress: Progress | None = None,
        task_id: TaskID | None = None,
        resume: bool = True,
        validate: bool = True,
        strict: bool = False,
        max_errors: int | None = None,
        validate_only: bool = False,
        error_callback: Callable[[dict], None] | None = None,
    ) -> Iterator[ParsedEmail]:
        """Ingest emails from a JSONL or MBOX file.

        Args:
            input_path: Path to the input file
            callback: Optional callback for each parsed email
            progress: Rich progress bar instance
            task_id: Task ID for progress updates
            resume: Whether to resume from checkpoint
            validate: Whether to validate JSONL records before parsing
            strict: Fail on first validation error
            max_errors: Stop after this many errors
            validate_only: Only validate records without parsing

        Yields:
            Parsed email objects
        """
        reader = self.reader_for(input_path)
        parser = self._parser_for(input_path)
        checkpoint = self._load_checkpoint(input_path) if resume else None
        start_index = checkpoint.get("processed", 0) if checkpoint else 0

        processed = 0
        last_checkpoint = time.monotonic()
        error_count = 0
        last_index = start_index - 1
        try:
            for idx, message in enumerate(reader):
                last_index = idx
                if idx < start_index:
                    if progress and task_id is not None:
                        progress.advance(task_id)
                    continue

                try:
                    if isinstance(message, dict) and message.get("__ragmail_error__") == "json_decode":
                        error_count += 1
                        if error_callback:
                            error_callback(
                                {
                                    "kind": "json_decode",
                                    "index": idx,
                                }
                            )
                        self._log_error(
                            input_path,
                            idx,
                            kind="json_decode",
                            line=message.get("__line__"),
                            message=message.get("__error__", "JSON decode error"),
                            raw=message.get("__raw__"),
                        )
                        if strict or (max_errors and error_count >= max_errors):
                            raise IngestStrictError("JSON decode error")
                        if progress and task_id is not None:
                            progress.advance(task_id)
                        continue

                    if validate and parser is self.json_parser:
                        issues = self.validator.validate(message)
                        if issues:
                            error_count += 1
                            extra = None
                            if isinstance(message, dict):
                                headers = message.get("headers")
                                if not isinstance(headers, dict):
                                    headers = {}
                                attachments = message.get("attachments")
                                content = message.get("content")
                                extra = {
                                    "subject": headers.get("subject"),
                                    "message_id": headers.get("message_id"),
                                    "thread_id": headers.get("thread_id"),
                                    "attachments": len(attachments)
                                    if isinstance(attachments, list)
                                    else None,
                                    "content_blocks": len(content) if isinstance(content, list) else None,
                                }
                            if error_callback:
                                error_callback(
                                    {
                                        "kind": "validation",
                                        "index": idx,
                                        "issues": issues,
                                        "extra": extra,
                                    }
                                )
                            self._log_error(
                                input_path,
                                idx,
                                kind="validation",
                                issues=issues,
                                extra=extra,
                            )
                            if strict or (max_errors and error_count >= max_errors):
                                raise IngestStrictError("Validation error")
                            if progress and task_id is not None:
                                progress.advance(task_id)
                            continue

                    if validate_only:
                        if progress and task_id is not None:
                            progress.advance(task_id)
                        continue

                    email = parser.parse(message)
                    if callback:
                        callback(email)
                    yield email
                    processed += 1

                    if progress and task_id is not None:
                        progress.advance(task_id)

                    if (
                        self.checkpoint_dir
                        and (time.monotonic() - last_checkpoint) >= self.checkpoint_interval
                    ):
                        self._save_checkpoint(input_path, idx + 1)
                        last_checkpoint = time.monotonic()

                except IngestStrictError:
                    raise
                except Exception as e:
                    if error_callback:
                        error_callback(
                            {
                                "kind": "parse",
                                "index": idx,
                            }
                        )
                    self._log_error(input_path, idx, kind="parse", message=str(e))
                    if strict:
                        raise
                    continue
        except KeyboardInterrupt:
            if self.checkpoint_dir and last_index >= 0:
                self._save_checkpoint(input_path, last_index + 1)
            raise

        if self.checkpoint_dir:
            self._clear_checkpoint(input_path)

    def validate(
        self,
        input_path: Path,
        progress: Progress | None = None,
        task_id: TaskID | None = None,
        strict: bool = False,
        max_errors: int | None = None,
    ) -> dict[str, int]:
        """Validate records without parsing or storing.

        Returns:
            Dictionary with total, valid, and errors counts.
        """
        reader = self.reader_for(input_path)
        parser = self._parser_for(input_path)

        if parser is not self.json_parser:
            raise ValueError("Validation only supported for JSONL inputs")

        total = 0
        valid = 0
        errors = 0

        for idx, message in enumerate(reader):
            total += 1
            if isinstance(message, dict) and message.get("__ragmail_error__") == "json_decode":
                errors += 1
                self._log_error(
                    input_path,
                    idx,
                    kind="json_decode",
                    line=message.get("__line__"),
                    message=message.get("__error__", "JSON decode error"),
                    raw=message.get("__raw__"),
                )
                if strict or (max_errors and errors >= max_errors):
                    break
                if progress and task_id is not None:
                    progress.advance(task_id)
                continue

            issues = self.validator.validate(message)
            if issues:
                errors += 1
                self._log_error(input_path, idx, kind="validation", issues=issues)
                if strict or (max_errors and errors >= max_errors):
                    break
            else:
                valid += 1

            if progress and task_id is not None:
                progress.advance(task_id)

        return {"total": total, "valid": valid, "errors": errors}

    def dry_run(
        self,
        input_path: Path,
        limit: int | None = None,
        progress: Progress | None = None,
        task_id: TaskID | None = None,
        validate: bool = True,
        strict: bool = False,
        max_errors: int | None = None,
    ) -> list[ParsedEmail]:
        """Parse emails without storing them.

        Args:
            input_path: Path to the input file
            limit: Maximum number of emails to parse
            progress: Rich progress bar instance
            task_id: Task ID for progress updates
            validate: Whether to validate JSONL records before parsing
            strict: Fail on first validation error
            max_errors: Stop after this many errors

        Returns:
            List of parsed emails
        """
        emails: list[ParsedEmail] = []
        count = 0

        for email in self.ingest(
            input_path,
            progress=progress,
            task_id=task_id,
            resume=False,
            validate=validate,
            strict=strict,
            max_errors=max_errors,
        ):
            emails.append(email)
            count += 1
            if limit and count >= limit:
                break

        return emails

    def _get_checkpoint_path(self, input_path: Path) -> Path | None:
        """Get checkpoint file path for an input file."""
        if not self.checkpoint_dir:
            return None
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        return self.checkpoint_dir / f"{input_path.stem}.checkpoint.json"

    def _load_checkpoint(self, input_path: Path) -> dict | None:
        """Load checkpoint for resumable ingestion."""
        checkpoint_path = self._get_checkpoint_path(input_path)
        if checkpoint_path and checkpoint_path.exists():
            with open(checkpoint_path) as f:
                return json.load(f)
        return None

    def _save_checkpoint(self, input_path: Path, processed: int) -> None:
        """Save ingestion checkpoint."""
        checkpoint_path = self._get_checkpoint_path(input_path)
        if checkpoint_path:
            with open(checkpoint_path, "w") as f:
                json.dump({"processed": processed, "source": str(input_path)}, f)

    def _clear_checkpoint(self, input_path: Path) -> None:
        """Remove checkpoint file after successful completion."""
        checkpoint_path = self._get_checkpoint_path(input_path)
        if checkpoint_path and checkpoint_path.exists():
            checkpoint_path.unlink()

    def _log_error(
        self,
        input_path: Path,
        index: int,
        *,
        kind: str,
        message: str | None = None,
        line: int | None = None,
        raw: str | None = None,
        issues: list[ValidationIssue] | None = None,
        extra: dict | None = None,
    ) -> None:
        """Log parsing or validation error for an email."""
        error_path = self._get_errors_path()
        if not error_path:
            return
        payload = {
            "timestamp": datetime.now().isoformat(),
            "source": str(input_path),
            "index": index,
            "kind": kind,
        }
        if message:
            payload["message"] = message
        if line is not None:
            payload["line"] = line
        if raw is not None:
            payload["raw"] = raw
        if issues:
            payload["issues"] = [
                {
                    "code": issue.code,
                    "field": issue.field,
                    "message": issue.message,
                    "value": issue.value,
                }
                for issue in issues
            ]
        if extra:
            payload["extra"] = extra

        error_path.parent.mkdir(parents=True, exist_ok=True)
        with open(error_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _get_errors_path(self) -> Path | None:
        if self.errors_path:
            return self.errors_path
        if self.checkpoint_dir:
            return self.checkpoint_dir / "errors.jsonl"
        return None

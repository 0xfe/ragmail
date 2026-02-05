"""Validation helpers for cleaned JSONL email records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    field: str
    message: str
    value: Any | None = None


class JsonEmailValidator:
    """Validates cleaned JSON email records before ingestion."""

    def validate(self, record: Any) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        if not isinstance(record, dict):
            return [
                ValidationIssue(
                    code="record.type",
                    field="record",
                    message="Record must be an object",
                    value=type(record).__name__,
                )
            ]

        headers = record.get("headers")
        if not isinstance(headers, dict):
            issues.append(
                ValidationIssue(
                    code="headers.type",
                    field="headers",
                    message="headers must be an object",
                    value=type(headers).__name__ if headers is not None else None,
                )
            )
            headers = {}

        content = record.get("content")
        if not isinstance(content, list):
            issues.append(
                ValidationIssue(
                    code="content.type",
                    field="content",
                    message="content must be a list",
                    value=type(content).__name__ if content is not None else None,
                )
            )
            content = []

        tags = record.get("tags")
        if tags is not None and not isinstance(tags, list):
            issues.append(
                ValidationIssue(
                    code="tags.type",
                    field="tags",
                    message="tags must be a list of strings",
                    value=type(tags).__name__,
                )
            )
        elif isinstance(tags, list):
            for idx, tag in enumerate(tags):
                if not isinstance(tag, str) or not tag.strip():
                    issues.append(
                        ValidationIssue(
                            code="tags.value",
                            field=f"tags[{idx}]",
                            message="tag must be a non-empty string",
                            value=tag,
                        )
                    )

        from_value = headers.get("from")
        if from_value is not None:
            email_addr = _extract_email(from_value)
            if not email_addr:
                issues.append(
                    ValidationIssue(
                        code="headers.from.email",
                        field="headers.from",
                        message="from email must be non-empty",
                        value=from_value,
                    )
                )

        for field in ("to", "cc", "bcc"):
            list_value = headers.get(field)
            if list_value is None:
                continue
            if not isinstance(list_value, list):
                issues.append(
                    ValidationIssue(
                        code=f"headers.{field}.type",
                        field=f"headers.{field}",
                        message=f"{field} must be a list",
                        value=type(list_value).__name__,
                    )
                )
                continue
            for idx, entry in enumerate(list_value):
                email_addr = _extract_email(entry)
                if not email_addr:
                    issues.append(
                        ValidationIssue(
                            code=f"headers.{field}.email",
                            field=f"headers.{field}[{idx}]",
                            message="address must include an email",
                            value=entry,
                        )
                    )

        date_value = headers.get("date")
        if date_value is not None and not _parse_date(date_value):
            issues.append(
                ValidationIssue(
                    code="headers.date",
                    field="headers.date",
                    message="date must be ISO 8601 or RFC 2822",
                    value=date_value,
                )
            )

        references = headers.get("references")
        if references is not None:
            if isinstance(references, list):
                for idx, ref in enumerate(references):
                    if not isinstance(ref, str) or not ref.strip():
                        issues.append(
                            ValidationIssue(
                                code="headers.references.value",
                                field=f"headers.references[{idx}]",
                                message="reference must be a non-empty string",
                                value=ref,
                            )
                        )
            elif isinstance(references, str):
                if not references.strip():
                    issues.append(
                        ValidationIssue(
                            code="headers.references.value",
                            field="headers.references",
                            message="references must be non-empty",
                            value=references,
                        )
                    )
            else:
                issues.append(
                    ValidationIssue(
                        code="headers.references.type",
                        field="headers.references",
                        message="references must be a list or string",
                        value=type(references).__name__,
                    )
                )

        attachments = record.get("attachments")
        has_attachments = isinstance(attachments, list) and len(attachments) > 0
        if attachments is not None:
            if not isinstance(attachments, list):
                issues.append(
                    ValidationIssue(
                        code="attachments.type",
                        field="attachments",
                        message="attachments must be a list",
                        value=type(attachments).__name__,
                    )
                )
            else:
                for idx, att in enumerate(attachments):
                    if not isinstance(att, dict):
                        issues.append(
                            ValidationIssue(
                                code="attachments.value",
                                field=f"attachments[{idx}]",
                                message="attachment must be an object",
                                value=att,
                            )
                        )
                        continue
                    size = att.get("size")
                    if size is not None and not isinstance(size, int):
                        issues.append(
                            ValidationIssue(
                                code="attachments.size",
                                field=f"attachments[{idx}].size",
                                message="attachment size must be an integer",
                                value=size,
                            )
                        )

        if content:
            has_text = False
            for idx, block in enumerate(content):
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "text":
                    continue
                body = block.get("body")
                if isinstance(body, str) and body.strip():
                    has_text = True
                    break
                issues.append(
                    ValidationIssue(
                        code="content.body",
                        field=f"content[{idx}].body",
                        message="text block body must be non-empty",
                        value=body,
                    )
                )
            if not has_text and not has_attachments:
                issues.append(
                    ValidationIssue(
                        code="content.text",
                        field="content",
                        message="content must include at least one text block",
                    )
                )
        else:
            if not has_attachments:
                issues.append(
                    ValidationIssue(
                        code="content.empty",
                        field="content",
                        message="content must include at least one text block",
                    )
                )

        return issues


def _extract_email(value: Any) -> str:
    if isinstance(value, dict):
        email_addr = str(value.get("email", "") or "").strip()
        return email_addr
    if isinstance(value, str):
        _, email_addr = parseaddr(value)
        return email_addr.strip()
    return ""


def _parse_date(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            try:
                return parsedate_to_datetime(value)
            except Exception:
                return None
    return None

"""Parser for cleaned JSONL email records."""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any

from .email_parser import Attachment, ParsedEmail


@dataclass
class JsonEmailParser:
    """Parse cleaned JSON email records into ParsedEmail objects."""

    def parse(self, record: dict[str, Any]) -> ParsedEmail:
        """Parse a cleaned JSON email record."""
        headers = record.get("headers", {}) or {}
        tags = record.get("tags", []) or []
        content_blocks = record.get("content", []) or []
        attachments_raw = record.get("attachments", []) or []
        mbox_info = record.get("mbox") if isinstance(record.get("mbox"), dict) else {}

        subject = headers.get("subject", "") or ""

        from_name, from_address = self._parse_single_address(headers.get("from"))
        to_addresses = self._parse_address_list(headers.get("to"))
        cc_addresses = self._parse_address_list(headers.get("cc"))
        date = self._parse_date(headers.get("date"))
        message_id = headers.get("message_id")
        in_reply_to = headers.get("in_reply_to")
        references = self._parse_references(headers.get("references"))
        thread_id = headers.get("thread_id")

        body_plain = self._extract_body(content_blocks)
        body_html = ""

        attachments = self._parse_attachments(attachments_raw)
        has_attachment = len(attachments) > 0

        email_id = self._generate_email_id(message_id, from_address, date, subject)
        if not thread_id:
            thread_id = self._generate_thread_id(references, in_reply_to, subject)

        return ParsedEmail(
            email_id=email_id,
            message_id=message_id,
            subject=subject,
            from_address=from_address,
            from_name=from_name,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            date=date,
            body_plain=body_plain,
            body_html=body_html,
            has_attachment=has_attachment,
            attachments=attachments,
            labels=[str(tag) for tag in tags if str(tag).strip()],
            in_reply_to=in_reply_to,
            references=references,
            thread_id=thread_id,
            mbox_file=self._parse_mbox_value(mbox_info, record, "file"),
            mbox_offset=self._parse_mbox_int(mbox_info, record, "offset"),
            mbox_length=self._parse_mbox_int(mbox_info, record, "length"),
        )

    def _parse_mbox_value(
        self,
        mbox_info: dict[str, Any],
        record: dict[str, Any],
        key: str,
    ) -> str | None:
        value = mbox_info.get(key)
        if not value:
            value = record.get(f"mbox_{key}")
        if value is None:
            return None
        return str(value)

    def _parse_mbox_int(
        self,
        mbox_info: dict[str, Any],
        record: dict[str, Any],
        key: str,
    ) -> int | None:
        value = mbox_info.get(key)
        if value is None:
            value = record.get(f"mbox_{key}")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _parse_single_address(self, value: Any) -> tuple[str, str]:
        if isinstance(value, dict):
            name = str(value.get("name", "") or "")
            email_addr = str(value.get("email", "") or "")
            return name, email_addr.lower()
        if isinstance(value, str):
            name, email_addr = parseaddr(value)
            return name, email_addr.lower()
        return "", ""

    def _parse_address_list(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            emails: list[str] = []
            for item in value:
                _, email_addr = self._parse_single_address(item)
                if email_addr:
                    emails.append(email_addr.lower())
            return emails
        if isinstance(value, str):
            _, email_addr = self._parse_single_address(value)
            return [email_addr.lower()] if email_addr else []
        return []

    def _parse_date(self, value: Any) -> datetime | None:
        if not value:
            return None
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

    def _parse_references(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split() if item.strip()]
        return []

    def _extract_body(self, blocks: list[dict[str, Any]]) -> str:
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "text":
                continue
            body = block.get("body", "")
            if body:
                parts.append(str(body).strip())
        return "\n\n".join(part for part in parts if part)

    def _parse_attachments(self, items: list[Any]) -> list[Attachment]:
        attachments: list[Attachment] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            filename = str(item.get("filename", "") or "unnamed")
            content_type = str(item.get("content_type", "") or "application/octet-stream")
            size = item.get("size")
            if isinstance(size, str) and size.isdigit():
                size = int(size)
            attachments.append(
                Attachment(
                    filename=filename,
                    content_type=content_type,
                    size=size if isinstance(size, int) else None,
                )
            )
        return attachments

    def _generate_email_id(
        self,
        message_id: str | None,
        from_address: str,
        date: datetime | None,
        subject: str,
    ) -> str:
        if message_id:
            return hashlib.sha256(message_id.encode()).hexdigest()[:16]

        components = [
            from_address,
            date.isoformat() if date else "",
            subject[:100],
        ]
        content = "|".join(components)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _generate_thread_id(
        self,
        references: list[str],
        in_reply_to: str | None,
        subject: str,
    ) -> str | None:
        if references:
            return hashlib.sha256(references[0].encode()).hexdigest()[:16]
        if in_reply_to:
            return hashlib.sha256(in_reply_to.encode()).hexdigest()[:16]

        normalized_subject = re.sub(
            r"^(re:|fwd?:)\s*", "", subject.lower(), flags=re.IGNORECASE
        )
        if normalized_subject:
            return hashlib.sha256(normalized_subject.encode()).hexdigest()[:16]

        return None

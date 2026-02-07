"""Email message parsing with encoding fallbacks."""

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from email.header import decode_header
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any


@dataclass
class Attachment:
    """Represents an email attachment."""

    filename: str
    content_type: str
    size: int | None = None


@dataclass
class ParsedEmail:
    """Parsed email with extracted fields."""

    email_id: str
    message_id: str | None
    subject: str
    from_address: str
    from_name: str
    to_addresses: list[str]
    cc_addresses: list[str]
    date: datetime | None
    body_plain: str
    body_html: str
    has_attachment: bool
    attachments: list[Attachment] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    in_reply_to: str | None = None
    references: list[str] = field(default_factory=list)
    thread_id: str | None = None
    mbox_file: str | None = None
    mbox_offset: int | None = None
    mbox_length: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "email_id": self.email_id,
            "message_id": self.message_id,
            "subject": self.subject,
            "from_address": self.from_address,
            "from_name": self.from_name,
            "to_addresses": self.to_addresses,
            "cc_addresses": self.cc_addresses,
            "date": self.date,
            "body_plain": self.body_plain,
            "body_html": self.body_html,
            "has_attachment": self.has_attachment,
            "labels": self.labels,
            "in_reply_to": self.in_reply_to,
            "references": self.references,
            "thread_id": self.thread_id,
            "mbox_file": self.mbox_file,
            "mbox_offset": self.mbox_offset,
            "mbox_length": self.mbox_length,
        }


class EmailParser:
    """Parse email messages with robust encoding handling."""

    ENCODINGS = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "ascii"]

    def parse(self, message: Message) -> ParsedEmail:
        """Parse an email message into structured data.

        Args:
            message: Email message object

        Returns:
            ParsedEmail with extracted fields
        """
        subject = self._decode_header(message.get("Subject", ""))
        from_name, from_address = self._parse_address(message.get("From", ""))
        to_addresses = self._parse_address_list(message.get("To", ""))
        cc_addresses = self._parse_address_list(message.get("Cc", ""))
        date = self._parse_date(message.get("Date"))
        message_id = message.get("Message-ID")
        in_reply_to = message.get("In-Reply-To")
        references = self._parse_references(message.get("References", ""))
        labels = self._extract_labels(message)

        body_plain, body_html, attachments = self._extract_body_and_attachments(message)

        email_id = self._generate_email_id(message_id, from_address, date, subject)
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
            has_attachment=len(attachments) > 0,
            attachments=attachments,
            labels=labels,
            in_reply_to=in_reply_to,
            references=references,
            thread_id=thread_id,
        )

    def _decode_header(self, header: str | None) -> str:
        """Decode an email header with encoding fallbacks."""
        if not header:
            return ""

        try:
            parts = decode_header(header)
            decoded_parts = []
            for content, charset in parts:
                if isinstance(content, bytes):
                    decoded_parts.append(
                        self._decode_bytes(content, charset)
                    )
                else:
                    decoded_parts.append(content)
            return " ".join(decoded_parts)
        except Exception:
            return str(header)

    def _decode_bytes(self, content: bytes, charset: str | None) -> str:
        """Decode bytes with charset and fallbacks."""
        encodings = [charset] if charset else []
        encodings.extend(self.ENCODINGS)

        for encoding in encodings:
            if not encoding:
                continue
            try:
                return content.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                continue

        return content.decode("utf-8", errors="replace")

    def _parse_address(self, addr: str | None) -> tuple[str, str]:
        """Parse email address into name and address."""
        if not addr:
            return "", ""
        decoded = self._decode_header(addr)
        name, address = parseaddr(decoded)
        return name, address.lower()

    def _parse_address_list(self, addresses: str | None) -> list[str]:
        """Parse comma-separated email addresses."""
        if not addresses:
            return []

        decoded = self._decode_header(addresses)
        result = []
        for addr in decoded.split(","):
            _, email = parseaddr(addr.strip())
            if email:
                result.append(email.lower())
        return result

    def _parse_date(self, date_str: str | None) -> datetime | None:
        """Parse email date with fallbacks."""
        if not date_str:
            return None

        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            return None

    def _parse_references(self, references: str | None) -> list[str]:
        """Parse References header into list of message IDs."""
        if not references:
            return []
        return [ref.strip() for ref in references.split() if ref.strip()]

    def _extract_labels(self, message: Message) -> list[str]:
        """Extract Gmail labels from X-Gmail-Labels header."""
        labels_header = message.get("X-Gmail-Labels", "")
        if not labels_header:
            return []
        return [label.strip() for label in labels_header.split(",") if label.strip()]

    def _extract_body_and_attachments(
        self, message: Message
    ) -> tuple[str, str, list[Attachment]]:
        """Extract body text and attachments from message."""
        body_plain = ""
        body_html = ""
        attachments: list[Attachment] = []

        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in content_disposition:
                    attachment = self._extract_attachment(part)
                    if attachment:
                        attachments.append(attachment)
                elif content_type == "text/plain" and not body_plain:
                    body_plain = self._get_text_content(part)
                elif content_type == "text/html" and not body_html:
                    body_html = self._get_text_content(part)
        else:
            content_type = message.get_content_type()
            if content_type == "text/plain":
                body_plain = self._get_text_content(message)
            elif content_type == "text/html":
                body_html = self._get_text_content(message)

        body_plain = self._clean_body(body_plain)

        return body_plain, body_html, attachments

    def _get_text_content(self, part: Message) -> str:
        """Extract text content from a message part."""
        try:
            payload = part.get_payload(decode=True)
            if payload is None:
                return ""
            if isinstance(payload, bytes):
                charset = part.get_content_charset()
                return self._decode_bytes(payload, charset)
            return str(payload)
        except Exception:
            return ""

    def _extract_attachment(self, part: Message) -> Attachment | None:
        """Extract attachment metadata and content."""
        try:
            filename = part.get_filename()
            if filename:
                filename = self._decode_header(filename)
            else:
                filename = "unnamed_attachment"

            size = None
            size_header = part.get("Content-Length")
            if size_header and size_header.isdigit():
                size = int(size_header)

            return Attachment(
                filename=filename,
                content_type=part.get_content_type(),
                size=size,
            )
        except Exception:
            return None

    def _clean_body(self, body: str) -> str:
        """Clean up body text."""
        body = re.sub(r"\r\n", "\n", body)
        body = re.sub(r"\n{3,}", "\n\n", body)
        return body.strip()

    def _generate_email_id(
        self,
        message_id: str | None,
        from_address: str,
        date: datetime | None,
        subject: str,
    ) -> str:
        """Generate a unique ID for the email."""
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
        """Generate thread ID from references or subject."""
        if references:
            return hashlib.sha256(references[0].encode()).hexdigest()[:16]
        if in_reply_to:
            return hashlib.sha256(in_reply_to.encode()).hexdigest()[:16]

        normalized_subject = re.sub(r"^(re:|fwd?:)\s*", "", subject.lower(), flags=re.IGNORECASE)
        if normalized_subject:
            return hashlib.sha256(normalized_subject.encode()).hexdigest()[:16]

        return None

"""Utilities for cleaning and chunking email text for embeddings."""

from __future__ import annotations

import re
_REPLY_SEPARATORS = [
    re.compile(r"^On .{0,200} wrote:\s*$", re.IGNORECASE),
    re.compile(r"^-----Original Message-----$", re.IGNORECASE),
    re.compile(r"^----- Forwarded message -----$", re.IGNORECASE),
    re.compile(r"^Begin forwarded message:\s*$", re.IGNORECASE),
    re.compile(r"^From:\s+.+", re.IGNORECASE),
]

_SIGNATURE_SEPARATORS = [
    re.compile(r"^--\s*$"),
    re.compile(r"^__+$"),
]

_FOOTER_KEYWORDS = [
    "unsubscribe",
    "manage preferences",
    "privacy policy",
    "confidential",
    "privileged",
    "intended recipient",
    "this message may contain",
    "do not distribute",
    "do not share",
]


def clean_body_for_embedding(body: str) -> str:
    """Clean email body for embedding.

    Removes reply chains, signatures, and common footer/disclaimer blocks.
    """
    if not body:
        return ""

    text = body.replace("\r\n", "\n").replace("\r", "\n")
    text = _strip_reply_chain(text)
    text = _strip_signature(text)
    text = _strip_footer_blocks(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def chunk_text(text: str, max_chars: int = 1200, overlap: int = 200) -> list[str]:
    """Chunk text into overlapping windows by approximate character length."""
    cleaned = text.strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]

    words = cleaned.split()
    chunks: list[str] = []
    start = 0
    while start < len(words):
        current_len = 0
        end = start
        while end < len(words):
            word_len = len(words[end]) + (1 if current_len > 0 else 0)
            if current_len + word_len > max_chars:
                break
            current_len += word_len
            end += 1

        if end == start:
            end = min(start + 1, len(words))

        chunk = " ".join(words[start:end]).strip()
        if chunk:
            chunks.append(chunk)

        if end >= len(words):
            break

        if overlap > 0:
            overlap_chars = 0
            back = end
            while back > start and overlap_chars < overlap:
                back -= 1
                overlap_chars += len(words[back]) + 1
            start = back if back > start else end
        else:
            start = end

    return chunks


def _strip_reply_chain(text: str) -> str:
    lines = text.split("\n")
    cut_idx = None

    for idx, line in enumerate(lines):
        stripped = line.strip()
        for pattern in _REPLY_SEPARATORS:
            if pattern.match(stripped):
                cut_idx = idx
                break
        if cut_idx is not None:
            break

    if cut_idx is not None:
        lines = lines[:cut_idx]

    while lines and lines[-1].lstrip().startswith(">"):
        lines.pop()

    return "\n".join(lines).rstrip()


def _strip_signature(text: str) -> str:
    lines = text.split("\n")
    for idx, line in enumerate(lines):
        if any(pattern.match(line.strip()) for pattern in _SIGNATURE_SEPARATORS):
            return "\n".join(lines[:idx]).rstrip()
        if line.strip().lower().startswith("sent from my"):
            return "\n".join(lines[:idx]).rstrip()
    return text


def _strip_footer_blocks(text: str) -> str:
    lines = text.split("\n")
    if not lines:
        return text

    lower_lines = [line.lower() for line in lines]
    last_third_start = max(int(len(lines) * 0.66), 0)

    for idx in range(len(lines) - 1, last_third_start - 1, -1):
        line = lower_lines[idx]
        if any(keyword in line for keyword in _FOOTER_KEYWORDS):
            return "\n".join(lines[:idx]).rstrip()

    return text

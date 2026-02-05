"""Input parsing and ingestion pipeline."""

from .email_parser import EmailParser, ParsedEmail
from .json_email_parser import JsonEmailParser
from .jsonl_reader import JsonlReader
from .mbox_reader import MboxReader
from .pipeline import IngestPipeline
from .validation import JsonEmailValidator, ValidationIssue

__all__ = [
    "MboxReader",
    "JsonlReader",
    "EmailParser",
    "JsonEmailParser",
    "ParsedEmail",
    "IngestPipeline",
    "JsonEmailValidator",
    "ValidationIssue",
]

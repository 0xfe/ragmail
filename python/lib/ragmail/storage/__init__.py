"""LanceDB storage layer."""

from .database import Database
from .repository import EmailRepository
from .schema import create_email_schema, create_email_schema_flat

__all__ = [
    "Database",
    "EmailRepository",
    "create_email_schema",
    "create_email_schema_flat",
]

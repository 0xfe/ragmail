"""LanceDB schema definitions with configurable dimensions."""

from datetime import datetime
from typing import Type

from lancedb.pydantic import LanceModel, Vector


def create_email_schema(dimension: int = 384) -> Type[LanceModel]:
    """Create email schema with configurable vector dimension.

    Args:
        dimension: Vector dimension for the configured embedding model

    Returns:
        LanceModel class configured with the specified dimension
    """

    class EmailRecord(LanceModel):
        """Email record schema for LanceDB storage."""

        email_id: str
        message_id: str | None = None
        subject: str
        from_address: str
        from_name: str
        to_addresses: list[str]
        cc_addresses: list[str]
        date: datetime | None = None
        body_plain: str
        has_attachment: bool = False
        labels: list[str] = []
        in_reply_to: str | None = None
        thread_id: str | None = None
        mbox_file: str | None = None
        mbox_offset: int | None = None
        mbox_length: int | None = None

        subject_vector: Vector(dimension)  # type: ignore[valid-type]
        body_vector: Vector(dimension)  # type: ignore[valid-type]

    return EmailRecord


def create_email_schema_flat(dimension: int = 384) -> Type[LanceModel]:
    """Create flattened email schema with configurable vector dimension.

    Args:
        dimension: Vector dimension for the configured embedding model

    Returns:
        LanceModel class configured with the specified dimension
    """

    class EmailRecordFlat(LanceModel):
        """Flattened email record for simpler queries."""

        email_id: str
        message_id: str | None = None
        subject: str
        from_address: str
        from_name: str
        to_addresses_str: str
        cc_addresses_str: str
        date: datetime | None = None
        body_plain: str
        has_attachment: bool = False
        attachment_names: list[str] = []
        attachment_types: list[str] = []
        labels_str: str
        in_reply_to: str | None = None
        thread_id: str | None = None
        year: int | None = None
        month: int | None = None
        mbox_file: str | None = None
        mbox_offset: int | None = None
        mbox_length: int | None = None

        subject_vector: Vector(dimension)  # type: ignore[valid-type]

    return EmailRecordFlat


def create_email_chunk_schema(dimension: int = 384) -> Type[LanceModel]:
    """Create schema for body chunk embeddings."""

    class EmailChunkRecord(LanceModel):
        """Chunk-level email record for body embeddings."""

        chunk_id: str
        email_id: str
        message_id: str | None = None
        thread_id: str | None = None
        chunk_index: int
        chunk_text: str
        subject: str
        from_address: str
        from_name: str
        to_addresses_str: str
        cc_addresses_str: str
        date: datetime | None = None
        has_attachment: bool = False
        attachment_names: list[str] = []
        attachment_types: list[str] = []
        labels_str: str
        in_reply_to: str | None = None
        year: int | None = None
        month: int | None = None
        mbox_file: str | None = None
        mbox_offset: int | None = None
        mbox_length: int | None = None

        body_vector: Vector(dimension)  # type: ignore[valid-type]

    return EmailChunkRecord

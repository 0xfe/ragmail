"""Pytest fixtures for email-search tests."""

from pathlib import Path
import json

import numpy as np
import pytest

from ragmail.ingest import ParsedEmail
from ragmail.ingest.email_parser import Attachment
from ragmail.storage import Database


@pytest.fixture
def sample_jsonl_path(tmp_path: Path) -> Path:
    """Create a sample JSONL file for testing."""
    records = [
        {
            "headers": {
                "from": {"name": "John Doe", "email": "john@example.com"},
                "to": [{"name": "", "email": "jane@example.com"}],
                "subject": "Meeting Tomorrow",
                "date": "2022-01-01T10:00:00-05:00",
                "message_id": "msg001@example.com",
            },
            "tags": [],
            "content": [
                {
                    "type": "text",
                    "body": "Hi Jane,\n\nLet's meet tomorrow at 2pm to discuss the project.\n\nBest,\nJohn",
                }
            ],
        },
        {
            "headers": {
                "from": {"name": "Jane Smith", "email": "jane@example.com"},
                "to": [{"name": "", "email": "john@example.com"}],
                "subject": "Re: Meeting Tomorrow",
                "date": "2022-01-02T09:00:00-05:00",
                "message_id": "msg002@example.com",
                "in_reply_to": "msg001@example.com",
                "references": ["msg001@example.com"],
            },
            "tags": [],
            "content": [
                {
                    "type": "text",
                    "body": "Hi John,\n\nSounds good! I'll prepare the quarterly report.\n\nJane",
                }
            ],
        },
        {
            "headers": {
                "from": {"name": "The Boss", "email": "boss@company.com"},
                "to": [{"name": "", "email": "team@company.com"}],
                "subject": "Q4 Results",
                "date": "2022-01-03T14:00:00-05:00",
                "message_id": "msg003@example.com",
            },
            "tags": ["Important", "Work"],
            "content": [
                {
                    "type": "text",
                    "body": "Team,\n\nGreat job on Q4! Revenue exceeded expectations.\n\nRegards,\nManagement",
                }
            ],
        },
    ]
    jsonl_file = tmp_path / "test.clean.jsonl"
    with jsonl_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return jsonl_file


@pytest.fixture
def temp_db(tmp_path: Path) -> Database:
    """Create a temporary LanceDB database."""
    db_path = tmp_path / "test.lancedb"
    return Database(db_path)


@pytest.fixture
def sample_emails() -> list[ParsedEmail]:
    """Create sample parsed emails for testing."""
    from datetime import datetime

    return [
        ParsedEmail(
            email_id="email001",
            message_id="<msg001@example.com>",
            subject="Meeting Tomorrow",
            from_address="john@example.com",
            from_name="John Doe",
            to_addresses=["jane@example.com"],
            cc_addresses=[],
            date=datetime(2022, 1, 1, 10, 0, 0),
            body_plain="Let's meet tomorrow at 2pm to discuss the project.",
            body_html="",
            has_attachment=True,
            attachments=[
                Attachment(
                    filename="agenda.pdf",
                    content_type="application/pdf",
                    size=12345,
                )
            ],
            labels=[],
        ),
        ParsedEmail(
            email_id="email002",
            message_id="<msg002@example.com>",
            subject="Re: Meeting Tomorrow",
            from_address="jane@example.com",
            from_name="Jane Smith",
            to_addresses=["john@example.com"],
            cc_addresses=[],
            date=datetime(2022, 1, 2, 9, 0, 0),
            body_plain="Sounds good! I'll prepare the quarterly report.",
            body_html="",
            has_attachment=False,
            labels=[],
            in_reply_to="<msg001@example.com>",
        ),
        ParsedEmail(
            email_id="email003",
            message_id="<msg003@example.com>",
            subject="Q4 Results",
            from_address="boss@company.com",
            from_name="The Boss",
            to_addresses=["team@company.com"],
            cc_addresses=[],
            date=datetime(2022, 1, 3, 14, 0, 0),
            body_plain="Team, Great job on Q4! Revenue exceeded expectations.",
            body_html="",
            has_attachment=False,
            labels=["Important", "Work"],
        ),
    ]


@pytest.fixture
def mock_embedding_provider():
    """Create a mock embedding provider for testing."""

    class MockEmbeddingProvider:
        dimension = 384
        model_name = "mock-model"

        def encode(self, texts, batch_size=32, show_progress=False):
            return np.random.randn(len(texts), 384).astype(np.float32)

        def encode_query(self, query):
            return np.random.randn(384).astype(np.float32)

    return MockEmbeddingProvider()

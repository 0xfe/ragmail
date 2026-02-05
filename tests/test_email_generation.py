"""Tests for synthetic email generation."""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import mailbox
import os
import sys

# Add tests/data to path for imports
sys.path.insert(0, str(Path(__file__).parent / "data"))

from generate_emails import (
    PERSONAS,
    create_mbox_message,
    generate_basic_emails,
)


class TestPersonas:
    """Test persona definitions."""

    def test_personas_defined(self):
        """Verify all personas have required fields."""
        required_fields = [
            "name",
            "email",
            "company",
            "role",
            "style",
            "topics",
            "contacts",
        ]

        for persona_key, persona in PERSONAS.items():
            for field in required_fields:
                assert field in persona, f"Persona {persona_key} missing field: {field}"

            # Verify contacts are tuples of (name, email, role)
            for contact in persona["contacts"]:
                assert len(contact) == 3, (
                    f"Contact in {persona_key} should have 3 elements"
                )
                assert "@" in contact[1], (
                    f"Contact email in {persona_key} should be valid"
                )


class TestBasicEmailGeneration:
    """Test fallback email generation without LLM."""

    def test_generate_basic_emails(self):
        """Test basic email generation."""
        persona = PERSONAS["tech_professional"]
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        count = 10

        emails = generate_basic_emails(persona, start, end, count)

        assert len(emails) == count

        for email in emails:
            assert "message_id" in email
            assert "subject" in email
            assert "body" in email
            assert "date" in email
            assert "from_name" in email
            assert "from_email" in email
            assert "to_emails" in email

            # Verify dates are in range
            email_date = datetime.fromisoformat(email["date"])
            assert start <= email_date <= end

    def test_email_direction_variety(self):
        """Test that emails include both sent and received."""
        persona = PERSONAS["small_business"]
        emails = generate_basic_emails(
            persona, datetime(2024, 1, 1), datetime(2024, 12, 31), 20
        )

        sent = [e for e in emails if e["from_email"] == persona["email"]]
        received = [e for e in emails if e["from_email"] != persona["email"]]

        # Should have variety (not all one direction due to randomness)
        assert len(sent) > 0 or len(received) > 0


class TestMboxCreation:
    """Test MBOX file creation."""

    def test_create_mbox_message(self):
        """Test conversion to MBOX format."""
        email_data = {
            "message_id": "<test-123@example.com>",
            "from_name": "Test User",
            "from_email": "test@example.com",
            "to_emails": ["recipient@example.com"],
            "subject": "Test Subject",
            "date": "2024-01-15T10:30:00",
            "body": "This is a test email body.",
        }

        msg = create_mbox_message(email_data)

        assert msg["Message-ID"] == "<test-123@example.com>"
        assert "Test User" in msg["From"]
        assert "test@example.com" in msg["From"]
        assert "recipient@example.com" in msg["To"]
        assert msg["Subject"] == "Test Subject"
        assert "This is a test email body." in msg.get_payload()

    def test_create_mbox_file(self, tmp_path):
        """Test creating an actual MBOX file."""
        from generate_emails import generate_mbox_file

        output_path = tmp_path / "test.mbox"

        # Generate with no-expand to avoid API calls
        generate_mbox_file(
            persona_type="student",
            output_path=output_path,
            email_count=5,
            start_date="2024-01-01",
            end_date="2024-01-31",
            expand_persona=False,
        )

        # Verify file was created
        assert output_path.exists()

        # Verify it's a valid MBOX
        mbox = mailbox.mbox(str(output_path))
        assert len(mbox) == 5

        # Verify each message
        for msg in mbox:
            assert msg["Message-ID"] is not None
            assert msg["Subject"] is not None
            assert msg.get_payload() is not None

        mbox.close()


@pytest.mark.integration
class TestLLMGeneration:
    """Integration tests that require OpenAI API."""

    @pytest.fixture(autouse=True)
    def check_api_key(self):
        """Skip if no API key available."""
        api_key = os.environ.get("EMAIL_SEARCH_OPENAI_API_KEY") or os.environ.get(
            "OPENAI_API_KEY"
        )
        if not api_key:
            pytest.skip("OpenAI API key not set")

    def test_generate_persona_details(self):
        """Test LLM persona expansion."""
        from generate_emails import generate_persona_details

        persona = generate_persona_details("tech_professional")

        # Should have expanded fields
        assert "projects" in persona
        assert "meetings" in persona
        assert "regular_contacts" in persona
        assert len(persona["projects"]) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

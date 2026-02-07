"""Tests for LLM prompts."""

import pytest
from datetime import datetime

from ragmail.prompts import RAGPrompt, QueryExpansionPrompt, SummarizationPrompt


class TestRAGPrompt:
    """Test RAG prompt generation."""

    def test_system_message(self):
        """Test that system message is present."""
        prompt = RAGPrompt()
        system_msg = prompt.system_message

        assert "email" in system_msg.lower()
        assert "assistant" in system_msg.lower()
        assert len(system_msg) > 0

    def test_format_method(self):
        """Test format method returns messages."""
        prompt = RAGPrompt()

        emails = [
            {
                "subject": "Project Update",
                "date": "2024-01-15",
                "from_name": "John",
                "from_address": "john@example.com",
                "body_plain": "The project is on track for completion.",
            },
            {
                "subject": "Re: Project Update",
                "date": "2024-01-16",
                "from_name": "Jane",
                "from_address": "jane@example.com",
                "body_plain": "Great news! Let me know if you need anything.",
            },
        ]

        question = "What did John say about the project?"
        messages = prompt.format(question, emails)

        # Should return system + user messages
        assert len(messages) == 2
        assert messages[0].role == "system"
        assert messages[1].role == "user"

        # User message should contain context and question
        user_content = messages[1].content
        assert "Project Update" in user_content
        assert "John" in user_content  # Uses from_name, not from_address
        assert question in user_content

    def test_empty_context(self):
        """Test handling of empty context."""
        prompt = RAGPrompt()

        messages = prompt.format("What about X?", [])

        # Should still work with empty context
        assert len(messages) == 2
        assert (
            "No relevant emails" in messages[1].content
            or "context" in messages[1].content.lower()
        )

    def test_long_context_truncation(self):
        """Test that very long context is handled."""
        prompt = RAGPrompt()

        # Create very long context
        long_content = "Word " * 5000  # Very long email
        emails = [
            {
                "subject": "Long Email",
                "date": "2024-01-01",
                "from_name": "Test",
                "from_address": "test@test.com",
                "body_plain": long_content,
            }
        ]

        messages = prompt.format("Question?", emails)
        user_content = messages[1].content

        # Should be truncated or handled
        assert len(user_content) < len(long_content) + 1000  # Some reasonable limit

    def test_context_formatting(self):
        """Test context formatting in user message."""
        prompt = RAGPrompt()

        emails = [
            {
                "subject": "Meeting Notes",
                "date": datetime(2024, 3, 15),
                "from_name": "Alice",
                "from_address": "alice@example.com",
                "body_plain": "Discussed Q1 goals and budget.",
            }
        ]

        messages = prompt.format("What did we discuss?", emails)
        user_content = messages[1].content

        # Should contain formatted email info
        assert "Alice" in user_content
        assert "Meeting Notes" in user_content
        assert "Discussed Q1 goals" in user_content


class TestQueryExpansionPrompt:
    """Test query expansion prompt."""

    def test_system_message(self):
        """Test system message content."""
        prompt = QueryExpansionPrompt()
        system_msg = prompt.system_message

        assert "search" in system_msg.lower()
        assert "query" in system_msg.lower()
        assert len(system_msg) > 0

    def test_format_method(self):
        """Test format method returns messages."""
        prompt = QueryExpansionPrompt()

        query = "emails from John in 2024 about the budget"
        messages = prompt.format(query)

        assert len(messages) == 2
        assert messages[0].role == "system"
        assert messages[1].role == "user"

        # User message should contain the query
        assert query in messages[1].content
        assert "JSON" in messages[1].content

    def test_format_output_structure(self):
        """Test that format output asks for JSON structure."""
        prompt = QueryExpansionPrompt()

        messages = prompt.format("test query")
        user_content = messages[1].content

        # Should request JSON format with expected fields
        assert "intent" in user_content.lower()
        assert "entities" in user_content.lower()
        assert "search_terms" in user_content.lower()


class TestSummarizationPrompt:
    """Test summarization prompt."""

    def test_system_message(self):
        """Test system message for summarization."""
        prompt = SummarizationPrompt()
        system_msg = prompt.system_message

        assert "summar" in system_msg.lower()
        assert "email" in system_msg.lower()
        assert len(system_msg) > 0

    def test_format_method(self):
        """Test format method returns messages."""
        prompt = SummarizationPrompt()

        emails = [
            {
                "subject": "Project Kickoff",
                "date": "2024-01-01",
                "from_name": "Manager",
                "from_address": "manager@example.com",
                "body_plain": "Let's start the new project next week.",
            },
            {
                "subject": "Re: Project Kickoff",
                "date": "2024-01-02",
                "from_name": "Developer",
                "from_address": "dev@example.com",
                "body_plain": "Sounds good, I'll prepare the specs.",
            },
        ]

        messages = prompt.format(emails)

        assert len(messages) == 2
        assert messages[0].role == "system"
        assert messages[1].role == "user"

        # User message should contain emails
        user_content = messages[1].content
        assert "Project Kickoff" in user_content
        assert "Manager" in user_content  # Uses from_name, not from_address
        assert "Developer" in user_content

    def test_single_email_summary(self):
        """Test summarization of single email."""
        prompt = SummarizationPrompt()

        emails = [
            {
                "subject": "Meeting Tomorrow",
                "date": datetime(2024, 1, 15),
                "from_name": "Boss",
                "from_address": "boss@example.com",
                "body_plain": "Please come prepared with your quarterly report.",
            }
        ]

        messages = prompt.format(emails)
        user_content = messages[1].content

        assert "Meeting Tomorrow" in user_content
        assert "Boss" in user_content
        assert "quarterly report" in user_content


class TestPromptConsistency:
    """Test consistency across all prompts."""

    @pytest.mark.parametrize(
        "prompt_class",
        [
            RAGPrompt,
            QueryExpansionPrompt,
            SummarizationPrompt,
        ],
    )
    def test_prompt_has_system_message(self, prompt_class):
        """All prompts should have a system message."""
        prompt = prompt_class()
        system_msg = prompt.system_message

        assert system_msg is not None
        assert len(system_msg) > 0
        assert isinstance(system_msg, str)

    @pytest.mark.parametrize(
        "prompt_class,expected_methods",
        [
            (RAGPrompt, ["format", "_format_context"]),
            (QueryExpansionPrompt, ["format"]),
            (SummarizationPrompt, ["format", "_format_emails"]),
        ],
    )
    def test_prompt_has_required_methods(self, prompt_class, expected_methods):
        """All prompts should have required methods."""
        prompt = prompt_class()

        for method in expected_methods:
            assert hasattr(prompt, method)
            assert callable(getattr(prompt, method))


class TestPromptOutputFormats:
    """Test that prompts produce expected output formats."""

    def test_rag_prompt_format(self):
        """Test RAG prompt output format instructions."""
        prompt = RAGPrompt()
        system_msg = prompt.system_message

        # Should mention citations or references
        assert any(
            word in system_msg.lower() for word in ["citation", "reference", "cite"]
        )

    def test_query_expansion_format(self):
        """Test query expansion output format."""
        prompt = QueryExpansionPrompt()
        user_template = prompt.user_template

        # Should mention JSON or structured output
        assert "json" in user_template.lower()
        assert "intent" in user_template.lower()

    def test_summarization_format(self):
        """Test summarization output format."""
        prompt = SummarizationPrompt()
        system_msg = prompt.system_message

        # Should mention length or conciseness
        assert any(
            word in system_msg.lower() for word in ["concise", "bullet", "summary"]
        )


class TestPromptInstances:
    """Test singleton instances."""

    def test_rag_prompt_instance(self):
        """Test RAG_PROMPT singleton."""
        from ragmail.prompts import RAG_PROMPT

        assert isinstance(RAG_PROMPT, RAGPrompt)
        assert RAG_PROMPT.system_message is not None

    def test_query_expansion_instance(self):
        """Test QUERY_EXPANSION_PROMPT singleton."""
        from ragmail.prompts import QUERY_EXPANSION_PROMPT

        assert isinstance(QUERY_EXPANSION_PROMPT, QueryExpansionPrompt)

    def test_summarization_instance(self):
        """Test SUMMARIZATION_PROMPT singleton."""
        from ragmail.prompts import SUMMARIZATION_PROMPT

        assert isinstance(SUMMARIZATION_PROMPT, SummarizationPrompt)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

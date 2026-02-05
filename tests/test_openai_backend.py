"""Tests for OpenAI LLM backend."""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock

from ragmail.llm.base import Message
from ragmail.llm.openai import OpenAIBackend


class TestOpenAIBackend:
    """Test OpenAI backend with mocked API."""

    @pytest.fixture
    def backend(self):
        """Create OpenAI backend with test API key."""
        return OpenAIBackend(model="gpt-5.2", api_key="test-key")

    @pytest.fixture
    def mock_response(self):
        """Create a mock OpenAI response."""
        mock = Mock()
        mock.choices = [Mock()]
        mock.choices[0].message = Mock()
        mock.choices[0].message.content = "This is a test response."
        mock.choices[0].finish_reason = "stop"
        mock.usage = Mock()
        mock.usage.prompt_tokens = 50
        mock.usage.completion_tokens = 10
        mock.usage.total_tokens = 60
        return mock

    def test_initialization(self, backend):
        """Test backend initialization."""
        assert backend.model_name == "gpt-5.2"
        # API key is stored in the client
        assert backend._client.api_key == "test-key"

    @patch("openai.OpenAI")
    def test_complete_success(self, mock_openai_class, backend, mock_response):
        """Test successful completion."""
        # Setup mock
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client

        # Create fresh backend with mocked client
        test_backend = OpenAIBackend(model="gpt-5.2", api_key="test-key")
        test_backend._client = mock_client

        messages = [
            Message(role="system", content="You are a helpful assistant."),
            Message(role="user", content="Hello!"),
        ]

        response = test_backend.complete(messages)

        assert response.content == "This is a test response."
        assert response.model == "gpt-5.2"
        assert response.usage["prompt_tokens"] == 50
        assert response.usage["completion_tokens"] == 10

    @patch("openai.OpenAI")
    def test_complete_api_error(self, mock_openai_class, backend):
        """Test handling of API errors."""
        # Setup mock to raise error
        mock_client = Mock()
        from openai import APIError

        mock_client.chat.completions.create.side_effect = APIError(
            message="Test error", request=Mock(), body=None
        )
        mock_openai_class.return_value = mock_client

        # Create fresh backend with mocked client
        test_backend = OpenAIBackend(model="gpt-5.2", api_key="test-key")
        test_backend._client = mock_client

        messages = [Message(role="user", content="Hello!")]

        with pytest.raises(Exception) as exc_info:
            test_backend.complete(messages)

        assert "API" in str(exc_info.value) or "error" in str(exc_info.value).lower()

    @patch("openai.OpenAI")
    def test_complete_with_parameters(self, mock_openai_class, backend, mock_response):
        """Test completion with custom parameters."""
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client

        # Create fresh backend with mocked client
        test_backend = OpenAIBackend(model="gpt-5.2", api_key="test-key")
        test_backend._client = mock_client

        messages = [Message(role="user", content="Test")]

        test_backend.complete(
            messages,
            max_tokens=100,
            temperature=0.5,
        )

        # Verify the call was made with correct parameters
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["max_completion_tokens"] == 100
        assert call_args.kwargs["temperature"] == 0.5
        assert call_args.kwargs["model"] == "gpt-5.2"

    @patch("openai.OpenAI")
    def test_complete_with_parameters_non_gpt5(self, mock_openai_class, mock_response):
        """Test completion parameters for non-gpt-5 models."""
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client

        test_backend = OpenAIBackend(model="gpt-4o-mini", api_key="test-key")
        test_backend._client = mock_client

        messages = [Message(role="user", content="Test")]

        test_backend.complete(
            messages,
            max_tokens=120,
            temperature=0.4,
        )

        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["max_tokens"] == 120
        assert call_args.kwargs["temperature"] == 0.4
        assert call_args.kwargs["model"] == "gpt-4o-mini"

    @pytest.mark.asyncio
    @patch("openai.AsyncOpenAI")
    async def test_stream(self, mock_async_openai_class, backend):
        """Test streaming responses."""
        # Setup mock streaming response as async generator
        mock_chunk1 = Mock()
        mock_chunk1.choices = [Mock()]
        mock_chunk1.choices[0].delta = Mock()
        mock_chunk1.choices[0].delta.content = "Hello"

        mock_chunk2 = Mock()
        mock_chunk2.choices = [Mock()]
        mock_chunk2.choices[0].delta = Mock()
        mock_chunk2.choices[0].delta.content = " world"

        mock_chunk3 = Mock()
        mock_chunk3.choices = [Mock()]
        mock_chunk3.choices[0].delta = Mock()
        mock_chunk3.choices[0].delta.content = "!"

        async def mock_stream(*args, **kwargs):
            for chunk in [mock_chunk1, mock_chunk2, mock_chunk3]:
                yield chunk

        mock_async_client = Mock()
        mock_async_client.chat.completions.create = mock_stream
        mock_async_openai_class.return_value = mock_async_client

        # Create fresh backend with mocked client
        test_backend = OpenAIBackend(model="gpt-5.2", api_key="test-key")
        test_backend._async_client = mock_async_client

        messages = [Message(role="user", content="Say hello")]

        chunks = []
        async for chunk in test_backend.stream(messages):
            chunks.append(chunk)

        assert "".join(chunks) == "Hello world!"


class TestOpenAIBackendModelConfiguration:
    """Test model configuration options."""

    def test_default_model_is_gpt_5_2(self):
        """Test that default model is gpt-5.2."""
        backend = OpenAIBackend(api_key="test-key")
        assert backend.model_name == "gpt-5.2"

    def test_custom_model(self):
        """Test using custom model."""
        backend = OpenAIBackend(model="gpt-4o-mini", api_key="test-key")
        assert backend.model_name == "gpt-4o-mini"


@pytest.mark.integration
class TestOpenAIIntegration:
    """Integration tests requiring real OpenAI API."""

    @pytest.fixture(autouse=True)
    def check_api_key(self):
        """Skip if no API key available."""
        api_key = os.environ.get("EMAIL_SEARCH_OPENAI_API_KEY") or os.environ.get(
            "OPENAI_API_KEY"
        )
        if not api_key:
            pytest.skip("OpenAI API key not set")

    @pytest.fixture
    def real_backend(self):
        """Create real OpenAI backend."""
        return OpenAIBackend(model="gpt-5.2")

    def test_real_completion(self, real_backend):
        """Test with real API call."""
        messages = [
            Message(role="system", content="You are a helpful assistant. Be concise."),
            Message(role="user", content="Say 'test passed' and nothing else."),
        ]

        response = real_backend.complete(messages, max_tokens=10)

        assert "test passed" in response.content.lower()
        assert response.usage["prompt_tokens"] > 0
        assert response.model == "gpt-5.2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

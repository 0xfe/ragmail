"""OpenAI LLM backend for GPT-4o inference."""

from collections.abc import AsyncIterator

import openai

from .base import LLMResponse, Message


class OpenAIBackend:
    """LLM backend using OpenAI API."""

    def __init__(
        self,
        model: str = "gpt-5.2",
        api_key: str | None = None,
        base_url: str | None = None,
    ):
        """Initialize OpenAI backend.

        Args:
            model: Model name to use (gpt-4o, gpt-4o-mini, etc.)
            api_key: API key (or use OPENAI_API_KEY env var)
            base_url: Optional custom base URL for API
        """
        self._model_name = model
        self._client = openai.OpenAI(api_key=api_key, base_url=base_url)
        self._async_client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)

    @property
    def model_name(self) -> str:
        """Return model name."""
        return self._model_name

    def _completion_kwargs(self, max_tokens: int, temperature: float) -> dict[str, float | int]:
        if self._model_name.startswith("gpt-5"):
            return {"max_completion_tokens": max_tokens, "temperature": temperature}
        return {"max_tokens": max_tokens, "temperature": temperature}

    def complete(
        self,
        messages: list[Message],
        max_tokens: int = 1024,
        temperature: float = 0.7,
    ) -> LLMResponse:
        """Generate a completion.

        Args:
            messages: List of chat messages
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature

        Returns:
            LLM response
        """
        formatted_messages = [
            {"role": msg.role, "content": msg.content} for msg in messages
        ]

        response = self._client.chat.completions.create(
            model=self._model_name,
            messages=formatted_messages,
            **self._completion_kwargs(max_tokens, temperature),
        )

        return LLMResponse(
            content=response.choices[0].message.content or "",
            model=self._model_name,
            usage={
                "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
                "completion_tokens": response.usage.completion_tokens
                if response.usage
                else 0,
            },
        )

    async def stream(
        self,
        messages: list[Message],
        max_tokens: int = 1024,
        temperature: float = 0.7,
    ) -> AsyncIterator[str]:
        """Stream a completion.

        Args:
            messages: List of chat messages
            max_tokens: Maximum tokens
            temperature: Sampling temperature

        Yields:
            Response chunks
        """
        formatted_messages = [
            {"role": msg.role, "content": msg.content} for msg in messages
        ]

        stream = self._async_client.chat.completions.create(
            model=self._model_name,
            messages=formatted_messages,
            **self._completion_kwargs(max_tokens, temperature),
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

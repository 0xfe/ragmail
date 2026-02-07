"""Base protocol for LLM backends."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable


@dataclass
class Message:
    """A chat message."""

    role: Literal["user", "assistant", "system"]
    content: str


@dataclass
class LLMResponse:
    """Response from an LLM."""

    content: str
    model: str
    usage: dict[str, int] | None = None


@runtime_checkable
class LLMBackend(Protocol):
    """Protocol for LLM backends."""

    @property
    def model_name(self) -> str:
        """Return model name."""
        ...

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
        ...

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
        ...

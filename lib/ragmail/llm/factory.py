"""Factory for creating LLM backends."""

from .base import LLMBackend
from .openai import OpenAIBackend


def create_llm_backend(
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> LLMBackend:
    """Create an OpenAI-compatible LLM backend.

    Args:
        model: Optional model name override
        api_key: Optional API key
        base_url: Optional custom base URL

    Returns:
        Configured LLM backend
    """
    return OpenAIBackend(
        model=model or "gpt-5.2",
        api_key=api_key,
        base_url=base_url or "https://api.openai.com/v1",
    )

"""Factory for creating embedding providers."""

from .base import EmbeddingProvider
from .sentence_transformer import SentenceTransformerProvider


def create_embedding_provider(
    provider: str = "sentence_transformer",
    model_name: str | None = None,
    model_revision: str | None = None,
) -> EmbeddingProvider:
    """Create an embedding provider based on configuration.

    Args:
        provider: Provider type ('sentence_transformer')
        model_name: Optional model name override

    Returns:
        Configured embedding provider

    Raises:
        ValueError: If provider type is unknown
    """
    if provider == "sentence_transformer":
        return SentenceTransformerProvider(
            model_name=model_name,
            revision=model_revision,
        )
    raise ValueError(f"Unknown embedding provider: {provider}")

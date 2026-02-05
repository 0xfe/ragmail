"""Embedding providers for semantic search."""

from .base import EmbeddingProvider
from .factory import create_embedding_provider
from .sentence_transformer import SentenceTransformerProvider

__all__ = [
    "EmbeddingProvider",
    "SentenceTransformerProvider",
    "create_embedding_provider",
]

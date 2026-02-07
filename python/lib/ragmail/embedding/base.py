"""Base protocol for embedding providers."""

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

import numpy as np
from numpy.typing import NDArray


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Protocol for embedding providers."""

    @property
    def dimension(self) -> int:
        """Return embedding dimension."""
        ...

    @property
    def model_name(self) -> str:
        """Return model name."""
        ...

    def encode(
        self,
        texts: Sequence[str],
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> NDArray[np.float32]:
        """Encode texts into embeddings.

        Args:
            texts: Sequence of texts to encode
            batch_size: Batch size for encoding
            show_progress: Whether to show progress bar

        Returns:
            Array of embeddings with shape (len(texts), dimension)
        """
        ...

    def encode_query(self, query: str) -> NDArray[np.float32]:
        """Encode a single query.

        Args:
            query: Query text

        Returns:
            Query embedding with shape (dimension,)
        """
        ...

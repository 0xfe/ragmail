"""Sentence Transformer embedding provider."""

from collections.abc import Sequence
import warnings

import numpy as np
from numpy.typing import NDArray
from sentence_transformers import SentenceTransformer

DEFAULT_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
DEFAULT_MODEL_REVISION = "e5cf08aadaa33385f5990def41f7a23405aec398"
LEGACY_MODEL_ALIASES = {
    "nomic-ai/nomic-embed-text-v1",
    "nomic-ai/nomic-embed-text-v1-unsupervised",
}


class SentenceTransformerProvider:
    """Embedding provider using Sentence Transformers."""

    def __init__(
        self,
        model_name: str | None = DEFAULT_MODEL_NAME,
        revision: str | None = None,
        trust_remote_code: bool | None = None,
    ):
        """Initialize the provider.

        Args:
            model_name: Name of the sentence-transformers model
            revision: Optional model revision (commit hash or tag)
            trust_remote_code: Whether to trust remote code for custom models
        """
        resolved_name = (model_name or DEFAULT_MODEL_NAME).strip()
        if resolved_name in LEGACY_MODEL_ALIASES:
            warnings.warn(
                f"{resolved_name} is deprecated; using {DEFAULT_MODEL_NAME} instead.",
                UserWarning,
                stacklevel=2,
            )
            resolved_name = DEFAULT_MODEL_NAME
        if revision is None and resolved_name == DEFAULT_MODEL_NAME:
            revision = DEFAULT_MODEL_REVISION
        elif revision == DEFAULT_MODEL_REVISION and resolved_name != DEFAULT_MODEL_NAME:
            revision = None
        self._model_name = resolved_name
        if trust_remote_code is None:
            trust_remote_code = "nomic" in self._model_name.lower()
        self._model = SentenceTransformer(
            self._model_name,
            revision=revision,
            trust_remote_code=trust_remote_code,
        )
        self._dimension = self._model.get_sentence_embedding_dimension()

    @property
    def dimension(self) -> int:
        """Return embedding dimension."""
        return self._dimension or 384

    @property
    def model_name(self) -> str:
        """Return model name."""
        return self._model_name

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
        embeddings = self._model.encode(
            list(texts),
            batch_size=batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return embeddings.astype(np.float32)

    def encode_query(self, query: str) -> NDArray[np.float32]:
        """Encode a single query.

        Args:
            query: Query text

        Returns:
            Query embedding with shape (dimension,)
        """
        embedding = self._model.encode(
            query,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return embedding.astype(np.float32)

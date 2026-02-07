"""Tests for SentenceTransformerProvider configuration."""

from unittest.mock import Mock, patch

import pytest

from ragmail.embedding.sentence_transformer import (
    DEFAULT_MODEL_NAME,
    DEFAULT_MODEL_REVISION,
    SentenceTransformerProvider,
)


def test_sentence_transformer_trust_remote_code_nomic():
    """Ensure nomic models enable trust_remote_code by default."""
    with patch("ragmail.embedding.sentence_transformer.SentenceTransformer") as mock_cls:
        mock_model = Mock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        mock_cls.return_value = mock_model

        SentenceTransformerProvider(model_name="nomic-ai/nomic-embed-text-v1.5")

        _, kwargs = mock_cls.call_args
        assert kwargs.get("trust_remote_code") is True


def test_sentence_transformer_trust_remote_code_default_false():
    """Ensure non-nomic models do not enable trust_remote_code by default."""
    with patch("ragmail.embedding.sentence_transformer.SentenceTransformer") as mock_cls:
        mock_model = Mock()
        mock_model.get_sentence_embedding_dimension.return_value = 384
        mock_cls.return_value = mock_model

        SentenceTransformerProvider(model_name="all-MiniLM-L6-v2")

        _, kwargs = mock_cls.call_args
        assert kwargs.get("trust_remote_code") is False


def test_sentence_transformer_revision_passed():
    """Ensure revision is forwarded to SentenceTransformer."""
    with patch("ragmail.embedding.sentence_transformer.SentenceTransformer") as mock_cls:
        mock_model = Mock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        mock_cls.return_value = mock_model

        SentenceTransformerProvider(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            revision="deadbeef",
        )

        _, kwargs = mock_cls.call_args
        assert kwargs.get("revision") == "deadbeef"


def test_sentence_transformer_default_revision_pinned():
    """Ensure the default model gets the pinned revision."""
    with patch("ragmail.embedding.sentence_transformer.SentenceTransformer") as mock_cls:
        mock_model = Mock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        mock_cls.return_value = mock_model

        SentenceTransformerProvider(model_name=DEFAULT_MODEL_NAME, revision=None)

        _, kwargs = mock_cls.call_args
        assert kwargs.get("revision") == DEFAULT_MODEL_REVISION


def test_sentence_transformer_legacy_alias_upgraded():
    """Ensure legacy Nomic model names are upgraded to the default."""
    with patch("ragmail.embedding.sentence_transformer.SentenceTransformer") as mock_cls:
        mock_model = Mock()
        mock_model.get_sentence_embedding_dimension.return_value = 768
        mock_cls.return_value = mock_model

        with pytest.warns(UserWarning):
            SentenceTransformerProvider(model_name="nomic-ai/nomic-embed-text-v1-unsupervised")

        args, _ = mock_cls.call_args
        assert args[0] == DEFAULT_MODEL_NAME

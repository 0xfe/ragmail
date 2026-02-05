"""Configuration settings using Pydantic."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_prefix="EMAIL_SEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # Database
    db_path: Path = Field(default=Path("./email_search.lancedb"))
    attachment_dir: Path = Field(default=Path("./attachments"))

    # Embedding
    embedding_provider: Literal["sentence_transformer"] = Field(default="sentence_transformer")
    embedding_model: str = Field(default="nomic-ai/nomic-embed-text-v1.5")
    embedding_model_revision: str | None = Field(
        default="e5cf08aadaa33385f5990def41f7a23405aec398"
    )
    embedding_dimension: int = Field(default=768)
    embedding_batch_size: int = Field(default=32)

    # LLM (OpenAI-compatible)
    openai_model: str = Field(default="gpt-5.2")
    openai_api_key: str | None = Field(default=None)
    openai_base_url: str = Field(default="https://api.openai.com/v1")

    # Search
    search_top_k: int = Field(default=20)
    search_rrf_k: int = Field(default=60)

    # Ingestion
    ingest_batch_size: int = Field(default=300)
    ingest_checkpoint_interval: int = Field(default=120)
    ingest_compact_every: int = Field(default=20000)
    ingest_chunk_size: int = Field(default=1200)
    ingest_chunk_overlap: int = Field(default=200)


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()

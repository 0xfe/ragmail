"""Workspace management for ragmail."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class Workspace:
    name: str
    root: Path

    @property
    def inputs_dir(self) -> Path:
        return self.root / "inputs"

    @property
    def split_dir(self) -> Path:
        return self.root / "split"

    @property
    def clean_dir(self) -> Path:
        return self.root / "clean"

    @property
    def embeddings_dir(self) -> Path:
        return self.root / "embeddings"

    @property
    def spam_dir(self) -> Path:
        return self.root / "spam"

    @property
    def db_dir(self) -> Path:
        return self.root / "db"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def checkpoints_dir(self) -> Path:
        return self.root / ".checkpoints"

    @property
    def reports_dir(self) -> Path:
        return self.root / "reports"

    @property
    def cache_dir(self) -> Path:
        return self.root / "cache"

    @property
    def config_path(self) -> Path:
        return self.root / "workspace.json"

    @property
    def state_path(self) -> Path:
        return self.root / "state.json"

    def ensure(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        for path in [
            self.inputs_dir,
            self.split_dir,
            self.clean_dir,
            self.embeddings_dir,
            self.spam_dir,
            self.db_dir,
            self.logs_dir,
            self.checkpoints_dir,
            self.reports_dir,
            self.cache_dir,
        ]:
            path.mkdir(parents=True, exist_ok=True)

        if not self.config_path.exists():
            self._write_config()

    def _write_config(self) -> None:
        payload = {
            "name": self.name,
            "root": str(self.root),
            "created_at": datetime.now().isoformat(),
            "paths": {
                "inputs": "inputs",
                "split": "split",
                "clean": "clean",
                "embeddings": "embeddings",
                "spam": "spam",
                "db": "db",
                "logs": "logs",
                "checkpoints": ".checkpoints",
                "reports": "reports",
                "cache": "cache",
            },
        }
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def load_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            with open(self.state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"stages": {}, "updated_at": datetime.now().isoformat()}

    def save_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = datetime.now().isoformat()
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def update_stage(self, stage: str, status: str, details: dict[str, Any] | None = None) -> None:
        state = self.load_state()
        stages = state.setdefault("stages", {})
        entry = stages.get(stage, {})
        entry["status"] = status
        if status == "running":
            entry["started_at"] = datetime.now().isoformat()
        if status in {"done", "failed"}:
            entry["completed_at"] = datetime.now().isoformat()
        if details:
            entry.setdefault("details", {}).update(details)
        stages[stage] = entry
        self.save_state(state)

    def stage_done(self, stage: str) -> bool:
        state = self.load_state()
        return state.get("stages", {}).get(stage, {}).get("status") == "done"

    def reset_state(self) -> None:
        self.save_state({"stages": {}, "updated_at": datetime.now().isoformat()})

    def apply_env(self, cache_dir: Path | None = None, base_dir: Path | None = None) -> None:
        """Configure cache env vars for Hugging Face and sentence-transformers."""
        cache_root = os.environ.get("RAGMAIL_CACHE_DIR")
        if cache_dir is None and cache_root:
            cache_dir = Path(cache_root).expanduser()
        if cache_dir is None:
            cache_dir = default_cache_root(base_dir)

        if "HF_HOME" in os.environ:
            hf_home = Path(os.environ["HF_HOME"]).expanduser()
        else:
            hf_home = cache_dir / "huggingface"

        hf_home.mkdir(parents=True, exist_ok=True)
        os.environ["HF_HOME"] = str(hf_home)

        if "HUGGINGFACE_HUB_CACHE" not in os.environ:
            os.environ["HUGGINGFACE_HUB_CACHE"] = str(hf_home / "hub")
        if "SENTENCE_TRANSFORMERS_HOME" not in os.environ:
            os.environ["SENTENCE_TRANSFORMERS_HOME"] = str(
                hf_home / "sentence-transformers"
            )
        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
        os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")


def workspace_root(base_dir: Path | None, name: str) -> Path:
    base = base_dir or (Path.cwd() / "workspaces")
    return base / name


def default_cache_root(base_dir: Path | None) -> Path:
    base = base_dir or Path.cwd()
    return base / ".ragmail-cache"


def get_workspace(name: str, base_dir: Path | None = None) -> Workspace:
    root = workspace_root(base_dir, name)
    return Workspace(name=name, root=root)

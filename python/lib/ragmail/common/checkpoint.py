"""
Checkpointing utilities for resumable processing.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional


class Checkpoint:
    """Manages checkpoint files for resumable processing."""

    def __init__(self, input_file: str, checkpoint_suffix: str = ".checkpoint"):
        self.input_file = input_file
        self.checkpoint_file = f"{input_file}{checkpoint_suffix}"
        self.data: Dict[str, Any] = {}

    def exists(self) -> bool:
        """Check if a checkpoint file exists."""
        return os.path.exists(self.checkpoint_file)

    def load(self) -> Optional[Dict[str, Any]]:
        """Load checkpoint data. Returns None if no checkpoint exists."""
        if not self.exists():
            return None

        try:
            with open(self.checkpoint_file, 'r') as f:
                self.data = json.load(f)
                return self.data
        except (json.JSONDecodeError, IOError):
            return None

    def save(self, position: int, stats: Dict[str, Any], extra: Dict[str, Any] = None):
        """Save checkpoint data."""
        self.data = {
            'position': position,
            'stats': stats,
            'timestamp': datetime.now().isoformat(),
            'input_file': self.input_file,
        }
        if extra:
            self.data.update(extra)

        # Write atomically (write to temp, then rename)
        temp_file = f"{self.checkpoint_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(self.data, f, indent=2)
        os.rename(temp_file, self.checkpoint_file)

    def remove(self):
        """Remove checkpoint file (call on successful completion)."""
        if self.exists():
            try:
                os.remove(self.checkpoint_file)
            except IOError:
                pass

    def get_position(self) -> int:
        """Get the saved position from checkpoint."""
        return self.data.get('position', 0)

    def get_stats(self) -> Dict[str, Any]:
        """Get the saved stats from checkpoint."""
        return self.data.get('stats', {})


def create_checkpoint_summary(checkpoint: Checkpoint) -> str:
    """Create a human-readable summary of checkpoint state."""
    if not checkpoint.exists():
        return "No checkpoint found"

    data = checkpoint.load()
    if not data:
        return "Checkpoint file corrupted"

    lines = [
        f"Checkpoint found:",
        f"  Position: {data.get('position', 0):,} bytes",
        f"  Saved at: {data.get('timestamp', 'unknown')}",
    ]

    stats = data.get('stats', {})
    if stats:
        lines.append("  Stats:")
        for key, value in stats.items():
            if isinstance(value, (int, float)):
                lines.append(f"    {key}: {value:,}")

    return '\n'.join(lines)

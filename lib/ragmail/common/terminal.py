"""
Terminal UI utilities for email processing scripts.
Provides colors, glyphs, and progress display.
"""

import sys
from datetime import datetime
from typing import Dict, Optional, List
from collections import defaultdict


class Colors:
    """ANSI color codes for terminal output."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"

    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_BLUE = "\033[44m"


class Glyphs:
    """Unicode glyphs for terminal display."""
    CHECK = ""
    CROSS = ""
    ARROW = ""
    BULLET = ""
    SPINNER = [""]
    BOX_H = "─"
    BOX_V = "│"
    BOX_TL = "┌"
    BOX_TR = "┐"
    BOX_BL = "└"
    BOX_BR = "┘"
    PROGRESS_FULL = "█"
    PROGRESS_EMPTY = "░"
    FOLDER = ""
    EMAIL = ""
    CLOCK = ""
    WARNING = ""
    TRASH = ""
    CLEAN = ""
    FIRE = ""


def format_bytes(b: int) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}TB"


def format_time(seconds: float) -> str:
    """Format seconds to readable time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h:.0f}h {m:.0f}m"


def progress_bar(ratio: float, width: int = 30) -> str:
    """Create a progress bar string."""
    filled = int(ratio * width)
    return (Colors.GREEN + Glyphs.PROGRESS_FULL * filled +
            Colors.DIM + Glyphs.PROGRESS_EMPTY * (width - filled) + Colors.RESET)


class ProgressDisplay:
    """Manages a persistent live area in the terminal for progress display."""

    def __init__(self, title: str = "Processing"):
        self.title = title
        self.total_items = 0
        self.processed_items = 0
        self.current_position = 0
        self.file_size = 0
        self.start_time = datetime.now()
        self.last_item_info = ""
        self.spinner_idx = 0
        self.lines_printed = 0

        # Customizable stats
        self.stats: Dict[str, int] = defaultdict(int)
        self.stat_colors: Dict[str, str] = {}

    def set_file_size(self, size: int):
        self.file_size = size

    def add_stat(self, name: str, color: str = Colors.WHITE):
        """Add a named statistic to track."""
        self.stat_colors[name] = color

    def increment_stat(self, name: str, amount: int = 1):
        """Increment a statistic."""
        self.stats[name] += amount

    def update(self, position: int, item_info: str = "", processed_count: int = None):
        """Update progress with current position and item info."""
        self.current_position = position
        if processed_count is not None:
            self.processed_items = processed_count
        else:
            self.processed_items += 1
        self.last_item_info = item_info
        self.spinner_idx = (self.spinner_idx + 1) % len(Glyphs.SPINNER)

    def _clear_lines(self, n: int):
        """Move cursor up and clear lines."""
        for _ in range(n):
            sys.stdout.write("\033[A\033[K")

    def render(self):
        """Render the progress display."""
        if self.lines_printed > 0:
            self._clear_lines(self.lines_printed)

        lines = []

        # Header
        spinner = Glyphs.SPINNER[self.spinner_idx]
        elapsed = (datetime.now() - self.start_time).total_seconds()

        lines.append(f"{Colors.CYAN}{Colors.BOLD}{spinner} {self.title}{Colors.RESET}")
        lines.append(f"{Colors.DIM}{Glyphs.BOX_H * 50}{Colors.RESET}")

        # Progress bar
        if self.file_size > 0:
            progress_ratio = self.current_position / self.file_size
            bar = progress_bar(progress_ratio)
            pct = progress_ratio * 100

            # ETA calculation
            if progress_ratio > 0.001 and elapsed > 5:
                eta = elapsed / progress_ratio - elapsed
                eta_str = f"ETA: {format_time(eta)}"
            else:
                eta_str = "calculating..."

            lines.append(f"{bar} {pct:5.1f}% {Colors.DIM}({eta_str}){Colors.RESET}")

        # Stats line
        stats_parts = [f"{Glyphs.EMAIL} {Colors.WHITE}{self.processed_items:,}{Colors.RESET} processed"]
        stats_parts.append(f"{Glyphs.CLOCK} {format_time(elapsed)}")

        if self.file_size > 0:
            stats_parts.append(f"Position: {format_bytes(self.current_position)}/{format_bytes(self.file_size)}")

        lines.append("  ".join(stats_parts))

        # Custom stats
        if self.stats:
            stat_parts = []
            for name, value in self.stats.items():
                color = self.stat_colors.get(name, Colors.WHITE)
                stat_parts.append(f"{color}{name}: {value:,}{Colors.RESET}")
            lines.append("  ".join(stat_parts))

        # Current item info
        if self.last_item_info:
            truncated = self.last_item_info[:60]
            if len(self.last_item_info) > 60:
                truncated += "..."
            lines.append(f"{Colors.DIM}Current: {truncated}{Colors.RESET}")

        # Print all lines
        output = "\n".join(lines)
        print(output)
        self.lines_printed = len(lines)

    def finalize(self, success: bool = True, message: str = None):
        """Show final summary."""
        if self.lines_printed > 0:
            self._clear_lines(self.lines_printed)

        elapsed = (datetime.now() - self.start_time).total_seconds()

        if success:
            msg = message or "Completed!"
            print(f"\n{Colors.GREEN}{Glyphs.CHECK} {msg}{Colors.RESET}\n")
        else:
            msg = message or "Interrupted - checkpoint saved"
            print(f"\n{Colors.YELLOW}{Glyphs.WARNING} {msg}{Colors.RESET}\n")

        print(f"{Colors.BOLD}Summary:{Colors.RESET}")
        print(f"{Glyphs.BOX_H * 40}")
        print(f"  Total processed: {Colors.WHITE}{self.processed_items:,}{Colors.RESET}")
        print(f"  Time elapsed:    {Colors.WHITE}{format_time(elapsed)}{Colors.RESET}")

        if self.stats:
            for name, value in self.stats.items():
                color = self.stat_colors.get(name, Colors.WHITE)
                print(f"  {name}: {color}{value:,}{Colors.RESET}")

        if elapsed > 0:
            rate = self.processed_items / elapsed
            print(f"  Rate:            {Colors.WHITE}{rate:.0f} items/sec{Colors.RESET}")

        print()

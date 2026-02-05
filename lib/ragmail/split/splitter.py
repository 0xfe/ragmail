#!/usr/bin/env python3
"""
MBOX Splitter by Month
======================
Splits a large MBOX file into separate files per month.
Handles Google Takeout MBOX format with robust error handling.

Features:
- Stream processing (memory efficient for large files)
- Pretty terminal output with live progress
- Graceful error handling with detailed diagnostics
- Checkpointing for resume capability
- Date filtering support
- Verification with SHA256 hashing
"""

import os
import sys
import re
import json
import hashlib
import argparse
from datetime import datetime
from typing import Optional, Dict, TextIO, Tuple, List, Callable
import time
from collections import defaultdict, OrderedDict
from email.utils import parsedate_to_datetime
import signal

# ANSI color codes
class Colors:
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

# Unicode glyphs
class Glyphs:
    CHECK = "✓"
    CROSS = "✗"
    ARROW = "→"
    BULLET = "•"
    SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    BOX_H = "─"
    BOX_V = "│"
    BOX_TL = "┌"
    BOX_TR = "┐"
    BOX_BL = "└"
    BOX_BR = "┘"
    PROGRESS_FULL = "█"
    PROGRESS_EMPTY = "░"
    FOLDER = "📁"
    EMAIL = "📧"
    CLOCK = "⏱"
    WARNING = "⚠"

# Global state for signal handling
interrupted = False

def signal_handler(signum, frame):
    global interrupted
    interrupted = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class ProgressDisplay:
    """Manages a persistent live area in the terminal for progress display."""

    def __init__(self):
        self.period_stats: Dict[str, dict] = defaultdict(lambda: {"count": 0, "bytes": 0})
        self.total_emails = 0
        self.total_bytes = 0
        self.errors = 0
        self.current_position = 0
        self.file_size = 0
        self.start_time = datetime.now()
        self.last_email_info = ""
        self.spinner_idx = 0
        self.lines_printed = 0
        self.filter_years: Optional[List[int]] = None

    def set_file_size(self, size: int):
        self.file_size = size

    def set_filter_years(self, years: Optional[List[int]]):
        self.filter_years = years

    def update(self, period: Optional[str], email_bytes: int, position: int, email_info: str = ""):
        self.current_position = position
        self.total_bytes += email_bytes
        self.total_emails += 1
        if period:
            self.period_stats[period]["count"] += 1
            self.period_stats[period]["bytes"] += email_bytes
        self.last_email_info = email_info
        self.spinner_idx = (self.spinner_idx + 1) % len(Glyphs.SPINNER)

    def add_error(self):
        self.errors += 1

    def _clear_lines(self, n: int):
        """Move cursor up and clear lines."""
        for _ in range(n):
            sys.stdout.write("\033[A\033[K")

    def _format_bytes(self, b: int) -> str:
        """Format bytes to human readable."""
        for unit in ["B", "KB", "MB", "GB"]:
            if b < 1024:
                return f"{b:.1f}{unit}"
            b /= 1024
        return f"{b:.1f}TB"

    def _format_time(self, seconds: float) -> str:
        """Format seconds to readable time."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
        else:
            h = seconds // 3600
            m = (seconds % 3600) // 60
            return f"{h:.0f}h {m:.0f}m"

    def _progress_bar(self, ratio: float, width: int = 30) -> str:
        """Create a progress bar."""
        filled = int(ratio * width)
        return (Colors.GREEN + Glyphs.PROGRESS_FULL * filled +
                Colors.DIM + Glyphs.PROGRESS_EMPTY * (width - filled) + Colors.RESET)

    def render(self):
        """Render the progress display."""
        if self.lines_printed > 0:
            self._clear_lines(self.lines_printed)

        lines = []

        # Header
        spinner = Glyphs.SPINNER[self.spinner_idx]
        elapsed = (datetime.now() - self.start_time).total_seconds()

        lines.append(f"{Colors.CYAN}{Colors.BOLD}{spinner} MBOX Splitter{Colors.RESET}")
        lines.append(f"{Colors.DIM}{Glyphs.BOX_H * 50}{Colors.RESET}")

        # Progress bar
        if self.file_size > 0:
            progress_ratio = self.current_position / self.file_size
            bar = self._progress_bar(progress_ratio)
            pct = progress_ratio * 100

            # ETA calculation
            if progress_ratio > 0.001 and elapsed > 5:
                eta = elapsed / progress_ratio - elapsed
                eta_str = f"ETA: {self._format_time(eta)}"
            else:
                eta_str = "calculating..."

            lines.append(f"{bar} {pct:5.1f}% {Colors.DIM}({eta_str}){Colors.RESET}")

        # Stats line
        stats = (f"{Glyphs.EMAIL} {Colors.WHITE}{self.total_emails:,}{Colors.RESET} emails  "
                f"{Glyphs.CLOCK} {self._format_time(elapsed)}  "
                f"Position: {self._format_bytes(self.current_position)}/{self._format_bytes(self.file_size)}")
        lines.append(stats)

        if self.errors > 0:
            lines.append(f"{Colors.YELLOW}{Glyphs.WARNING} {self.errors} errors{Colors.RESET}")

        # Filter info
        if self.filter_years:
            filter_str = ", ".join(str(y) for y in sorted(self.filter_years))
            lines.append(f"{Colors.MAGENTA}Filtering: years {filter_str}{Colors.RESET}")

        lines.append("")

        # Year breakdown (sorted)
        lines.append(f"{Colors.BOLD}Month Statistics:{Colors.RESET}")

        sorted_periods = sorted(self.period_stats.keys())
        for period in sorted_periods:
            stats = self.period_stats[period]
            count = stats["count"]
            size = stats["bytes"]
            lines.append(f"  {Colors.BLUE}{period}{Colors.RESET}: "
                        f"{Colors.WHITE}{count:>7,}{Colors.RESET} emails  "
                        f"{Colors.DIM}({self._format_bytes(size)}){Colors.RESET}")

        if not sorted_periods:
            lines.append(f"  {Colors.DIM}(no emails processed yet){Colors.RESET}")

        # Current email info
        if self.last_email_info:
            lines.append("")
            lines.append(f"{Colors.DIM}Current: {self.last_email_info[:60]}...{Colors.RESET}")

        # Print all lines
        output = "\n".join(lines)
        print(output)
        self.lines_printed = len(lines)

    def finalize(self, success: bool = True):
        """Show final summary."""
        if self.lines_printed > 0:
            self._clear_lines(self.lines_printed)

        elapsed = (datetime.now() - self.start_time).total_seconds()

        if success:
            print(f"\n{Colors.GREEN}{Glyphs.CHECK} Completed!{Colors.RESET}\n")
        else:
            print(f"\n{Colors.YELLOW}{Glyphs.WARNING} Interrupted - checkpoint saved{Colors.RESET}\n")

        print(f"{Colors.BOLD}Summary:{Colors.RESET}")
        print(f"{Glyphs.BOX_H * 40}")
        print(f"  Total emails: {Colors.WHITE}{self.total_emails:,}{Colors.RESET}")
        print(f"  Total size:   {Colors.WHITE}{self._format_bytes(self.total_bytes)}{Colors.RESET}")
        print(f"  Time elapsed: {Colors.WHITE}{self._format_time(elapsed)}{Colors.RESET}")
        if self.errors > 0:
            print(f"  Errors:       {Colors.YELLOW}{self.errors}{Colors.RESET}")
        print()

        print(f"{Colors.BOLD}Output Files:{Colors.RESET}")
        for period in sorted(self.period_stats.keys()):
            stats = self.period_stats[period]
            print(f"  {Glyphs.FOLDER} {period}.mbox: "
                  f"{stats['count']:,} emails ({self._format_bytes(stats['bytes'])})")


class MboxSplitter:
    """Main class for splitting MBOX files by month."""

    # Regex for the MBOX "From " envelope line
    # Format: From <id>@xxx <weekday> <month> <day> <time> <tz> <year>
    FROM_LINE_PATTERN = re.compile(
        r'^From \S+ (Mon|Tue|Wed|Thu|Fri|Sat|Sun) '
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
        r'\s*(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{4}|\w+) (\d{4})$'
    )

    MONTHS = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    def __init__(
        self,
        input_file: str,
        output_dir: str,
        filter_years: Optional[List[int]] = None,
        checkpoint_file: Optional[str] = None,
        progress_callback: Callable[[dict], None] | None = None,
        show_progress: bool = True,
    ):
        self.input_file = input_file
        self.output_dir = output_dir
        self.filter_years = set(filter_years) if filter_years else None
        self.checkpoint_file = checkpoint_file or f"{input_file}.checkpoint"
        self.progress_callback = progress_callback
        self.show_progress = show_progress

        self.output_files: "OrderedDict[str, TextIO]" = OrderedDict()
        self.hashers: Dict[str, hashlib.sha256] = {}
        self.display = ProgressDisplay()

        if filter_years:
            self.display.set_filter_years(filter_years)

        # Error tracking
        self.last_good_position = 0
        self.last_good_date = ""
        self.current_email_start = 0
        self.processed_emails = 0
        self.written_emails = 0
        self.skipped_emails = 0
        self.error_emails = 0
        self._last_progress_time = 0.0
        self._progress_interval = 0.1
        self._progress_step = 25
        self._last_progress_count = 0
        self.max_open_files = 64
        self._last_flush_time = 0.0
        self._flush_interval = 2.0

    def _emit_progress(self, position: int, file_size: int, force: bool = False) -> None:
        if not self.progress_callback:
            return
        now = time.monotonic()
        if not force:
            if (now - self._last_progress_time) < self._progress_interval and (
                self.processed_emails - self._last_progress_count
            ) < self._progress_step:
                return
        self.progress_callback(
            {
                "processed": self.processed_emails,
                "written": self.written_emails,
                "skipped": self.skipped_emails,
                "errors": self.error_emails,
                "position": position,
                "file_size": file_size,
            }
        )
        self._last_progress_time = now
        self._last_progress_count = self.processed_emails

    def _get_output_file(self, year: int, month: int) -> Optional[TextIO]:
        """Get or create output file for a year-month."""
        if self.filter_years and year not in self.filter_years:
            return None

        period = f"{year:04d}-{month:02d}"
        handle = self.output_files.get(period)
        if handle is not None:
            self.output_files.move_to_end(period)
            return handle

        filepath = os.path.join(self.output_dir, f"{period}.mbox")
        mode = 'ab' if os.path.exists(filepath) else 'wb'
        handle = open(filepath, mode, buffering=1024 * 1024)
        self.output_files[period] = handle
        self.output_files.move_to_end(period)
        if period not in self.hashers:
            self.hashers[period] = hashlib.sha256()

        if self.max_open_files and len(self.output_files) > self.max_open_files:
            old_period, old_handle = self.output_files.popitem(last=False)
            try:
                old_handle.flush()
                old_handle.close()
            except:
                pass

        return handle

    def _extract_year_month_from_envelope(self, line: str) -> tuple[int, int] | None:
        """Extract year and month from MBOX envelope 'From ' line."""
        match = self.FROM_LINE_PATTERN.match(line.rstrip())
        if not match:
            return None
        month_str = match.group(2)
        year = int(match.group(6))
        month = self.MONTHS.get(month_str)
        if not month:
            return None
        return year, month

    def _parse_date_header(self, header_value: str) -> tuple[int, int] | None:
        """Try to parse year/month from Date header with multiple format support."""
        header_value = header_value.strip()

        # Remove any HTML/junk (some corrupted headers)
        if '<' in header_value and '>' in header_value:
            return None
        if '=' in header_value:  # Likely encoded junk
            return None

        # Try standard email date parsing first
        try:
            dt = parsedate_to_datetime(header_value)
            return dt.year, dt.month
        except:
            pass

        # Try various date formats
        formats = [
            # US formats
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+(\d{4})',
            # ISO-ish
            r'(\d{4})-(\d{2})-\d{2}',
            # European
            r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            # Full month name
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(\d{4})',
        ]

        for pattern in formats:
            match = re.search(pattern, header_value, re.IGNORECASE)
            if match:
                groups = match.groups()
                year = None
                month = None
                for g in groups:
                    if not g:
                        continue
                    if g.isdigit() and len(g) == 4:
                        year = int(g)
                    elif g.isdigit() and len(g) <= 2:
                        month = int(g)
                    else:
                        month = self._month_from_name(g)
                if year and month and 1990 <= year <= 2100 and 1 <= month <= 12:
                    return year, month

        return None

    def _process_email(
        self,
        lines: List[bytes],
        envelope_date: tuple[int, int] | None,
    ) -> tuple[tuple[int, int] | None, str]:
        """Process a single email and return ((year, month), info_string)."""
        year = envelope_date[0] if envelope_date else None
        month = envelope_date[1] if envelope_date else None
        subject = ""
        date_str = ""
        from_addr = ""

        # Parse headers for Date if we don't have year from envelope
        in_headers = True
        for line in lines[:100]:  # Only check first 100 lines for headers
            try:
                decoded = line.decode('utf-8', errors='replace')
            except:
                decoded = str(line)

            if in_headers:
                if decoded.strip() == "":
                    in_headers = False
                    continue

                lower = decoded.lower()
                if lower.startswith('date:'):
                    date_str = decoded[5:].strip()
                    if not year or not month:
                        parsed = self._parse_date_header(date_str)
                        if parsed:
                            year, month = parsed
                elif lower.startswith('subject:'):
                    subject = decoded[8:].strip()[:50]
                elif lower.startswith('from:'):
                    from_addr = decoded[5:].strip()[:30]

        info = f"{from_addr} - {subject}" if subject else from_addr
        if year and month:
            return (year, month), info
        return None, info

    def _load_checkpoint(self) -> Tuple[int, Dict[str, dict]]:
        """Load checkpoint if exists. Returns (position, period_stats)."""
        if not os.path.exists(self.checkpoint_file):
            return 0, {}

        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                return data.get('position', 0), data.get('period_stats', {})
        except:
            return 0, {}

    def _save_checkpoint(self, position: int, period_stats: Dict[str, dict]):
        """Save checkpoint for resume capability."""
        data = {
            'position': position,
            'period_stats': {str(k): v for k, v in period_stats.items()},
            'timestamp': datetime.now().isoformat(),
            'input_file': self.input_file,
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _save_hashes(self):
        """Save hash verification file."""
        hashes = {}
        for period, hasher in self.hashers.items():
            hashes[f"{period}.mbox"] = hasher.hexdigest()

        hash_file = os.path.join(self.output_dir, "checksums.sha256")
        with open(hash_file, 'w') as f:
            for filename, hash_val in sorted(hashes.items()):
                f.write(f"{hash_val}  {filename}\n")

        if self.show_progress:
            print(f"\n{Colors.CYAN}Checksums saved to: {hash_file}{Colors.RESET}")

    def close_files(self):
        """Close all output files."""
        for f in self.output_files.values():
            try:
                f.flush()
                f.close()
            except:
                pass
        self.output_files.clear()

    def _month_from_name(self, value: str) -> int | None:
        key = value.strip()[:3].title()
        return self.MONTHS.get(key)

    def run(self, resume: bool = False):
        """Main processing loop."""
        global interrupted

        # Get file size
        file_size = os.path.getsize(self.input_file)
        self.display.set_file_size(file_size)

        # Check for resume
        start_position = 0
        if resume:
            start_position, saved_stats = self._load_checkpoint()
            if start_position > 0:
                if self.show_progress:
                    print(
                        f"{Colors.CYAN}Resuming from position {start_position:,} "
                        f"({start_position * 100 / file_size:.1f}%){Colors.RESET}\n"
                    )
                # Restore stats
                for period, stats in saved_stats.items():
                    self.display.period_stats[str(period)] = stats
                self.display.current_position = start_position

        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)

        current_email_lines: List[bytes] = []
        current_envelope_date: tuple[int, int] | None = None
        email_start_position = start_position
        position = 0

        try:
            with open(self.input_file, 'rb') as f:
                # Seek to resume position
                if start_position > 0:
                    f.seek(start_position)
                    position = start_position
                    # Read until we find a "From " line to sync
                    while True:
                        line = f.readline()
                        if not line:
                            break
                        position = f.tell()
                        try:
                            decoded = line.decode('utf-8', errors='replace')
                            if decoded.startswith('From ') and self._extract_year_month_from_envelope(decoded):
                                # Found a good sync point
                                current_envelope_date = self._extract_year_month_from_envelope(decoded)
                                current_email_lines = [line]
                                email_start_position = position - len(line)
                                break
                        except:
                            continue

                last_render_time = datetime.now()
                render_interval = 0.25  # Render every 250ms

                while True:
                    if interrupted:
                        break

                    line = f.readline()
                    if not line:
                        break

                    position = f.tell()

                    # Try to decode line for pattern matching
                    try:
                        decoded = line.decode('utf-8', errors='replace')
                    except:
                        decoded = ""

                    # Check for new email boundary
                    is_from_line = decoded.startswith('From ')
                    envelope_date = None

                    if is_from_line:
                        envelope_date = self._extract_year_month_from_envelope(decoded)

                    if is_from_line and envelope_date:
                        # Process previous email if we have one
                        if current_email_lines:
                            period, info = self._process_email(current_email_lines, current_envelope_date)

                            self.processed_emails += 1
                            email_size = sum(len(chunk) for chunk in current_email_lines)
                            if period:
                                period_key = f"{period[0]:04d}-{period[1]:02d}"
                                out_file = self._get_output_file(period[0], period[1])
                                if out_file:
                                    email_bytes = b''.join(current_email_lines)
                                    out_file.write(email_bytes)
                                    self.hashers[period_key].update(email_bytes)
                                    self.display.update(period_key, email_size, position, info)
                                    self.written_emails += 1
                                else:
                                    # Filtered out
                                    self.display.update(None, email_size, position, info)
                                    self.skipped_emails += 1
                            else:
                                # Couldn't determine date
                                self.display.add_error()
                                self.display.update(
                                    None,
                                    email_size,
                                    position,
                                    f"[unknown date] {info}",
                                )
                                self.skipped_emails += 1
                                self.error_emails += 1
                            self._emit_progress(position, file_size)

                            self.last_good_position = email_start_position
                            self.last_good_date = info

                        # Start new email
                        current_email_lines = [line]
                        current_envelope_date = envelope_date
                        email_start_position = position - len(line)
                    else:
                        # Continue current email
                        current_email_lines.append(line)

                    # Update display periodically (time-based)
                    now = datetime.now()
                    if (now - last_render_time).total_seconds() >= render_interval:
                        if self.show_progress:
                            self.display.render()
                        self._emit_progress(position, file_size)
                        last_render_time = now
                        if (time.monotonic() - self._last_flush_time) >= self._flush_interval:
                            for out_f in self.output_files.values():
                                out_f.flush()
                            self._last_flush_time = time.monotonic()

                # Process final email
                if current_email_lines and not interrupted:
                    period, info = self._process_email(current_email_lines, current_envelope_date)
                    self.processed_emails += 1
                    email_size = sum(len(chunk) for chunk in current_email_lines)
                    if period:
                        period_key = f"{period[0]:04d}-{period[1]:02d}"
                        out_file = self._get_output_file(period[0], period[1])
                        if out_file:
                            email_bytes = b''.join(current_email_lines)
                            out_file.write(email_bytes)
                            self.hashers[period_key].update(email_bytes)
                            self.display.update(period_key, email_size, position, info)
                            self.written_emails += 1
                        else:
                            self.display.update(None, email_size, position, info)
                            self.skipped_emails += 1
                    else:
                        self.display.add_error()
                        self.display.update(None, email_size, position,
                                           f"[unknown date] {info}")
                        self.skipped_emails += 1
                        self.error_emails += 1
                    self._emit_progress(position, file_size)

            # Final display
            if self.show_progress:
                self.display.finalize(success=not interrupted)
            self._emit_progress(position, file_size, force=True)

            if not interrupted:
                self._save_hashes()
                # Remove checkpoint on success
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            else:
                # Save checkpoint for resume
                period_stats_dict = {k: dict(v) for k, v in self.display.period_stats.items()}
                self._save_checkpoint(self.last_good_position, period_stats_dict)
                if self.show_progress:
                    print(f"\n{Colors.YELLOW}Checkpoint saved at position {self.last_good_position:,}{Colors.RESET}")
                    print(f"Last successfully processed: {self.last_good_date}")
                    print(f"\nTo resume, run with --resume flag")

        except Exception as e:
            if self.show_progress:
                self.display.finalize(success=False)
                print(f"\n{Colors.RED}{Colors.BOLD}ERROR:{Colors.RESET} {str(e)}")
                print(f"\n{Colors.BOLD}Error Details:{Colors.RESET}")
                print(f"  File position: {position:,} bytes")
                print(f"  Last good position: {self.last_good_position:,} bytes")
                print(f"  Current email start: {email_start_position:,} bytes")
                print(f"  Last processed: {self.last_good_date}")
                print(
                    f"\n{Colors.DIM}To reproduce, examine the file around byte position "
                    f"{email_start_position}{Colors.RESET}"
                )

            # Save checkpoint
            period_stats_dict = {k: dict(v) for k, v in self.display.period_stats.items()}
            self._save_checkpoint(self.last_good_position, period_stats_dict)
            raise

        finally:
            self.close_files()


def verify_output(output_dir: str) -> bool:
    """Verify output files using checksums."""
    hash_file = os.path.join(output_dir, "checksums.sha256")

    if not os.path.exists(hash_file):
        print(f"{Colors.RED}No checksum file found at {hash_file}{Colors.RESET}")
        return False

    print(f"\n{Colors.CYAN}{Colors.BOLD}Verifying output files...{Colors.RESET}\n")

    all_ok = True
    with open(hash_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            expected_hash, filename = line.split('  ', 1)
            filepath = os.path.join(output_dir, filename)

            if not os.path.exists(filepath):
                print(f"  {Colors.RED}{Glyphs.CROSS}{Colors.RESET} {filename} - MISSING")
                all_ok = False
                continue

            # Calculate hash
            hasher = hashlib.sha256()
            with open(filepath, 'rb') as file:
                for chunk in iter(lambda: file.read(8192), b''):
                    hasher.update(chunk)

            actual_hash = hasher.hexdigest()

            if actual_hash == expected_hash:
                size = os.path.getsize(filepath)
                print(f"  {Colors.GREEN}{Glyphs.CHECK}{Colors.RESET} {filename} - OK ({size:,} bytes)")
            else:
                print(f"  {Colors.RED}{Glyphs.CROSS}{Colors.RESET} {filename} - HASH MISMATCH")
                print(f"      Expected: {expected_hash}")
                print(f"      Actual:   {actual_hash}")
                all_ok = False

    print()
    if all_ok:
        print(f"{Colors.GREEN}{Colors.BOLD}All files verified successfully!{Colors.RESET}")
    else:
        print(f"{Colors.RED}{Colors.BOLD}Verification failed!{Colors.RESET}")

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description='Split MBOX file by month with progress tracking and verification.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s all-emails.mbox
  %(prog)s all-emails.mbox --years 2022 2023
  %(prog)s all-emails.mbox --resume
  %(prog)s --verify output/
        """
    )

    parser.add_argument('input', nargs='?', help='Input MBOX file')
    parser.add_argument('-o', '--output', default='.', help='Output directory (default: current)')
    parser.add_argument('-y', '--years', nargs='+', type=int, help='Only extract specific years')
    parser.add_argument('-r', '--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('-v', '--verify', action='store_true', help='Verify output files')

    args = parser.parse_args()

    # Handle verify mode
    if args.verify:
        verify_dir = args.input if args.input else args.output
        sys.exit(0 if verify_output(verify_dir) else 1)

    # Need input file for splitting
    if not args.input:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.input):
        print(f"{Colors.RED}Error: File not found: {args.input}{Colors.RESET}")
        sys.exit(1)

    # Print banner
    print(f"""
{Colors.CYAN}{Colors.BOLD}╔══════════════════════════════════════╗
║       MBOX Splitter by Year          ║
╚══════════════════════════════════════╝{Colors.RESET}
""")

    print(f"Input:  {Colors.WHITE}{args.input}{Colors.RESET}")
    print(f"Output: {Colors.WHITE}{args.output}/{Colors.RESET}")
    if args.years:
        print(f"Filter: {Colors.MAGENTA}years {', '.join(map(str, args.years))}{Colors.RESET}")
    print()

    splitter = MboxSplitter(
        input_file=args.input,
        output_dir=args.output,
        filter_years=args.years,
    )

    try:
        splitter.run(resume=args.resume)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user{Colors.RESET}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{Colors.RED}Fatal error: {e}{Colors.RESET}")
        sys.exit(1)


if __name__ == '__main__':
    main()

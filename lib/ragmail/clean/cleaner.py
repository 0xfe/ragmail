#!/usr/bin/env python3
"""
Email Cleaner for RAG
=====================
Cleans Gmail MBOX files for use in RAG (Retrieval-Augmented Generation) systems.

Outputs JSON Lines (NDJSON) format - one JSON object per line for each email.

Features:
- Structured JSON output with normalized headers
- Extracts clean plain text from HTML emails
- Removes attachments but preserves metadata
- Detects and removes email signatures
- Filters spam and low-value newsletters
- Generates detailed summary statistics
- Supports resume from checkpoint after interruption

Usage:
    python email-clean.py gmail-2004.mbox
    python email-clean.py gmail-2004.mbox --resume
"""

import os
import sys
import re
import json
import argparse
import email
from email.utils import parseaddr, parsedate_to_datetime
from email import policy
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional, List, Tuple, Dict, Any
from ragmail.common import signals

from ragmail.common.terminal import Colors, Glyphs, ProgressDisplay, format_bytes, format_time
from ragmail.common.checkpoint import Checkpoint

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print(f"{Colors.YELLOW}Warning: beautifulsoup4 not installed. HTML extraction will be limited.{Colors.RESET}")

try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False


# =============================================================================
# CONFIGURATION
# =============================================================================

# Spam/Newsletter detection patterns
SPAM_SENDERS = {
    'discard-report@pobox.com',
    'mailer-daemon@',
    'postmaster@',
}

NEWSLETTER_MAILERS = {
    'cheetahmailer',
    'mailchimp',
    'sailthru',
    'constant contact',
    'sendgrid',
    'exacttarget',
    'marketo',
    'hubspot',
    'campaign monitor',
    'xyzmailer',
    'wiredmessenger',
}

# Gmail labels that indicate spam/low-value content
SPAM_LABELS = {
    'spam',
    'trash',
}

# Labels that might be filtered (configurable)
LOW_VALUE_LABELS = {
    'category promotions',
}

# Signature detection patterns
SIGNATURE_MARKERS = [
    re.compile(r'^-- ?$', re.MULTILINE),
    re.compile(r'^—$', re.MULTILINE),
    re.compile(r'^_{3,}$', re.MULTILINE),
    re.compile(r'^-{3,}$', re.MULTILINE),
]

SIGNATURE_PHRASES = [
    'best regards',
    'kind regards',
    'warm regards',
    'regards,',
    'cheers,',
    'thanks,',
    'thank you,',
    'sincerely,',
    'sent from my iphone',
    'sent from my ipad',
    'sent from my android',
    'get outlook for',
]

# Patterns to clean from text content
GARBAGE_PATTERNS = [
    # Invisible preview text characters
    re.compile(r'[\u200c\u200d\u034f]+'),
    # Zero-width spaces used for tracking
    re.compile(r'[\u00ad\u200b\ufeff]+'),
    # Repeated special characters (often used as spacers)
    re.compile(r'[͏]{2,}'),
    # Unsubscribe boilerplate at end
    re.compile(r'\n.*unsubscribe.*$', re.IGNORECASE | re.MULTILINE),
    # View in browser links
    re.compile(r'^.*view (this |in )?(email |message )?(in |your )?(browser|web).*$', re.IGNORECASE | re.MULTILINE),
    # Email display issues notice
    re.compile(r'^.*trouble (viewing|displaying|reading).*$', re.IGNORECASE | re.MULTILINE),
    # Multiple blank lines
    re.compile(r'\n{4,}'),
]


# =============================================================================
# STATISTICS TRACKING
# =============================================================================

class CleaningStats:
    def __init__(self):
        self.total_emails = 0
        self.clean_emails = 0
        self.spam_emails = 0
        self.error_emails = 0

        self.spam_reasons = defaultdict(int)
        self.attachment_types = defaultdict(int)
        self.attachments_removed = 0
        self.attachments_size = 0

        self.original_size = 0
        self.clean_size = 0
        self.spam_size = 0

        self.labels_seen = defaultdict(int)
        self.senders = defaultdict(int)

        self.html_only_converted = 0
        self.signatures_removed = 0

        self.start_time = datetime.now()
        self.last_good_position = 0

    def to_dict(self) -> dict:
        return {
            'total_emails': self.total_emails,
            'clean_emails': self.clean_emails,
            'spam_emails': self.spam_emails,
            'error_emails': self.error_emails,
            'spam_reasons': dict(self.spam_reasons),
            'attachment_types': dict(self.attachment_types),
            'attachments_removed': self.attachments_removed,
            'attachments_size': self.attachments_size,
            'html_only_converted': self.html_only_converted,
            'signatures_removed': self.signatures_removed,
            'last_good_position': self.last_good_position,
        }

    def from_dict(self, data: dict):
        self.total_emails = data.get('total_emails', 0)
        self.clean_emails = data.get('clean_emails', 0)
        self.spam_emails = data.get('spam_emails', 0)
        self.error_emails = data.get('error_emails', 0)
        self.spam_reasons = defaultdict(int, data.get('spam_reasons', {}))
        self.attachment_types = defaultdict(int, data.get('attachment_types', {}))
        self.attachments_removed = data.get('attachments_removed', 0)
        self.attachments_size = data.get('attachments_size', 0)
        self.html_only_converted = data.get('html_only_converted', 0)
        self.signatures_removed = data.get('signatures_removed', 0)
        self.last_good_position = data.get('last_good_position', 0)


# =============================================================================
# EMAIL CLEANING FUNCTIONS
# =============================================================================

def decode_header_value(value: str) -> str:
    """Decode RFC 2047 encoded header values."""
    if not value:
        return ""

    try:
        decoded_parts = email.header.decode_header(value)
        result = []
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                try:
                    if encoding:
                        result.append(part.decode(encoding, errors='replace'))
                    else:
                        result.append(part.decode('utf-8', errors='replace'))
                except (LookupError, UnicodeDecodeError):
                    result.append(part.decode('latin-1', errors='replace'))
            else:
                result.append(part)
        return ' '.join(result)
    except Exception:
        return str(value)


def decode_payload(part) -> str:
    """Decode email payload to string."""
    try:
        payload = part.get_payload(decode=True)
        if payload is None:
            return ""

        charset = part.get_content_charset() or 'utf-8'

        try:
            return payload.decode(charset, errors='replace')
        except (LookupError, UnicodeDecodeError):
            if HAS_CHARDET:
                detected = chardet.detect(payload)
                if detected and detected['encoding']:
                    try:
                        return payload.decode(detected['encoding'], errors='replace')
                    except (LookupError, UnicodeDecodeError):
                        pass
            return payload.decode('latin-1', errors='replace')
    except Exception:
        return ""


def html_to_text(html: str) -> str:
    """Convert HTML to clean plain text."""
    if not html:
        return ""

    if not HAS_BS4:
        # Basic fallback without BeautifulSoup
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&quot;', '"', text)
        text = re.sub(r'&#\d+;', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    try:
        soup = BeautifulSoup(html, 'lxml')

        # Remove non-content elements
        for element in soup(['script', 'style', 'head', 'meta', 'link', 'noscript', 'header', 'footer', 'nav']):
            element.decompose()

        # Remove tracking pixels (1x1 images)
        for img in soup.find_all('img'):
            width = img.get('width', '')
            height = img.get('height', '')
            if width == '1' or height == '1' or 'tracking' in str(img.get('src', '')).lower():
                img.decompose()

        # Remove hidden elements
        for element in soup.find_all(style=re.compile(r'display:\s*none', re.IGNORECASE)):
            element.decompose()
        for element in soup.find_all(attrs={'hidden': True}):
            element.decompose()

        # Remove elements with only invisible content
        for element in soup.find_all(['div', 'span', 'td']):
            text = element.get_text(strip=True)
            # Check if text is all invisible/whitespace characters
            if text and all(c in '\u200c\u200d\u034f\u00ad\u200b\ufeff͏ \t\n' for c in text):
                element.decompose()

        # Get text with some structure preserved
        text = soup.get_text(separator='\n')

        # Clean up the text
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            if line:
                lines.append(line)

        text = '\n'.join(lines)

        # Clean up excessive newlines
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()
    except Exception:
        # Fallback: just strip tags
        return re.sub(r'<[^>]+>', '', html).strip()


def clean_text(text: str) -> str:
    """Clean up text content, removing garbage patterns."""
    if not text:
        return ""

    # Apply garbage patterns
    for pattern in GARBAGE_PATTERNS:
        text = pattern.sub('', text)

    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def remove_signature(text: str) -> Tuple[str, bool]:
    """Remove email signature from text."""
    if not text:
        return text, False

    lines = text.split('\n')

    for i, line in enumerate(lines):
        stripped = line.strip()

        for pattern in SIGNATURE_MARKERS:
            if pattern.match(stripped):
                cleaned = '\n'.join(lines[:i]).rstrip()
                if cleaned:
                    return cleaned, True

        if i > len(lines) * 0.8:
            lower = stripped.lower()
            for phrase in SIGNATURE_PHRASES:
                if lower.startswith(phrase):
                    cleaned = '\n'.join(lines[:i]).rstrip()
                    if cleaned:
                        return cleaned, True

    return text, False


def parse_address(addr_str: str) -> Dict[str, str]:
    """Parse an email address into name and email components."""
    if not addr_str:
        return None
    # Ensure we're working with a string
    addr_str = str(addr_str) if not isinstance(addr_str, str) else addr_str
    name, email_addr = parseaddr(addr_str)
    name = decode_header_value(name) if name else ""
    return {"name": name, "email": email_addr} if email_addr else None


def parse_address_list(addr_str: str) -> List[Dict[str, str]]:
    """Parse a comma-separated list of addresses."""
    if not addr_str:
        return []

    # Ensure we're working with a string
    addr_str = str(addr_str) if not isinstance(addr_str, str) else addr_str

    # Handle encoded headers first
    addr_str = decode_header_value(addr_str)

    # Simple split on comma (may not handle all edge cases)
    result = []
    for part in addr_str.split(','):
        part = part.strip()
        if part:
            parsed = parse_address(part)
            if parsed:
                result.append(parsed)
    return result


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to ISO 8601 format."""
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except Exception:
        return date_str  # Return original if parsing fails


def parse_references(refs_str: str) -> List[str]:
    """Parse References header into list of message IDs."""
    if not refs_str:
        return []
    # Message IDs are enclosed in angle brackets
    return re.findall(r'<([^>]+)>', refs_str)


def extract_text_content(msg) -> Tuple[List[Dict], bool, List[Dict]]:
    """Extract content blocks from email message.

    Returns:
        (content_blocks, was_html_only, attachments)
    """
    text_parts = []
    html_parts = []
    attachments = []
    was_html = False

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get('Content-Disposition', ''))

            # Handle attachments
            if 'attachment' in content_disposition or content_type.startswith(('image/', 'audio/', 'video/', 'application/')):
                if 'attachment' in content_disposition or part.get_filename():
                    filename = part.get_filename() or 'unnamed'
                    filename = decode_header_value(filename)
                    payload = part.get_payload(decode=True)
                    size = len(payload) if payload else 0

                    attachments.append({
                        'filename': filename,
                        'content_type': content_type,
                        'size': size,
                    })
                    continue

            if content_type == 'text/plain':
                text = decode_payload(part)
                if text:
                    text_parts.append(text)
            elif content_type == 'text/html':
                html = decode_payload(part)
                if html:
                    html_parts.append(html)
    else:
        content_type = msg.get_content_type()
        if content_type == 'text/plain':
            text = decode_payload(msg)
            if text:
                text_parts.append(text)
        elif content_type == 'text/html':
            html = decode_payload(msg)
            if html:
                html_parts.append(html)

    # Build content blocks
    content_blocks = []

    if text_parts:
        combined_text = '\n\n'.join(text_parts)
        cleaned = clean_text(combined_text)
        if cleaned:
            content_blocks.append({
                'type': 'text',
                'body': cleaned
            })
    elif html_parts:
        # Only use HTML if no plain text available
        was_html = True
        for html in html_parts:
            text = html_to_text(html)
            cleaned = clean_text(text)
            if cleaned:
                content_blocks.append({
                    'type': 'text',
                    'body': cleaned
                })

    return content_blocks, was_html, attachments


def is_spam(msg, stats: CleaningStats) -> Tuple[bool, str]:
    """Determine if an email should be classified as spam/low-value."""
    labels = str(msg.get('X-Gmail-Labels', '') or '').lower()
    for label in SPAM_LABELS:
        if label in labels:
            return True, f"label:{label}"

    for label in LOW_VALUE_LABELS:
        if label in labels:
            return True, f"label:{label}"

    from_header = str(msg.get('From', '') or '').lower()
    for spam_sender in SPAM_SENDERS:
        if spam_sender in from_header:
            return True, f"sender:{spam_sender}"

    mailer = str(msg.get('X-Mailer', '') or '').lower()
    for newsletter_mailer in NEWSLETTER_MAILERS:
        if newsletter_mailer in mailer:
            return True, f"mailer:{newsletter_mailer}"

    precedence = str(msg.get('Precedence', '') or '').lower()
    if precedence == 'bulk':
        list_id = str(msg.get('List-ID', '') or '')
        if not list_id:
            return True, "precedence:bulk"

    return False, ""


def email_to_json(msg, stats: CleaningStats) -> Optional[Dict[str, Any]]:
    """Convert an email message to JSON structure."""
    try:
        # Extract content
        content_blocks, was_html, attachments = extract_text_content(msg)

        if was_html:
            stats.html_only_converted += 1

        # Remove signatures from content blocks
        cleaned_blocks = []
        for block in content_blocks:
            if block['type'] == 'text':
                text, sig_removed = remove_signature(block['body'])
                if sig_removed:
                    stats.signatures_removed += 1
                if text:
                    cleaned_blocks.append({'type': 'text', 'body': text})
            else:
                cleaned_blocks.append(block)

        # Track attachments in stats
        for att in attachments:
            stats.attachments_removed += 1
            stats.attachments_size += att['size']
            stats.attachment_types[att['content_type']] += 1

        # Build headers
        headers = {}

        # Helper to safely get header as string
        def get_header(name: str) -> str:
            val = msg.get(name, '')
            return str(val) if val else ''

        # From (single address)
        from_parsed = parse_address(get_header('From'))
        if from_parsed:
            headers['from'] = from_parsed

        # To, Cc, Bcc (address lists)
        to_list = parse_address_list(get_header('To'))
        if to_list:
            headers['to'] = to_list

        cc_list = parse_address_list(get_header('Cc'))
        if cc_list:
            headers['cc'] = cc_list

        bcc_list = parse_address_list(get_header('Bcc'))
        if bcc_list:
            headers['bcc'] = bcc_list

        # Reply-To
        reply_to = parse_address(get_header('Reply-To'))
        if reply_to:
            headers['reply_to'] = reply_to

        # Subject
        subject = decode_header_value(get_header('Subject'))
        if subject:
            headers['subject'] = subject

        # Date
        date_str = parse_date(get_header('Date'))
        if date_str:
            headers['date'] = date_str

        # Message IDs and threading
        message_id = get_header('Message-ID')
        if message_id:
            # Strip angle brackets if present
            message_id = message_id.strip('<>')
            headers['message_id'] = message_id

        in_reply_to = get_header('In-Reply-To')
        if in_reply_to:
            in_reply_to = in_reply_to.strip('<>')
            headers['in_reply_to'] = in_reply_to

        refs = parse_references(get_header('References'))
        if refs:
            headers['references'] = refs

        # Gmail thread ID
        thread_id = get_header('X-GM-THRID')
        if thread_id:
            headers['thread_id'] = thread_id

        # Mailing list
        list_id = get_header('List-ID')
        if list_id:
            # Clean up list ID format
            match = re.search(r'<([^>]+)>', list_id)
            headers['list_id'] = match.group(1) if match else list_id.strip()

        # Extract tags from Gmail labels
        tags = []
        labels_str = get_header('X-Gmail-Labels')
        if labels_str:
            for label in labels_str.split(','):
                label = label.strip()
                if label:
                    tags.append(label)

        # Build final message object
        result = {
            'headers': headers,
            'tags': tags,
            'content': cleaned_blocks,
        }

        # Add attachments if any
        if attachments:
            result['attachments'] = attachments

        return result

    except Exception as e:
        stats.error_emails += 1
        return None


# =============================================================================
# MBOX STREAM PARSER
# =============================================================================

class MboxStreamParser:
    """
    Stream-based mbox parser for memory-efficient processing of large files.
    """

    FROM_LINE_PATTERN = re.compile(
        rb'^From \S+ (Mon|Tue|Wed|Thu|Fri|Sat|Sun) '
        rb'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
        rb'\s*(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{4}|\w+) (\d{4})$'
    )

    def __init__(self, filepath: str, start_position: int = 0):
        self.filepath = filepath
        self.file = None
        self.start_position = start_position
        self.current_position = start_position

    def __enter__(self):
        self.file = open(self.filepath, 'rb')
        if self.start_position > 0:
            self.file.seek(self.start_position)
            self._sync_to_email_boundary()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def _sync_to_email_boundary(self):
        """Find the next email boundary after seeking."""
        while True:
            line = self.file.readline()
            if not line:
                break
            if self._is_from_line(line):
                # Back up to include this line
                self.file.seek(-len(line), 1)
                self.current_position = self.file.tell()
                break
            self.current_position = self.file.tell()

    def _is_from_line(self, line: bytes) -> bool:
        """Check if a line is a valid mbox From line."""
        return line.startswith(b'From ') and self.FROM_LINE_PATTERN.match(line.rstrip())

    def __iter__(self):
        return self

    def __next__(self):
        """Return the next email as (position, raw_bytes)."""
        if not self.file:
            raise StopIteration

        email_lines = []
        email_start_pos = self.current_position

        while True:
            line = self.file.readline()
            if not line:
                # EOF
                if email_lines:
                    self.current_position = self.file.tell()
                    return email_start_pos, b''.join(email_lines)
                raise StopIteration

            self.current_position = self.file.tell()

            if self._is_from_line(line) and email_lines:
                # Start of next email - back up
                self.file.seek(-len(line), 1)
                self.current_position = self.file.tell()
                return email_start_pos, b''.join(email_lines)

            email_lines.append(line)


def parse_email_bytes(raw_bytes: bytes):
    """Parse raw email bytes into a message object.
    Returns tuple of (message, envelope_from) or (None, None) on error.
    """
    try:
        # Skip the From line for parsing
        lines = raw_bytes.split(b'\n', 1)
        if len(lines) > 1 and lines[0].startswith(b'From '):
            from_line = lines[0].decode('utf-8', errors='replace').rstrip()
            envelope_from = from_line[5:]  # Remove "From " prefix
            msg = email.message_from_bytes(lines[1], policy=policy.compat32)
            return msg, envelope_from
        else:
            msg = email.message_from_bytes(raw_bytes, policy=policy.compat32)
            return msg, None
    except Exception:
        return None, None


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_mbox(
    input_path: str,
    resume: bool = False,
    verbose: bool = False,
    progress_callback=None,
    show_progress: bool = True,
    index_writer=None,
):
    """Process a single mbox file with streaming and checkpointing."""
    signals.install_signal_handlers()

    # Determine output paths
    base_name = os.path.splitext(input_path)[0]
    clean_path = f"{base_name}.clean.jsonl"
    spam_path = f"{base_name}.spam.jsonl"
    summary_path = f"{input_path}.summary"

    # Initialize stats and checkpoint
    stats = CleaningStats()
    checkpoint = Checkpoint(input_path)
    start_position = 0

    # Check for resume
    if resume and checkpoint.exists():
        saved_data = checkpoint.load()
        if saved_data:
            start_position = checkpoint.get_position()
            stats.from_dict(checkpoint.get_stats())
            if show_progress:
                print(f"\n{Colors.CYAN}Resuming from checkpoint at position {start_position:,}{Colors.RESET}")
                print(f"  Previously processed: {stats.total_emails:,} emails")
                print()

    # Get file size
    file_size = os.path.getsize(input_path)
    stats.original_size = file_size

    # Print header
    if show_progress:
        print(f"\n{Colors.CYAN}{Colors.BOLD}{'─' * 50}{Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}  Email Cleaner for RAG (JSON Output){Colors.RESET}")
        print(f"{Colors.CYAN}{Colors.BOLD}{'─' * 50}{Colors.RESET}\n")
        print(f"Input:  {Colors.BOLD}{input_path}{Colors.RESET}")
        print(f"Size:   {format_bytes(file_size)}")
        print(f"\nOutput files:")
        print(f"  {Glyphs.CLEAN} {clean_path}")
        print(f"  {Glyphs.TRASH} {spam_path}")
        print(f"  📊 {summary_path}")
        print()

    # Initialize progress display
    display = ProgressDisplay("Email Cleaner")
    display.set_file_size(file_size)
    display.add_stat("Clean", Colors.GREEN)
    display.add_stat("Spam", Colors.YELLOW)
    display.add_stat("Errors", Colors.RED)

    # Open output files (append mode if resuming)
    open_mode = 'a' if (resume and start_position > 0) else 'w'
    clean_file = open(clean_path, open_mode, encoding='utf-8')
    spam_file = open(spam_path, open_mode, encoding='utf-8')

    last_checkpoint_time = datetime.now()
    last_render_time = datetime.now()
    last_progress_time = datetime.now()
    progress_step = 25

    try:
        with MboxStreamParser(input_path, start_position) as parser:
            for position, raw_bytes in parser:
                if signals.interrupted():
                    break

                msg, envelope_from = parse_email_bytes(raw_bytes)
                if not msg:
                    stats.error_emails += 1
                    display.increment_stat("Errors")
                    continue

                stats.total_emails += 1

                if index_writer is not None:
                    index_writer.write_from_message(
                        msg=msg,
                        mbox_file=os.path.basename(input_path),
                        offset=position,
                        length=len(raw_bytes),
                    )

                # Track labels
                labels = str(msg.get('X-Gmail-Labels', '') or '')
                for label in labels.split(','):
                    label = label.strip()
                    if label:
                        stats.labels_seen[label] += 1

                # Track senders
                from_addr = parseaddr(str(msg.get('From', '') or ''))[1]
                if from_addr:
                    stats.senders[from_addr] += 1

                # Check if spam
                is_spam_msg, spam_reason = is_spam(msg, stats)

                if is_spam_msg:
                    stats.spam_emails += 1
                    stats.spam_reasons[spam_reason] += 1
                    display.increment_stat("Spam")

                    # Write minimal spam record (ensure all values are strings)
                    spam_record = {
                        'from': str(msg.get('From', '') or ''),
                        'subject': decode_header_value(str(msg.get('Subject', '') or '')),
                        'date': str(msg.get('Date', '') or ''),
                        'reason': spam_reason,
                    }
                    spam_file.write(json.dumps(spam_record, ensure_ascii=False) + '\n')
                else:
                    json_email = email_to_json(msg, stats)
                    if json_email:
                        json_email["mbox"] = {
                            "file": os.path.basename(input_path),
                            "offset": position,
                            "length": len(raw_bytes),
                        }
                        clean_file.write(json.dumps(json_email, ensure_ascii=False) + '\n')
                        stats.clean_emails += 1
                        display.increment_stat("Clean")
                    else:
                        stats.error_emails += 1
                        display.increment_stat("Errors")

                stats.last_good_position = position

                # Update display
                now = datetime.now()
                if progress_callback:
                    if (
                        (now - last_progress_time).total_seconds() >= 0.1
                        or stats.total_emails % progress_step == 0
                    ):
                        progress_callback(
                            {
                                "processed": stats.total_emails,
                                "clean": stats.clean_emails,
                                "spam": stats.spam_emails,
                                "errors": stats.error_emails,
                                "skipped": stats.spam_emails + stats.error_emails,
                                "position": parser.current_position,
                                "file_size": file_size,
                            }
                        )
                        last_progress_time = now

                if (now - last_render_time).total_seconds() >= 0.25:
                    subject = decode_header_value(str(msg.get('Subject', '') or ''))
                    display.update(parser.current_position, subject, stats.total_emails)
                    if show_progress:
                        display.render()
                    last_render_time = now

                    # Flush files periodically
                    clean_file.flush()
                    spam_file.flush()

                # Save checkpoint periodically (every 30 seconds)
                if (now - last_checkpoint_time).total_seconds() >= 30:
                    checkpoint.save(stats.last_good_position, stats.to_dict())
                    last_checkpoint_time = now

    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        if show_progress:
            print(f"\n{Colors.RED}Error: {e}{Colors.RESET}")
        else:
            print(f"ERROR: {e}")
        checkpoint.save(stats.last_good_position, stats.to_dict())

    finally:
        clean_file.close()
        spam_file.close()

    # Get output sizes
    stats.clean_size = os.path.getsize(clean_path) if os.path.exists(clean_path) else 0
    stats.spam_size = os.path.getsize(spam_path) if os.path.exists(spam_path) else 0

    # Ensure final summary reflects total processed even for fast runs.
    display.processed_items = stats.total_emails
    display.current_position = stats.last_good_position

    # Final display
    if signals.interrupted():
        checkpoint.save(stats.last_good_position, stats.to_dict())
        if show_progress:
            display.finalize(success=False, message="Interrupted - checkpoint saved")
            print(f"\n{Colors.YELLOW}To resume, run with --resume flag{Colors.RESET}")
    else:
        checkpoint.remove()
        if show_progress:
            display.finalize(success=True)
    if progress_callback:
        progress_callback(
            {
                "processed": stats.total_emails,
                "clean": stats.clean_emails,
                "spam": stats.spam_emails,
                "errors": stats.error_emails,
                "skipped": stats.spam_emails + stats.error_emails,
                "position": stats.last_good_position,
                "file_size": file_size,
            }
        )

    # Print detailed summary
    if show_progress:
        print_detailed_summary(stats, clean_path, spam_path)

    # Write summary file
    write_summary_file(summary_path, stats, input_path, clean_path, spam_path)

    if show_progress:
        print(f"\n{Colors.GREEN}Summary written to: {summary_path}{Colors.RESET}")

    return stats


def print_detailed_summary(stats: CleaningStats, clean_path: str, spam_path: str):
    """Print detailed summary to terminal."""
    print(f"\n{Colors.BOLD}Size Reduction:{Colors.RESET}")
    print(f"  Original: {format_bytes(stats.original_size)}")
    print(f"  Clean:    {format_bytes(stats.clean_size)}")
    reduction = (1 - stats.clean_size / stats.original_size) * 100 if stats.original_size > 0 else 0
    print(f"  Reduction: {Colors.GREEN}{reduction:.1f}%{Colors.RESET}")

    print(f"\n{Colors.BOLD}Processing:{Colors.RESET}")
    print(f"  HTML converted:     {stats.html_only_converted:,}")
    print(f"  Signatures removed: {stats.signatures_removed:,}")
    print(f"  Attachments removed: {stats.attachments_removed:,} ({format_bytes(stats.attachments_size)})")

    if stats.spam_reasons:
        print(f"\n{Colors.BOLD}Spam/Filter Breakdown:{Colors.RESET}")
        for reason, count in sorted(stats.spam_reasons.items(), key=lambda x: -x[1])[:10]:
            print(f"  {reason}: {count:,}")


def write_summary_file(path: str, stats: CleaningStats, input_path: str, clean_path: str, spam_path: str):
    """Write detailed summary to file."""
    elapsed = (datetime.now() - stats.start_time).total_seconds()
    reduction = (1 - stats.clean_size / stats.original_size) * 100 if stats.original_size > 0 else 0

    with open(path, 'w', encoding='utf-8') as f:
        f.write("Email Cleanup Summary\n")
        f.write("=" * 50 + "\n\n")

        f.write(f"Source: {input_path}\n")
        f.write(f"Processed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Processing time: {elapsed:.1f} seconds\n\n")

        f.write("Statistics\n")
        f.write("-" * 30 + "\n")
        f.write(f"Total emails processed: {stats.total_emails:,}\n")
        f.write(f"Clean emails written: {stats.clean_emails:,}\n")
        f.write(f"Spam/filtered: {stats.spam_emails:,}\n")
        f.write(f"Errors: {stats.error_emails:,}\n\n")

        if stats.spam_reasons:
            f.write("Spam/Filter Breakdown:\n")
            for reason, count in sorted(stats.spam_reasons.items(), key=lambda x: -x[1]):
                f.write(f"  - {reason}: {count:,}\n")
            f.write("\n")

        f.write("Content Processing\n")
        f.write("-" * 30 + "\n")
        f.write(f"HTML-only emails converted: {stats.html_only_converted:,}\n")
        f.write(f"Signatures removed: {stats.signatures_removed:,}\n\n")

        f.write("Attachments\n")
        f.write("-" * 30 + "\n")
        f.write(f"Total attachments removed: {stats.attachments_removed:,}\n")
        f.write(f"Total attachment size: {format_bytes(stats.attachments_size)}\n")
        if stats.attachment_types:
            f.write("Attachment types:\n")
            for atype, count in sorted(stats.attachment_types.items(), key=lambda x: -x[1])[:20]:
                f.write(f"  - {atype}: {count:,}\n")
        f.write("\n")

        f.write("Size Analysis\n")
        f.write("-" * 30 + "\n")
        f.write(f"Original size: {format_bytes(stats.original_size)}\n")
        f.write(f"Clean file size: {format_bytes(stats.clean_size)}\n")
        f.write(f"Spam file size: {format_bytes(stats.spam_size)}\n")
        f.write(f"Size reduction: {reduction:.1f}%\n\n")

        f.write("Output Files\n")
        f.write("-" * 30 + "\n")
        f.write(f"Clean: {clean_path}\n")
        f.write(f"Spam: {spam_path}\n\n")

        if stats.labels_seen:
            f.write("Top Gmail Labels\n")
            f.write("-" * 30 + "\n")
            for label, count in sorted(stats.labels_seen.items(), key=lambda x: -x[1])[:30]:
                f.write(f"  {label}: {count:,}\n")
            f.write("\n")

        if stats.senders:
            f.write("Top Senders\n")
            f.write("-" * 30 + "\n")
            for sender, count in sorted(stats.senders.items(), key=lambda x: -x[1])[:30]:
                f.write(f"  {sender}: {count:,}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Clean Gmail MBOX files for RAG systems. Outputs JSON Lines format.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s gmail-2004.mbox
  %(prog)s private/gmail-2015.mbox --verbose
  %(prog)s gmail-2015.mbox --resume    # Resume from checkpoint

Output files:
  <name>.clean.jsonl   - Cleaned emails (JSON Lines)
  <name>.spam.jsonl    - Filtered spam/newsletters
  <name>.mbox.summary  - Processing statistics
        """
    )

    parser.add_argument('input', help='Input MBOX file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show detailed progress and errors')
    parser.add_argument('-r', '--resume', action='store_true', help='Resume from checkpoint if available')

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"{Colors.RED}Error: File not found: {args.input}{Colors.RESET}")
        sys.exit(1)

    process_mbox(args.input, resume=args.resume, verbose=args.verbose)


if __name__ == '__main__':
    main()

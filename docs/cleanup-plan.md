# Email Cleanup Plan for RAG

This document describes the strategy and implementation plan for cleaning Gmail MBOX files for use in a RAG (Retrieval-Augmented Generation) vector database.

## Data Analysis Summary

### Dataset Overview
- **Source**: Google Takeout Gmail exports
- **Time Range**: 2004-2026 (22 years)
- **Total Size**: ~16GB across 23 mbox files
- **File Sizes**: 39MB (2004) to 2.1GB (2015)

### Email Structure (Google Takeout MBOX Format)

#### Gmail-Specific Headers (IMPORTANT - Keep for RAG)
| Header | Purpose | RAG Value |
|--------|---------|-----------|
| `X-GM-THRID` | Gmail thread ID | **Critical** - Links conversations together |
| `X-Gmail-Labels` | Gmail labels/categories | **High** - Categorization, filtering |

#### Standard Headers to Keep
| Header | Purpose | RAG Value |
|--------|---------|-----------|
| `From` | Sender | **Critical** - Who sent the message |
| `To` | Recipients | **High** - Relationships |
| `Cc` | CC recipients | **Medium** - Relationships |
| `Date` | Timestamp | **Critical** - Timeline |
| `Subject` | Email subject | **Critical** - Topic identification |
| `Message-ID` | Unique identifier | **High** - Deduplication |
| `In-Reply-To` | Reply chain | **High** - Threading |
| `References` | Thread references | **High** - Threading |
| `List-ID` | Mailing list identifier | **Medium** - Source classification |

#### Headers to REMOVE (Noise)
- **Authentication/Routing** (100+ lines per email): `DKIM-Signature`, `ARC-*`, `Authentication-Results`, `Received-SPF`, `Received`, `Return-Path`, `X-Google-*`, `X-Received`
- **Mail Service Headers**: `X-Pobox-*`, `X-ME-*`, `X-SG-*`, `X-MC-*` (marketing tools)
- **Client/Technical**: `X-Mailer`, `X-Priority`, `X-MimeOLE`, `X-MS-*`, `MIME-Version`
- **Delivery**: `Delivered-To`, `X-Spam-*`

### Gmail Label Patterns Observed
```
Category Forums     - GitHub, mailing lists, etc.
Category Updates    - Service notifications
Category Personal   - Personal correspondence
Category Social     - Social network notifications
Category Promotions - Marketing emails
Category Purchases  - Receipts, shipping
Category Travel     - Travel bookings
Sent               - Sent mail
Inbox              - Inbox items
Archived           - Archived items
Important          - Gmail-marked important
Opened             - Read messages
Custom labels      - User labels (.me, .camera, etc.)
```

### Content Types Distribution
```
multipart/alternative - 60% (text + html versions)
text/plain           - 25% (plain text only)
text/html            - 10% (HTML only - need extraction)
multipart/mixed      - 5% (attachments)
```

### Top Email Sources (2015 sample)
1. `0xfe@vexflow.com` - ~10k (mailing list)
2. `mohit@muthanna.com` - 821 (sent)
3. `discard-report@pobox.com` - 358 (spam reports)
4. `*@chromium.org` - ~2k (mailing list)
5. `notifications@github.com` - 222 (automated)
6. Service emails (Amazon, Mint, FreshDirect, Google Calendar, LinkedIn)

---

## Spam/Noise Detection Strategy

### Automatic Spam Detection Rules

1. **Label-Based**:
   - Contains "Spam" in X-Gmail-Labels
   - `discard-report@pobox.com` sender (spam reports)

2. **Header-Based**:
   - `Precedence: bulk` (mass mailings)
   - Known marketing X-Mailer values: CheetahMailer, Mailchimp, Sailthru, Constant Contact

3. **Sender-Based Newsletters** (move to spam file - low RAG value):
   - `noreply@` addresses
   - `no-reply@` addresses
   - Known newsletter senders (configurable)

4. **Content-Based**:
   - Empty body after cleanup
   - Very short body (< 50 chars) with no subject

### Categories to Consider Filtering
Based on Gmail labels, consider moving to spam file:
- `Category Promotions` - Marketing emails
- `Category Social` - Social network notifications (LinkedIn, etc.)
- Automated notifications (dependabot, CI/CD, etc.)

---

## Content Cleaning Strategy

### Attachment Handling
**Strategy**: Remove binary content, preserve metadata

For each attachment found:
1. Extract metadata: filename, content-type, size
2. Create new header: `X-Cleaned-Attachment: filename=example.pdf; type=application/pdf; size=188796`
3. Remove base64-encoded content

### HTML Processing
**Strategy**: Extract plain text, preserve structure

For HTML-only emails:
1. Use BeautifulSoup to parse HTML
2. Extract text content preserving paragraph structure
3. Remove: scripts, styles, tracking pixels
4. Keep: links (converted to markdown-style), basic formatting

For multipart/alternative:
1. Prefer text/plain part
2. Fall back to HTML extraction if no text part

### Signature Removal
**Strategy**: Detect and remove email signatures

Detection patterns:
1. RFC 5322 standard: `-- \n` (dash-dash-space-newline)
2. Common variants: `--\n`, `—\n`
3. Phrase-based: Lines starting with "Best regards", "Cheers", "Thanks", "Sent from", etc.
4. Position-based: Content after signature marker at end of email

### Quote Removal (Optional)
**Strategy**: Keep full content but mark quoted text

- Don't remove quotes entirely (they provide context)
- Consider marking them for potential RAG filtering
- Patterns: `> `, `On ... wrote:`

---

## Output Format

### Clean Email Structure
```
From <id>@xxx <timestamp>
X-GM-THRID: <thread-id>
X-Gmail-Labels: <labels>
From: <sender>
To: <recipients>
Cc: <cc-recipients>
Date: <date>
Subject: <subject>
Message-ID: <message-id>
In-Reply-To: <reply-to>
References: <references>
List-ID: <list-id>
X-Cleaned-Attachment: <attachment-meta>
Content-Type: text/plain; charset=utf-8

<cleaned body text>

```

### Summary File Format (name.mbox.summary)
```
Email Cleanup Summary
=====================
Source: gmail-2015.mbox
Processed: 2024-02-03 10:30:00

Statistics
----------
Total emails processed: 15,432
Clean emails written: 12,891
Spam/filtered: 2,541

By filter type:
  - Label-based spam: 1,234
  - Newsletter/bulk: 892
  - Empty content: 415

Attachments
-----------
Total attachments removed: 3,421
Attachment types:
  - application/pdf: 456
  - image/png: 1,234
  - image/jpeg: 892

Size reduction: 1.8GB -> 423MB (76% reduction)

Custom Headers Added
--------------------
X-Cleaned-Attachment - Metadata for removed attachments
  Format: filename=<name>; type=<mime>; size=<bytes>
```

---

## Implementation Plan

### Phase 1: Core Infrastructure
1. Set up uv virtual environment with dependencies
2. Create base email parser using Python `mailbox` module
3. Implement header filtering logic
4. Build content extraction (plain text + HTML)

### Phase 2: Cleaning Pipeline
1. Implement attachment removal with metadata preservation
2. Build HTML-to-text extraction (BeautifulSoup)
3. Implement signature detection and removal
4. Build spam/newsletter detection

### Phase 3: Output Generation
1. Create clean mbox writer
2. Implement spam mbox writer
3. Build summary generator with statistics

### Phase 4: Testing & Optimization
1. Test on small mbox files (2004, 2026)
2. Validate output format
3. Optimize for large files (streaming)
4. Add progress reporting

---

## Dependencies

```
# Core
mailbox (stdlib)
email (stdlib)

# HTML Processing
beautifulsoup4
lxml (html parser)

# Text Processing
chardet (encoding detection)

# CLI
argparse (stdlib)
```

---

## Command Line Interface

```bash
# Basic usage (pipeline)
ragmail pipeline private/gmail-2004.mbox --workspace gmail-2004

# Output files (workspace):
#   workspaces/gmail-2004/clean/YYYY-MM.clean.jsonl
#   workspaces/gmail-2004/spam/YYYY-MM.spam.jsonl
#   workspaces/gmail-2004/reports/YYYY-MM.mbox.summary

# Options (future):
#   --keep-promotions    Keep Category Promotions emails
#   --keep-social        Keep Category Social emails
#   --no-signature-strip Don't remove signatures
#   --verbose            Show detailed progress
```

---

## Risk Mitigation

1. **Encoding Issues**: Use `chardet` for detection, fall back gracefully
2. **Malformed Emails**: Wrap parsing in try/catch, log errors, continue
3. **Large Files**: Stream processing, don't load entire file in memory
4. **Data Loss**: Never modify original files, only create new ones
5. **Progress Monitoring**: Display progress for large files (split stage / MboxSplitter)

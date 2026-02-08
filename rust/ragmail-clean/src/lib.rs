//! Email cleaning stage implementation (Rust migration).

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::DateTime;
use ragmail_index::record_from_message;
use ragmail_mbox::{MboxError, MboxMessageStream};
use serde::Serialize;
use thiserror::Error;

/// Run-time options for cleaning.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CleanOptions {
    pub start_offset: u64,
    pub append: bool,
    pub mbox_file_name: Option<String>,
    pub summary_output: Option<PathBuf>,
    pub index_output: Option<PathBuf>,
}

/// Stage summary statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CleanStats {
    pub processed: u64,
    pub clean: u64,
    pub spam: u64,
    pub errors: u64,
    pub spam_reasons: BTreeMap<String, u64>,
}

/// Clean stage error type.
#[derive(Debug, Error)]
pub enum CleanError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Mbox(#[from] MboxError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Address {
    name: String,
    email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    kind: String,
    body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct AttachmentMeta {
    filename: String,
    content_type: String,
    size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MboxRef {
    file: String,
    offset: u64,
    length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
struct CleanHeaders {
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<Address>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    to: Vec<Address>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    cc: Vec<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    references: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    list_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CleanEmailRecord {
    headers: CleanHeaders,
    tags: Vec<String>,
    content: Vec<ContentBlock>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<AttachmentMeta>,
    mbox: MboxRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SpamRecord {
    from: String,
    subject: String,
    date: String,
    reason: String,
}

enum CleanOutcome {
    Clean(Box<CleanEmailRecord>),
    Spam(SpamRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedMimePart {
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

const SPAM_SENDERS: [&str; 3] = ["discard-report@pobox.com", "mailer-daemon@", "postmaster@"];
const NEWSLETTER_MAILERS: [&str; 11] = [
    "cheetahmailer",
    "mailchimp",
    "sailthru",
    "constant contact",
    "sendgrid",
    "exacttarget",
    "marketo",
    "hubspot",
    "campaign monitor",
    "xyzmailer",
    "wiredmessenger",
];
const SIGNATURE_PREFIXES: [&str; 11] = [
    "best regards",
    "kind regards",
    "warm regards",
    "regards,",
    "cheers,",
    "thanks,",
    "thank you,",
    "sincerely,",
    "sent from my iphone",
    "sent from my ipad",
    "sent from my android",
];

/// Cleans an mbox file into clean + spam JSONL outputs.
pub fn clean_mbox_file(
    input_path: &Path,
    output_clean: &Path,
    output_spam: &Path,
    options: &CleanOptions,
) -> Result<CleanStats, CleanError> {
    clean_mbox_file_with_progress(
        input_path,
        output_clean,
        output_spam,
        options,
        Duration::from_millis(250),
        None,
    )
}

/// Cleans an mbox file into clean + spam JSONL outputs and emits periodic progress callbacks.
pub fn clean_mbox_file_with_progress(
    input_path: &Path,
    output_clean: &Path,
    output_spam: &Path,
    options: &CleanOptions,
    progress_every: Duration,
    mut progress_callback: Option<&mut dyn FnMut(&CleanStats)>,
) -> Result<CleanStats, CleanError> {
    if let Some(parent) = output_clean.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = output_spam.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let clean_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(options.append)
        .truncate(!options.append)
        .open(output_clean)?;
    let spam_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(options.append)
        .truncate(!options.append)
        .open(output_spam)?;
    let mut clean_writer = BufWriter::new(clean_file);
    let mut spam_writer = BufWriter::new(spam_file);
    let mut index_writer = if let Some(index_output) = options.index_output.as_ref() {
        if let Some(parent) = index_output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(options.append)
            .truncate(!options.append)
            .open(index_output)?;
        Some(BufWriter::new(index_file))
    } else {
        None
    };

    let mbox_file_name = options
        .mbox_file_name
        .clone()
        .unwrap_or_else(|| file_name_or_path(input_path));
    let mut stream = MboxMessageStream::from_path(input_path, options.start_offset)?;
    let mut stats = CleanStats::default();
    let mut last_progress = std::time::Instant::now();

    loop {
        let Some(message) = stream.next_message()? else {
            break;
        };
        stats.processed += 1;
        if let Some(writer) = index_writer.as_mut() {
            let record = record_from_message(&message.raw, &mbox_file_name, message.offset);
            serde_json::to_writer(&mut *writer, &record)?;
            writer.write_all(b"\n")?;
        }
        match clean_message(
            &message.raw,
            &mbox_file_name,
            message.offset,
            message.raw.len() as u64,
        ) {
            Ok(CleanOutcome::Clean(record)) => {
                serde_json::to_writer(&mut clean_writer, &record)?;
                clean_writer.write_all(b"\n")?;
                stats.clean += 1;
            }
            Ok(CleanOutcome::Spam(record)) => {
                let reason = record.reason.clone();
                serde_json::to_writer(&mut spam_writer, &record)?;
                spam_writer.write_all(b"\n")?;
                stats.spam += 1;
                *stats.spam_reasons.entry(reason).or_insert(0) += 1;
            }
            Err(_) => {
                stats.errors += 1;
            }
        }
        emit_clean_progress(
            &mut progress_callback,
            &mut last_progress,
            progress_every,
            &stats,
        );
    }

    clean_writer.flush()?;
    spam_writer.flush()?;
    if let Some(writer) = index_writer.as_mut() {
        writer.flush()?;
    }
    let summary_path = options
        .summary_output
        .clone()
        .unwrap_or_else(|| default_summary_output(input_path));
    write_summary_file(&summary_path, input_path, output_clean, output_spam, &stats)?;
    if let Some(callback) = progress_callback.as_mut() {
        callback(&stats);
    }
    Ok(stats)
}

fn emit_clean_progress(
    progress_callback: &mut Option<&mut dyn FnMut(&CleanStats)>,
    last_progress: &mut std::time::Instant,
    progress_every: Duration,
    stats: &CleanStats,
) {
    if let Some(callback) = progress_callback.as_mut() {
        let due = stats.processed == 1
            || progress_every.is_zero()
            || last_progress.elapsed() >= progress_every;
        if due {
            callback(stats);
            *last_progress = std::time::Instant::now();
        }
    }
}

fn clean_message(
    raw: &[u8],
    mbox_file_name: &str,
    offset: u64,
    length: u64,
) -> Result<CleanOutcome, CleanError> {
    let headers = parse_headers(raw);
    if let Some(reason) = spam_reason(&headers) {
        return Ok(CleanOutcome::Spam(SpamRecord {
            from: decode_header_value(headers.get("from").map(String::as_str).unwrap_or_default()),
            subject: headers
                .get("subject")
                .map(|value| decode_header_value(value))
                .unwrap_or_default(),
            date: headers
                .get("date")
                .map_or_else(String::new, |value| normalize_date_value(value)),
            reason,
        }));
    }

    let (body, attachments) = extract_content_and_attachments(raw, &headers);
    let body = clean_text(&body);
    let (body, _) = remove_signature(&body);
    let content = if body.is_empty() {
        vec![ContentBlock {
            kind: "text".to_string(),
            body: String::new(),
        }]
    } else {
        vec![ContentBlock {
            kind: "text".to_string(),
            body,
        }]
    };

    let tags = split_csv(headers.get("x-gmail-labels").map(String::as_str));
    let headers_out = build_clean_headers(&headers);

    Ok(CleanOutcome::Clean(
        CleanEmailRecord {
            headers: headers_out,
            tags,
            content,
            attachments,
            mbox: MboxRef {
                file: mbox_file_name.to_string(),
                offset,
                length,
            },
        }
        .into(),
    ))
}

fn build_clean_headers(headers: &BTreeMap<String, String>) -> CleanHeaders {
    CleanHeaders {
        from: parse_single_address(headers.get("from").map(String::as_str)),
        to: parse_address_list(headers.get("to").map(String::as_str)),
        cc: parse_address_list(headers.get("cc").map(String::as_str)),
        subject: headers
            .get("subject")
            .map(|value| decode_header_value(value))
            .filter(|value| !value.trim().is_empty()),
        date: headers
            .get("date")
            .map(|value| normalize_date_value(value))
            .filter(|value| !value.trim().is_empty()),
        message_id: headers
            .get("message-id")
            .and_then(|value| normalize_message_id(value)),
        in_reply_to: headers
            .get("in-reply-to")
            .and_then(|value| normalize_message_id(value)),
        references: headers
            .get("references")
            .map_or_else(Vec::new, |value| parse_references(value)),
        thread_id: as_non_empty(headers.get("x-gm-thrid")),
        list_id: headers
            .get("list-id")
            .map(|value| normalize_list_id(value))
            .filter(|value| !value.is_empty()),
    }
}

fn as_non_empty(value: Option<&String>) -> Option<String> {
    value
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
}

fn split_csv(value: Option<&str>) -> Vec<String> {
    value
        .map(|entry| {
            entry
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_references(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .filter_map(normalize_message_id)
        .collect::<Vec<_>>()
}

fn decode_header_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut remainder = value;
    loop {
        let Some(start) = remainder.find("=?") else {
            output.push_str(remainder);
            break;
        };
        output.push_str(&remainder[..start]);
        let candidate = &remainder[start..];
        if let Some((decoded, consumed)) = decode_encoded_word(candidate) {
            output.push_str(&decoded);
            remainder = &candidate[consumed..];
        } else {
            output.push_str("=?");
            remainder = &candidate[2..];
        }
    }
    output.trim().to_string()
}

fn decode_encoded_word(value: &str) -> Option<(String, usize)> {
    if !value.starts_with("=?") {
        return None;
    }
    let end = value.find("?=")?;
    let end = end + 2;
    let word = &value[..end];
    let payload = &word[2..word.len() - 2];
    let mut parts = payload.splitn(3, '?');
    let charset = parts.next()?.trim();
    let encoding = parts.next()?.trim();
    let data = parts.next()?;
    if charset.is_empty() || encoding.is_empty() {
        return None;
    }

    let decoded_bytes = if encoding.eq_ignore_ascii_case("b") {
        decode_base64(data.as_bytes())
    } else if encoding.eq_ignore_ascii_case("q") {
        decode_rfc2047_q(data.as_bytes())
    } else {
        return None;
    };
    let decoded = decode_with_charset(&decoded_bytes, charset);
    Some((decoded, end))
}

fn decode_rfc2047_q(input: &[u8]) -> Vec<u8> {
    let mut output: Vec<u8> = Vec::with_capacity(input.len());
    let mut i = 0_usize;
    while i < input.len() {
        if input[i] == b'_' {
            output.push(b' ');
            i += 1;
            continue;
        }
        if input[i] == b'=' && i + 2 < input.len() {
            if let (Some(hi), Some(lo)) = (hex_value(input[i + 1]), hex_value(input[i + 2])) {
                output.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        output.push(input[i]);
        i += 1;
    }
    output
}

fn decode_with_charset(bytes: &[u8], charset: &str) -> String {
    let charset = charset.trim().to_ascii_lowercase();
    if charset == "iso-8859-1" || charset == "latin1" || charset == "latin-1" {
        return bytes.iter().map(|byte| *byte as char).collect::<String>();
    }
    String::from_utf8_lossy(bytes).to_string()
}

fn normalize_date_value(value: &str) -> String {
    let decoded = decode_header_value(value);
    let trimmed = decoded.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(parsed) = DateTime::parse_from_rfc2822(trimmed) {
        return parsed.to_rfc3339();
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return parsed.to_rfc3339();
    }
    trimmed.to_string()
}

fn clean_text(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let without_invisible = normalized
        .chars()
        .filter(|ch| {
            !matches!(
                *ch,
                '\u{200C}' | '\u{200D}' | '\u{034F}' | '\u{00AD}' | '\u{200B}' | '\u{FEFF}'
            )
        })
        .collect::<String>();

    let mut lines: Vec<String> = vec![];
    for line in without_invisible.lines() {
        let trimmed = line.trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower.contains("unsubscribe") {
            continue;
        }
        if lower.contains("browser")
            && (lower.contains("view this email")
                || lower.contains("view in browser")
                || lower.contains("view this message"))
        {
            continue;
        }
        if lower.contains("trouble")
            && (lower.contains("viewing")
                || lower.contains("displaying")
                || lower.contains("reading"))
        {
            continue;
        }
        lines.push(trimmed.to_string());
    }
    collapse_blank_lines(&lines.join("\n"), 2)
}

fn remove_signature(value: &str) -> (String, bool) {
    if value.trim().is_empty() {
        return (String::new(), false);
    }
    let lines: Vec<&str> = value.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        let stripped = line.trim();
        if is_signature_marker(stripped) {
            let cleaned = lines[..idx].join("\n").trim().to_string();
            if !cleaned.is_empty() {
                return (cleaned, true);
            }
        }
        if idx * 10 > lines.len() * 8 {
            let lower = stripped.to_ascii_lowercase();
            if SIGNATURE_PREFIXES
                .iter()
                .any(|prefix| lower.starts_with(prefix))
            {
                let cleaned = lines[..idx].join("\n").trim().to_string();
                if !cleaned.is_empty() {
                    return (cleaned, true);
                }
            }
        }
    }
    (value.trim().to_string(), false)
}

fn is_signature_marker(value: &str) -> bool {
    if value == "--" || value == "-- " || value == "—" {
        return true;
    }
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() >= 3 && chars.iter().all(|ch| *ch == '_' || *ch == '-') {
        return true;
    }
    false
}

fn collapse_blank_lines(value: &str, max_blank_run: u8) -> String {
    let mut out = String::with_capacity(value.len());
    let mut blank_run: u16 = 0;
    let max_blank_run = u16::from(max_blank_run);
    for line in value.lines() {
        if line.trim().is_empty() {
            blank_run = blank_run.saturating_add(1);
            if blank_run > max_blank_run {
                continue;
            }
            out.push('\n');
            continue;
        }
        blank_run = 0;
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.trim().to_string()
}

fn normalize_list_id(value: &str) -> String {
    let decoded = decode_header_value(value);
    let trimmed = decoded.trim();
    if let (Some(start), Some(end)) = (trimmed.find('<'), trimmed.find('>')) {
        if end > start + 1 {
            return trimmed[start + 1..end].to_string();
        }
    }
    trimmed.to_string()
}

fn normalize_message_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let trimmed = trimmed
        .strip_prefix('<')
        .unwrap_or(trimmed)
        .strip_suffix('>')
        .unwrap_or(trimmed)
        .trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_single_address(value: Option<&str>) -> Option<Address> {
    let value = decode_header_value(value?).trim().to_string();
    let value = value.as_str();
    if value.is_empty() {
        return None;
    }
    if let (Some(start), Some(end)) = (value.find('<'), value.find('>')) {
        if end > start {
            let name = value[..start].trim().trim_matches('"').to_string();
            let email = value[start + 1..end].trim().to_ascii_lowercase();
            if !email.is_empty() {
                return Some(Address { name, email });
            }
        }
    }
    if value.contains('@') {
        return Some(Address {
            name: String::new(),
            email: value.trim_matches('"').to_ascii_lowercase(),
        });
    }
    None
}

fn parse_address_list(value: Option<&str>) -> Vec<Address> {
    let Some(entry) = value else {
        return vec![];
    };
    let decoded = decode_header_value(entry);
    decoded
        .split(',')
        .filter_map(|part| parse_single_address(Some(part)))
        .collect::<Vec<_>>()
}

fn spam_reason(headers: &BTreeMap<String, String>) -> Option<String> {
    let labels = headers
        .get("x-gmail-labels")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if labels.contains("\\spam") {
        return Some("label:spam".to_string());
    }
    if labels.contains("\\trash") {
        return Some("label:trash".to_string());
    }
    if labels.contains("category_promotions") {
        return Some("label:promotions".to_string());
    }

    let from_header = headers
        .get("from")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    for sender in SPAM_SENDERS {
        if from_header.contains(sender) {
            return Some(format!("sender:{sender}"));
        }
    }

    let mailer = headers
        .get("x-mailer")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    for newsletter_mailer in NEWSLETTER_MAILERS {
        if mailer.contains(newsletter_mailer) {
            return Some(format!("mailer:{newsletter_mailer}"));
        }
    }

    let precedence_bulk = headers
        .get("precedence")
        .map(|value| value.to_ascii_lowercase().contains("bulk"))
        .unwrap_or(false);
    let has_list_id = headers.contains_key("list-id");
    if precedence_bulk && !has_list_id {
        return Some("precedence:bulk".to_string());
    }
    None
}

fn extract_content_and_attachments(
    raw: &[u8],
    headers: &BTreeMap<String, String>,
) -> (String, Vec<AttachmentMeta>) {
    let body_start = header_body_split_offset(raw).unwrap_or(0);
    let body_bytes = &raw[body_start..];

    if let Some(boundary) = multipart_boundary(headers.get("content-type").map(String::as_str)) {
        return extract_from_multipart(body_bytes, &boundary);
    }

    let content_type = headers
        .get("content-type")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| "text/plain".to_string());
    let decoded = decode_transfer(body_bytes, headers.get("content-transfer-encoding"));
    let text = String::from_utf8_lossy(&decoded).to_string();
    if content_type.starts_with("text/html") {
        (normalize_body_text(&html_to_text(&text)), vec![])
    } else {
        (normalize_body_text(&text), vec![])
    }
}

fn extract_from_multipart(body: &[u8], boundary: &str) -> (String, Vec<AttachmentMeta>) {
    let mut text_parts: Vec<String> = vec![];
    let mut html_parts: Vec<String> = vec![];
    let mut attachments: Vec<AttachmentMeta> = vec![];

    for part in parse_multipart_parts(body, boundary) {
        let content_type = part
            .headers
            .get("content-type")
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_else(|| "text/plain".to_string());
        let content_type_only = mime_type_only(&content_type);
        let disposition = part
            .headers
            .get("content-disposition")
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_default();
        let filename = attachment_file_name(&part.headers);

        let is_attachment = (disposition.contains("attachment")
            || is_binary_content_type(&content_type_only))
            && (disposition.contains("attachment") || filename.is_some());
        if is_attachment {
            let decoded =
                decode_transfer(&part.body, part.headers.get("content-transfer-encoding"));
            attachments.push(AttachmentMeta {
                filename: filename.unwrap_or_else(|| "unnamed".to_string()),
                content_type: content_type_only,
                size: Some(decoded.len() as u64),
            });
            continue;
        }

        let decoded = decode_transfer(&part.body, part.headers.get("content-transfer-encoding"));
        let text = String::from_utf8_lossy(&decoded).to_string();
        if content_type_only.starts_with("text/plain") {
            let normalized = normalize_body_text(&text);
            if !normalized.is_empty() {
                text_parts.push(normalized);
            }
        } else if content_type_only.starts_with("text/html") {
            let normalized = normalize_body_text(&html_to_text(&text));
            if !normalized.is_empty() {
                html_parts.push(normalized);
            }
        }
    }

    if !text_parts.is_empty() {
        return (text_parts.join("\n\n"), attachments);
    }
    if !html_parts.is_empty() {
        return (html_parts.join("\n\n"), attachments);
    }
    (String::new(), attachments)
}

fn parse_multipart_parts(body: &[u8], boundary: &str) -> Vec<ParsedMimePart> {
    if boundary.is_empty() {
        return vec![];
    }
    let marker = format!("--{boundary}");
    let end_marker = format!("--{boundary}--");
    let normalized = String::from_utf8_lossy(body)
        .replace("\r\n", "\n")
        .replace('\r', "\n");

    let mut collecting = false;
    let mut current = String::new();
    let mut parts: Vec<ParsedMimePart> = vec![];

    for line in normalized.lines() {
        if line == marker || line == end_marker {
            if collecting && !current.trim().is_empty() {
                if let Some(part) = parse_mime_part(&current) {
                    parts.push(part);
                }
                current.clear();
            }
            if line == end_marker {
                break;
            }
            collecting = true;
            continue;
        }
        if collecting {
            current.push_str(line);
            current.push('\n');
        }
    }

    if collecting && !current.trim().is_empty() {
        if let Some(part) = parse_mime_part(&current) {
            parts.push(part);
        }
    }

    parts
}

fn parse_mime_part(raw: &str) -> Option<ParsedMimePart> {
    let bytes = raw.as_bytes();
    let body_start = header_body_split_offset(bytes).unwrap_or(bytes.len());
    let headers = parse_headers_no_envelope(&bytes[..body_start]);
    let body = bytes[body_start..].to_vec();
    Some(ParsedMimePart { headers, body })
}

fn parse_headers_no_envelope(raw: &[u8]) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    let text = String::from_utf8_lossy(raw);
    let mut current_key: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(key) = current_key.as_ref() {
                headers
                    .entry(key.clone())
                    .and_modify(|value: &mut String| {
                        value.push(' ');
                        value.push_str(line.trim());
                    })
                    .or_insert_with(|| line.trim().to_string());
            }
            continue;
        }
        parse_header_line(line, &mut headers, &mut current_key);
    }
    headers
}

fn multipart_boundary(content_type: Option<&str>) -> Option<String> {
    let value = content_type?;
    let lower = value.to_ascii_lowercase();
    if !lower.starts_with("multipart/") {
        return None;
    }
    header_param(value, "boundary")
}

fn header_param(value: &str, name: &str) -> Option<String> {
    for part in value.split(';').skip(1) {
        let Some((key, raw_value)) = part.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case(name) {
            let cleaned = raw_value
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            if !cleaned.is_empty() {
                return Some(cleaned);
            }
        }
    }
    None
}

fn attachment_file_name(headers: &BTreeMap<String, String>) -> Option<String> {
    if let Some(disposition) = headers.get("content-disposition") {
        if let Some(filename) = header_param(disposition, "filename") {
            return Some(decode_header_value(&filename));
        }
    }
    if let Some(content_type) = headers.get("content-type") {
        if let Some(name) = header_param(content_type, "name") {
            return Some(decode_header_value(&name));
        }
    }
    None
}

fn mime_type_only(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase()
}

fn is_binary_content_type(content_type: &str) -> bool {
    content_type.starts_with("image/")
        || content_type.starts_with("audio/")
        || content_type.starts_with("video/")
        || content_type.starts_with("application/")
}

fn decode_transfer(body: &[u8], encoding: Option<&String>) -> Vec<u8> {
    let encoding = encoding
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if encoding.contains("quoted-printable") {
        return decode_quoted_printable(body);
    }
    if encoding.contains("base64") {
        let decoded = decode_base64(body);
        if !decoded.is_empty() {
            return decoded;
        }
    }
    body.to_vec()
}

fn decode_quoted_printable(input: &[u8]) -> Vec<u8> {
    let mut output: Vec<u8> = Vec::with_capacity(input.len());
    let mut i = 0_usize;
    while i < input.len() {
        if input[i] == b'=' {
            if i + 1 < input.len() && input[i + 1] == b'\n' {
                i += 2;
                continue;
            }
            if i + 2 < input.len() && input[i + 1] == b'\r' && input[i + 2] == b'\n' {
                i += 3;
                continue;
            }
            if i + 2 < input.len() {
                if let (Some(hi), Some(lo)) = (hex_value(input[i + 1]), hex_value(input[i + 2])) {
                    output.push((hi << 4) | lo);
                    i += 3;
                    continue;
                }
            }
        }
        output.push(input[i]);
        i += 1;
    }
    output
}

fn decode_base64(input: &[u8]) -> Vec<u8> {
    let mut clean: Vec<u8> = vec![];
    for byte in input {
        if byte.is_ascii_whitespace() {
            continue;
        }
        clean.push(*byte);
    }

    let mut output: Vec<u8> = vec![];
    let mut chunk: [u8; 4] = [0; 4];
    let mut used = 0_usize;
    for byte in clean {
        let value = if byte == b'=' {
            Some(64_u8)
        } else {
            base64_value(byte)
        };
        let Some(value) = value else {
            continue;
        };
        chunk[used] = value;
        used += 1;
        if used == 4 {
            output.push((chunk[0] << 2) | (chunk[1] >> 4));
            if chunk[2] != 64 {
                output.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            if chunk[3] != 64 {
                output.push((chunk[2] << 6) | chunk[3]);
            }
            used = 0;
        }
    }
    output
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn html_to_text(input: &str) -> String {
    let without_script = strip_tag_block(input, "script");
    let without_style = strip_tag_block(&without_script, "style");
    let mut out = String::with_capacity(without_style.len());
    let mut in_tag = false;
    for ch in without_style.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                if in_tag {
                    in_tag = false;
                    out.push('\n');
                }
            }
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    decode_html_entities(&out)
}

fn strip_tag_block(input: &str, tag: &str) -> String {
    let mut output = input.to_string();
    let start_pat = format!("<{tag}");
    let end_pat = format!("</{tag}>");
    loop {
        let lowered = output.to_ascii_lowercase();
        let Some(start) = lowered.find(&start_pat) else {
            break;
        };
        let Some(end_rel) = lowered[start..].find(&end_pat) else {
            output.truncate(start);
            break;
        };
        let end = start + end_rel + end_pat.len();
        output.replace_range(start..end, "");
    }
    output
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn normalize_body_text(value: &str) -> String {
    let value = value.replace("\r\n", "\n").replace('\r', "\n");
    let mut out = String::with_capacity(value.len());
    let mut blank_run: u16 = 0;
    for line in value.lines() {
        let trimmed = line.trim_end();
        let normalized = if trimmed.starts_with(">From ") {
            &trimmed[1..]
        } else {
            trimmed
        };
        if normalized.is_empty() {
            blank_run = blank_run.saturating_add(1);
            if blank_run > 2 {
                continue;
            }
        } else {
            blank_run = 0;
        }
        out.push_str(normalized);
        out.push('\n');
    }
    out.trim().to_string()
}

fn header_body_split_offset(raw: &[u8]) -> Option<usize> {
    raw.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|idx| idx + 4)
        .or_else(|| {
            raw.windows(2)
                .position(|window| window == b"\n\n")
                .map(|idx| idx + 2)
        })
}

fn parse_headers(raw: &[u8]) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.lines();

    // Skip envelope line if present.
    if let Some(first) = lines.next() {
        if !first.starts_with("From ") {
            parse_header_line(first, &mut headers, &mut None::<String>);
        }
    }

    let mut current_key: Option<String> = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(key) = current_key.as_ref() {
                headers
                    .entry(key.clone())
                    .and_modify(|value| {
                        value.push(' ');
                        value.push_str(line.trim());
                    })
                    .or_insert_with(|| line.trim().to_string());
            }
            continue;
        }
        parse_header_line(line, &mut headers, &mut current_key);
    }

    headers
}

fn parse_header_line(
    line: &str,
    headers: &mut BTreeMap<String, String>,
    current_key: &mut Option<String>,
) {
    let Some((key, value)) = line.split_once(':') else {
        return;
    };
    let key = key.trim().to_ascii_lowercase();
    let value = value.trim().to_string();
    headers.insert(key.clone(), value);
    *current_key = Some(key);
}

fn file_name_or_path(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

/// Returns default output paths for `clean` and `spam` JSONL from an mbox path.
#[must_use]
pub fn default_clean_outputs(input_path: &Path) -> (PathBuf, PathBuf) {
    let base = input_path.with_extension("");
    let clean = PathBuf::from(format!("{}.clean.jsonl", base.display()));
    let spam = PathBuf::from(format!("{}.spam.jsonl", base.display()));
    (clean, spam)
}

/// Returns default summary output path (`<input>.summary`) for an mbox path.
#[must_use]
pub fn default_summary_output(input_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.summary", input_path.display()))
}

fn write_summary_file(
    summary_path: &Path,
    input_path: &Path,
    clean_path: &Path,
    spam_path: &Path,
    stats: &CleanStats,
) -> Result<(), CleanError> {
    if let Some(parent) = summary_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let original_size = std::fs::metadata(input_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let clean_size = std::fs::metadata(clean_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let spam_size = std::fs::metadata(spam_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let reduction = if original_size > 0 {
        (1.0_f64 - (clean_size as f64 / original_size as f64)) * 100.0
    } else {
        0.0
    };

    let mut output = String::new();
    output.push_str("Email Cleanup Summary\n");
    output.push_str("==================================================\n\n");
    output.push_str(&format!("Source: {}\n", input_path.display()));
    output.push_str("Statistics\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Total emails processed: {}\n", stats.processed));
    output.push_str(&format!("Clean emails written: {}\n", stats.clean));
    output.push_str(&format!("Spam/filtered: {}\n", stats.spam));
    output.push_str(&format!("Errors: {}\n\n", stats.errors));
    if !stats.spam_reasons.is_empty() {
        output.push_str("Spam/Filter Breakdown:\n");
        for (reason, count) in &stats.spam_reasons {
            output.push_str(&format!("  - {reason}: {count}\n"));
        }
        output.push('\n');
    }
    output.push_str("Size Analysis\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Original size: {original_size}\n"));
    output.push_str(&format!("Clean file size: {clean_size}\n"));
    output.push_str(&format!("Spam file size: {spam_size}\n"));
    output.push_str(&format!("Size reduction: {reduction:.1}%\n\n"));
    output.push_str("Output Files\n");
    output.push_str("------------------------------\n");
    output.push_str(&format!("Clean: {}\n", clean_path.display()));
    output.push_str(&format!("Spam: {}\n", spam_path.display()));
    std::fs::write(summary_path, output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn address_parsing_handles_name_and_email() {
        let parsed = parse_single_address(Some("\"Alpha\" <alpha@example.com>")).expect("address");
        assert_eq!(parsed.name, "Alpha");
        assert_eq!(parsed.email, "alpha@example.com");
    }

    #[test]
    fn spam_detection_uses_labels_and_bulk_precedence() {
        let mut headers = BTreeMap::new();
        headers.insert("x-gmail-labels".to_string(), "\\Spam,foo".to_string());
        assert_eq!(spam_reason(&headers).as_deref(), Some("label:spam"));

        headers.clear();
        headers.insert("precedence".to_string(), "bulk".to_string());
        assert_eq!(spam_reason(&headers).as_deref(), Some("precedence:bulk"));

        headers.insert("list-id".to_string(), "list.example.com".to_string());
        assert!(spam_reason(&headers).is_none());

        headers.clear();
        headers.insert(
            "from".to_string(),
            "Mailer-Daemon <mailer-daemon@example.com>".to_string(),
        );
        assert_eq!(
            spam_reason(&headers).as_deref(),
            Some("sender:mailer-daemon@")
        );

        headers.clear();
        headers.insert("x-mailer".to_string(), "MailChimp Delivery".to_string());
        assert_eq!(spam_reason(&headers).as_deref(), Some("mailer:mailchimp"));
    }

    #[test]
    fn clean_message_extracts_core_fields() {
        let raw = b"From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n\
Message-ID: <a@example.com>\n\
From: Alpha <alpha@example.com>\n\
To: Beta <beta@example.com>\n\
Cc: Gamma <gamma@example.com>\n\
Date: Mon, 1 Jan 2024 01:02:03 +0000\n\
	Subject: Hello\n\
	X-Gmail-Labels: Inbox,Work\n\
	\n\
	Hello world.\n\
	-- \n\
	Alpha\n";
        let outcome = clean_message(raw, "2024-01.mbox", 10, raw.len() as u64).expect("clean");
        let CleanOutcome::Clean(record) = outcome else {
            panic!("expected clean outcome");
        };
        assert_eq!(record.headers.subject.as_deref(), Some("Hello"));
        assert_eq!(record.headers.message_id.as_deref(), Some("a@example.com"));
        assert_eq!(record.tags, vec!["Inbox".to_string(), "Work".to_string()]);
        assert_eq!(record.content[0].kind, "text");
        assert_eq!(record.content[0].body, "Hello world.");
        assert_eq!(record.mbox.file, "2024-01.mbox");
        assert_eq!(record.mbox.offset, 10);
    }

    #[test]
    fn clean_message_multipart_prefers_plain_and_extracts_attachment() {
        let raw = b"From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n\
Message-ID: <a@example.com>\n\
From: Alpha <alpha@example.com>\n\
Date: Mon, 1 Jan 2024 01:02:03 +0000\n\
Subject: Multipart\n\
Content-Type: multipart/mixed; boundary=\"mix-1\"\n\
\n\
--mix-1\n\
Content-Type: text/plain; charset=utf-8\n\
\n\
Plain body.\n\
--mix-1\n\
Content-Type: text/html; charset=utf-8\n\
\n\
<html><body><b>HTML body.</b></body></html>\n\
--mix-1\n\
Content-Type: application/pdf; name=\"report.pdf\"\n\
Content-Disposition: attachment; filename=\"report.pdf\"\n\
\n\
JVBERi0xLjcK\n\
--mix-1--\n";
        let outcome = clean_message(raw, "2024-01.mbox", 0, raw.len() as u64).expect("clean");
        let CleanOutcome::Clean(record) = outcome else {
            panic!("expected clean outcome");
        };
        assert_eq!(record.content[0].body, "Plain body.");
        assert_eq!(record.attachments.len(), 1);
        assert_eq!(record.attachments[0].filename, "report.pdf");
        assert_eq!(record.attachments[0].content_type, "application/pdf");
    }

    #[test]
    fn clean_message_html_only_falls_back_to_text() {
        let raw = b"From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n\
Message-ID: <a@example.com>\n\
From: Alpha <alpha@example.com>\n\
Date: Mon, 1 Jan 2024 01:02:03 +0000\n\
Subject: HtmlOnly\n\
Content-Type: text/html; charset=utf-8\n\
\n\
<html><body><h1>Hello</h1><script>nope()</script><p>World</p></body></html>\n";
        let outcome = clean_message(raw, "2024-01.mbox", 0, raw.len() as u64).expect("clean");
        let CleanOutcome::Clean(record) = outcome else {
            panic!("expected clean outcome");
        };
        assert!(record.content[0].body.contains("Hello"));
        assert!(record.content[0].body.contains("World"));
        assert!(!record.content[0].body.contains("nope()"));
    }

    #[test]
    fn clean_mbox_file_writes_clean_and_spam_rows() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("duration")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("ragmail-clean-test-{unique}"));
        std::fs::create_dir_all(&root).expect("mkdir");
        let input = root.join("input.mbox");
        let clean = root.join("input.clean.jsonl");
        let spam = root.join("input.spam.jsonl");

        std::fs::write(
            &input,
            "From alpha@example.com Mon Jan  1 01:02:03 +0000 2024\n\
Message-ID: <a@example.com>\n\
From: Alpha <alpha@example.com>\n\
Date: Mon, 1 Jan 2024 01:02:03 +0000\n\
Subject: One\n\
\n\
Body one.\n\
\n\
From spam@example.com Tue Jan  2 01:02:03 +0000 2024\n\
Message-ID: <s@example.com>\n\
From: Spammer <spam@example.com>\n\
Date: Tue, 2 Jan 2024 01:02:03 +0000\n\
Subject: Buy now\n\
X-Gmail-Labels: \\Spam\n\
\n\
Body two.\n",
        )
        .expect("write input");

        let stats =
            clean_mbox_file(&input, &clean, &spam, &CleanOptions::default()).expect("clean run");
        assert_eq!(stats.processed, 2);
        assert_eq!(stats.clean, 1);
        assert_eq!(stats.spam, 1);
        assert_eq!(stats.errors, 0);

        let clean_text = std::fs::read_to_string(&clean).expect("read clean");
        let spam_text = std::fs::read_to_string(&spam).expect("read spam");
        assert_eq!(clean_text.lines().count(), 1);
        assert_eq!(spam_text.lines().count(), 1);
        let summary_text =
            std::fs::read_to_string(root.join("input.mbox.summary")).expect("read summary");
        assert!(summary_text.contains("Email Cleanup Summary"));
        assert!(summary_text.contains("Total emails processed: 2"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn decode_header_and_date_normalization_work() {
        let decoded = decode_header_value("=?UTF-8?B?SGVsbG8g4piD?=");
        assert_eq!(decoded, "Hello ☃");

        let parsed = normalize_date_value("Mon, 1 Jan 2024 01:02:03 +0000");
        assert_eq!(parsed, "2024-01-01T01:02:03+00:00");
    }
}

use std::collections::BTreeMap;

use crate::codec::{decode_base64, decode_quoted_printable};
use crate::header::{decode_header_value, header_body_split_offset, parse_headers_no_envelope};
use crate::text::{html_to_text, normalize_body_text};
use crate::types::{AttachmentMeta, ParsedMimePart};

pub(crate) fn extract_content_and_attachments(
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

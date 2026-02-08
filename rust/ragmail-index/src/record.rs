use sha2::{Digest, Sha256};

use crate::IndexRecord;

/// Creates an index row from a raw message blob.
#[must_use]
pub fn record_from_message(raw: &[u8], mbox_file_name: &str, offset: u64) -> IndexRecord {
    let headers = parse_headers(raw);
    let message_id = headers
        .get("message-id")
        .and_then(|value| normalize_message_id(value));
    let message_id_lower = message_id.as_ref().map(|value| value.to_lowercase());

    let email_id = generate_email_id(
        message_id.as_deref(),
        headers.get("from").map(String::as_str).unwrap_or(""),
        headers.get("date").map(String::as_str).unwrap_or(""),
        headers.get("subject").map(String::as_str).unwrap_or(""),
    );

    IndexRecord {
        email_id,
        message_id,
        message_id_lower,
        mbox_file: mbox_file_name.to_string(),
        offset,
        length: raw.len() as u64,
    }
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

fn parse_headers(raw: &[u8]) -> std::collections::BTreeMap<String, String> {
    let mut headers = std::collections::BTreeMap::new();
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.lines();

    // Skip envelope line if present.
    if let Some(first) = lines.next() {
        if !first.starts_with("From ") {
            // If no envelope line, treat first line as header candidate.
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
    headers: &mut std::collections::BTreeMap<String, String>,
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

fn generate_email_id(
    message_id: Option<&str>,
    from_header: &str,
    date: &str,
    subject: &str,
) -> String {
    let material = if let Some(message_id) = message_id {
        message_id.to_string()
    } else {
        let from_address = parse_from_address(from_header).to_ascii_lowercase();
        format!(
            "{from_address}|{date}|{}",
            subject.chars().take(100).collect::<String>()
        )
    };
    let digest = Sha256::digest(material.as_bytes());
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

fn parse_from_address(value: &str) -> String {
    if let Some(start) = value.find('<') {
        if let Some(end) = value[start + 1..].find('>') {
            let address = &value[start + 1..start + 1 + end];
            return address.trim().to_string();
        }
    }
    value
        .split_whitespace()
        .find(|part| part.contains('@'))
        .unwrap_or("")
        .trim_matches('"')
        .to_string()
}

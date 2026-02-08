use std::collections::BTreeMap;

use chrono::DateTime;

use crate::codec::{decode_base64, decode_rfc2047_q, decode_with_charset};
use crate::types::{Address, CleanHeaders, NEWSLETTER_MAILERS, SPAM_SENDERS};

pub(crate) fn build_clean_headers(headers: &BTreeMap<String, String>) -> CleanHeaders {
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

pub(crate) fn split_csv(value: Option<&str>) -> Vec<String> {
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

pub(crate) fn decode_header_value(value: &str) -> String {
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

pub(crate) fn normalize_date_value(value: &str) -> String {
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

pub(crate) fn normalize_message_id(value: &str) -> Option<String> {
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

pub(crate) fn parse_single_address(value: Option<&str>) -> Option<Address> {
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

pub(crate) fn parse_address_list(value: Option<&str>) -> Vec<Address> {
    let Some(entry) = value else {
        return vec![];
    };
    let decoded = decode_header_value(entry);
    decoded
        .split(',')
        .filter_map(|part| parse_single_address(Some(part)))
        .collect::<Vec<_>>()
}

pub(crate) fn spam_reason(headers: &BTreeMap<String, String>) -> Option<String> {
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

pub(crate) fn header_body_split_offset(raw: &[u8]) -> Option<usize> {
    raw.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|idx| idx + 4)
        .or_else(|| {
            raw.windows(2)
                .position(|window| window == b"\n\n")
                .map(|idx| idx + 2)
        })
}

pub(crate) fn parse_headers(raw: &[u8]) -> BTreeMap<String, String> {
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

pub(crate) fn parse_headers_no_envelope(raw: &[u8]) -> BTreeMap<String, String> {
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

pub(crate) fn parse_header_line(
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

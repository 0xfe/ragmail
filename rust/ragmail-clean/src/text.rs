use crate::types::SIGNATURE_PREFIXES;

pub(crate) fn clean_text(value: &str) -> String {
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

pub(crate) fn remove_signature(value: &str) -> (String, bool) {
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

pub(crate) fn html_to_text(input: &str) -> String {
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

pub(crate) fn normalize_body_text(value: &str) -> String {
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

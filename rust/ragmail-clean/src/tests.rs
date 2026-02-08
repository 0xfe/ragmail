use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::header::{decode_header_value, normalize_date_value, parse_single_address, spam_reason};
use crate::pipeline::clean_message;
use crate::types::CleanOutcome;
use crate::{clean_mbox_file, CleanOptions};

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

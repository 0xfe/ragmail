use std::collections::BTreeMap;
use std::path::PathBuf;

use ragmail_mbox::MboxError;
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
pub(crate) struct Address {
    pub(crate) name: String,
    pub(crate) email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ContentBlock {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AttachmentMeta {
    pub(crate) filename: String,
    pub(crate) content_type: String,
    pub(crate) size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MboxRef {
    pub(crate) file: String,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub(crate) struct CleanHeaders {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<Address>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) to: Vec<Address>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) cc: Vec<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) in_reply_to: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) references: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) list_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CleanEmailRecord {
    pub(crate) headers: CleanHeaders,
    pub(crate) tags: Vec<String>,
    pub(crate) content: Vec<ContentBlock>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) attachments: Vec<AttachmentMeta>,
    pub(crate) mbox: MboxRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SpamRecord {
    pub(crate) from: String,
    pub(crate) subject: String,
    pub(crate) date: String,
    pub(crate) reason: String,
}

pub(crate) enum CleanOutcome {
    Clean(Box<CleanEmailRecord>),
    Spam(SpamRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedMimePart {
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Vec<u8>,
}

pub(crate) const SPAM_SENDERS: [&str; 3] =
    ["discard-report@pobox.com", "mailer-daemon@", "postmaster@"];
pub(crate) const NEWSLETTER_MAILERS: [&str; 11] = [
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
pub(crate) const SIGNATURE_PREFIXES: [&str; 11] = [
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

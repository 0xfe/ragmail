"""Ignore-list rule engine for ragmail."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class IgnoreRule:
    rule_id: str
    rule_type: str
    value: str
    reason: str
    field: str | None = None
    match: str | None = None
    created_at: str | None = None


@dataclass
class IgnoreMatch:
    matched: bool
    rule: IgnoreRule | None = None


class IgnoreList:
    def __init__(self, rules: list[IgnoreRule]):
        self.rules = rules
        self._regex_cache: dict[str, re.Pattern[str]] = {}

    def match(self, record: dict[str, Any]) -> IgnoreMatch:
        for rule in self.rules:
            if _rule_matches(record, rule, self._regex_cache):
                return IgnoreMatch(True, rule)
        return IgnoreMatch(False, None)


def load_ignore_list(path: Path) -> IgnoreList:
    data = json.loads(path.read_text(encoding="utf-8"))
    rules_raw = data.get("rules", []) if isinstance(data, dict) else []
    rules: list[IgnoreRule] = []
    for raw in rules_raw:
        if not isinstance(raw, dict):
            continue
        rule_id = str(raw.get("id") or raw.get("rule_id") or uuid.uuid4().hex[:8])
        rule_type = str(raw.get("type") or raw.get("rule_type") or "").strip()
        value = str(raw.get("value") or "").strip()
        reason = str(raw.get("reason") or "").strip() or "unspecified"
        field = raw.get("field")
        match = raw.get("match")
        created_at = raw.get("created_at")
        if not rule_type or not value:
            continue
        rules.append(
            IgnoreRule(
                rule_id=rule_id,
                rule_type=rule_type,
                value=value,
                reason=reason,
                field=str(field) if field else None,
                match=str(match) if match else None,
                created_at=str(created_at) if created_at else None,
            )
        )
    return IgnoreList(rules)


def write_ignore_list_template(path: Path) -> None:
    payload = {
        "version": 1,
        "created_at": datetime.now().isoformat(),
        "rules": [
            {
                "id": "example-sender",
                "type": "sender",
                "value": "news@example.com",
                "reason": "Newsletter",
            },
            {
                "id": "example-domain",
                "type": "domain",
                "value": "marketing.example.com",
                "reason": "Marketing domain",
            },
            {
                "id": "example-subject",
                "type": "subject_contains",
                "value": "unsubscribe",
                "reason": "Bulk email",
            },
            {
                "id": "example-label",
                "type": "label",
                "value": "Category Promotions",
                "reason": "Gmail promotions",
            },
            {
                "id": "example-header",
                "type": "header_contains",
                "field": "list_id",
                "value": "newsletter",
                "reason": "List mail",
            },
        ],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def apply_ignore_list_stream(
    input_path: Path,
    ignore_list: IgnoreList,
    output_path: Path,
    ignored_path: Path,
) -> dict[str, int]:
    total = 0
    kept = 0
    ignored = 0

    with output_path.open("w", encoding="utf-8") as kept_handle, ignored_path.open(
        "w", encoding="utf-8"
    ) as ignored_handle:
        for record in iter_jsonl(input_path):
            total += 1
            match = ignore_list.match(record)
            if match.matched and match.rule:
                stamped = dict(record)
                stamped["ignore_rule_id"] = match.rule.rule_id
                stamped["ignore_reason"] = match.rule.reason
                ignored_handle.write(
                    json.dumps(stamped, ensure_ascii=False) + "\n"
                )
                ignored += 1
            else:
                kept_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                kept += 1

    return {"total": total, "kept": kept, "ignored": ignored}


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _rule_matches(
    record: dict[str, Any],
    rule: IgnoreRule,
    regex_cache: dict[str, re.Pattern[str]],
) -> bool:
    headers = record.get("headers", {}) if isinstance(record, dict) else {}
    tags = record.get("tags", []) if isinstance(record, dict) else []

    rule_type = rule.rule_type
    value = rule.value

    if rule_type == "sender":
        email = _get_from_email(headers)
        return email == value.lower()
    if rule_type == "domain":
        email = _get_from_email(headers)
        return email.endswith("@" + value.lower())
    if rule_type == "subject_contains":
        subject = str(headers.get("subject", "") or "").lower()
        return value.lower() in subject
    if rule_type == "subject_regex":
        subject = str(headers.get("subject", "") or "")
        return _regex_match(value, subject, regex_cache)
    if rule_type == "label":
        return any(str(tag).lower() == value.lower() for tag in tags)
    if rule_type == "header_equals":
        header_val = _get_header_value(headers, rule.field)
        return header_val.lower() == value.lower()
    if rule_type == "header_contains":
        header_val = _get_header_value(headers, rule.field)
        return value.lower() in header_val.lower()

    return False


def _get_from_email(headers: dict[str, Any]) -> str:
    from_value = headers.get("from") or {}
    if isinstance(from_value, dict):
        return str(from_value.get("email", "") or "").lower()
    if isinstance(from_value, str):
        return from_value.lower()
    return ""


def _get_header_value(headers: dict[str, Any], field: str | None) -> str:
    if not field:
        return ""
    value = headers.get(field, "")
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value or "")


def _regex_match(pattern: str, text: str, cache: dict[str, re.Pattern[str]]) -> bool:
    compiled = cache.get(pattern)
    if compiled is None:
        compiled = re.compile(pattern, re.IGNORECASE)
        cache[pattern] = compiled
    return bool(compiled.search(text))

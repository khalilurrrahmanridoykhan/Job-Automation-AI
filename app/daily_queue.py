from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .utils import ensure_parent_dir, normalize_whitespace


REMOTE_STRONG_HINTS = (
    "100% remote work",
    "100% remote",
    "us national",
    "us-remote",
    "us remote",
    "remote, us",
)
REMOTE_HINTS = REMOTE_STRONG_HINTS + (
    "remote work",
    "remote,",
    "work from home",
    "telecommute",
    "virtual,",
    "fully remote",
)
HYBRID_HINTS = ("hybrid remote work", "hybrid")
US_HINTS = ("united states", "u.s.", " usa ", " us national ", " us-remote ", " us remote ")
GLOBAL_REMOTE_HINTS = (
    "work from anywhere",
    "anywhere in the world",
    "worldwide",
    "global remote",
    "international applicants",
    "distributed team",
    "location independent",
)
BANGLADESH_FRIENDLY_HINTS = GLOBAL_REMOTE_HINTS + (
    "asia",
    "apac",
    "south asia",
    "southeast asia",
    "bangladesh",
    "utc+6",
    "utc +6",
    "gmt+6",
    "gmt +6",
)
REGION_RESTRICTION_RULES = (
    ("united states", ("united states only", "u.s. only", "us only", "must reside in the us", "us national")),
    ("canada", ("canada only", "must reside in canada")),
    ("united kingdom", ("united kingdom only", "uk only", "must reside in the uk")),
    ("europe", ("europe only", "eu only", "european union only", "eea only")),
    ("emea", ("emea only",)),
    ("latam", ("latam only", "latin america only")),
    ("australia", ("australia only",)),
    ("new zealand", ("new zealand only",)),
    ("india", ("india only",)),
)
US_STATE_NAMES = (
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district of columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
)
US_STATE_CODE_RE = re.compile(
    r"(?:,\s*|\bremote,\s*|\bvirtual,\s*|\bus[- ]?)"
    r"(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b",
    re.IGNORECASE,
)
GUEST_APPLY_HINTS = (
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "workable.com",
    "smartrecruiters.com",
    "metlifecareers.com",
)
GUIDED_APPLY_HINTS = (
    "icims.com",
    "brassring.com",
    "myapps.paychex.com",
    "jobportal.altis.com",
)
ACCOUNT_REQUIRED_HINTS = (
    "myworkdayjobs.com",
    "wd1.myworkdayjobs.com",
    "wd3.myworkdayjobs.com",
    "wd5.myworkdayjobs.com",
    "successfactors.com",
    "hfsinclair.com",
    "gallo.com/job",
)


@dataclass(slots=True)
class DailyQueueItem:
    job_id: int
    application_id: int | None
    title: str
    company: str | None
    location: str | None
    fit_score: float | None
    application_url: str
    application_status: str | None
    remote_bucket: str
    apply_bucket: str
    next_action: str
    priority_score: float
    remote_reason: str
    notes: list[str]


def _combined_job_text(row: sqlite3.Row) -> str:
    parts = [
        row["title"] or "",
        row["company"] or "",
        row["location"] or "",
        row["fit_reason"] or "",
        row["detail_text"] or "",
        row["requirements_text"] or "",
        row["benefits_text"] or "",
        row["company_overview_text"] or "",
    ]
    for column in ("raw_payload", "detail_raw_payload"):
        raw_value = row[column] if column in row.keys() else None
        if raw_value:
            parts.append(str(raw_value))
    return normalize_whitespace(" ".join(parts)).lower()


def _json_payload(row: sqlite3.Row, column: str) -> dict[str, Any]:
    try:
        raw_value = row[column]
    except Exception:
        raw_value = None
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _remote_signal_text(row: sqlite3.Row) -> str:
    raw_payload = _json_payload(row, "raw_payload")
    detail_payload = _json_payload(row, "detail_raw_payload")
    parts = [
        row["location"] or "",
        raw_payload.get("container_text") or "",
        detail_payload.get("requirements_text") or "",
        detail_payload.get("detail_text") or "",
    ]
    shortened_parts: list[str] = []
    for value in parts:
        cleaned = normalize_whitespace(str(value or ""))
        if not cleaned:
            continue
        shortened_parts.append(cleaned[:1400])
    return " | ".join(shortened_parts).lower()


def _looks_us_text(text: str) -> bool:
    padded = f" {text.strip()} "
    if any(hint in padded for hint in US_HINTS):
        return True
    if any(state in padded for state in US_STATE_NAMES):
        return True
    return bool(US_STATE_CODE_RE.search(text))


def _detected_region_restriction(text: str) -> str | None:
    for label, hints in REGION_RESTRICTION_RULES:
        if any(hint in text for hint in hints):
            return label
    return None


def _classify_remote_bucket(row: sqlite3.Row, *, allow_hybrid: bool, region_mode: str) -> tuple[str, bool, str]:
    text = _remote_signal_text(row)
    looks_us = _looks_us_text(text)
    has_hybrid = any(hint in text for hint in HYBRID_HINTS) or "work arrangement | hybrid" in text or "hybrid telecommute" in text
    has_strong_remote = any(hint in text for hint in REMOTE_STRONG_HINTS)
    has_remote = has_strong_remote or any(
        hint in text
        for hint in (
            "100% remote work",
            "100% remote",
            "us-remote",
            "remote,",
            "virtual,",
            "remote work",
            "fully remote",
        )
    )
    region_restriction = _detected_region_restriction(text)

    if has_hybrid:
        if not allow_hybrid:
            return ("hybrid", False, "Hybrid role excluded in remote-only mode.")
        if region_mode == "us" and looks_us:
            return ("hybrid_us", True, "Hybrid U.S. role included by flag.")
        if region_mode in {"global", "bangladesh"} and not region_restriction:
            return ("hybrid_global", True, "Hybrid role included by flag and no hard region restriction detected.")
        return ("hybrid", False, "Hybrid role is outside the current region filter.")

    if region_mode == "us":
        if has_strong_remote and looks_us:
            return ("remote_us_strong", True, "Strong U.S. remote signal detected.")
        if has_remote and looks_us:
            return ("remote_us", True, "U.S. remote signal detected.")
        if has_remote and not looks_us:
            return ("remote_unknown_region", False, "Remote role found, but U.S. eligibility is unclear.")
        return ("not_remote", False, "No strong U.S. remote signal detected.")

    if region_restriction:
        return ("region_restricted", False, f"Listing appears restricted to {region_restriction}.")

    if any(hint in text for hint in GLOBAL_REMOTE_HINTS):
        return ("remote_global", True, "Work-from-anywhere or global remote signal detected.")
    if any(hint in text for hint in BANGLADESH_FRIENDLY_HINTS):
        return ("remote_bangladesh_friendly", True, "Asia or Bangladesh-friendly remote signal detected.")
    if has_remote:
        return ("remote_unknown_region", False, "Remote role found, but Bangladesh eligibility is unclear.")
    return ("not_remote", False, "No strong remote signal detected.")


def _classify_apply_bucket(row: sqlite3.Row) -> tuple[str, str]:
    application_url = (row["application_url"] or "").lower()
    last_error = normalize_whitespace(row["application_last_error"] or "").lower()

    if "sign in or account creation" in last_error:
        return ("account_required", "Existing application hit an account gate.")
    if any(hint in application_url for hint in GUEST_APPLY_HINTS):
        return ("guest_apply", "Common direct-apply ATS detected.")
    if any(hint in application_url for hint in GUIDED_APPLY_HINTS):
        return ("guided_apply", "Guided ATS detected.")
    if any(hint in application_url for hint in ACCOUNT_REQUIRED_HINTS):
        return ("account_required", "ATS likely requires account creation or sign-in.")
    return ("manual_or_unknown", "Unknown employer flow; manual review likely needed.")


def _next_action(row: sqlite3.Row, apply_bucket: str) -> str:
    status = row["application_status"]
    last_error = normalize_whitespace(row["application_last_error"] or "").lower()

    if status in {"reviewing", "error"} and "sign in or account creation" in last_error:
        return "manual_account_then_resume"
    if status in {"prepared", "reviewing", "reviewed"}:
        return "autofill_or_review"
    if apply_bucket == "account_required":
        return "prepare_then_account_gate"
    return "prepare_then_autofill"


def _priority_score(
    row: sqlite3.Row,
    *,
    remote_bucket: str,
    apply_bucket: str,
    next_action: str,
) -> float:
    base = float(row["fit_score"] or 0.0)
    remote_bonus = {
        "remote_us_strong": 35.0,
        "remote_us": 25.0,
        "hybrid_us": 5.0,
        "remote_global": 35.0,
        "remote_bangladesh_friendly": 30.0,
        "hybrid_global": 4.0,
    }.get(remote_bucket, -20.0)
    apply_bonus = {
        "guest_apply": 30.0,
        "guided_apply": 18.0,
        "manual_or_unknown": 5.0,
        "account_required": -8.0,
    }.get(apply_bucket, 0.0)
    action_bonus = {
        "autofill_or_review": 15.0,
        "prepare_then_autofill": 10.0,
        "prepare_then_account_gate": -5.0,
        "manual_account_then_resume": -12.0,
    }.get(next_action, 0.0)
    return base + remote_bonus + apply_bonus + action_bonus


def build_daily_queue(
    rows: list[sqlite3.Row],
    *,
    limit: int,
    remote_us_only: bool = True,
    allow_hybrid: bool = False,
    region_mode: str = "us",
) -> list[DailyQueueItem]:
    items: list[DailyQueueItem] = []
    for row in rows:
        remote_bucket, remote_ok, remote_reason = _classify_remote_bucket(
            row,
            allow_hybrid=allow_hybrid,
            region_mode=region_mode,
        )
        if remote_us_only and not remote_ok:
            continue

        apply_bucket, apply_reason = _classify_apply_bucket(row)
        next_action = _next_action(row, apply_bucket)
        priority_score = _priority_score(
            row,
            remote_bucket=remote_bucket,
            apply_bucket=apply_bucket,
            next_action=next_action,
        )
        items.append(
            DailyQueueItem(
                job_id=int(row["id"]),
                application_id=int(row["application_id"]) if row["application_id"] is not None else None,
                title=row["title"],
                company=row["company"],
                location=row["location"],
                fit_score=float(row["fit_score"]) if row["fit_score"] is not None else None,
                application_url=row["application_url"],
                application_status=row["application_status"],
                remote_bucket=remote_bucket,
                apply_bucket=apply_bucket,
                next_action=next_action,
                priority_score=priority_score,
                remote_reason=remote_reason,
                notes=[remote_reason, apply_reason],
            )
        )

    items.sort(
        key=lambda item: (
            -item.priority_score,
            -(item.fit_score or 0.0),
            item.company or "",
            item.title,
        )
    )
    return items[:limit]


def daily_queue_as_dicts(items: list[DailyQueueItem]) -> list[dict[str, Any]]:
    return [
        {
            "job_id": item.job_id,
            "application_id": item.application_id,
            "title": item.title,
            "company": item.company,
            "location": item.location,
            "fit_score": item.fit_score,
            "application_url": item.application_url,
            "application_status": item.application_status,
            "remote_bucket": item.remote_bucket,
            "apply_bucket": item.apply_bucket,
            "next_action": item.next_action,
            "priority_score": round(item.priority_score, 1),
            "notes": item.notes,
        }
        for item in items
    ]


def render_daily_queue_markdown(items: list[DailyQueueItem]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "# Daily Application Queue",
        "",
        f"Generated: {timestamp}",
        "",
        f"Jobs in queue: {len(items)}",
        "",
    ]
    for index, item in enumerate(items, start=1):
        score = f"{item.fit_score:.1f}" if item.fit_score is not None else "n/a"
        lines.append(f"## {index}. {item.title} - {item.company or 'Unknown Company'}")
        lines.append("")
        lines.append(f"- Job ID: {item.job_id}")
        if item.application_id is not None:
            lines.append(f"- Application ID: {item.application_id}")
        lines.append(f"- Fit Score: {score}")
        lines.append(f"- Priority Score: {item.priority_score:.1f}")
        lines.append(f"- Location: {item.location or 'n/a'}")
        lines.append(f"- Remote Bucket: {item.remote_bucket}")
        lines.append(f"- Apply Bucket: {item.apply_bucket}")
        lines.append(f"- Next Action: {item.next_action}")
        lines.append(f"- Application Status: {item.application_status or 'not prepared'}")
        lines.append(f"- Apply URL: {item.application_url}")
        for note in item.notes:
            lines.append(f"- Note: {note}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_daily_queue_markdown(items: list[DailyQueueItem], output_path: Path) -> Path:
    ensure_parent_dir(output_path)
    output_path.write_text(render_daily_queue_markdown(items), encoding="utf-8")
    return output_path


def write_daily_queue_json(items: list[DailyQueueItem], output_path: Path) -> Path:
    ensure_parent_dir(output_path)
    output_path.write_text(json.dumps(daily_queue_as_dicts(items), indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return output_path

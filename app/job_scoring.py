from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from .db import fetch_jobs_for_scoring, update_job_score
from .utils import normalize_whitespace


TOKEN_RE = re.compile(r"[a-z0-9]+")
STOPWORDS = {
    "and",
    "for",
    "the",
    "with",
    "from",
    "work",
    "job",
    "jobs",
    "role",
    "roles",
    "to",
    "of",
    "in",
    "on",
    "remote",
}
REMOTE_HINTS = ("remote", "hybrid", "us national", "work from anywhere", "telecommute")
SENIOR_TITLE_HINTS = ("staff", "senior", "sr.", "principal", "director", "head of")
JUNIOR_TITLE_HINTS = ("intern", "internship", "junior", "entry level")


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in TOKEN_RE.findall(text.lower())
        if len(token) > 1 and token not in STOPWORDS
    }


def _parse_raw_payload(value: str | None) -> str:
    if not value:
        return ""

    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return value

    if isinstance(payload, dict):
        flattened: list[str] = []
        for key in ("text", "container_text", "title", "company", "location"):
            item = payload.get(key)
            if isinstance(item, str):
                flattened.append(item)
        return " ".join(flattened)

    return str(payload)


def _profile_keywords(profile: dict[str, Any]) -> list[str]:
    keywords: list[str] = []
    for key in ("skills", "core_competencies", "target_roles"):
        keywords.extend(profile.get(key, []))
    keywords.extend(profile.get("application_answers", {}).get("top_strengths", []))
    for item in profile.get("projects", []):
        if item.get("title"):
            keywords.append(item["title"])
        if item.get("description"):
            keywords.append(item["description"])
    return [keyword for keyword in keywords if keyword]


def _job_text(job: sqlite3.Row) -> str:
    parts = [
        job["title"] or "",
        job["company"] or "",
        job["location"] or "",
        job["salary_text"] or "",
        job["detail_text"] or "",
        job["requirements_text"] or "",
        job["benefits_text"] or "",
        job["company_overview_text"] or "",
        _parse_raw_payload(job["raw_payload"]),
    ]
    return normalize_whitespace(" ".join(parts))


def _experience_overlap(profile: dict[str, Any], job_text: str) -> tuple[int, list[str]]:
    job_tokens = _tokens(job_text)
    scored: list[tuple[int, str]] = []
    for item in profile.get("experience", []):
        title = item.get("title") or ""
        employer = item.get("employer") or ""
        highlights = item.get("highlights", [])
        combined = " ".join([title, employer, *highlights])
        overlap = len(_tokens(combined) & job_tokens)
        if overlap:
            label = title if title else employer
            if label:
                scored.append((overlap, label))
    scored.sort(key=lambda row: (-row[0], row[1]))
    top_labels = [label for _, label in scored[:3]]
    total_overlap = sum(score for score, _ in scored[:2])
    return total_overlap, top_labels


def score_job(profile: dict[str, Any], job: sqlite3.Row) -> tuple[float, str]:
    job_text = _job_text(job)
    job_text_lower = job_text.lower()
    title_lower = (job["title"] or "").lower()
    score = 0.0
    reasons: list[str] = []

    preferred_roles: list[str] = profile.get("target_roles", [])
    for role in preferred_roles:
        role_lower = role.lower()
        role_tokens = _tokens(role)
        overlap = len(role_tokens & _tokens(title_lower))

        if role_lower in title_lower:
            score += 38
            reasons.append(f"title strongly matches '{role}'")
            break

        if role_tokens and overlap / len(role_tokens) >= 0.5:
            score += 24
            reasons.append(f"title partially matches '{role}'")
            break

    skills: list[str] = profile.get("skills", [])
    matched_skills = [skill for skill in skills if skill.lower() in job_text_lower]
    if matched_skills:
        score += min(len(matched_skills) * 4, 24)
        reasons.append(f"skill overlap: {', '.join(matched_skills[:4])}")

    profile_keywords = _profile_keywords(profile)
    matched_keywords = [keyword for keyword in profile_keywords if keyword.lower() in job_text_lower]
    if matched_keywords:
        score += min(len(matched_keywords) * 2.5, 14)
        reasons.append(f"profile keyword overlap: {', '.join(matched_keywords[:4])}")

    overlap_score, overlap_labels = _experience_overlap(profile, job_text)
    if overlap_score:
        score += min(overlap_score * 1.8, 18)
        reasons.append(f"relevant experience: {', '.join(overlap_labels)}")

    if any(hint in job_text_lower for hint in REMOTE_HINTS):
        score += 10
        reasons.append("remote or hybrid work signal detected")

    years_experience = profile.get("years_experience")
    if years_experience and years_experience >= 5 and any(token in title_lower for token in ("senior", "lead")):
        score += 8
        reasons.append("seniority matches profile")
    if years_experience and years_experience < 4 and any(token in title_lower for token in SENIOR_TITLE_HINTS):
        score -= 8
        reasons.append("title may be above current experience level")
    if years_experience and years_experience > 3 and any(token in title_lower for token in JUNIOR_TITLE_HINTS):
        score -= 4

    if "power bi" in job_text_lower or "tableau" in job_text_lower:
        score += 6
    if "etl" in job_text_lower or "data pipeline" in job_text_lower:
        score += 6
    if "python" in job_text_lower and "sql" in job_text_lower:
        score += 6
    if "geospatial" in job_text_lower or "gis" in job_text_lower:
        score += 4

    score = min(score, 100.0)
    if not reasons:
        reasons.append("limited profile overlap from available listing data")

    return score, "; ".join(reasons[:4])


def score_unscored_jobs(profile: dict[str, Any], db_path, limit: int | None = None) -> int:
    jobs = fetch_jobs_for_scoring(db_path, limit=limit)
    for job in jobs:
        fit_score, fit_reason = score_job(profile, job)
        update_job_score(db_path, int(job["id"]), fit_score, fit_reason)
    return len(jobs)

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from sqlite3 import Row
from typing import Any

from .utils import cleaned_lines, dedupe_preserve_order, ensure_parent_dir, normalize_whitespace, strip_bullet_prefix


TOKEN_RE = re.compile(r"[a-z0-9]+")
REMOTE_HINTS = ("remote", "hybrid", "us national", "work from anywhere", "telecommute")
NON_US_HINTS = (
    "united kingdom",
    "eng",
    "canada",
    "greece",
    "australia",
    "colombia",
)
REGION_RESTRICTION_HINTS = (
    "united states only",
    "u.s. only",
    "us only",
    "canada only",
    "united kingdom only",
    "uk only",
    "europe only",
    "eu only",
    "emea only",
    "latam only",
)
MANUAL_CONFIRMATION_FIELDS = [
    "Work authorization / visa sponsorship",
    "Salary expectations",
    "Earliest start date",
    "Willingness to relocate or travel",
    "Background check / licensing questions",
]


def _tokens(text: str) -> set[str]:
    return set(TOKEN_RE.findall(text.lower()))


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "job"


def application_packet_paths(out_dir: Path, job_id: int, company: str | None, title: str | None) -> tuple[Path, Path]:
    job_slug = _slugify(f"{job_id}-{company or 'company'}-{title or 'role'}")
    json_path = out_dir / f"{job_slug}.json"
    md_path = out_dir / f"{job_slug}.md"
    return json_path, md_path


def _job_text(job: Row) -> str:
    parts = [
        job["title"] or "",
        job["company"] or "",
        job["location"] or "",
        job["detail_text"] or "",
        job["requirements_text"] or "",
        job["benefits_text"] or "",
        job["company_overview_text"] or "",
        job["fit_reason"] or "",
    ]
    return normalize_whitespace(" ".join(parts))


def _profile_keywords(profile: dict[str, Any]) -> list[str]:
    values = []
    values.extend(profile.get("skills", []))
    values.extend(profile.get("core_competencies", []))
    values.extend(profile.get("application_answers", {}).get("top_strengths", []))
    values.extend(profile.get("target_roles", []))
    for item in profile.get("projects", []):
        values.append(item.get("title", ""))
        values.append(item.get("description", ""))
    return dedupe_preserve_order([normalize_whitespace(value) for value in values if value])


def _extract_requirement_bullets(text: str, *, limit: int = 8) -> list[str]:
    bullets: list[str] = []
    current_section: str | None = None
    for line in cleaned_lines(text):
        if line.endswith(":"):
            current_section = line[:-1].strip()
            continue
        cleaned = strip_bullet_prefix(line)
        if not cleaned:
            continue
        if cleaned.lower().startswith(("title:", "location:", "workplace:", "job description:")):
            continue
        if current_section:
            bullets.append(f"{current_section}: {cleaned}")
        else:
            bullets.append(cleaned)
        if len(bullets) >= limit:
            break
    return bullets


def _experience_overlap_score(experience: dict[str, Any], job_tokens: set[str]) -> int:
    text_parts = [experience.get("title", ""), experience.get("employer", "")]
    text_parts.extend(experience.get("highlights", []))
    tokens = _tokens(" ".join(text_parts))
    return len(tokens & job_tokens)


def _select_relevant_experience(profile: dict[str, Any], job: Row, *, limit: int = 2) -> list[dict[str, Any]]:
    job_tokens = _tokens(_job_text(job))
    ranked = sorted(
        profile.get("experience", []),
        key=lambda item: (
            -_experience_overlap_score(item, job_tokens),
            item.get("date_range", ""),
        ),
    )
    selected: list[dict[str, Any]] = []
    for experience in ranked[:limit]:
        highlights = sorted(
            experience.get("highlights", []),
            key=lambda line: -len(_tokens(line) & job_tokens),
        )
        selected.append(
            {
                "employer": experience.get("employer"),
                "title": experience.get("title"),
                "date_range": experience.get("date_range"),
                "highlights": highlights[:3],
            }
        )
    return selected


def _select_resume_highlights(profile: dict[str, Any], job: Row, *, limit: int = 5) -> list[str]:
    job_tokens = _tokens(_job_text(job))
    ranked_lines: list[tuple[int, str]] = []
    for experience in profile.get("experience", []):
        for highlight in experience.get("highlights", []):
            score = len(_tokens(highlight) & job_tokens)
            prefix = f"{experience.get('employer')}: {normalize_whitespace(highlight)}"
            ranked_lines.append((score, prefix))
    ranked_lines.sort(key=lambda item: (-item[0], item[1]))
    return [line for _, line in ranked_lines[:limit]]


def _matched_keywords(profile: dict[str, Any], job: Row, *, limit: int = 10) -> list[str]:
    job_text_lower = _job_text(job).lower()
    matches = [keyword for keyword in _profile_keywords(profile) if keyword.lower() in job_text_lower]
    return matches[:limit]


def _build_fit_highlights(profile: dict[str, Any], job: Row, relevant_experience: list[dict[str, Any]]) -> list[str]:
    summary = profile.get("summary") or ""
    if summary:
        highlights = [summary]
    elif profile.get("years_experience"):
        highlights = [f"{profile['years_experience']}+ years across analytics, data, and software delivery."]
    else:
        highlights = ["Background includes analytics, reporting, and data workflow delivery."]

    if relevant_experience:
        recent = relevant_experience[0]
        if recent.get("title") and recent.get("employer"):
            highlights.append(f"Recent directly relevant experience: {recent['title']} at {recent['employer']}.")

    matched = _matched_keywords(profile, job, limit=4)
    if matched:
        highlights.append(f"Strong overlap with job language: {', '.join(matched)}.")

    job_text_lower = _job_text(job).lower()
    if any(hint in job_text_lower for hint in REMOTE_HINTS):
        highlights.append("Listing includes remote or hybrid flexibility.")

    if profile.get("languages"):
        highlights.append(f"Languages: {', '.join(profile['languages'])}.")

    return highlights[:4]


def _build_caution_flags(job: Row) -> list[str]:
    flags: list[str] = []
    location = normalize_whitespace(job["location"] or "")
    location_lower = location.lower()
    requirements = normalize_whitespace(job["requirements_text"] or "").lower()
    detail = normalize_whitespace(job["detail_text"] or "").lower()

    if location and any(hint in location_lower for hint in NON_US_HINTS):
        flags.append(f"Location appears non-U.S.: {location}.")

    if any(hint in requirements or hint in detail or hint in location_lower for hint in REGION_RESTRICTION_HINTS):
        flags.append("Listing appears to include region or residency restrictions.")

    if "travel required" in requirements or "travel required" in detail:
        flags.append("Role appears to require travel.")

    if location and not any(hint in location_lower for hint in REMOTE_HINTS) and "remote" not in requirements:
        flags.append(f"Role may be location-bound or primarily onsite: {location}.")

    if not job["salary_text"]:
        flags.append("Compensation is not clearly stated in the listing.")

    return flags[:4]


def _tailored_summary(
    profile: dict[str, Any],
    job: Row,
    relevant_experience: list[dict[str, Any]],
    matched_keywords: list[str],
) -> str:
    title = job["title"] or "this role"
    company = job["company"] or "the company"
    summary = profile.get("summary") or ""
    if summary:
        base = f"{profile.get('name', 'The candidate')} is a strong match because {summary}"
    elif profile.get("years_experience"):
        base = (
            f"{profile.get('name', 'The candidate')} brings {profile.get('years_experience', 0)}+ years of "
            "analytics and software experience focused on data-driven problem solving."
        )
    else:
        base = f"{profile.get('name', 'The candidate')} brings relevant analytics and software experience."

    if relevant_experience:
        recent = relevant_experience[0]
        exp_line = (
            f" The most relevant recent experience is {recent.get('title', 'a recent role')} "
            f"at {recent.get('employer', 'a major employer')}."
        )
    else:
        exp_line = ""

    if matched_keywords:
        keyword_line = f" This {title} opportunity at {company} overlaps strongly with {', '.join(matched_keywords[:4])}."
    else:
        keyword_line = f" This {title} opportunity at {company} aligns with the candidate's analytics background."

    return normalize_whitespace(base + exp_line + keyword_line)


def _why_this_role_answer(profile: dict[str, Any], job: Row, relevant_experience: list[dict[str, Any]]) -> str:
    title = job["title"] or "this role"
    company = job["company"] or "your team"
    answer = (
        f"I am interested in {title} at {company} because it fits my background in analytics, reporting, and data-focused delivery."
    )
    if relevant_experience:
        recent = relevant_experience[0]
        answer += (
            f" My recent experience as {recent.get('title', 'a data professional')} at "
            f"{recent.get('employer', 'my current employer')} is especially relevant."
        )
    answer += " I am looking for a role where I can add value through clear analysis, automation, and actionable reporting."
    return normalize_whitespace(answer)


def _relevant_experience_answer(relevant_experience: list[dict[str, Any]]) -> str:
    segments: list[str] = []
    for experience in relevant_experience:
        employer = experience.get("employer") or "Employer"
        title = experience.get("title") or "Role"
        highlights = experience.get("highlights", [])
        if highlights:
            segments.append(f"{title} at {employer}: {highlights[0]}")
        else:
            segments.append(f"{title} at {employer}")
    return normalize_whitespace(" ".join(segments))


def _draft_cover_letter(
    profile: dict[str, Any],
    job: Row,
    tailored_summary: str,
    relevant_experience: list[dict[str, Any]],
) -> str:
    title = job["title"] or "the role"
    company = job["company"] or "your company"
    intro = f"Dear Hiring Team,\n\nI am applying for the {title} position at {company}."
    middle = tailored_summary
    evidence = ""
    if relevant_experience:
        recent = relevant_experience[0]
        first_highlight = (recent.get("highlights") or [""])[0]
        evidence = (
            f" In my recent role as {recent.get('title', 'a relevant professional')} at "
            f"{recent.get('employer', 'my employer')}, I {first_highlight.lower().rstrip('.')}"
            if first_highlight
            else ""
        )
        if evidence and not evidence.endswith("."):
            evidence += "."
    close = (
        f"\n\nI would welcome the opportunity to discuss how my background can support {company}'s needs. "
        "Thank you for your consideration."
    )
    return f"{intro}\n\n{middle}{evidence}{close}".strip()


def build_application_packet(profile: dict[str, Any], job: Row) -> dict[str, Any]:
    return build_application_packet_with_defaults(profile, job, application_defaults=None)


def build_application_packet_with_defaults(
    profile: dict[str, Any],
    job: Row,
    application_defaults: dict[str, Any] | None,
) -> dict[str, Any]:
    relevant_experience = _select_relevant_experience(profile, job)
    matched_keywords = _matched_keywords(profile, job)
    tailored_summary = _tailored_summary(profile, job, relevant_experience, matched_keywords)
    requirement_bullets = _extract_requirement_bullets(job["requirements_text"] or job["detail_text"] or "")
    benefit_bullets = _extract_requirement_bullets(job["benefits_text"] or "", limit=6)
    defaults = application_defaults or {}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "job": {
            "id": int(job["id"]),
            "title": job["title"],
            "company": job["company"],
            "location": job["location"],
            "salary_text": job["salary_text"],
            "fit_score": job["fit_score"],
            "fit_reason": job["fit_reason"],
            "job_url": job["job_url"],
            "application_url": job["application_url"],
        },
        "candidate": {
            "name": profile.get("name"),
            "email": profile.get("email"),
            "phone": profile.get("phone"),
            "languages": profile.get("languages", []),
            "years_experience": profile.get("years_experience"),
            "education": profile.get("education", []),
            "certifications": profile.get("certifications", []),
            "resume_path": profile.get("application_answers", {}).get("resume_path"),
        },
        "fit": {
            "matched_keywords": matched_keywords,
            "fit_highlights": _build_fit_highlights(profile, job, relevant_experience),
            "caution_flags": _build_caution_flags(job),
        },
        "target_requirements": requirement_bullets,
        "benefits": benefit_bullets,
        "relevant_experience": relevant_experience,
        "resume_highlights": _select_resume_highlights(profile, job),
        "tailored_summary": tailored_summary,
        "draft_cover_letter": _draft_cover_letter(profile, job, tailored_summary, relevant_experience),
        "form_answers": {
            "professional_summary": tailored_summary,
            "why_this_role": _why_this_role_answer(profile, job, relevant_experience),
            "relevant_experience": _relevant_experience_answer(relevant_experience),
            "languages": ", ".join(profile.get("languages", [])),
            "middle_name": defaults.get("middle_name"),
            "account_login": defaults.get("account_login"),
            "account_password": defaults.get("account_password"),
            "phone_type": defaults.get("phone_type"),
            "address_type": defaults.get("address_type"),
            "work_authorization": defaults.get("work_authorization"),
            "require_sponsorship": defaults.get("require_sponsorship"),
            "willing_to_relocate": defaults.get("willing_to_relocate"),
            "salary_expectations": defaults.get("salary_expectations"),
            "start_date": defaults.get("start_date"),
            "linkedin_url": defaults.get("linkedin_url"),
            "github_url": defaults.get("github_url"),
            "address_line1": defaults.get("address_line1"),
            "address_line2": defaults.get("address_line2"),
            "city": defaults.get("city"),
            "region": defaults.get("region"),
            "postal_code": defaults.get("postal_code"),
            "country": defaults.get("country"),
            "county": defaults.get("county"),
            "accept_terms": defaults.get("accept_terms"),
        },
        "manual_review_items": MANUAL_CONFIRMATION_FIELDS,
    }


def _render_bullet_block(lines: list[str]) -> list[str]:
    if not lines:
        return ["- n/a"]
    return [f"- {line}" for line in lines]


def render_application_packet_markdown(packet: dict[str, Any]) -> str:
    job = packet["job"]
    candidate = packet["candidate"]
    fit = packet["fit"]

    lines = [
        f"# Application Packet: {job['title']} - {job['company'] or 'Unknown Company'}",
        "",
        "## Job",
        f"- Job ID: {job['id']}",
        f"- Title: {job['title']}",
        f"- Company: {job['company'] or 'n/a'}",
        f"- Location: {job['location'] or 'n/a'}",
        f"- Salary: {job['salary_text'] or 'n/a'}",
        f"- Fit Score: {job['fit_score'] if job['fit_score'] is not None else 'n/a'}",
        f"- FlexJobs URL: {job['job_url']}",
        f"- Apply URL: {job['application_url'] or 'n/a'}",
        "",
        "## Candidate",
        f"- Name: {candidate['name'] or 'n/a'}",
        f"- Email: {candidate['email'] or 'n/a'}",
        f"- Phone: {candidate['phone'] or 'n/a'}",
        f"- Languages: {', '.join(candidate['languages']) if candidate['languages'] else 'n/a'}",
        f"- Resume: {candidate['resume_path'] or 'n/a'}",
        "",
        "## Tailored Summary",
        packet["tailored_summary"],
        "",
        "## Fit Highlights",
        *_render_bullet_block(fit["fit_highlights"]),
        "",
        "## Matched Keywords",
        *_render_bullet_block(fit["matched_keywords"]),
        "",
        "## Target Requirements",
        *_render_bullet_block(packet["target_requirements"]),
        "",
        "## Benefits",
        *_render_bullet_block(packet["benefits"]),
        "",
        "## Relevant Experience",
    ]

    if packet["relevant_experience"]:
        for experience in packet["relevant_experience"]:
            lines.append(
                f"- {experience['title']} | {experience['employer']} | {experience['date_range'] or 'n/a'}"
            )
            for highlight in experience.get("highlights", []):
                lines.append(f"  - {highlight}")
    else:
        lines.append("- n/a")

    lines.extend(
        [
            "",
            "## Resume Highlights",
            *_render_bullet_block(packet["resume_highlights"]),
            "",
            "## Draft Cover Letter",
            packet["draft_cover_letter"],
            "",
            "## Suggested Form Answers",
            f"- Professional Summary: {packet['form_answers']['professional_summary']}",
            f"- Why This Role: {packet['form_answers']['why_this_role']}",
            f"- Relevant Experience: {packet['form_answers']['relevant_experience']}",
            f"- Languages: {packet['form_answers']['languages'] or 'n/a'}",
            "",
            "## Caution Flags",
            *_render_bullet_block(fit["caution_flags"]),
            "",
            "## Manual Review Items",
            *_render_bullet_block(packet["manual_review_items"]),
        ]
    )
    return "\n".join(lines) + "\n"


def write_application_packet(packet: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    job = packet["job"]
    json_path, md_path = application_packet_paths(out_dir, int(job["id"]), job["company"], job["title"])

    ensure_parent_dir(json_path)
    json_path.write_text(json.dumps(packet, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    md_path.write_text(render_application_packet_markdown(packet), encoding="utf-8")
    return json_path, md_path

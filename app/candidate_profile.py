from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

from .utils import cleaned_lines, dedupe_preserve_order, normalize_whitespace, strip_bullet_prefix, write_json


EMAIL_RE = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")
PHONE_CANDIDATE_RE = re.compile(r"(?:\+\d[\d().\-\s]{7,}\d|\b\d[\d().\-\s]{7,}\d\b)")
URL_RE = re.compile(r"(?:https?://)?(?:www\.)?[\w.-]+\.[a-z]{2,}(?:/[^\s]*)?", re.IGNORECASE)
DATE_LINE_RE = re.compile(
    r"(?ix)^("
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{4}\s*[-–]\s*(?:present|current|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{4})"
    r"|project[- ]based"
    r"|ongoing"
    r"|contract"
    r"|\d{4}\s*[-–]\s*(?:present|\d{4})"
    r")$"
)
SECTION_HEADERS = {
    "SUMMARY": "summary",
    "PROFESSIONAL SUMMARY": "summary",
    "SKILLS": "skills",
    "CORE COMPETENCIES": "skills",
    "EXPERIENCE": "experience",
    "PROFESSIONAL EXPERIENCE": "experience",
    "PROJECTS": "projects",
    "CERTIFICATIONS": "certifications",
    "LEADERSHIP": "leadership",
    "EDUCATION": "education",
    "EDUCATION & CREDENTIALS": "education",
}
LINK_MARKERS = {"github", "live demo", "website", "portfolio"}
ROLE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Data Analyst", ("data analyst", "python", "sql", "dashboard", "analysis")),
    ("Business Intelligence Analyst", ("power bi", "tableau", "dashboard", "reporting", "bi tools")),
    ("Reporting Analyst", ("reporting", "kpi", "dashboard", "metrics", "excel")),
    ("BI Developer", ("power bi", "tableau", "dashboard", "visualization")),
    ("ETL Developer", ("etl", "data pipeline", "data cleaning", "data warehousing")),
    ("Analytics Engineer", ("etl", "sql", "data warehouse", "python", "metrics")),
    ("Data Engineer", ("data pipeline", "etl", "data warehousing", "sql", "python")),
    ("Data Quality Analyst", ("data quality", "validation", "standardized metrics", "clean datasets")),
    ("Geospatial Data Analyst", ("geospatial", "mapping", "map portal", "village-level")),
    ("Public Health Data Analyst", ("public health", "malaria", "surveillance", "community data")),
]
TOP_STRENGTH_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Python and SQL analysis", ("python", "sql", "pandas", "numpy")),
    ("Dashboard development", ("dashboard", "power bi", "tableau", "chart.js", "d3.js")),
    ("ETL and data cleaning", ("etl", "data cleaning", "data warehousing", "pipeline")),
    ("Automated reporting", ("automated reports", "reporting", "kpi", "metrics")),
    ("Geospatial and field-data workflows", ("geospatial", "mapping", "field data", "survey")),
    ("Public-health data reporting", ("public health", "malaria", "surveillance")),
]


def extract_pdf_text(cv_path: Path) -> str:
    if not cv_path.exists():
        raise FileNotFoundError(f"CV file not found: {cv_path}")

    command = ["pdftotext", str(cv_path), "-"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def _canonical_section_name(line: str) -> str | None:
    return SECTION_HEADERS.get(line.strip().upper())


def _split_sections(lines: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    preamble: list[str] = []
    sections: dict[str, list[str]] = {}
    current: str | None = None

    for line in lines:
        section_name = _canonical_section_name(line)
        if section_name is not None:
            current = section_name
            sections.setdefault(section_name, [])
            continue

        if current is None:
            preamble.append(line)
        else:
            sections[current].append(line)

    return preamble, sections


def _extract_contact(preamble: list[str]) -> tuple[str | None, str | None]:
    joined = " ".join(preamble)
    email_match = EMAIL_RE.search(joined)

    phone: str | None = None
    for match in PHONE_CANDIDATE_RE.finditer(joined):
        candidate = match.group(0).strip()
        digit_count = sum(char.isdigit() for char in candidate)
        if 8 <= digit_count <= 15:
            phone = candidate
            break

    return phone, email_match.group(0) if email_match else None


def _extract_links(preamble: list[str]) -> dict[str, str]:
    joined = " ".join(preamble)
    links: dict[str, str] = {}

    for match in URL_RE.finditer(joined):
        value = match.group(0).strip().rstrip(".,")
        lowered = value.lower()
        if "@" in value:
            continue
        if "github.com" in lowered:
            links.setdefault("github", value if value.startswith("http") else f"https://{value}")
        elif "." in value and any(token in joined.lower() for token in ("website:", "portfolio:", value.lower())):
            links.setdefault("website", value if value.startswith("http") else f"https://{value}")

    github_handle_match = re.search(r"github:\s*([a-z0-9_.-]+)", joined, re.IGNORECASE)
    if github_handle_match:
        links.setdefault("github", f"https://github.com/{github_handle_match.group(1)}")

    return links


def _parse_summary(lines: list[str]) -> str:
    return normalize_whitespace(" ".join(lines))


def _parse_skills(lines: list[str], summary: str) -> tuple[list[str], list[str]]:
    focus_areas: list[str] = []
    skills: list[str] = []

    for line in lines:
        if "," in line:
            skills.extend(normalize_whitespace(part) for part in line.split(",") if normalize_whitespace(part))
            continue

        word_count = len(line.split())
        if word_count <= 4 and line == line.title():
            focus_areas.append(line)
            continue

        skills.append(line)

    summary_skill_map = {
        "dashboarding": "Dashboarding",
        "etl": "ETL",
        "geospatial": "Geospatial Reporting",
        "public health": "Public Health Reporting",
        "automation": "Automation",
    }
    lowered_summary = summary.lower()
    for token, label in summary_skill_map.items():
        if token in lowered_summary:
            skills.append(label)

    return dedupe_preserve_order(focus_areas), dedupe_preserve_order(skills)


def _looks_like_date_line(value: str) -> bool:
    lowered = value.lower()
    if DATE_LINE_RE.match(value):
        return True
    return any(token in lowered for token in ("present", "ongoing", "project-based", "contract"))


def _split_employer_and_date(value: str) -> tuple[str | None, str | None]:
    for separator in (" – ", " — ", " - "):
        if separator not in value:
            continue
        left, right = [part.strip() for part in value.split(separator, 1)]
        if _looks_like_date_line(right):
            return (left or None), right
    return None, None


def _parse_experience_header(lines: list[str]) -> tuple[str | None, str | None, str | None]:
    if not lines:
        return None, None, None

    title = lines[0]
    employer: str | None = None
    date_range: str | None = None

    for line in lines[1:]:
        split_employer, split_date = _split_employer_and_date(line)
        if split_date is not None:
            if employer is None and split_employer:
                employer = split_employer
            date_range = split_date
            continue

        if _looks_like_date_line(line):
            date_range = line
            continue

        if employer is None:
            employer = line
        else:
            employer = normalize_whitespace(f"{employer} {line}")

    return title, employer, date_range


def _flush_experience_block(
    roles: list[dict[str, Any]],
    header_lines: list[str],
    bullet_lines: list[str],
) -> None:
    if not header_lines and not bullet_lines:
        return

    title, employer, date_range = _parse_experience_header(header_lines)
    if not title and not bullet_lines:
        return

    roles.append(
        {
            "employer": employer,
            "title": title,
            "date_range": date_range,
            "highlights": [normalize_whitespace(strip_bullet_prefix(line)) for line in bullet_lines if strip_bullet_prefix(line)],
        }
    )


def _parse_experience(lines: list[str]) -> list[dict[str, Any]]:
    roles: list[dict[str, Any]] = []
    header_lines: list[str] = []
    bullet_lines: list[str] = []
    in_bullets = False

    for line in lines:
        is_bullet = line.startswith(("•", "-", "*"))
        if is_bullet:
            bullet_lines.append(line)
            in_bullets = True
            continue

        if in_bullets:
            _flush_experience_block(roles, header_lines, bullet_lines)
            header_lines = [line]
            bullet_lines = []
            in_bullets = False
            continue

        header_lines.append(line)

    _flush_experience_block(roles, header_lines, bullet_lines)
    return roles


def _parse_projects(lines: list[str]) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    buffer: list[str] = []
    link_markers: list[str] = []

    def flush() -> None:
        nonlocal buffer, link_markers
        if not buffer:
            return
        title = buffer[0]
        description = normalize_whitespace(" ".join(buffer[1:])) if len(buffer) > 1 else ""
        projects.append(
            {
                "title": title,
                "description": description,
                "links": dedupe_preserve_order(link_markers),
            }
        )
        buffer = []
        link_markers = []

    for line in lines:
        lowered = line.lower()
        if lowered in LINK_MARKERS:
            link_markers.append(line)
            flush()
            continue

        if buffer and line[0].isupper() and not line.endswith(".") and len(buffer) > 1:
            flush()

        buffer.append(line)

    flush()
    return projects


def _collapse_multiline_items(lines: list[str]) -> list[str]:
    items: list[str] = []
    for line in lines:
        if not items:
            items.append(line)
            continue

        if re.fullmatch(r"\d{4}", line) or line.lower().startswith(("credential id:", "coursera", "microsoft learn")):
            items[-1] = normalize_whitespace(f"{items[-1]} {line}")
            continue

        if line.lower().startswith(("project management foundations", "verify at:", "credential id:")):
            items[-1] = normalize_whitespace(f"{items[-1]} {line}")
            continue

        items.append(line)
    return items


def _parse_years_experience(summary: str) -> int | None:
    values = [int(match) for match in re.findall(r"(\d+)\+?\s+year", summary.lower())]
    if not values:
        return None
    if len(values) >= 2 and "software engineering" in summary.lower():
        return sum(values[:2])
    return max(values)


def _infer_target_roles(summary: str, experience: list[dict[str, Any]], skills: list[str], projects: list[dict[str, Any]]) -> list[str]:
    inferred_text = " ".join(
        [summary]
        + skills
        + [item.get("title") or "" for item in experience]
        + [item.get("employer") or "" for item in experience]
        + [project.get("title") or "" for project in projects]
        + [project.get("description") or "" for project in projects]
    ).lower()

    scored_roles: list[tuple[int, str]] = []
    for role, keywords in ROLE_RULES:
        score = sum(1 for keyword in keywords if keyword in inferred_text)
        if score:
            scored_roles.append((score, role))

    scored_roles.sort(key=lambda item: (-item[0], item[1]))
    roles = [role for _, role in scored_roles]
    defaults = ["Data Analyst", "Business Intelligence Analyst", "Reporting Analyst"]
    return dedupe_preserve_order(roles + defaults)[:8]


def _infer_top_strengths(summary: str, skills: list[str], experience: list[dict[str, Any]], projects: list[dict[str, Any]]) -> list[str]:
    inferred_text = " ".join(
        [summary]
        + skills
        + [item.get("title") or "" for item in experience]
        + [highlight for item in experience for highlight in item.get("highlights", [])]
        + [project.get("title") or "" for project in projects]
        + [project.get("description") or "" for project in projects]
    ).lower()

    strengths = [
        label
        for label, keywords in TOP_STRENGTH_RULES
        if any(keyword in inferred_text for keyword in keywords)
    ]
    if not strengths:
        strengths = skills[:5]
    return dedupe_preserve_order(strengths)[:5]


def build_candidate_profile(cv_path: Path) -> dict[str, Any]:
    text = extract_pdf_text(cv_path)
    lines = cleaned_lines(text)
    if len(lines) < 4:
        raise ValueError("The CV text could not be parsed into enough structured lines.")

    preamble, sections = _split_sections(lines)
    name = preamble[0] if preamble else None
    phone, email = _extract_contact(preamble)
    links = _extract_links(preamble)

    summary = _parse_summary(sections.get("summary", []))
    focus_areas, skills = _parse_skills(sections.get("skills", []), summary)
    experience = _parse_experience(sections.get("experience", []))
    projects = _parse_projects(sections.get("projects", []))
    certifications = _collapse_multiline_items(sections.get("certifications", []))
    leadership = [strip_bullet_prefix(line) for line in sections.get("leadership", [])]
    education = _collapse_multiline_items(sections.get("education", []))

    target_roles = _infer_target_roles(summary, experience, skills, projects)
    top_strengths = _infer_top_strengths(summary, skills, experience, projects)
    years_experience = _parse_years_experience(summary)

    return {
        "name": name,
        "phone": phone,
        "email": email,
        "summary": summary,
        "years_experience": years_experience,
        "focus_areas": focus_areas,
        "core_competencies": dedupe_preserve_order(focus_areas + top_strengths),
        "skills": skills,
        "links": links,
        "languages": [],
        "target_roles": target_roles,
        "experience": experience,
        "projects": projects,
        "education": education,
        "certifications": certifications,
        "leadership": leadership,
        "application_answers": {
            "professional_summary": summary,
            "top_strengths": top_strengths,
            "preferred_roles": target_roles,
            "resume_path": str(cv_path),
        },
    }


def save_candidate_profile(cv_path: Path, output_path: Path) -> dict[str, Any]:
    profile = build_candidate_profile(cv_path)
    write_json(output_path, profile)
    return profile

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .utils import normalize_whitespace


def _text_snippet(value: str | None, limit: int = 320) -> str | None:
    if not value:
        return None

    cleaned = normalize_whitespace(value)
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def shortlist_rows_as_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "score": float(row["fit_score"]) if row["fit_score"] is not None else None,
                "title": row["title"],
                "company": row["company"],
                "location": row["location"],
                "salary_text": row["salary_text"],
                "posted_at": row["posted_at"],
                "application_url": row["application_url"],
                "job_url": row["job_url"],
                "fit_reason": row["fit_reason"],
                "summary": _text_snippet(row["detail_text"]),
            }
        )
    return result


def render_shortlist_markdown(rows: list[sqlite3.Row]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        "# FlexJobs Shortlist",
        "",
        f"Generated: {timestamp}",
        "",
    ]

    for index, row in enumerate(rows, start=1):
        score = row["fit_score"]
        score_text = f"{score:.1f}" if score is not None else "n/a"
        lines.append(f"## {index}. {row['title']} - {row['company'] or 'Unknown Company'}")
        lines.append("")
        lines.append(f"- Score: {score_text}")
        lines.append(f"- Location: {row['location'] or 'n/a'}")
        if row["salary_text"]:
            lines.append(f"- Salary: {row['salary_text']}")
        if row["posted_at"]:
            lines.append(f"- Posted: {row['posted_at']}")
        if row["application_url"]:
            lines.append(f"- Apply URL: {row['application_url']}")
        lines.append(f"- FlexJobs URL: {row['job_url']}")
        if row["fit_reason"]:
            lines.append(f"- Fit Reason: {row['fit_reason']}")
        summary = _text_snippet(row["detail_text"], limit=420)
        if summary:
            lines.append(f"- Summary: {summary}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_shortlist_markdown(rows: list[sqlite3.Row], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_shortlist_markdown(rows), encoding="utf-8")
    return output_path

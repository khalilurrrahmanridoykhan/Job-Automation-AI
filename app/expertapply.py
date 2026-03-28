from __future__ import annotations

import json
from pathlib import Path
from sqlite3 import Row
from typing import Any

from .application_prep import build_application_packet_with_defaults, write_application_packet
from .config import Settings
from .db import update_job_details, upsert_prepared_application
from .job_search import _connect_to_manual_chrome, _extract_job_detail_payload, _safe_goto, load_candidate_profile
from .utils import normalize_whitespace, playwright_environment_hint


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def is_expertapply_job(row: Row) -> bool:
    raw_payload = _parse_json(row["raw_payload"])
    container_text = normalize_whitespace(raw_payload.get("container_text") or "").lower()
    if "expertapply" in container_text:
        return True

    detail_raw_payload = _parse_json(row["detail_raw_payload"])
    next_data_payload = detail_raw_payload.get("next_data_payload") if isinstance(detail_raw_payload, dict) else None
    if isinstance(next_data_payload, dict):
        next_data_job = next_data_payload.get("next_data_job")
        if isinstance(next_data_job, dict) and bool(next_data_job.get("eligibleForExpertApply")):
            return True

    combined = " ".join(value for value in (row["raw_payload"] or "", row["detail_raw_payload"] or "") if value).lower()
    return "expertapply" in combined or "eligibleforexpertapply" in combined


def search_title_matches(row: Row, search_title: str | None) -> bool:
    if not search_title:
        return True
    raw_payload = _parse_json(row["raw_payload"])
    row_search_title = normalize_whitespace(raw_payload.get("search_title") or "").lower()
    return row_search_title == normalize_whitespace(search_title).lower()


def query_matches(row: Row, query: str | None) -> bool:
    if not query:
        return True
    needle = normalize_whitespace(query).lower()
    haystack = " ".join(
        [
            row["title"] or "",
            row["company"] or "",
            row["location"] or "",
            row["fit_reason"] or "",
            row["detail_text"] or "",
            row["raw_payload"] or "",
        ]
    ).lower()
    return needle in haystack


def exact_company_matches(row: Row, exact_company: str | None) -> bool:
    if not exact_company:
        return True
    company = normalize_whitespace(row["company"] or "").lower()
    return company == normalize_whitespace(exact_company).lower()


def min_fit_score_matches(row: Row, min_fit_score: float | None) -> bool:
    if min_fit_score is None:
        return True
    try:
        fit_score = float(row["fit_score"])
    except (TypeError, ValueError):
        return False
    return fit_score >= min_fit_score


def select_expertapply_jobs(
    rows: list[Row],
    *,
    search_title: str | None,
    query: str | None,
    exact_company: str | None,
    min_fit_score: float | None,
    limit: int | None,
) -> list[Row]:
    selected = [
        row
        for row in rows
        if is_expertapply_job(row)
        and search_title_matches(row, search_title)
        and query_matches(row, query)
        and exact_company_matches(row, exact_company)
        and min_fit_score_matches(row, min_fit_score)
    ]
    if limit is not None:
        return selected[:limit]
    return selected


def enrich_expertapply_jobs(settings: Settings, rows: list[Row]) -> tuple[int, list[dict[str, Any]]]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    updated = 0
    failures: list[dict[str, Any]] = []
    target_rows = [
        row
        for row in rows
        if not (row["application_url"] and row["detail_text"] and normalize_whitespace(row["detail_text"]))
    ]
    if not target_rows:
        return (0, failures)

    with sync_playwright() as playwright:
        browser, context, page = _connect_to_manual_chrome(playwright, settings)
        try:
            for row in target_rows:
                try:
                    _safe_goto(page, row["job_url"], settings.flexjobs_timeout_ms)
                    page.wait_for_timeout(1800)
                    payload = _extract_job_detail_payload(page, row["job_url"])
                    update_job_details(
                        settings.jobs_db_path,
                        int(row["id"]),
                        application_url=payload.get("application_url"),
                        detail_text=payload.get("detail_text"),
                        requirements_text=payload.get("requirements_text"),
                        benefits_text=payload.get("benefits_text"),
                        company_overview_text=payload.get("company_overview_text"),
                        detail_raw_payload=payload,
                    )
                    updated += 1
                except Exception as exc:
                    failures.append(
                        {
                            "job_id": int(row["id"]),
                            "title": row["title"],
                            "error": str(exc),
                        }
                    )
        finally:
            browser.close()

    return (updated, failures)


def prepare_expertapply_applications(
    settings: Settings,
    rows: list[Row],
    *,
    application_defaults: dict[str, Any],
    out_dir: Path,
) -> list[int]:
    profile = load_candidate_profile(settings.candidate_profile_path)
    prepared_ids: list[int] = []
    for row in rows:
        packet = build_application_packet_with_defaults(profile, row, application_defaults)
        application_id = upsert_prepared_application(
            settings.jobs_db_path,
            job_id=int(row["id"]),
            prepared_payload=packet,
            notes="Prepared for ExpertApply automation",
        )
        write_application_packet(packet, out_dir)
        prepared_ids.append(application_id)
    return prepared_ids

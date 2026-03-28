from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from sqlite3 import Row

from .application_prep import application_packet_paths
from .config import Settings
from .job_search import _connect_to_manual_chrome, _safe_goto
from .utils import playwright_environment_hint


@dataclass(slots=True)
class OpenedApplication:
    application_id: int
    job_id: int
    title: str
    company: str | None
    application_url: str
    packet_json_path: Path
    packet_markdown_path: Path


def open_application_pages(
    settings: Settings,
    rows: list[Row],
    *,
    packet_dir: Path,
    dry_run: bool = False,
) -> list[OpenedApplication]:
    opened: list[OpenedApplication] = []
    for row in rows:
        application_url = row["job_application_url"]
        if not application_url:
            continue
        json_path, md_path = application_packet_paths(packet_dir, int(row["job_id"]), row["company"], row["title"])
        opened.append(
            OpenedApplication(
                application_id=int(row["id"]),
                job_id=int(row["job_id"]),
                title=row["title"],
                company=row["company"],
                application_url=application_url,
                packet_json_path=json_path,
                packet_markdown_path=md_path,
            )
        )

    if dry_run or not opened:
        return opened

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    with sync_playwright() as playwright:
        _browser, context, attached_page = _connect_to_manual_chrome(playwright, settings)
        print("Application review mode is open.")
        print("A normal Chrome window is being used for this flow, not a Playwright-launched browser.")
        print(f"Chrome profile: {settings.flexjobs_manual_chrome_profile_dir}")
        print(f"Attached tab: {attached_page.url or 'about:blank'}")

        for item in opened:
            page = context.new_page()
            try:
                page.bring_to_front()
            except Exception:
                pass
            _safe_goto(page, item.application_url, settings.flexjobs_timeout_ms)
            page.wait_for_timeout(1200)

    return opened


def opened_applications_as_dicts(rows: list[OpenedApplication]) -> list[dict[str, object]]:
    return [
        {
            "application_id": row.application_id,
            "job_id": row.job_id,
            "title": row.title,
            "company": row.company,
            "application_url": row.application_url,
            "json_path": str(row.packet_json_path),
            "markdown_path": str(row.packet_markdown_path),
        }
        for row in rows
    ]

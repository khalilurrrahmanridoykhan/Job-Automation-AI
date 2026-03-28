from __future__ import annotations

import re
import socket
import subprocess
import sys
import time
import json
import html
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any

from .config import Settings
from .db import (
    connect,
    fetch_jobs_for_enrichment,
    finish_search_run,
    start_search_run,
    update_job_details,
    upsert_job,
)
from .utils import dedupe_preserve_order, normalize_whitespace, read_json, write_json
from .utils import playwright_environment_hint


FLEXJOBS_HOME_URL = "https://www.flexjobs.com/"
FLEXJOBS_LOGIN_URL = "https://www.flexjobs.com/signin"
FLEXJOBS_SEARCH_URL = "https://www.flexjobs.com/searchOptions.aspx"
NAV_TEXT_BLACKLIST = {
    "find a better way to work",
    "advanced",
    "career advice",
    "how flexjobs works",
    "log in",
    "sign up",
    "new remote jobs hiring now",
    "remote jobs near me",
    "browse remote jobs by category",
    "find your next remote job",
    "find work-from-home jobs",
    "save time",
    "expertapply: auto-apply for jobs",
    "expertapply",
    "featured",
}
TOKEN_RE = re.compile(r"[a-z0-9]+")
TITLE_PREFIX_RE = re.compile(r"^(today\s+)?new!\s*", re.IGNORECASE)
POSTED_AT_RE = re.compile(
    r"\b(?:today|yesterday|\d+\+?\s+(?:day|days|week|weeks|month|months)\s+ago)\b",
    re.IGNORECASE,
)
JOB_LINK_PATTERNS = ("hostedjob.aspx", "/hostedjob.aspx")
LOCATION_HINTS = (
    "100% remote",
    "hybrid remote",
    "no remote",
    "us national",
    "work from anywhere",
    "alternative schedule",
    "flexible schedule",
    "full-time",
    "part-time",
)
DETAIL_HEADING_MAP = {
    "about the role": "description",
    "job description": "description",
    "summary": "description",
    "responsibilities": "description",
    "job duties": "description",
    "what you'll do": "description",
    "what you will do": "description",
    "experience": "requirements",
    "requirements": "requirements",
    "required skills": "requirements",
    "qualifications": "requirements",
    "education level": "requirements",
    "education": "requirements",
    "preferred qualifications": "requirements",
    "minimum qualifications": "requirements",
    "benefits": "benefits",
    "about the company": "company_overview",
    "overview": "company_overview",
}


@dataclass(slots=True)
class CollectedJob:
    title: str
    job_url: str
    company: str | None = None
    location: str | None = None
    salary_text: str | None = None
    posted_at: str | None = None
    application_url: str | None = None
    external_id: str | None = None
    raw_payload: dict[str, Any] | None = None


@dataclass(slots=True)
class EnrichedJob:
    job_id: int
    title: str
    job_url: str
    application_url: str | None = None
    detail_text: str | None = None
    requirements_text: str | None = None
    benefits_text: str | None = None
    company_overview_text: str | None = None
    raw_payload: dict[str, Any] | None = None


def recommended_search_titles(profile: dict[str, Any]) -> list[str]:
    target_roles = profile.get("target_roles", [])
    role_variants: list[str] = []

    for role in target_roles:
        role_variants.append(role)
        if any(token in role for token in ("Analyst", "Engineer", "Developer")):
            role_variants.append(f"{role} Remote")

    skills_lower = {skill.lower() for skill in profile.get("skills", [])}
    if "power bi" in skills_lower:
        role_variants.append("Power BI Developer")
    if "tableau" in skills_lower:
        role_variants.append("Tableau Developer")
    if "etl" in skills_lower:
        role_variants.append("ETL Developer")
    if "sql" in skills_lower:
        role_variants.append("SQL Analyst")

    return dedupe_preserve_order(role_variants)


def save_search_titles(profile: dict[str, Any], output_path: Path) -> list[str]:
    titles = recommended_search_titles(profile)
    write_json(output_path, {"titles": titles})
    return titles


def load_candidate_profile(profile_path: Path) -> dict[str, Any]:
    return read_json(profile_path)


def _resolve_browser_type(playwright, browser_name: str):
    browser_name = browser_name.strip().lower()
    if browser_name not in {"chromium", "firefox", "webkit"}:
        raise ValueError(f"Unsupported browser type: {browser_name}")
    return getattr(playwright, browser_name)


def _launch_context(playwright, settings: Settings):
    browser_type = _resolve_browser_type(playwright, settings.flexjobs_browser)
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(settings.flexjobs_profile_dir),
        "headless": settings.flexjobs_headless,
        "viewport": {"width": 1440, "height": 960},
        "locale": "en-US",
        "ignore_https_errors": True,
    }

    if settings.flexjobs_browser == "chromium":
        launch_kwargs["args"] = [
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
        ]
        if settings.flexjobs_browser_channel:
            launch_kwargs["channel"] = settings.flexjobs_browser_channel

    settings.flexjobs_profile_dir.mkdir(parents=True, exist_ok=True)
    return browser_type.launch_persistent_context(**launch_kwargs)


def _chrome_binary_path(settings: Settings) -> Path:
    candidates: list[Path] = []
    if settings.flexjobs_manual_chrome_binary:
        candidates.append(Path(settings.flexjobs_manual_chrome_binary))

    candidates.extend(
        [
            Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
            Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
            Path("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
            Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
        ]
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise RuntimeError(
        "Could not find a local Chrome-compatible browser binary for manual capture. "
        "Set FLEXJOBS_MANUAL_CHROME_BINARY in .env."
    )


def _is_local_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _launch_manual_chrome_process(settings: Settings) -> subprocess.Popen[str] | None:
    port = settings.flexjobs_manual_chrome_cdp_port
    if _is_local_port_open(port):
        return None

    chrome_binary = _chrome_binary_path(settings)
    profile_dir = settings.flexjobs_manual_chrome_profile_dir
    profile_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(chrome_binary),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--new-window",
        FLEXJOBS_HOME_URL,
    ]

    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    deadline = time.time() + 15
    while time.time() < deadline:
        if _is_local_port_open(port):
            return process
        time.sleep(0.25)

    raise RuntimeError(
        f"Chrome did not expose the remote debugging port {port}. "
        "Close any stale Chrome debugging sessions and try again."
    )


def _connect_to_manual_chrome(playwright, settings: Settings):
    _launch_manual_chrome_process(settings)
    cdp_url = f"http://127.0.0.1:{settings.flexjobs_manual_chrome_cdp_port}"
    browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=settings.flexjobs_timeout_ms)
    if not browser.contexts:
        raise RuntimeError("Connected to Chrome DevTools, but no default browser context is available.")
    context = browser.contexts[0]
    page = context.pages[-1] if context.pages else context.new_page()
    page = _ensure_manual_capture_page(context, page, settings.flexjobs_timeout_ms)
    return browser, context, page


def _get_or_create_page(context):
    return context.pages[0] if context.pages else context.new_page()


def _page_is_open(page) -> bool:
    try:
        return not page.is_closed()
    except Exception:
        return False


def _safe_page_url(page) -> str:
    if not _page_is_open(page):
        return ""

    try:
        return page.url or ""
    except Exception:
        return ""


def _safe_page_title(page) -> str:
    if not _page_is_open(page):
        return ""

    try:
        return page.title() or ""
    except Exception:
        return ""


def _select_manual_capture_page(context, fallback_page=None):
    candidates = [page for page in context.pages if _page_is_open(page)]
    if fallback_page is not None and _page_is_open(fallback_page) and fallback_page not in candidates:
        candidates.append(fallback_page)

    if not candidates:
        return context.new_page()

    def rank(candidate) -> tuple[int, int]:
        current_url = _safe_page_url(candidate).lower()
        current_title = _safe_page_title(candidate).lower()
        flexjobs_bonus = 1 if "flexjobs.com" in current_url or "flexjobs" in current_title else 0
        search_bonus = 1 if "search" in current_url or "job search results" in current_title else 0
        url_bonus = 1 if current_url and current_url != "about:blank" else 0
        return (flexjobs_bonus * 10 + search_bonus * 5 + url_bonus, len(current_url))

    selected = max(candidates, key=rank)
    try:
        selected.bring_to_front()
    except Exception:
        pass
    return selected


def _manual_capture_pages(context, fallback_page=None) -> list[Any]:
    pages: list[Any] = []
    seen_ids: set[int] = set()
    candidates = list(context.pages)
    if fallback_page is not None:
        candidates.append(fallback_page)

    for page in candidates:
        if not _page_is_open(page):
            continue
        marker = id(page)
        if marker in seen_ids:
            continue
        seen_ids.add(marker)
        pages.append(page)

    return pages


def _ensure_manual_capture_page(context, page, timeout_ms: int):
    selected = _select_manual_capture_page(context, fallback_page=page)
    if _safe_page_url(selected).lower() in {"", "about:blank"}:
        try:
            _safe_goto(selected, FLEXJOBS_HOME_URL, timeout_ms)
        except Exception:
            pass
        selected = _select_manual_capture_page(context, fallback_page=selected)
    return selected


def _safe_goto(page, url: str, timeout_ms: int) -> None:
    last_error: Exception | None = None
    for wait_until in ("domcontentloaded", "load", "commit"):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            return
        except Exception as exc:  # pragma: no cover - network/site dependent
            last_error = exc
    if last_error:
        raise last_error


def _first_visible_locator(page, selectors: list[str]):
    for selector in selectors:
        locator = page.locator(selector)
        count = locator.count()
        for index in range(count):
            candidate = locator.nth(index)
            try:
                if candidate.is_visible() and candidate.is_enabled():
                    return candidate
            except Exception:
                continue
    return None


def _is_logged_in(page) -> bool:
    current_url = _safe_page_url(page).lower()
    if "/members" in current_url:
        return True
    if "searchoptions.aspx" in current_url:
        return True
    return False


def _is_login_gate(page) -> bool:
    if not _page_is_open(page):
        return False

    current_url = _safe_page_url(page).lower()
    current_title = _safe_page_title(page).lower()
    password_locator = _first_visible_locator(
        page,
        [
            'input[type="password"]',
            'input[name="password"]',
        ],
    )
    email_locator = _first_visible_locator(
        page,
        [
            '#email',
            'input[name="email"]',
            'input[type="email"]',
            'input[type="text"]',
        ],
    )
    if "signin" in current_url or "login" in current_url:
        return True
    if current_title.startswith("login"):
        return True
    if password_locator is not None:
        return True
    if email_locator is not None and page.locator("#login-submit").count():
        return True
    return False


def _is_access_denied_page(page) -> bool:
    if not _page_is_open(page):
        return False

    current_title = _safe_page_title(page).lower()
    if "access denied" in current_title:
        return True

    try:
        body_text = page.locator("body").inner_text(timeout=2000).lower()
    except Exception:
        body_text = ""

    if "access denied" in body_text or "you don't have permission to access" in body_text:
        return True
    if "powered and protected by" in body_text and "akamai" in body_text:
        return True

    try:
        html_text = page.content().lower()
    except Exception:
        return False

    return 'id="sec-if-cpt-container"' in html_text or "akamai" in html_text


def _find_best_manual_capture_page(
    context,
    fallback_page,
    *,
    search_title: str,
    limit: int,
) -> tuple[Any | None, list[CollectedJob], list[str]]:
    best_page = None
    best_jobs: list[CollectedJob] = []
    inspected: list[str] = []

    for candidate in _manual_capture_pages(context, fallback_page=fallback_page):
        current_url = _safe_page_url(candidate) or "about:blank"
        current_title = _safe_page_title(candidate) or "(untitled)"

        if not current_url.startswith(("http://", "https://")):
            inspected.append(f"{current_title} | {current_url} | skipped")
            continue

        if "flexjobs.com" not in current_url.lower() and "flexjobs" not in current_title.lower():
            inspected.append(f"{current_title} | {current_url} | skipped")
            continue

        if _is_login_gate(candidate):
            inspected.append(f"{current_title} | {current_url} | login")
            continue

        if _is_access_denied_page(candidate):
            inspected.append(f"{current_title} | {current_url} | access denied")
            continue

        try:
            jobs = _extract_job_candidates(candidate, search_title, limit)
        except Exception as exc:
            inspected.append(f"{current_title} | {current_url} | parse error: {exc}")
            continue

        inspected.append(f"{current_title} | {current_url} | jobs={len(jobs)}")
        if len(jobs) > len(best_jobs):
            best_page = candidate
            best_jobs = jobs

    return best_page, best_jobs, inspected


def _open_password_login_mode(page) -> None:
    password_toggle = page.get_by_text("Log in using password", exact=False)
    if password_toggle.count():
        password_toggle.first.click()
        page.wait_for_timeout(1000)


def open_flexjobs_browser(settings: Settings) -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    with sync_playwright() as playwright:
        context = _launch_context(playwright, settings)
        page = _get_or_create_page(context)

        try:
            _safe_goto(page, FLEXJOBS_LOGIN_URL, settings.flexjobs_timeout_ms)
        except Exception:
            try:
                _safe_goto(page, FLEXJOBS_HOME_URL, settings.flexjobs_timeout_ms)
            except Exception:
                pass

        if not _is_logged_in(page) and settings.flexjobs_email and settings.flexjobs_password:
            _open_password_login_mode(page)
            email_locator = _first_visible_locator(
                page,
                [
                    '#email',
                    'form input[type="email"]',
                    'form input[type="text"]',
                    'form input[name="email"]',
                    'form input[name="username"]',
                    'input[type="email"]',
                    'input[type="text"]',
                    'input[name="email"]',
                    'input[name="username"]',
                ],
            )
            password_locator = _first_visible_locator(
                page,
                [
                    '#password',
                    'input[type="password"]',
                    'input[name="password"]',
                ],
            )
            if email_locator and password_locator:
                try:
                    email_locator.fill(settings.flexjobs_email)
                    password_locator.fill(settings.flexjobs_password)
                except Exception as exc:
                    print(f"Automatic credential fill was skipped: {exc}")
                    print("Continue with manual login in the open browser window.")
            else:
                print("Could not find the full password login form automatically.")
                print("If needed, click 'Log in using password' in the browser, then log in manually.")

        print("FlexJobs browser session is open.")
        print("Use this persistent profile to log in manually if a captcha or anti-bot check appears.")
        print("Do not press Enter in the terminal until you are fully logged in on FlexJobs.")
        print(f"Persistent profile: {settings.flexjobs_profile_dir}")

        try:
            _safe_goto(page, FLEXJOBS_SEARCH_URL, settings.flexjobs_timeout_ms)
            print(f"Search page opened: {page.url}")
        except Exception as exc:
            print(f"Could not preload the FlexJobs search page automatically: {exc}")
            print("Navigate there manually in the open browser if needed.")

        if sys.stdin.isatty():
            input("Press Enter after you finish logging in and want to close this browser session...")
        else:
            print("Non-interactive session detected. Keeping the browser open for 600 seconds.")
            page.wait_for_timeout(600000)

        context.close()


def _search_tokens(search_title: str) -> set[str]:
    return {
        token
        for token in TOKEN_RE.findall(search_title.lower())
        if token not in {"remote", "work", "from", "home"}
    }


def _clean_job_title(value: str) -> str:
    cleaned = normalize_whitespace(value)
    cleaned = TITLE_PREFIX_RE.sub("", cleaned)
    return cleaned.strip(" -|")


def _container_lines(container_text: str) -> list[str]:
    return [
        normalize_whitespace(line)
        for line in re.split(r"[\r\n]+", container_text)
        if normalize_whitespace(line)
    ]


def _clean_company_candidate(value: str, title: str) -> str | None:
    candidate = normalize_whitespace(value)
    candidate = re.sub(r"^(?:expertapply|featured)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\b(?:new!|featured)\b", "", candidate, flags=re.IGNORECASE)
    candidate = normalize_whitespace(candidate.strip(" -|,"))

    if not candidate:
        return None

    candidate_lower = candidate.lower()
    title_lower = title.lower()
    if candidate_lower == title_lower or candidate_lower.startswith(title_lower):
        return None
    if candidate_lower in NAV_TEXT_BLACKLIST:
        return None
    if any(hint in candidate_lower for hint in LOCATION_HINTS):
        return None
    if POSTED_AT_RE.search(candidate):
        return None
    if _extract_salary(candidate):
        return None
    return candidate


def _extract_company_from_flat_text(container_text: str, title: str) -> str | None:
    flat_text = normalize_whitespace(container_text)
    flat_text = re.sub(r"^(?:expertapply|featured)\s+", "", flat_text, flags=re.IGNORECASE)
    title_match = re.search(re.escape(title) + r"(?:\s*new!)?\s+(?P<tail>.+)", flat_text, re.IGNORECASE)
    if not title_match:
        return None

    tail = title_match.group("tail").strip()
    posted_match = POSTED_AT_RE.search(tail)
    if posted_match:
        candidate = tail[: posted_match.start()]
    else:
        metadata_match = re.search(
            r"\b(?:100% remote work|hybrid remote work|100% remote|hybrid remote|us national|"
            r"work from anywhere|full-time employee|part-time employee|temporary|employee)\b",
            tail,
            re.IGNORECASE,
        )
        candidate = tail[: metadata_match.start()] if metadata_match else tail

    return _clean_company_candidate(candidate, title)


def _extract_company(container_text: str, title: str) -> str | None:
    company = _extract_company_from_flat_text(container_text, title)
    if company:
        return company

    lines = _container_lines(container_text)
    for line in lines:
        company = _clean_company_candidate(line, title)
        if company is None:
            continue
        return company

    return None


def _extract_posted_at(container_text: str) -> str | None:
    posted_match = POSTED_AT_RE.search(normalize_whitespace(container_text))
    if posted_match:
        return posted_match.group(0)
    return None


def _clean_location_candidate(value: str) -> str | None:
    candidate = normalize_whitespace(value.strip(" -|,"))
    if not candidate:
        return None

    if "US National" in candidate:
        return "US National"
    if re.fullmatch(r"(?:[A-Z]{2})(?:,\s*[A-Z]{2}){1,}", candidate):
        return candidate
    if any(token in candidate for token in (", Canada", ", United Kingdom", ", Australia", ", Colombia")):
        return candidate
    if re.search(r"\b[A-Z][A-Za-z.'-]+,\s*[A-Z]{2,3}\b", candidate):
        return candidate
    return None


def _extract_location_from_flat_text(container_text: str) -> str | None:
    flat_text = normalize_whitespace(container_text)
    salary_text = _extract_salary(flat_text)
    if salary_text:
        salary_index = flat_text.rfind(salary_text)
        if salary_index >= 0:
            candidate = _clean_location_candidate(flat_text[salary_index + len(salary_text) :])
            if candidate:
                return candidate

    for match in reversed(
        list(
            re.finditer(
                r"\b(?:employee|temporary|contract|freelance|alternative schedule|full-time|part-time|"
                r"100% remote work|hybrid remote work|work from anywhere)\b",
                flat_text,
                re.IGNORECASE,
            )
        )
    ):
        candidate = _clean_location_candidate(flat_text[match.end() :])
        if candidate:
            return candidate

    explicit_locations = re.findall(r"[A-Z][A-Za-z.'-]+,\s*[A-Z]{2,3}(?:,\s*[A-Z][A-Za-z.'-]+,\s*[A-Z]{2,3})*", flat_text)
    if explicit_locations:
        return explicit_locations[-1]

    state_list_match = re.search(r"\b(?:[A-Z]{2})(?:,\s*[A-Z]{2}){1,}\b", flat_text)
    if state_list_match:
        return state_list_match.group(0)

    return _clean_location_candidate(flat_text)


def _extract_location(container_text: str) -> str | None:
    lines = _container_lines(container_text)

    for line in lines:
        line_lower = line.lower()
        if any(
            token in line_lower
            for token in ("expertapply", "employee", "today", "yesterday", "ago", "annually", "hourly")
        ):
            continue
        if re.search(r"\b[A-Z][a-z]+,\s*[A-Z]{2}\b", line):
            return line
        if any(token in line for token in (", Canada", ", United Kingdom", ", Australia", ", Colombia")):
            return line
        if "US National" in line:
            return "US National"

    flat_text = normalize_whitespace(container_text)
    flat_location = _extract_location_from_flat_text(flat_text)
    if flat_location:
        return flat_location

    low = flat_text.lower()
    for hint in LOCATION_HINTS:
        if hint in low:
            return hint.title()
    return None


def _company_needs_repair(company: str | None, title: str) -> bool:
    if not company:
        return True

    company_lower = normalize_whitespace(company).lower()
    title_lower = title.lower()
    if "new!" in company_lower:
        return True
    if company_lower == title_lower:
        return True
    if company_lower.startswith(title_lower):
        return True
    return False


def _extract_salary(container_text: str) -> str | None:
    salary_match = re.search(
        r"\d[\d,]*(?:\.\d+)?\s*-\s*\d[\d,]*(?:\.\d+)?\s+[A-Z]{3}\s+(?:Annually|Hourly)",
        container_text,
    )
    if salary_match:
        return salary_match.group(0)

    salary_match = re.search(r"\$\d[\d,]*(?:\s*-\s*\$\d[\d,]*)?", container_text)
    if salary_match:
        return salary_match.group(0)
    return None


def _extract_job_id(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    job_ids = query.get("id")
    return job_ids[0] if job_ids else None


def _candidate_rank(candidate: dict[str, Any], search_tokens: set[str]) -> tuple[int, int]:
    haystack = f"{candidate['text']} {candidate['container_text']}".lower()
    token_matches = len(search_tokens & set(TOKEN_RE.findall(haystack)))
    remote_bonus = 1 if "remote" in haystack or "hybrid" in haystack else 0
    return (token_matches, remote_bonus)


def _is_probable_job(candidate: dict[str, Any], search_tokens: set[str]) -> bool:
    text = candidate["text"].strip()
    href = candidate["href"].strip()
    if not text or not href:
        return False

    href_lower = href.lower()
    if not any(pattern in href_lower for pattern in JOB_LINK_PATTERNS):
        return False

    low_text = text.lower()
    if low_text in NAV_TEXT_BLACKLIST:
        return False
    if len(text.split()) < 2 or len(text) > 180:
        return False

    haystack_tokens = set(TOKEN_RE.findall(f"{candidate['text']} {candidate['container_text']}".lower()))
    overlap = len(search_tokens & haystack_tokens)
    return overlap > 0


def repair_saved_jobs(db_path: Path) -> int:
    repaired_count = 0
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, title, company, location, salary_text, posted_at, raw_payload
            FROM jobs
            WHERE raw_payload IS NOT NULL AND TRIM(raw_payload) != ''
            """
        ).fetchall()

        for row in rows:
            try:
                payload = json.loads(row["raw_payload"])
            except (TypeError, json.JSONDecodeError):
                continue

            if not isinstance(payload, dict):
                continue

            container_text = payload.get("container_text") or payload.get("text")
            if not isinstance(container_text, str) or not container_text.strip():
                continue

            new_company = row["company"]
            new_location = row["location"]
            new_salary_text = row["salary_text"]
            new_posted_at = row["posted_at"]

            if _company_needs_repair(row["company"], row["title"]):
                extracted_company = _extract_company(container_text, row["title"])
                if extracted_company:
                    new_company = extracted_company

            if not new_location:
                extracted_location = _extract_location(container_text)
                if extracted_location:
                    new_location = extracted_location

            if not new_salary_text:
                extracted_salary = _extract_salary(container_text)
                if extracted_salary:
                    new_salary_text = extracted_salary

            if not new_posted_at:
                extracted_posted_at = _extract_posted_at(container_text)
                if extracted_posted_at:
                    new_posted_at = extracted_posted_at

            if (
                new_company != row["company"]
                or new_location != row["location"]
                or new_salary_text != row["salary_text"]
                or new_posted_at != row["posted_at"]
            ):
                conn.execute(
                    """
                    UPDATE jobs
                    SET company = ?,
                        location = ?,
                        salary_text = ?,
                        posted_at = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (new_company, new_location, new_salary_text, new_posted_at, row["id"]),
                )
                repaired_count += 1

    return repaired_count


def _extract_job_candidates(page, search_title: str, limit: int) -> list[CollectedJob]:
    search_tokens = _search_tokens(search_title)
    raw_candidates = page.locator('a[href*="HostedJob.aspx"], a[href*="hostedjob.aspx"]').evaluate_all(
        """
        links => {
          function jobIdFromHref(href) {
            try {
              const url = new URL(href, window.location.origin);
              return url.searchParams.get('id');
            } catch (err) {
              return null;
            }
          }

          function pickContainer(anchor) {
            const jobId = jobIdFromHref(anchor.href);
            let node = anchor;
            for (let depth = 0; depth < 6 && node; depth += 1, node = node.parentElement) {
              if (jobId && node.id === jobId) {
                return (node.innerText || '').trim();
              }
              const text = (node.innerText || '').trim();
              const lineCount = text.split('\\n').filter(Boolean).length;
              if (text && text.length <= 1600 && lineCount >= 6) {
                return text;
              }
            }
            return (anchor.parentElement?.innerText || '').trim();
          }

          return links.map(anchor => ({
            text: (anchor.innerText || anchor.textContent || '').trim(),
            href: anchor.href || '',
            job_id: jobIdFromHref(anchor.href),
            aria_label: anchor.getAttribute('aria-label') || '',
            container_text: pickContainer(anchor),
          }));
        }
        """
    )

    probable = [candidate for candidate in raw_candidates if _is_probable_job(candidate, search_tokens)]
    probable.sort(key=lambda candidate: _candidate_rank(candidate, search_tokens), reverse=True)

    jobs: list[CollectedJob] = []
    seen_urls: set[str] = set()
    for candidate in probable:
        url = candidate["href"].split("#", 1)[0].strip()
        if url in seen_urls:
            continue
        seen_urls.add(url)

        title = _clean_job_title(candidate["text"])
        container_text = normalize_whitespace(candidate["container_text"])
        jobs.append(
            CollectedJob(
                title=title,
                job_url=url,
                company=_extract_company(candidate["container_text"], title),
                location=_extract_location(candidate["container_text"]),
                salary_text=_extract_salary(candidate["container_text"]),
                posted_at=_extract_posted_at(candidate["container_text"]),
                raw_payload={
                    "text": candidate["text"],
                    "container_text": container_text,
                    "search_title": search_title,
                    "page_url": page.url,
                },
                external_id=candidate.get("job_id") or _extract_job_id(url),
            )
        )
        if len(jobs) >= limit:
            break

    return jobs


def _section_key_for_line(line: str) -> str | None:
    normalized = normalize_whitespace(line).strip(" :").lower()
    if not normalized or len(normalized) > 80:
        return None
    return DETAIL_HEADING_MAP.get(normalized)


def _split_detail_sections(detail_text: str) -> dict[str, str]:
    lines = [
        line
        for line in (normalize_whitespace(raw_line) for raw_line in detail_text.splitlines())
        if line
    ]
    sections: dict[str, list[str]] = {"description": []}
    current_key = "description"

    for line in lines:
        section_key = _section_key_for_line(line)
        if section_key:
            current_key = section_key
            sections.setdefault(current_key, [])
            continue
        sections.setdefault(current_key, []).append(line)

    return {
        key: "\n".join(values).strip()
        for key, values in sections.items()
        if any(value.strip() for value in values)
    }


def _extract_detail_text_candidates(page) -> list[dict[str, Any]]:
    return page.locator("main, article, [role='main'], section").evaluate_all(
        """
        nodes => nodes
          .map((node, index) => {
            const text = (node.innerText || '').trim();
            return {
              index,
              tag: node.tagName.toLowerCase(),
              text,
              length: text.length,
            };
          })
          .filter(item => item.length >= 300)
          .sort((a, b) => b.length - a.length)
          .slice(0, 8)
        """
    )


def _extract_apply_candidates(page) -> list[dict[str, str]]:
    return page.locator("a[href]").evaluate_all(
        """
        anchors => anchors
          .map(anchor => ({
            text: (anchor.innerText || anchor.textContent || '').trim(),
            href: anchor.href || '',
          }))
          .filter(item => item.href && !item.href.startsWith('javascript:'))
          .filter(item => /(apply|expertapply|company site|company website|submit application|apply now)/i.test(item.text))
          .slice(0, 12)
        """
    )


def _pick_application_url(job_url: str, apply_candidates: list[dict[str, str]]) -> str | None:
    normalized_job_url = job_url.split("#", 1)[0]
    for candidate in apply_candidates:
        href = candidate.get("href", "").split("#", 1)[0].strip()
        if not href or href == normalized_job_url:
            continue
        if href.rstrip("/").lower() == "https://www.flexjobs.com/expertapply/applications":
            continue
        return href
    return None


def _html_to_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", value)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</li\s*>", "\n", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)
    text = re.sub(r"(?i)</?(ul|ol|div|section|article|strong|em|span)[^>]*>", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    lines = [normalize_whitespace(line) for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _extract_next_data_payload(page) -> dict[str, Any] | None:
    try:
        script_text = page.locator("script#__NEXT_DATA__").inner_text(timeout=5000)
    except Exception:
        return None

    if not script_text.strip():
        return None

    try:
        data = json.loads(script_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None
    return data


def _extract_job_detail_from_next_data(next_data: dict[str, Any]) -> dict[str, Any] | None:
    page_props = next_data.get("props", {}).get("pageProps", {})
    job_list = page_props.get("jobList")
    if not isinstance(job_list, dict):
        return None

    detail_text = _html_to_text(job_list.get("description"))
    summary_text = normalize_whitespace(job_list.get("jobSummary") or "")
    if summary_text and summary_text not in detail_text:
        detail_text = f"{summary_text}\n\n{detail_text}".strip()

    company = job_list.get("company") if isinstance(job_list.get("company"), dict) else {}
    company_description = _html_to_text(company.get("description"))
    benefits = job_list.get("jobBenefits") if isinstance(job_list.get("jobBenefits"), list) else []
    benefits_text = "\n".join(f"- {normalize_whitespace(str(item))}" for item in benefits if normalize_whitespace(str(item)))
    requirements_parts: list[str] = []

    education_levels = job_list.get("educationLevels") if isinstance(job_list.get("educationLevels"), list) else []
    if education_levels:
        requirements_parts.append("Education:\n" + "\n".join(f"- {normalize_whitespace(str(item))}" for item in education_levels))

    career_levels = job_list.get("careerLevel") if isinstance(job_list.get("careerLevel"), list) else []
    if career_levels:
        requirements_parts.append("Career Level:\n" + "\n".join(f"- {normalize_whitespace(str(item))}" for item in career_levels))

    remote_options = job_list.get("remoteOptions") if isinstance(job_list.get("remoteOptions"), list) else []
    if remote_options:
        requirements_parts.append("Remote Options:\n" + "\n".join(f"- {normalize_whitespace(str(item))}" for item in remote_options))

    job_types = job_list.get("jobTypes") if isinstance(job_list.get("jobTypes"), list) else []
    if job_types:
        requirements_parts.append("Job Types:\n" + "\n".join(f"- {normalize_whitespace(str(item))}" for item in job_types))

    job_schedules = job_list.get("jobSchedules") if isinstance(job_list.get("jobSchedules"), list) else []
    if job_schedules:
        requirements_parts.append("Schedules:\n" + "\n".join(f"- {normalize_whitespace(str(item))}" for item in job_schedules))

    travel_required = normalize_whitespace(job_list.get("travelRequired") or "")
    if travel_required and travel_required.lower() != "no specification":
        requirements_parts.append(f"Travel Required:\n- {travel_required}")

    categories = job_list.get("categories") if isinstance(job_list.get("categories"), list) else []
    category_names = [normalize_whitespace(str(item.get("name"))) for item in categories if isinstance(item, dict)]
    category_names = [name for name in category_names if name]
    if category_names:
        requirements_parts.append("Categories:\n" + "\n".join(f"- {name}" for name in category_names))

    requirements_text = "\n\n".join(part for part in requirements_parts if part).strip() or None
    application_url = normalize_whitespace(job_list.get("applyURL") or "") or None

    return {
        "detail_text": detail_text or None,
        "requirements_text": requirements_text,
        "benefits_text": benefits_text or None,
        "company_overview_text": company_description or None,
        "application_url": application_url,
        "next_data_job": job_list,
        "next_data_company": company,
    }


def _extract_job_detail_payload(page, job_url: str) -> dict[str, Any]:
    next_data = _extract_next_data_payload(page)
    next_data_payload = _extract_job_detail_from_next_data(next_data) if next_data else None

    detail_candidates = _extract_detail_text_candidates(page)
    body_text = ""
    try:
        body_text = page.locator("body").inner_text(timeout=4000).strip()
    except Exception:
        body_text = ""

    detail_text = next_data_payload.get("detail_text") if next_data_payload else None
    if not detail_text:
        detail_text = detail_candidates[0]["text"] if detail_candidates else body_text
    detail_text = (detail_text or "").strip()
    sections = _split_detail_sections(detail_text) if detail_text else {}
    apply_candidates = _extract_apply_candidates(page)
    application_url = None
    if next_data_payload:
        application_url = next_data_payload.get("application_url")
    if not application_url:
        application_url = _pick_application_url(job_url, apply_candidates)

    return {
        "page_title": _safe_page_title(page),
        "page_url": _safe_page_url(page),
        "detail_text": detail_text,
        "requirements_text": (next_data_payload or {}).get("requirements_text") or sections.get("requirements"),
        "benefits_text": (next_data_payload or {}).get("benefits_text") or sections.get("benefits"),
        "company_overview_text": (next_data_payload or {}).get("company_overview_text") or sections.get("company_overview"),
        "application_url": application_url,
        "apply_candidates": apply_candidates,
        "detail_candidates": detail_candidates,
        "sections": sections,
        "body_text": body_text,
        "next_data_present": bool(next_data),
        "next_data_payload": next_data_payload,
    }


def enrich_jobs(
    settings: Settings,
    *,
    limit: int = 20,
    job_ids: list[int] | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> list[EnrichedJob]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    rows = fetch_jobs_for_enrichment(
        settings.jobs_db_path,
        limit=limit,
        job_ids=job_ids,
        force=force,
    )
    if not rows:
        return []

    with sync_playwright() as playwright:
        browser, context, page = _connect_to_manual_chrome(playwright, settings)
        page = _ensure_manual_capture_page(context, page, settings.flexjobs_timeout_ms)

        print("Job enrichment mode is open.")
        print("A normal Chrome window is being used for this flow, not a Playwright-launched browser.")
        print(f"Chrome profile: {settings.flexjobs_manual_chrome_profile_dir}")
        print(f"Attached tab: {_safe_page_url(page) or 'about:blank'}")
        print("Log in to FlexJobs in that Chrome window if needed, then press Enter to start detail extraction.")

        if sys.stdin.isatty():
            input("Press Enter to start enriching saved jobs...")
        else:
            print("Non-interactive session detected. Waiting 120 seconds before enrichment starts.")
            page.wait_for_timeout(120000)

        work_page = context.new_page()
        try:
            work_page.bring_to_front()
        except Exception:
            pass

        enriched_jobs: list[EnrichedJob] = []
        for row in rows:
            _safe_goto(work_page, row["job_url"], settings.flexjobs_timeout_ms)
            work_page.wait_for_timeout(1500)

            if _is_login_gate(work_page):
                raise RuntimeError(
                    "FlexJobs opened a login page during enrichment. Log in again in the Chrome window, "
                    "then rerun enrich-jobs."
                )

            if _is_access_denied_page(work_page):
                raise RuntimeError(
                    f"FlexJobs returned an Access Denied page while enriching job id={row['id']}."
                )

            payload = _extract_job_detail_payload(work_page, row["job_url"])
            if not payload.get("detail_text"):
                raise RuntimeError(
                    f"Could not extract detail text for job id={row['id']} at {row['job_url']}."
                )

            enriched_job = EnrichedJob(
                job_id=int(row["id"]),
                title=row["title"],
                job_url=row["job_url"],
                application_url=payload.get("application_url"),
                detail_text=payload.get("detail_text"),
                requirements_text=payload.get("requirements_text"),
                benefits_text=payload.get("benefits_text"),
                company_overview_text=payload.get("company_overview_text"),
                raw_payload=payload,
            )
            enriched_jobs.append(enriched_job)

            if not dry_run:
                update_job_details(
                    settings.jobs_db_path,
                    int(row["id"]),
                    application_url=enriched_job.application_url,
                    detail_text=enriched_job.detail_text,
                    requirements_text=enriched_job.requirements_text,
                    benefits_text=enriched_job.benefits_text,
                    company_overview_text=enriched_job.company_overview_text,
                    detail_raw_payload=enriched_job.raw_payload,
                )

        try:
            work_page.close()
        except Exception:
            pass

    return enriched_jobs


def _prepare_search_form(page, search_title: str, open_to_anywhere_us: bool, timeout_ms: int) -> None:
    _safe_goto(page, FLEXJOBS_SEARCH_URL, timeout_ms)
    page.wait_for_timeout(1500)

    if _is_login_gate(page):
        raise RuntimeError(
            "FlexJobs redirected the collector to the login page. Open the browser with "
            "'python3 -m app.main open-flexjobs', finish logging in completely, and then retry collect-jobs."
        )

    keyword_locator = _first_visible_locator(
        page,
        [
            'input[name*="keyword" i]',
            'input[id*="keyword" i]',
            'input[placeholder*="keyword" i]',
            'main input[type="text"]',
            'form input[type="text"]',
        ],
    )
    if keyword_locator is None:
        raise RuntimeError("Could not find the FlexJobs keyword input on the search page.")

    keyword_locator.fill("")
    keyword_locator.fill(search_title)

    if open_to_anywhere_us:
        anywhere_locator = _first_visible_locator(
            page,
            [
                'input[type="checkbox"][name*="anywhere" i]',
                'input[type="checkbox"][id*="anywhere" i]',
            ],
        )
        if anywhere_locator and not anywhere_locator.is_checked():
            anywhere_locator.check()
        else:
            label_locator = page.get_by_text("Open to candidates anywhere in U.S.", exact=False)
            if label_locator.count():
                label_locator.first.click()

    search_button = _first_visible_locator(
        page,
        [
            'button:has-text("Search for Jobs")',
            'input[type="submit"][value*="Search"]',
            'button:has-text("Search")',
        ],
    )
    if search_button is None:
        raise RuntimeError("Could not find the FlexJobs search submit button.")

    search_button.click()
    page.wait_for_timeout(3000)

    if _is_access_denied_page(page):
        raise RuntimeError(
            "FlexJobs returned an Access Denied page for the automated search request. "
            "Use the manual capture flow instead: 'python3 -m app.main collect-manual-page --title \"Data Analyst\"'."
        )


def _dump_debug_artifacts(settings: Settings, page, slug: str) -> tuple[Path, Path]:
    screenshots_dir = settings.data_dir / "screenshots"
    logs_dir = settings.data_dir / "logs"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    screenshot_path = screenshots_dir / f"{slug}.png"
    html_path = logs_dir / f"{slug}.html"

    page.screenshot(path=str(screenshot_path), full_page=True)
    html_path.write_text(page.content(), encoding="utf-8")
    return screenshot_path, html_path


def _should_retry_collect_with_manual_session(error: Exception) -> bool:
    message = normalize_whitespace(str(error)).lower()
    return (
        "login page" in message
        or "login gate" in message
        or "access denied" in message
        or "akamai" in message
        or "challenge" in message
    )


def collect_jobs(
    settings: Settings,
    *,
    titles: list[str] | None = None,
    limit_per_title: int = 10,
    open_to_anywhere_us: bool = True,
    dry_run: bool = False,
) -> list[CollectedJob]:
    profile = load_candidate_profile(settings.candidate_profile_path)
    effective_titles = titles or recommended_search_titles(profile)
    save_search_titles(profile, settings.search_titles_path)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    collected_jobs: list[CollectedJob] = []
    with sync_playwright() as playwright:
        context = _launch_context(playwright, settings)
        page = _get_or_create_page(context)
        manual_browser = None
        using_manual_context = False

        def collect_title_jobs(current_page, title: str) -> list[CollectedJob]:
            _prepare_search_form(current_page, title, open_to_anywhere_us, settings.flexjobs_timeout_ms)
            if _is_login_gate(current_page):
                raise RuntimeError(
                    "FlexJobs is showing a login gate. Run 'python3 -m app.main open-flexjobs' "
                    "and complete the login in the persistent browser profile first."
                )
            jobs = _extract_job_candidates(current_page, title, limit_per_title)
            if not jobs and _is_access_denied_page(current_page):
                raise RuntimeError(
                    "FlexJobs returned an Akamai or access challenge page. "
                    "Open a manual browser session with 'python3 -m app.main open-flexjobs' and finish the challenge first."
                )
            return jobs

        for title in effective_titles:
            run_id = start_search_run(settings.jobs_db_path, title)
            try:
                try:
                    jobs = collect_title_jobs(page, title)
                except Exception as exc:
                    if not using_manual_context and _should_retry_collect_with_manual_session(exc):
                        try:
                            context.close()
                        except Exception:
                            pass
                        manual_browser, context, page = _connect_to_manual_chrome(playwright, settings)
                        page = _ensure_manual_capture_page(context, page, settings.flexjobs_timeout_ms)
                        using_manual_context = True
                        jobs = collect_title_jobs(page, title)
                    else:
                        raise

                if not jobs:
                    screenshot_path, html_path = _dump_debug_artifacts(
                        settings,
                        page,
                        slug=f"collect-no-results-{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}",
                    )
                    finish_search_run(
                        settings.jobs_db_path,
                        run_id,
                        status="empty",
                        found_count=0,
                        notes=f"No jobs parsed. Screenshot: {screenshot_path.name}; HTML: {html_path.name}",
                    )
                    continue

                if not dry_run:
                    for job in jobs:
                        upsert_job(
                            settings.jobs_db_path,
                            {
                                "title": job.title,
                                "company": job.company,
                                "location": job.location,
                                "salary_text": job.salary_text,
                                "job_url": job.job_url,
                                "application_url": job.application_url,
                                "posted_at": job.posted_at,
                                "external_id": job.external_id,
                                "raw_payload": job.raw_payload,
                            },
                        )

                collected_jobs.extend(jobs)
                finish_search_run(
                    settings.jobs_db_path,
                    run_id,
                    status="completed",
                    found_count=len(jobs),
                    notes=f"Collected from {page.url}",
                )
            except Exception as exc:
                screenshot_path, html_path = _dump_debug_artifacts(
                    settings,
                    page,
                    slug=f"collect-failed-{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}",
                )
                finish_search_run(
                    settings.jobs_db_path,
                    run_id,
                    status="failed",
                    found_count=0,
                    notes=f"{exc} | Screenshot: {screenshot_path.name}; HTML: {html_path.name}",
                )
                raise
        if manual_browser is not None:
            manual_browser.close()
        else:
            context.close()

    return collected_jobs


def collect_manual_page(
    settings: Settings,
    *,
    title_hint: str | None,
    limit: int,
    dry_run: bool = False,
) -> list[CollectedJob]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(playwright_environment_hint(settings.root_dir)) from exc

    with sync_playwright() as playwright:
        browser, context, page = _connect_to_manual_chrome(playwright, settings)
        page = _ensure_manual_capture_page(context, page, settings.flexjobs_timeout_ms)

        print("Manual capture mode is open.")
        print("A normal Chrome window is being used for this flow, not a Playwright-launched browser.")
        print(f"Chrome profile: {settings.flexjobs_manual_chrome_profile_dir}")
        print(f"Attached tab: {_safe_page_url(page) or 'about:blank'}")
        print("In that browser window, log in if needed and manually run the FlexJobs search you want to capture.")
        print("If the results page opens in another tab, leave that tab open before pressing Enter.")
        print("When the search results page is visible, come back here and press Enter.")

        if sys.stdin.isatty():
            input("Press Enter when the desired FlexJobs results page is open...")
        else:
            print("Non-interactive session detected. Waiting 120 seconds for manual navigation.")
            page.wait_for_timeout(120000)

        page = _ensure_manual_capture_page(context, page, settings.flexjobs_timeout_ms)
        current_url = _safe_page_url(page)
        if current_url.lower() in {"", "about:blank"}:
            raise RuntimeError(
                "Chrome is connected, but the selected tab is still blank. Navigate to FlexJobs in the opened "
                "Chrome window, then rerun collect-manual-page."
            )

        if _is_login_gate(page):
            raise RuntimeError(
                "The current page is still a FlexJobs login page. Finish logging in first, then retry manual capture."
            )

        if _is_access_denied_page(page):
            raise RuntimeError(
                "The current page is an Access Denied page. Try again after navigating manually from a fresh logged-in session."
            )

        effective_title = title_hint or _safe_page_title(page) or "FlexJobs manual capture"
        selected_page, jobs, inspected_pages = _find_best_manual_capture_page(
            context,
            page,
            search_title=effective_title,
            limit=limit,
        )
        if selected_page is not None:
            page = selected_page

        if not jobs:
            inspected_summary = "\n".join(inspected_pages[:8]) if inspected_pages else "No open Chrome tabs were available."
            _dump_debug_artifacts(settings, page, slug="manual-capture-no-results")
            raise RuntimeError(
                "No FlexJobs job cards were found in the open Chrome tabs. "
                "Make sure you are on a FlexJobs search-results page before pressing Enter.\n"
                f"Inspected tabs:\n{inspected_summary}"
            )
        elif not dry_run:
            run_id = start_search_run(settings.jobs_db_path, effective_title, notes="manual page capture")
            for job in jobs:
                upsert_job(
                    settings.jobs_db_path,
                    {
                        "title": job.title,
                        "company": job.company,
                        "location": job.location,
                        "salary_text": job.salary_text,
                        "job_url": job.job_url,
                        "application_url": job.application_url,
                        "posted_at": job.posted_at,
                        "external_id": job.external_id,
                        "raw_payload": job.raw_payload,
                    },
                )
            finish_search_run(
                settings.jobs_db_path,
                run_id,
                status="completed",
                found_count=len(jobs),
                notes=f"Manual page capture from {page.url}",
            )

        return jobs


def jobs_as_dicts(jobs: list[CollectedJob]) -> list[dict[str, Any]]:
    return [
        {
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "salary_text": job.salary_text,
            "job_url": job.job_url,
            "raw_payload": job.raw_payload,
        }
        for job in jobs
    ]


def enriched_jobs_as_dicts(jobs: list[EnrichedJob]) -> list[dict[str, Any]]:
    return [
        {
            "job_id": job.job_id,
            "title": job.title,
            "job_url": job.job_url,
            "application_url": job.application_url,
            "detail_length": len(job.detail_text or ""),
            "requirements_length": len(job.requirements_text or ""),
            "benefits_length": len(job.benefits_text or ""),
        }
        for job in jobs
    ]

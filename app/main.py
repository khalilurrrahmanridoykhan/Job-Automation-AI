from __future__ import annotations

import argparse
import json
from pathlib import Path

from .application_autofill import autofill_application_pages, autofill_results_as_dicts
from .application_prep import build_application_packet_with_defaults, write_application_packet
from .application_review import open_application_pages, opened_applications_as_dicts
from .candidate_profile import save_candidate_profile
from .config import Settings
from .daily_queue import build_daily_queue, daily_queue_as_dicts, write_daily_queue_json, write_daily_queue_markdown
from .db import (
    fetch_applications,
    fetch_daily_queue_candidates,
    fetch_jobs,
    fetch_jobs_for_page,
    fetch_jobs_for_preparation,
    fetch_shortlist_jobs,
    init_database,
    update_application_status,
    upsert_prepared_application,
)
from .expertapply import enrich_expertapply_jobs, prepare_expertapply_applications, select_expertapply_jobs
from .flexjobs_native import apply_native_expertapply_jobs, native_expertapply_results_as_dicts
from .jobs_page import write_jobs_page
from .job_scoring import score_unscored_jobs
from .job_search import (
    collect_jobs,
    collect_manual_page,
    enrich_jobs,
    enriched_jobs_as_dicts,
    jobs_as_dicts,
    load_candidate_profile,
    open_flexjobs_browser,
    repair_saved_jobs,
    recommended_search_titles,
    save_search_titles,
)
from .shortlist import shortlist_rows_as_dicts, write_shortlist_markdown


def _application_defaults(settings: Settings) -> dict[str, object]:
    return {
        "linkedin_url": settings.candidate_linkedin_url,
        "github_url": settings.candidate_github_url,
        "english_level": settings.candidate_english_level,
        "middle_name": settings.candidate_middle_name,
        "account_login": settings.candidate_account_login,
        "account_password": settings.candidate_account_password,
        "phone_type": settings.candidate_phone_type,
        "address_type": settings.candidate_address_type,
        "address_line1": settings.candidate_address_line1,
        "address_line2": settings.candidate_address_line2,
        "city": settings.candidate_location_city,
        "region": settings.candidate_location_region,
        "postal_code": settings.candidate_postal_code,
        "country": settings.candidate_country,
        "county": settings.candidate_county,
        "accept_terms": settings.candidate_accept_terms,
        "work_authorization": settings.candidate_work_authorized_us,
        "require_sponsorship": settings.candidate_require_sponsorship,
        "willing_to_relocate": settings.candidate_willing_to_relocate,
        "salary_expectations": settings.candidate_salary_expectations,
        "start_date": settings.candidate_start_date,
    }


def bootstrap(settings: Settings) -> None:
    profile = save_candidate_profile(settings.client_cv_path, settings.candidate_profile_path)
    init_database(settings.jobs_db_path)
    save_search_titles(profile, settings.search_titles_path)
    print(f"Candidate profile written to: {settings.candidate_profile_path}")
    print(f"Database initialized at: {settings.jobs_db_path}")
    print(f"Search titles written to: {settings.search_titles_path}")
    print("Recommended search titles:")
    for title in recommended_search_titles(profile):
        print(f"- {title}")


def extract_profile(settings: Settings, cv_path: Path | None = None, output_path: Path | None = None) -> None:
    profile = save_candidate_profile(cv_path or settings.client_cv_path, output_path or settings.candidate_profile_path)
    print(f"Candidate profile written to: {settings.candidate_profile_path if output_path is None else output_path}")
    print(f"Detected target roles: {', '.join(profile.get('target_roles', []))}")


def show_search_titles(settings: Settings) -> None:
    profile = load_candidate_profile(settings.candidate_profile_path)
    for title in recommended_search_titles(profile):
        print(title)


def collect_jobs_command(
    settings: Settings,
    *,
    titles: list[str] | None,
    limit: int,
    dry_run: bool,
    open_to_anywhere_us: bool,
) -> None:
    jobs = collect_jobs(
        settings,
        titles=titles,
        limit_per_title=limit,
        dry_run=dry_run,
        open_to_anywhere_us=open_to_anywhere_us,
    )
    print(f"Collected {len(jobs)} jobs.")
    print(json.dumps(jobs_as_dicts(jobs), indent=2, ensure_ascii=True))


def score_jobs_command(settings: Settings, limit: int | None) -> None:
    profile = load_candidate_profile(settings.candidate_profile_path)
    count = score_unscored_jobs(profile, settings.jobs_db_path, limit=limit)
    print(f"Scored {count} jobs.")


def list_jobs_command(settings: Settings, limit: int, status: str | None) -> None:
    rows = fetch_jobs(settings.jobs_db_path, status=status, limit=limit)
    if not rows:
        print("No jobs found.")
        return

    for row in rows:
        fit_score = row["fit_score"]
        score_text = f"{fit_score:.1f}" if fit_score is not None else "n/a"
        print(
            " | ".join(
                [
                    f"id={row['id']}",
                    f"score={score_text}",
                    f"status={row['status']}",
                    row["title"],
                    row["company"] or "company=n/a",
                    row["location"] or "location=n/a",
                    row["job_url"],
                ]
            )
        )


def jobs_page_command(settings: Settings, *, limit: int | None, out_path: Path) -> None:
    rows = fetch_jobs_for_page(settings.jobs_db_path, limit=limit)
    if not rows:
        print("No jobs found.")
        return

    written_path = write_jobs_page(rows, out_path)
    print(f"Jobs page written to: {written_path}")
    print(f"Included {len(rows)} jobs.")


def collect_manual_page_command(
    settings: Settings,
    *,
    title: str | None,
    limit: int,
    dry_run: bool,
) -> None:
    jobs = collect_manual_page(
        settings,
        title_hint=title,
        limit=limit,
        dry_run=dry_run,
    )
    print(f"Collected {len(jobs)} jobs from the manually opened page.")
    print(json.dumps(jobs_as_dicts(jobs), indent=2, ensure_ascii=True))


def enrich_jobs_command(
    settings: Settings,
    *,
    limit: int,
    job_ids: list[int] | None,
    dry_run: bool,
    force: bool,
) -> None:
    jobs = enrich_jobs(
        settings,
        limit=limit,
        job_ids=job_ids,
        dry_run=dry_run,
        force=force,
    )
    print(f"Enriched {len(jobs)} jobs.")
    print(json.dumps(enriched_jobs_as_dicts(jobs), indent=2, ensure_ascii=True))
    if jobs and not dry_run:
        profile = load_candidate_profile(settings.candidate_profile_path)
        rescored_count = score_unscored_jobs(profile, settings.jobs_db_path)
        print(f"Rescored {rescored_count} jobs after enrichment.")


def shortlist_jobs_command(
    settings: Settings,
    *,
    limit: int,
    min_score: float,
    query: str | None,
    out_path: Path | None,
) -> None:
    rows = fetch_shortlist_jobs(
        settings.jobs_db_path,
        limit=limit,
        min_score=min_score,
        query=query,
        require_apply_url=True,
        require_details=True,
    )
    if not rows:
        print("No shortlisted jobs found.")
        return

    for row in rows:
        fit_score = row["fit_score"]
        score_text = f"{fit_score:.1f}" if fit_score is not None else "n/a"
        print(
            " | ".join(
                [
                    f"id={row['id']}",
                    f"score={score_text}",
                    row["title"],
                    row["company"] or "company=n/a",
                    row["location"] or "location=n/a",
                ]
            )
        )
        print(f"apply={row['application_url'] or 'n/a'}")
        print(f"reason={row['fit_reason'] or 'n/a'}")
        print("")

    if out_path is not None:
        written_path = write_shortlist_markdown(rows, out_path)
        print(f"Shortlist markdown written to: {written_path}")
        print(json.dumps(shortlist_rows_as_dicts(rows), indent=2, ensure_ascii=True))


def prepare_application_command(
    settings: Settings,
    *,
    limit: int,
    min_score: float,
    query: str | None,
    job_ids: list[int] | None,
    out_dir: Path,
) -> None:
    rows = fetch_jobs_for_preparation(
        settings.jobs_db_path,
        limit=limit,
        min_score=min_score,
        query=query,
        job_ids=job_ids,
    )
    if not rows:
        print("No jobs available for application preparation.")
        return

    profile = load_candidate_profile(settings.candidate_profile_path)
    application_defaults = _application_defaults(settings)
    prepared_rows: list[dict[str, object]] = []
    for row in rows:
        packet = build_application_packet_with_defaults(profile, row, application_defaults)
        application_id = upsert_prepared_application(
            settings.jobs_db_path,
            job_id=int(row["id"]),
            prepared_payload=packet,
            notes="Prepared from enriched job details and candidate profile",
        )
        json_path, md_path = write_application_packet(packet, out_dir)
        prepared_rows.append(
            {
                "application_id": application_id,
                "job_id": int(row["id"]),
                "title": row["title"],
                "company": row["company"],
                "fit_score": row["fit_score"],
                "application_url": row["application_url"],
                "json_path": str(json_path),
                "markdown_path": str(md_path),
            }
        )

    print(f"Prepared {len(prepared_rows)} application packets.")
    print(json.dumps(prepared_rows, indent=2, ensure_ascii=True))


def list_applications_command(
    settings: Settings,
    *,
    limit: int,
    status: str | None,
    query: str | None,
) -> None:
    rows = fetch_applications(settings.jobs_db_path, limit=limit, status=status, query=query)
    if not rows:
        print("No applications found.")
        return

    for row in rows:
        fit_score = row["fit_score"]
        score_text = f"{fit_score:.1f}" if fit_score is not None else "n/a"
        print(
            " | ".join(
                [
                    f"application_id={row['id']}",
                    f"job_id={row['job_id']}",
                    f"status={row['status']}",
                    f"score={score_text}",
                    row["title"],
                    row["company"] or "company=n/a",
                ]
            )
        )
        print(f"apply={row['job_application_url'] or 'n/a'}")
        print(f"reviewed_at={row['reviewed_at'] or 'n/a'} | applied_at={row['applied_at'] or 'n/a'}")
        print("")


def review_applications_command(
    settings: Settings,
    *,
    limit: int,
    status: str | None,
    query: str | None,
    application_ids: list[int] | None,
    job_ids: list[int] | None,
    out_dir: Path,
    dry_run: bool,
) -> None:
    effective_status = status
    if effective_status is None and not application_ids and not job_ids:
        effective_status = "prepared"

    rows = fetch_applications(
        settings.jobs_db_path,
        limit=limit,
        status=effective_status,
        query=query,
        application_ids=application_ids,
        job_ids=job_ids,
    )
    if not rows:
        print("No applications available for review.")
        return

    opened = open_application_pages(settings, rows, packet_dir=out_dir, dry_run=dry_run)
    if opened and not dry_run:
        update_application_status(
            settings.jobs_db_path,
            application_ids=[row.application_id for row in opened],
            status="reviewing",
            notes="Opened external application URL for manual review",
        )

    print(f"{'Prepared to open' if dry_run else 'Opened'} {len(opened)} application pages.")
    print(json.dumps(opened_applications_as_dicts(opened), indent=2, ensure_ascii=True))


def autofill_applications_command(
    settings: Settings,
    *,
    limit: int,
    status: str | None,
    query: str | None,
    application_ids: list[int] | None,
    job_ids: list[int] | None,
    dry_run: bool,
    submit: bool,
    close_pages: bool,
) -> None:
    effective_status = status
    if effective_status is None and not application_ids and not job_ids:
        effective_status = "prepared"

    rows = fetch_applications(
        settings.jobs_db_path,
        limit=limit,
        status=effective_status,
        query=query,
        application_ids=application_ids,
        job_ids=job_ids,
    )
    if not rows:
        print("No applications available for autofill.")
        return

    results = autofill_application_pages(settings, rows, dry_run=dry_run, submit=submit, close_pages=close_pages)
    if results and not dry_run:
        for result in results:
            is_login_gate = bool(result.error and "sign in or account creation" in result.error.lower())
            if result.submitted:
                status_value = "applied"
            else:
                status_value = "error" if result.error and not is_login_gate else "reviewing"
            note_parts = [
                f"Autofill platform={result.platform}",
                f"filled={', '.join(result.filled_fields) or 'none'}",
                f"uploads={', '.join(result.uploaded_files) or 'none'}",
            ]
            if result.submitted:
                note_parts.append("submitted=yes")
            if result.missing_required_fields:
                note_parts.append(f"missing_required={', '.join(result.missing_required_fields[:6])}")
            if result.notes:
                note_parts.append("notes=" + " | ".join(result.notes))
            update_application_status(
                settings.jobs_db_path,
                application_ids=[result.application_id],
                status=status_value,
                notes="; ".join(note_parts),
                last_error=result.error,
            )

    print(f"{'Prepared autofill plan for' if dry_run else 'Autofill attempted for'} {len(results)} applications.")
    print(json.dumps(autofill_results_as_dicts(results), indent=2, ensure_ascii=True))


def apply_expertapply_jobs_command(
    settings: Settings,
    *,
    search_title: str | None,
    query: str | None,
    exact_company: str | None,
    min_fit_score: float | None,
    limit: int,
    out_dir: Path,
    submit: bool,
) -> None:
    rows = fetch_jobs(settings.jobs_db_path, limit=5000, order_by="COALESCE(fit_score, -1) DESC, discovered_at DESC")
    selected = select_expertapply_jobs(
        rows,
        search_title=search_title,
        query=query,
        exact_company=exact_company,
        min_fit_score=min_fit_score,
        limit=limit,
    )
    if not selected:
        print("No ExpertApply jobs matched the current filters.")
        return

    enriched_count, enrich_failures = enrich_expertapply_jobs(settings, selected)
    selected_ids = {int(row["id"]) for row in selected}
    refreshed_rows = fetch_jobs(settings.jobs_db_path, limit=5000, order_by="COALESCE(fit_score, -1) DESC, discovered_at DESC")
    selected_refreshed = [row for row in refreshed_rows if int(row["id"]) in selected_ids]

    application_defaults = _application_defaults(settings)
    application_ids = prepare_expertapply_applications(
        settings,
        selected_refreshed,
        application_defaults=application_defaults,
        out_dir=out_dir,
    )
    application_rows = fetch_applications(
        settings.jobs_db_path,
        limit=max(len(application_ids), 1),
        application_ids=application_ids,
    )
    results = autofill_application_pages(
        settings,
        application_rows,
        dry_run=False,
        submit=submit,
        close_pages=True,
    )

    for result in results:
        is_login_gate = bool(result.error and "sign in or account creation" in result.error.lower())
        if result.submitted:
            status_value = "applied"
        else:
            status_value = "error" if result.error and not is_login_gate else "reviewing"

        note_parts = [
            f"ExpertApply platform={result.platform}",
            f"filled={', '.join(result.filled_fields) or 'none'}",
            f"uploads={', '.join(result.uploaded_files) or 'none'}",
        ]
        if result.submitted:
            note_parts.append("submitted=yes")
        if result.missing_required_fields:
            note_parts.append(f"missing_required={', '.join(result.missing_required_fields[:6])}")
        if result.notes:
            note_parts.append("notes=" + " | ".join(result.notes))

        update_application_status(
            settings.jobs_db_path,
            application_ids=[result.application_id],
            status=status_value,
            notes="; ".join(note_parts),
            last_error=result.error,
        )

    print(f"Matched {len(selected)} ExpertApply jobs.")
    print(f"Enriched {enriched_count} jobs before automation.")
    if enrich_failures:
        print(f"Enrichment failures: {len(enrich_failures)}")

    submitted_count = sum(1 for result in results if result.submitted)
    error_count = sum(1 for result in results if result.error and not result.submitted)
    review_count = len(results) - submitted_count - error_count
    print(f"Submitted {submitted_count} applications.")
    print(f"Needs review: {review_count}")
    print(f"Errors: {error_count}")
    print(json.dumps(autofill_results_as_dicts(results), indent=2, ensure_ascii=True))


def apply_native_expertapply_jobs_command(
    settings: Settings,
    *,
    search_title: str | None,
    query: str | None,
    exact_company: str | None,
    min_fit_score: float | None,
    limit: int,
    submit: bool,
    dry_run: bool,
    jobs_page_out: Path | None,
) -> None:
    rows = fetch_jobs_for_page(settings.jobs_db_path, limit=5000)
    selected = select_expertapply_jobs(
        rows,
        search_title=search_title,
        query=query,
        exact_company=exact_company,
        min_fit_score=min_fit_score,
        limit=limit,
    )
    if not selected:
        print("No FlexJobs-native ExpertApply jobs matched the current filters.")
        return

    results = apply_native_expertapply_jobs(settings, selected, submit=submit, dry_run=dry_run)

    if jobs_page_out is not None and not dry_run:
        page_rows = fetch_jobs_for_page(settings.jobs_db_path, limit=500)
        if page_rows:
            written_path = write_jobs_page(page_rows, jobs_page_out)
            print(f"Jobs page written to: {written_path}")

    created_count = sum(1 for result in results if result.action in {"created", "created_pending_sync"})
    submitted_now_count = sum(
        1 for result in results if result.action in {"submitted_quick_apply", "submitted_review"}
    )
    review_count = sum(
        1 for result in results if result.action in {"needs_manual_review", "quick_apply_blocked", "review_submit_blocked"}
    )
    pending_sync_count = sum(1 for result in results if result.action in {"created_pending_sync", "awaiting_tracker_sync"})
    error_count = sum(1 for result in results if result.error or result.action == "error")
    final_status_counts: dict[str, int] = {}
    for result in results:
        key = result.final_status or "unknown"
        final_status_counts[key] = final_status_counts.get(key, 0) + 1

    print(f"Matched {len(selected)} FlexJobs-native ExpertApply jobs.")
    print(f"Created tracker items: {created_count}")
    print(f"Submitted now: {submitted_now_count}")
    print(f"Needs review: {review_count}")
    print(f"Pending tracker sync: {pending_sync_count}")
    print(f"Errors: {error_count}")
    print("Final tracker statuses:")
    print(json.dumps(final_status_counts, indent=2, ensure_ascii=True))
    print(json.dumps(native_expertapply_results_as_dicts(results), indent=2, ensure_ascii=True))


def set_application_status_command(
    settings: Settings,
    *,
    status: str,
    application_ids: list[int] | None,
    job_ids: list[int] | None,
    notes: str | None,
    last_error: str | None,
) -> None:
    if not application_ids and not job_ids:
        raise SystemExit("set-application-status requires --application-id or --job-id.")

    count = update_application_status(
        settings.jobs_db_path,
        application_ids=application_ids,
        job_ids=job_ids,
        status=status,
        notes=notes,
        last_error=last_error,
    )
    print(f"Updated {count} application rows to status={status}.")


def daily_queue_command(
    settings: Settings,
    *,
    limit: int,
    min_score: float,
    query: str | None,
    candidate_limit: int,
    allow_hybrid: bool,
    region: str,
    prepare_missing: bool,
    out_path: Path | None,
    json_out_path: Path | None,
    out_dir: Path,
) -> None:
    rows = fetch_daily_queue_candidates(
        settings.jobs_db_path,
        limit=candidate_limit,
        min_score=min_score,
        query=query,
    )
    if not rows:
        print("No jobs available for the daily queue.")
        return

    queue_items = build_daily_queue(
        rows,
        limit=limit,
        remote_us_only=True,
        allow_hybrid=allow_hybrid,
        region_mode=region,
    )
    if not queue_items:
        print(f"No {region}-eligible remote jobs matched the current queue rules.")
        return

    prepared_count = 0
    if prepare_missing:
        profile = load_candidate_profile(settings.candidate_profile_path)
        application_defaults = _application_defaults(settings)
        row_by_id = {int(row["id"]): row for row in rows}
        for item in queue_items:
            if item.application_id is not None:
                continue
            row = row_by_id[item.job_id]
            packet = build_application_packet_with_defaults(profile, row, application_defaults)
            application_id = upsert_prepared_application(
                settings.jobs_db_path,
                job_id=item.job_id,
                prepared_payload=packet,
                notes=f"Prepared from daily {region} remote application queue",
            )
            write_application_packet(packet, out_dir)
            item.application_id = application_id
            item.application_status = "prepared"
            item.next_action = "autofill_or_review"
            prepared_count += 1

    print(f"Daily queue contains {len(queue_items)} jobs.")
    if prepared_count:
        print(f"Prepared {prepared_count} new application packets from the queue.")

    for item in queue_items:
        fit_score = f"{item.fit_score:.1f}" if item.fit_score is not None else "n/a"
        app_id_text = str(item.application_id) if item.application_id is not None else "n/a"
        print(
            " | ".join(
                [
                    f"job_id={item.job_id}",
                    f"application_id={app_id_text}",
                    f"score={fit_score}",
                    f"priority={item.priority_score:.1f}",
                    f"remote={item.remote_bucket}",
                    f"apply={item.apply_bucket}",
                    f"next={item.next_action}",
                    item.title,
                    item.company or "company=n/a",
                ]
            )
        )
        print(f"location={item.location or 'n/a'}")
        print(f"url={item.application_url}")
        print(f"status={item.application_status or 'not prepared'}")
        print("")

    if out_path is not None:
        written_path = write_daily_queue_markdown(queue_items, out_path)
        print(f"Daily queue markdown written to: {written_path}")
    if json_out_path is not None:
        written_json = write_daily_queue_json(queue_items, json_out_path)
        print(f"Daily queue JSON written to: {written_json}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FlexJobs automation workspace")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Extract the candidate profile and initialize the database")

    extract_parser = subparsers.add_parser("extract-profile", help="Extract client CV into candidate_profile.json")
    extract_parser.add_argument("--cv", type=Path, default=None, help="Override the source CV path")
    extract_parser.add_argument("--out", type=Path, default=None, help="Override the output JSON path")

    subparsers.add_parser("init-db", help="Initialize the SQLite database")
    subparsers.add_parser("show-search-titles", help="Print recommended search titles from the candidate profile")
    subparsers.add_parser("open-flexjobs", help="Open a persistent FlexJobs browser profile")

    collect_parser = subparsers.add_parser("collect-jobs", help="Collect jobs from FlexJobs into SQLite")
    collect_parser.add_argument(
        "--title",
        action="append",
        dest="titles",
        default=None,
        help="Override search titles. Repeat for multiple titles.",
    )
    collect_parser.add_argument("--limit", type=int, default=10, help="Maximum jobs to keep per search title")
    collect_parser.add_argument("--dry-run", action="store_true", help="Collect without writing jobs to SQLite")
    collect_parser.add_argument(
        "--disable-anywhere-us",
        action="store_true",
        help="Do not enable the 'Open to candidates anywhere in U.S.' filter",
    )

    manual_parser = subparsers.add_parser(
        "collect-manual-page",
        help="Open the persistent browser profile and parse a search results page you navigate to manually",
    )
    manual_parser.add_argument("--title", type=str, default=None, help="Optional title hint used for result matching")
    manual_parser.add_argument("--limit", type=int, default=10, help="Maximum jobs to keep from the current page")
    manual_parser.add_argument("--dry-run", action="store_true", help="Parse without writing jobs to SQLite")

    enrich_parser = subparsers.add_parser(
        "enrich-jobs",
        help="Open saved FlexJobs job pages and store full detail text plus apply-link data",
    )
    enrich_parser.add_argument("--limit", type=int, default=10, help="Maximum jobs to enrich")
    enrich_parser.add_argument(
        "--job-id",
        action="append",
        type=int,
        dest="job_ids",
        default=None,
        help="Specific saved job id to enrich. Repeat for multiple ids.",
    )
    enrich_parser.add_argument("--dry-run", action="store_true", help="Extract details without writing them to SQLite")
    enrich_parser.add_argument("--force", action="store_true", help="Re-enrich jobs even if details already exist")

    shortlist_parser = subparsers.add_parser(
        "shortlist-jobs",
        help="Show the strongest enriched jobs with real apply URLs and optionally export them to markdown",
    )
    shortlist_parser.add_argument("--limit", type=int, default=15, help="Maximum shortlisted jobs to show")
    shortlist_parser.add_argument("--min-score", type=float, default=75.0, help="Minimum fit score to include")
    shortlist_parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Optional case-insensitive filter matched against title, company, location, fit reason, and detail text",
    )
    shortlist_parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional markdown output path, for example data/shortlist.md",
    )

    daily_queue_parser = subparsers.add_parser(
        "daily-queue",
        help="Build a strict remote application queue filtered by target region and optionally prepare missing packets",
    )
    daily_queue_parser.add_argument("--limit", type=int, default=50, help="Target number of jobs in the queue")
    daily_queue_parser.add_argument("--min-score", type=float, default=70.0, help="Minimum fit score to consider")
    daily_queue_parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Optional filter matched against title, company, location, detail text, and application notes",
    )
    daily_queue_parser.add_argument(
        "--candidate-limit",
        type=int,
        default=250,
        help="How many DB candidates to inspect before building the final queue",
    )
    daily_queue_parser.add_argument(
        "--allow-hybrid",
        action="store_true",
        help="Include hybrid roles that still pass the selected region filter",
    )
    daily_queue_parser.add_argument(
        "--region",
        choices=("us", "global", "bangladesh"),
        default="us",
        help="Target region filter for queue eligibility",
    )
    daily_queue_parser.add_argument(
        "--prepare-missing",
        action="store_true",
        help="Generate application packets for queued jobs that do not have an application row yet",
    )
    daily_queue_parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/daily-queue.md"),
        help="Markdown output path for the daily queue report",
    )
    daily_queue_parser.add_argument(
        "--json-out",
        type=Path,
        default=Path("data/daily-queue.json"),
        help="JSON output path for the daily queue report",
    )
    daily_queue_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/applications"),
        help="Directory for generated application packets when --prepare-missing is used",
    )

    prepare_parser = subparsers.add_parser(
        "prepare-application",
        help="Generate saved application packets from enriched jobs and the candidate profile",
    )
    prepare_parser.add_argument(
        "--job-id",
        action="append",
        type=int,
        dest="job_ids",
        default=None,
        help="Specific saved job id to prepare. Repeat for multiple ids.",
    )
    prepare_parser.add_argument("--limit", type=int, default=5, help="Maximum jobs to prepare when using shortlist mode")
    prepare_parser.add_argument("--min-score", type=float, default=80.0, help="Minimum fit score in shortlist mode")
    prepare_parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Optional shortlist filter matched against title, company, location, fit reason, and detail text",
    )
    prepare_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/applications"),
        help="Directory for generated markdown and JSON application packets",
    )

    applications_parser = subparsers.add_parser(
        "list-applications",
        help="List prepared/reviewing/applied application rows from SQLite",
    )
    applications_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to display")
    applications_parser.add_argument("--status", type=str, default=None, help="Optional application status filter")
    applications_parser.add_argument("--query", type=str, default=None, help="Optional title/company/status filter")

    review_parser = subparsers.add_parser(
        "review-applications",
        help="Open prepared application URLs in the normal Chrome profile and mark them as reviewing",
    )
    review_parser.add_argument("--application-id", action="append", type=int, dest="application_ids", default=None)
    review_parser.add_argument("--job-id", action="append", type=int, dest="job_ids", default=None)
    review_parser.add_argument("--limit", type=int, default=3, help="Maximum applications to open")
    review_parser.add_argument("--status", type=str, default=None, help="Application status filter")
    review_parser.add_argument("--query", type=str, default=None, help="Optional title/company/status filter")
    review_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/applications"),
        help="Directory containing generated application packet files",
    )
    review_parser.add_argument("--dry-run", action="store_true", help="Show which application pages would open")

    autofill_parser = subparsers.add_parser(
        "autofill-applications",
        help="Open external application pages and fill common fields from the prepared packet without submitting",
    )
    autofill_parser.add_argument("--application-id", action="append", type=int, dest="application_ids", default=None)
    autofill_parser.add_argument("--job-id", action="append", type=int, dest="job_ids", default=None)
    autofill_parser.add_argument("--limit", type=int, default=1, help="Maximum applications to autofill")
    autofill_parser.add_argument("--status", type=str, default=None, help="Application status filter")
    autofill_parser.add_argument("--query", type=str, default=None, help="Optional title/company/status filter")
    autofill_parser.add_argument("--dry-run", action="store_true", help="Show the autofill plan without opening pages")
    autofill_parser.add_argument("--submit", action="store_true", help="Attempt the final submit action when no required fields are missing")
    autofill_parser.add_argument("--close-pages", action="store_true", help="Close each opened page after processing")

    expertapply_parser = subparsers.add_parser(
        "apply-expertapply-jobs",
        help="Enrich, prepare, and autofill ExpertApply jobs in bulk, with optional final submission",
    )
    expertapply_parser.add_argument("--search-title", type=str, default=None, help="Exact saved search title filter, for example Supabase")
    expertapply_parser.add_argument("--query", type=str, default=None, help="Optional filter matched against title, company, location, and saved payload text")
    expertapply_parser.add_argument("--exact-company", type=str, default=None, help="Optional exact company-name filter, for example Supabase")
    expertapply_parser.add_argument("--min-fit-score", type=float, default=None, help="Optional minimum fit score required for automation")
    expertapply_parser.add_argument("--limit", type=int, default=100, help="Maximum ExpertApply jobs to process")
    expertapply_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/applications"),
        help="Directory for generated application packet files",
    )
    expertapply_parser.add_argument("--submit", action="store_true", help="Attempt final submission when the form has no missing required fields")

    native_expertapply_parser = subparsers.add_parser(
        "apply-native-expertapply-jobs",
        help="Run the FlexJobs-native ExpertApply tracker flow so successful jobs appear in the FlexJobs tracker",
    )
    native_expertapply_parser.add_argument("--search-title", type=str, default=None, help="Exact saved search title filter")
    native_expertapply_parser.add_argument("--query", type=str, default=None, help="Optional filter matched against title, company, location, and saved payload text")
    native_expertapply_parser.add_argument("--exact-company", type=str, default=None, help="Optional exact company-name filter")
    native_expertapply_parser.add_argument("--min-fit-score", type=float, default=None, help="Optional minimum fit score required for automation")
    native_expertapply_parser.add_argument("--limit", type=int, default=25, help="Maximum native ExpertApply jobs to process")
    native_expertapply_parser.add_argument("--submit", action="store_true", help="Click the native FlexJobs submit actions when the tracker item is ready")
    native_expertapply_parser.add_argument("--dry-run", action="store_true", help="Show matches without creating or submitting tracker items")
    native_expertapply_parser.add_argument(
        "--jobs-page-out",
        type=Path,
        default=Path("data/jobs.html"),
        help="Regenerate the local jobs HTML after syncing tracker statuses",
    )

    update_parser = subparsers.add_parser(
        "set-application-status",
        help="Update application status after manual review or submission",
    )
    update_parser.add_argument(
        "--status",
        required=True,
        choices=["prepared", "reviewing", "reviewed", "applied", "skipped", "error"],
        help="New application status",
    )
    update_parser.add_argument("--application-id", action="append", type=int, dest="application_ids", default=None)
    update_parser.add_argument("--job-id", action="append", type=int, dest="job_ids", default=None)
    update_parser.add_argument("--notes", type=str, default=None, help="Optional note appended to the application row")
    update_parser.add_argument("--last-error", type=str, default=None, help="Optional error text for status=error")

    score_parser = subparsers.add_parser("score-jobs", help="Score unscored jobs in SQLite")
    score_parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to score")

    list_parser = subparsers.add_parser("list-jobs", help="List saved jobs from SQLite")
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to display")
    list_parser.add_argument("--status", type=str, default=None, help="Optional status filter")

    jobs_page_parser = subparsers.add_parser(
        "jobs-page",
        help="Generate a simple local HTML page that lists saved jobs with search and filters",
    )
    jobs_page_parser.add_argument("--limit", type=int, default=500, help="Maximum jobs to include in the page")
    jobs_page_parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/jobs.html"),
        help="HTML output path, for example data/jobs.html",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.load()
    init_database(settings.jobs_db_path)
    repair_saved_jobs(settings.jobs_db_path)

    if args.command == "bootstrap":
        bootstrap(settings)
        return

    if args.command == "extract-profile":
        extract_profile(settings, cv_path=args.cv, output_path=args.out)
        return

    if args.command == "init-db":
        init_database(settings.jobs_db_path)
        print(f"Database initialized at: {settings.jobs_db_path}")
        return

    if args.command == "show-search-titles":
        show_search_titles(settings)
        return

    if args.command == "open-flexjobs":
        open_flexjobs_browser(settings)
        return

    if args.command == "collect-jobs":
        collect_jobs_command(
            settings,
            titles=args.titles,
            limit=args.limit,
            dry_run=args.dry_run,
            open_to_anywhere_us=not args.disable_anywhere_us,
        )
        return

    if args.command == "score-jobs":
        score_jobs_command(settings, limit=args.limit)
        return

    if args.command == "list-jobs":
        list_jobs_command(settings, limit=args.limit, status=args.status)
        return

    if args.command == "jobs-page":
        jobs_page_command(settings, limit=args.limit, out_path=args.out)
        return

    if args.command == "collect-manual-page":
        collect_manual_page_command(
            settings,
            title=args.title,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        return

    if args.command == "enrich-jobs":
        enrich_jobs_command(
            settings,
            limit=args.limit,
            job_ids=args.job_ids,
            dry_run=args.dry_run,
            force=args.force,
        )
        return

    if args.command == "shortlist-jobs":
        shortlist_jobs_command(
            settings,
            limit=args.limit,
            min_score=args.min_score,
            query=args.query,
            out_path=args.out,
        )
        return

    if args.command == "prepare-application":
        prepare_application_command(
            settings,
            limit=args.limit,
            min_score=args.min_score,
            query=args.query,
            job_ids=args.job_ids,
            out_dir=args.out_dir,
        )
        return

    if args.command == "daily-queue":
        daily_queue_command(
            settings,
            limit=args.limit,
            min_score=args.min_score,
            query=args.query,
            candidate_limit=args.candidate_limit,
            allow_hybrid=args.allow_hybrid,
            region=args.region,
            prepare_missing=args.prepare_missing,
            out_path=args.out,
            json_out_path=args.json_out,
            out_dir=args.out_dir,
        )
        return

    if args.command == "list-applications":
        list_applications_command(
            settings,
            limit=args.limit,
            status=args.status,
            query=args.query,
        )
        return

    if args.command == "review-applications":
        review_applications_command(
            settings,
            limit=args.limit,
            status=args.status,
            query=args.query,
            application_ids=args.application_ids,
            job_ids=args.job_ids,
            out_dir=args.out_dir,
            dry_run=args.dry_run,
        )
        return

    if args.command == "set-application-status":
        set_application_status_command(
            settings,
            status=args.status,
            application_ids=args.application_ids,
            job_ids=args.job_ids,
            notes=args.notes,
            last_error=args.last_error,
        )
        return

    if args.command == "autofill-applications":
        autofill_applications_command(
            settings,
            limit=args.limit,
            status=args.status,
            query=args.query,
            application_ids=args.application_ids,
            job_ids=args.job_ids,
            dry_run=args.dry_run,
            submit=args.submit,
            close_pages=args.close_pages,
        )
        return

    if args.command == "apply-expertapply-jobs":
        apply_expertapply_jobs_command(
            settings,
            search_title=args.search_title,
            query=args.query,
            exact_company=args.exact_company,
            min_fit_score=args.min_fit_score,
            limit=args.limit,
            out_dir=args.out_dir,
            submit=args.submit,
        )
        return

    if args.command == "apply-native-expertapply-jobs":
        apply_native_expertapply_jobs_command(
            settings,
            search_title=args.search_title,
            query=args.query,
            exact_company=args.exact_company,
            min_fit_score=args.min_fit_score,
            limit=args.limit,
            submit=args.submit,
            dry_run=args.dry_run,
            jobs_page_out=args.jobs_page_out,
        )
        return

    parser.error(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()

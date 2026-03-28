from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _derive_external_id(job_url: str | None) -> str | None:
    if not job_url:
        return None

    parsed = urlparse(job_url)
    query = parse_qs(parsed.query)
    job_ids = query.get("id")
    if not job_ids:
        return None

    external_id = job_ids[0].strip()
    return external_id or None


def _row_priority(row: sqlite3.Row) -> tuple[int, int, int]:
    completeness = sum(
        1
        for column in (
            "company",
            "location",
            "salary_text",
            "application_url",
            "posted_at",
            "detail_text",
            "requirements_text",
            "benefits_text",
            "company_overview_text",
            "fit_score",
            "fit_reason",
            "raw_payload",
            "detail_raw_payload",
        )
        if _has_value(row[column])
    )
    has_score = 1 if row["fit_score"] is not None else 0
    return (-completeness, -has_score, int(row["id"]))


def _pick_merged_value(rows: list[sqlite3.Row], column: str) -> Any:
    for row in rows:
        value = row[column]
        if _has_value(value):
            return value
    return None


def _backfill_external_ids(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, job_url
        FROM jobs
        WHERE source = 'flexjobs' AND (external_id IS NULL OR TRIM(external_id) = '')
        """
    ).fetchall()

    for row in rows:
        external_id = _derive_external_id(row["job_url"])
        if external_id:
            conn.execute(
                """
                UPDATE jobs
                SET external_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (external_id, row["id"]),
            )


def _ensure_job_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
    }
    required_columns = {
        "detail_text": "TEXT",
        "requirements_text": "TEXT",
        "benefits_text": "TEXT",
        "company_overview_text": "TEXT",
        "detail_raw_payload": "TEXT",
        "detail_fetched_at": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {column_name} {column_type}")


def _dedupe_jobs_by_external_id(conn: sqlite3.Connection) -> None:
    duplicate_keys = conn.execute(
        """
        SELECT source, external_id
        FROM jobs
        WHERE external_id IS NOT NULL AND TRIM(external_id) != ''
        GROUP BY source, external_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    for key in duplicate_keys:
        rows = list(
            conn.execute(
                """
                SELECT *
                FROM jobs
                WHERE source = ? AND external_id = ?
                ORDER BY id ASC
                """,
                (key["source"], key["external_id"]),
            ).fetchall()
        )
        if len(rows) < 2:
            continue

        ordered_rows = sorted(rows, key=_row_priority)
        keeper = ordered_rows[0]
        duplicate_ids = [int(row["id"]) for row in ordered_rows[1:]]

        for duplicate_id in duplicate_ids:
            conn.execute("UPDATE applications SET job_id = ? WHERE job_id = ?", (keeper["id"], duplicate_id))

        conn.execute(
            """
            UPDATE jobs
            SET title = ?,
                company = ?,
                location = ?,
                salary_text = ?,
                application_url = ?,
                posted_at = ?,
                detail_text = ?,
                requirements_text = ?,
                benefits_text = ?,
                company_overview_text = ?,
                detail_raw_payload = ?,
                detail_fetched_at = ?,
                fit_score = ?,
                fit_reason = ?,
                status = ?,
                raw_payload = ?,
                discovered_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                _pick_merged_value(ordered_rows, "title") or keeper["title"],
                _pick_merged_value(ordered_rows, "company"),
                _pick_merged_value(ordered_rows, "location"),
                _pick_merged_value(ordered_rows, "salary_text"),
                _pick_merged_value(ordered_rows, "application_url"),
                _pick_merged_value(ordered_rows, "posted_at"),
                _pick_merged_value(ordered_rows, "detail_text"),
                _pick_merged_value(ordered_rows, "requirements_text"),
                _pick_merged_value(ordered_rows, "benefits_text"),
                _pick_merged_value(ordered_rows, "company_overview_text"),
                _pick_merged_value(ordered_rows, "detail_raw_payload"),
                _pick_merged_value(ordered_rows, "detail_fetched_at"),
                _pick_merged_value(ordered_rows, "fit_score"),
                _pick_merged_value(ordered_rows, "fit_reason"),
                _pick_merged_value(ordered_rows, "status") or keeper["status"],
                _pick_merged_value(ordered_rows, "raw_payload"),
                min(row["discovered_at"] for row in ordered_rows),
                keeper["id"],
            ),
        )

        conn.executemany("DELETE FROM jobs WHERE id = ?", [(duplicate_id,) for duplicate_id in duplicate_ids])


def init_database(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL DEFAULT 'flexjobs',
                external_id TEXT,
                title TEXT NOT NULL,
                company TEXT,
                location TEXT,
                salary_text TEXT,
                job_url TEXT NOT NULL UNIQUE,
                application_url TEXT,
                posted_at TEXT,
                fit_score REAL,
                fit_reason TEXT,
                status TEXT NOT NULL DEFAULT 'discovered',
                raw_payload TEXT,
                detail_text TEXT,
                requirements_text TEXT,
                benefits_text TEXT,
                company_overview_text TEXT,
                detail_raw_payload TEXT,
                detail_fetched_at TEXT,
                discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                notes TEXT,
                last_error TEXT,
                prepared_payload TEXT,
                reviewed_at TEXT,
                applied_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS search_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_title TEXT NOT NULL,
                status TEXT NOT NULL,
                found_count INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT
            );
            """
        )

        _ensure_job_columns(conn)
        _backfill_external_ids(conn)
        _dedupe_jobs_by_external_id(conn)
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_source_external_id
            ON jobs(source, external_id)
            WHERE external_id IS NOT NULL AND TRIM(external_id) != ''
            """
        )


def start_search_run(db_path: Path, search_title: str, notes: str | None = None) -> int:
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO search_runs (search_title, status, notes)
            VALUES (?, 'running', ?)
            """,
            (search_title, notes),
        )
        return int(cursor.lastrowid)


def finish_search_run(
    db_path: Path,
    run_id: int,
    status: str,
    found_count: int,
    notes: str | None = None,
) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE search_runs
            SET status = ?, found_count = ?, notes = ?, finished_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, found_count, notes, run_id),
        )


def upsert_job(db_path: Path, job: dict[str, Any]) -> int:
    raw_payload = job.get("raw_payload")
    if raw_payload is not None and not isinstance(raw_payload, str):
        raw_payload = json.dumps(raw_payload, ensure_ascii=True)

    source = job.get("source", "flexjobs")
    external_id = job.get("external_id") or _derive_external_id(job.get("job_url"))
    title = job["title"]
    company = job.get("company")
    location = job.get("location")
    salary_text = job.get("salary_text")
    job_url = job["job_url"]
    application_url = job.get("application_url")
    posted_at = job.get("posted_at")
    fit_score = job.get("fit_score")
    fit_reason = job.get("fit_reason")
    status = job.get("status", "discovered")

    params = (
        source,
        external_id,
        title,
        company,
        location,
        salary_text,
        job_url,
        application_url,
        posted_at,
        fit_score,
        fit_reason,
        status,
        raw_payload,
    )

    with connect(db_path) as conn:
        row = None
        if external_id:
            row = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE source = ? AND external_id = ?
                """,
                (source, external_id),
            ).fetchone()

        if row is None:
            row = conn.execute("SELECT id FROM jobs WHERE job_url = ?", (job_url,)).fetchone()

        if row is None:
            cursor = conn.execute(
                """
                INSERT INTO jobs (
                    source, external_id, title, company, location, salary_text, job_url,
                    application_url, posted_at, fit_score, fit_reason, status, raw_payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            return int(cursor.lastrowid)

        job_id = int(row["id"])
        conn.execute(
            """
            UPDATE jobs
            SET source = ?,
                external_id = COALESCE(NULLIF(?, ''), external_id),
                title = ?,
                company = COALESCE(NULLIF(?, ''), company),
                location = COALESCE(NULLIF(?, ''), location),
                salary_text = COALESCE(NULLIF(?, ''), salary_text),
                application_url = COALESCE(NULLIF(?, ''), application_url),
                posted_at = COALESCE(NULLIF(?, ''), posted_at),
                fit_score = COALESCE(?, fit_score),
                fit_reason = COALESCE(NULLIF(?, ''), fit_reason),
                status = COALESCE(NULLIF(?, ''), status),
                raw_payload = COALESCE(NULLIF(?, ''), raw_payload),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                source,
                external_id,
                title,
                company,
                location,
                salary_text,
                application_url,
                posted_at,
                fit_score,
                fit_reason,
                status,
                raw_payload,
                job_id,
            ),
        )
        return job_id


def fetch_jobs(
    db_path: Path,
    *,
    status: str | None = None,
    limit: int = 25,
    order_by: str = "COALESCE(fit_score, -1) DESC, discovered_at DESC",
) -> list[sqlite3.Row]:
    query = "SELECT * FROM jobs"
    params: list[Any] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)

    query += f" ORDER BY {order_by} LIMIT ?"
    params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query, params).fetchall())


def fetch_jobs_for_page(
    db_path: Path,
    *,
    limit: int | None = 500,
) -> list[sqlite3.Row]:
    query_sql = """
        SELECT
            j.*,
            a.id AS application_id,
            a.status AS application_status,
            a.notes AS application_notes,
            a.updated_at AS application_updated_at
        FROM jobs j
        LEFT JOIN (
            SELECT a1.*
            FROM applications a1
            JOIN (
                SELECT job_id, MAX(id) AS latest_id
                FROM applications
                GROUP BY job_id
            ) latest ON latest.latest_id = a1.id
        ) a ON a.job_id = j.id
        ORDER BY
            COALESCE(j.fit_score, -1) DESC,
            COALESCE(j.detail_fetched_at, j.updated_at, j.discovered_at) DESC,
            j.discovered_at DESC
    """
    params: list[Any] = []
    if limit is not None:
        query_sql += " LIMIT ?"
        params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query_sql, params).fetchall())


def fetch_jobs_for_scoring(db_path: Path, limit: int | None = None) -> list[sqlite3.Row]:
    query = """
        SELECT *
        FROM jobs
        WHERE fit_score IS NULL OR fit_reason IS NULL
        ORDER BY discovered_at DESC
    """
    params: list[Any] = []
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query, params).fetchall())


def fetch_jobs_for_enrichment(
    db_path: Path,
    *,
    limit: int = 20,
    job_ids: list[int] | None = None,
    force: bool = False,
) -> list[sqlite3.Row]:
    query = "SELECT * FROM jobs"
    params: list[Any] = []
    conditions: list[str] = []

    if job_ids:
        placeholders = ", ".join("?" for _ in job_ids)
        conditions.append(f"id IN ({placeholders})")
        params.extend(job_ids)

    if not force:
        conditions.append(
            "("
            "detail_fetched_at IS NULL "
            "OR detail_text IS NULL "
            "OR TRIM(detail_text) = '' "
            "OR detail_text LIKE 'Skip to content%' "
            "OR application_url = 'https://www.flexjobs.com/expertapply/applications'"
            ")"
        )

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY COALESCE(fit_score, -1) DESC, discovered_at DESC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query, params).fetchall())


def fetch_shortlist_jobs(
    db_path: Path,
    *,
    limit: int = 15,
    min_score: float = 75.0,
    query: str | None = None,
    require_apply_url: bool = True,
    require_details: bool = True,
) -> list[sqlite3.Row]:
    conditions = ["COALESCE(fit_score, -1) >= ?"]
    params: list[Any] = [min_score]

    if require_apply_url:
        conditions.append(
            "("
            "application_url IS NOT NULL "
            "AND TRIM(application_url) != '' "
            "AND application_url != 'https://www.flexjobs.com/expertapply/applications'"
            ")"
        )

    if require_details:
        conditions.append(
            "("
            "detail_text IS NOT NULL "
            "AND TRIM(detail_text) != '' "
            "AND detail_text NOT LIKE 'Skip to content%'"
            ")"
        )

    if query:
        wildcard = f"%{query.strip().lower()}%"
        conditions.append(
            "("
            "LOWER(COALESCE(title, '')) LIKE ? "
            "OR LOWER(COALESCE(company, '')) LIKE ? "
            "OR LOWER(COALESCE(location, '')) LIKE ? "
            "OR LOWER(COALESCE(fit_reason, '')) LIKE ? "
            "OR LOWER(COALESCE(detail_text, '')) LIKE ?"
            ")"
        )
        params.extend([wildcard] * 5)

    query_sql = """
        SELECT *
        FROM jobs
    """
    if conditions:
        query_sql += " WHERE " + " AND ".join(conditions)

    query_sql += """
        ORDER BY COALESCE(fit_score, -1) DESC,
                 COALESCE(detail_fetched_at, updated_at, discovered_at) DESC,
                 discovered_at DESC
        LIMIT ?
    """
    params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query_sql, params).fetchall())


def fetch_daily_queue_candidates(
    db_path: Path,
    *,
    limit: int = 250,
    min_score: float = 70.0,
    query: str | None = None,
) -> list[sqlite3.Row]:
    conditions = ["COALESCE(j.fit_score, -1) >= ?"]
    params: list[Any] = [min_score]

    conditions.append(
        "("
        "j.application_url IS NOT NULL "
        "AND TRIM(j.application_url) != '' "
        "AND j.application_url != 'https://www.flexjobs.com/expertapply/applications'"
        ")"
    )
    conditions.append(
        "("
        "j.detail_text IS NOT NULL "
        "AND TRIM(j.detail_text) != '' "
        "AND j.detail_text NOT LIKE 'Skip to content%'"
        ")"
    )
    conditions.append("(a.id IS NULL OR a.status NOT IN ('applied', 'skipped'))")

    if query:
        wildcard = f"%{query.strip().lower()}%"
        conditions.append(
            "("
            "LOWER(COALESCE(j.title, '')) LIKE ? "
            "OR LOWER(COALESCE(j.company, '')) LIKE ? "
            "OR LOWER(COALESCE(j.location, '')) LIKE ? "
            "OR LOWER(COALESCE(j.fit_reason, '')) LIKE ? "
            "OR LOWER(COALESCE(j.detail_text, '')) LIKE ? "
            "OR LOWER(COALESCE(a.status, '')) LIKE ? "
            "OR LOWER(COALESCE(a.notes, '')) LIKE ? "
            "OR LOWER(COALESCE(a.last_error, '')) LIKE ?"
            ")"
        )
        params.extend([wildcard] * 8)

    query_sql = """
        SELECT
            j.*,
            a.id AS application_id,
            a.status AS application_status,
            a.notes AS application_notes,
            a.last_error AS application_last_error,
            a.prepared_payload AS application_prepared_payload,
            a.reviewed_at AS application_reviewed_at,
            a.applied_at AS application_applied_at,
            a.updated_at AS application_updated_at
        FROM jobs j
        LEFT JOIN (
            SELECT a1.*
            FROM applications a1
            JOIN (
                SELECT job_id, MAX(id) AS latest_id
                FROM applications
                GROUP BY job_id
            ) latest ON latest.latest_id = a1.id
        ) a ON a.job_id = j.id
    """
    if conditions:
        query_sql += " WHERE " + " AND ".join(conditions)

    query_sql += """
        ORDER BY
            COALESCE(j.fit_score, -1) DESC,
            CASE COALESCE(a.status, '')
                WHEN 'prepared' THEN 0
                WHEN 'reviewing' THEN 1
                WHEN 'reviewed' THEN 2
                WHEN 'error' THEN 3
                ELSE 4
            END,
            COALESCE(a.updated_at, j.detail_fetched_at, j.updated_at, j.discovered_at) DESC,
            j.discovered_at DESC
        LIMIT ?
    """
    params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query_sql, params).fetchall())


def update_job_score(db_path: Path, job_id: int, fit_score: float, fit_reason: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE jobs
            SET fit_score = ?, fit_reason = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (fit_score, fit_reason, job_id),
        )


def update_job_details(
    db_path: Path,
    job_id: int,
    *,
    application_url: str | None,
    detail_text: str | None,
    requirements_text: str | None,
    benefits_text: str | None,
    company_overview_text: str | None,
    detail_raw_payload: dict[str, Any] | str | None,
) -> None:
    raw_payload = detail_raw_payload
    if raw_payload is not None and not isinstance(raw_payload, str):
        raw_payload = json.dumps(raw_payload, ensure_ascii=True)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        conn.execute(
            """
            UPDATE jobs
            SET application_url = COALESCE(NULLIF(?, ''), application_url),
                detail_text = COALESCE(NULLIF(?, ''), detail_text),
                requirements_text = COALESCE(NULLIF(?, ''), requirements_text),
                benefits_text = COALESCE(NULLIF(?, ''), benefits_text),
                company_overview_text = COALESCE(NULLIF(?, ''), company_overview_text),
                detail_raw_payload = COALESCE(NULLIF(?, ''), detail_raw_payload),
                detail_fetched_at = CURRENT_TIMESTAMP,
                fit_score = NULL,
                fit_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                application_url,
                detail_text,
                requirements_text,
                benefits_text,
                company_overview_text,
                raw_payload,
                job_id,
            ),
        )


def fetch_job_by_id(db_path: Path, job_id: int) -> sqlite3.Row | None:
    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def fetch_jobs_for_preparation(
    db_path: Path,
    *,
    limit: int = 5,
    min_score: float = 80.0,
    query: str | None = None,
    job_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    if job_ids:
        placeholders = ", ".join("?" for _ in job_ids)
        params: list[Any] = list(job_ids)
        query_sql = f"""
            SELECT *
            FROM jobs
            WHERE id IN ({placeholders})
            ORDER BY COALESCE(fit_score, -1) DESC, discovered_at DESC
        """
        with connect(db_path) as conn:
            _ensure_job_columns(conn)
            return list(conn.execute(query_sql, params).fetchall())

    return fetch_shortlist_jobs(
        db_path,
        limit=limit,
        min_score=min_score,
        query=query,
        require_apply_url=True,
        require_details=True,
    )


def upsert_prepared_application(
    db_path: Path,
    *,
    job_id: int,
    prepared_payload: dict[str, Any] | str,
    notes: str | None = None,
    status: str = "prepared",
) -> int:
    payload_text = prepared_payload
    if not isinstance(payload_text, str):
        payload_text = json.dumps(payload_text, indent=2, ensure_ascii=True)

    with connect(db_path) as conn:
        existing = conn.execute(
            """
            SELECT id, status
            FROM applications
            WHERE job_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()

        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO applications (job_id, status, notes, prepared_payload)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, status, notes, payload_text),
            )
            return int(cursor.lastrowid)

        application_id = int(existing["id"])
        preserved_statuses = {"reviewing", "reviewed", "applied", "skipped"}
        next_status = existing["status"] if existing["status"] in preserved_statuses else status
        conn.execute(
            """
            UPDATE applications
            SET status = ?,
                notes = COALESCE(NULLIF(?, ''), notes),
                prepared_payload = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (next_status, notes, payload_text, application_id),
        )
        return application_id


def fetch_applications(
    db_path: Path,
    *,
    limit: int = 20,
    status: str | None = None,
    query: str | None = None,
    application_ids: list[int] | None = None,
    job_ids: list[int] | None = None,
) -> list[sqlite3.Row]:
    conditions: list[str] = []
    params: list[Any] = []

    if application_ids:
        placeholders = ", ".join("?" for _ in application_ids)
        conditions.append(f"a.id IN ({placeholders})")
        params.extend(application_ids)

    if job_ids:
        placeholders = ", ".join("?" for _ in job_ids)
        conditions.append(f"a.job_id IN ({placeholders})")
        params.extend(job_ids)

    if status:
        conditions.append("a.status = ?")
        params.append(status)

    if query:
        wildcard = f"%{query.strip().lower()}%"
        conditions.append(
            "("
            "LOWER(COALESCE(j.title, '')) LIKE ? "
            "OR LOWER(COALESCE(j.company, '')) LIKE ? "
            "OR LOWER(COALESCE(j.location, '')) LIKE ? "
            "OR LOWER(COALESCE(a.notes, '')) LIKE ? "
            "OR LOWER(COALESCE(a.status, '')) LIKE ?"
            ")"
        )
        params.extend([wildcard] * 5)

    query_sql = """
        SELECT
            a.*,
            j.title,
            j.company,
            j.location,
            j.fit_score,
            j.job_url,
            CASE
                WHEN j.application_url IS NOT NULL
                     AND TRIM(j.application_url) != ''
                     THEN j.application_url
                WHEN LOWER(COALESCE(j.raw_payload, '')) LIKE '%expertapply%'
                     OR LOWER(COALESCE(j.detail_raw_payload, '')) LIKE '%eligibleforexpertapply%'
                     THEN j.job_url
                ELSE j.application_url
            END AS job_application_url
        FROM applications a
        JOIN jobs j ON j.id = a.job_id
    """
    if conditions:
        query_sql += " WHERE " + " AND ".join(conditions)

    query_sql += """
        ORDER BY
            CASE a.status
                WHEN 'prepared' THEN 0
                WHEN 'reviewing' THEN 1
                WHEN 'reviewed' THEN 2
                WHEN 'applied' THEN 3
                WHEN 'skipped' THEN 4
                ELSE 5
            END,
            COALESCE(j.fit_score, -1) DESC,
            a.updated_at DESC,
            a.id DESC
        LIMIT ?
    """
    params.append(limit)

    with connect(db_path) as conn:
        _ensure_job_columns(conn)
        return list(conn.execute(query_sql, params).fetchall())


def update_application_status(
    db_path: Path,
    *,
    application_ids: list[int] | None = None,
    job_ids: list[int] | None = None,
    status: str,
    notes: str | None = None,
    last_error: str | None = None,
) -> int:
    conditions: list[str] = []
    params: list[Any] = []

    if application_ids:
        placeholders = ", ".join("?" for _ in application_ids)
        conditions.append(f"id IN ({placeholders})")
        params.extend(application_ids)

    if job_ids:
        placeholders = ", ".join("?" for _ in job_ids)
        conditions.append(f"job_id IN ({placeholders})")
        params.extend(job_ids)

    if not conditions:
        raise ValueError("application_ids or job_ids is required to update application status.")

    query_sql = """
        UPDATE applications
        SET status = ?,
            notes = CASE
                WHEN NULLIF(?, '') IS NULL THEN notes
                WHEN notes IS NULL OR TRIM(notes) = '' THEN ?
                ELSE notes || '\n' || ?
            END,
            last_error = CASE
                WHEN NULLIF(?, '') IS NULL THEN last_error
                ELSE ?
            END,
            reviewed_at = CASE
                WHEN ? IN ('reviewed', 'applied') THEN COALESCE(reviewed_at, CURRENT_TIMESTAMP)
                ELSE reviewed_at
            END,
            applied_at = CASE
                WHEN ? = 'applied' THEN COALESCE(applied_at, CURRENT_TIMESTAMP)
                ELSE applied_at
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE
    """
    query_sql += " AND ".join(conditions)

    with connect(db_path) as conn:
        cursor = conn.execute(
            query_sql,
            (
                status,
                notes,
                notes,
                notes,
                last_error,
                last_error,
                status,
                status,
                *params,
            ),
        )
        return int(cursor.rowcount or 0)

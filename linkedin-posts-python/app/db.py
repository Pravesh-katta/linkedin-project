from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from .config import Settings, get_settings
from .post_age import linkedin_post_is_within_hours, linkedin_posted_at
from .scoring import keyword_match_score


SCHEMA = """
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keywords TEXT NOT NULL,
    state_scope TEXT NOT NULL DEFAULT 'custom',
    enabled_states_json TEXT NOT NULL DEFAULT '[]',
    capture_mode TEXT NOT NULL DEFAULT 'standard',
    window_hours INTEGER NOT NULL DEFAULT 24,
    max_results_per_state INTEGER NOT NULL DEFAULT 20,
    schedule_minutes INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_run_at TEXT
);

CREATE TABLE IF NOT EXISTS search_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER NOT NULL,
    state_code TEXT NOT NULL,
    query_text TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    found_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    permalink TEXT,
    author_name TEXT,
    author_profile_url TEXT,
    content_text TEXT NOT NULL,
    relative_time_text TEXT,
    absolute_posted_at TEXT,
    best_state_code TEXT,
    state_confidence REAL NOT NULL DEFAULT 0,
    source_query TEXT,
    collected_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    viewed_at TEXT
);

CREATE TABLE IF NOT EXISTS search_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    matched_state_code TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE (search_id, post_id, matched_state_code),
    FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES search_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_search_runs_search_id ON search_runs(search_id);
CREATE INDEX IF NOT EXISTS idx_search_results_search_id ON search_results(search_id);
CREATE INDEX IF NOT EXISTS idx_search_results_post_id ON search_results(post_id);
CREATE INDEX IF NOT EXISTS idx_posts_last_seen_at ON posts(last_seen_at);

CREATE TABLE IF NOT EXISTS resumes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    extracted_text TEXT NOT NULL,
    extracted_keywords_json TEXT NOT NULL DEFAULT '[]',
    match_threshold REAL NOT NULL DEFAULT 0.30,
    uploaded_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def get_connection(settings: Settings | None = None) -> Iterator[sqlite3.Connection]:
    app_settings = settings or get_settings()
    app_settings.data_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(app_settings.database_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def init_db(settings: Settings | None = None) -> None:
    with get_connection(settings) as connection:
        connection.executescript(SCHEMA)
        _ensure_search_capture_mode(connection)
        _remove_search_run_audit(connection)
        _ensure_post_view_tracking(connection)
        _ensure_resumes_table(connection)


def _ensure_search_capture_mode(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(searches)").fetchall()
    }
    if "capture_mode" not in columns:
        connection.execute("ALTER TABLE searches ADD COLUMN capture_mode TEXT NOT NULL DEFAULT 'standard'")


def _remove_search_run_audit(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(search_runs)").fetchall()
    }
    if "audit_json" in columns:
        connection.execute("ALTER TABLE search_runs DROP COLUMN audit_json")


def _ensure_post_view_tracking(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(posts)").fetchall()
    }
    if "viewed_at" not in columns:
        connection.execute("ALTER TABLE posts ADD COLUMN viewed_at TEXT")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_posts_viewed_at ON posts(viewed_at)")


def _ensure_resumes_table(connection: sqlite3.Connection) -> None:
    tables = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "resumes" not in tables:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS resumes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                extracted_text TEXT NOT NULL,
                extracted_keywords_json TEXT NOT NULL DEFAULT '[]',
                match_threshold REAL NOT NULL DEFAULT 0.30,
                uploaded_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = {key: row[key] for key in row.keys()}
    if "capture_mode" in data and data["capture_mode"] not in {"standard", "balanced", "deep"}:
        data["capture_mode"] = "standard"
    raw_enabled_states = data.get("enabled_states_json")
    if isinstance(raw_enabled_states, str):
        try:
            parsed_enabled_states = json.loads(raw_enabled_states)
        except json.JSONDecodeError:
            parsed_enabled_states = []
        if isinstance(parsed_enabled_states, list):
            data["enabled_states"] = [str(code) for code in parsed_enabled_states]
        else:
            data["enabled_states"] = []
    return data


def create_search(
    keywords: str,
    *,
    state_scope: str = "custom",
    enabled_states: list[str] | None = None,
    capture_mode: str = "standard",
    window_hours: int = 24,
    max_results_per_state: int = 20,
    schedule_minutes: int = 0,
    is_active: bool = True,
    settings: Settings | None = None,
) -> int:
    now = utcnow_iso()
    enabled_states_json = json.dumps(enabled_states or [])
    with get_connection(settings) as connection:
        cursor = connection.execute(
            """
            INSERT INTO searches (
                keywords, state_scope, enabled_states_json, capture_mode, window_hours,
                max_results_per_state, schedule_minutes, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                keywords.strip(),
                state_scope,
                enabled_states_json,
                capture_mode,
                window_hours,
                max_results_per_state,
                schedule_minutes,
                int(is_active),
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def list_searches(settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT
                s.*,
                COUNT(DISTINCT sr.post_id) AS total_posts,
                MAX(r.started_at) AS last_run_started_at
            FROM searches s
            LEFT JOIN search_results sr ON sr.search_id = s.id
            LEFT JOIN search_runs r ON r.search_id = s.id
            GROUP BY s.id
            ORDER BY s.created_at DESC
            """
        ).fetchall()
    return [row_to_dict(row) for row in rows]


def list_active_scheduled_searches(settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT * FROM searches
            WHERE is_active = 1 AND schedule_minutes > 0
            ORDER BY created_at ASC
            """
        ).fetchall()
    return [row_to_dict(row) for row in rows]


def get_search(search_id: int, settings: Settings | None = None) -> dict[str, Any] | None:
    with get_connection(settings) as connection:
        row = connection.execute("SELECT * FROM searches WHERE id = ?", (search_id,)).fetchone()
    return row_to_dict(row)


def update_search_last_run(search_id: int, settings: Settings | None = None) -> None:
    now = utcnow_iso()
    with get_connection(settings) as connection:
        connection.execute(
            "UPDATE searches SET last_run_at = ?, updated_at = ? WHERE id = ?",
            (now, now, search_id),
        )


def create_search_run(search_id: int, state_code: str, query_text: str, settings: Settings | None = None) -> int:
    now = utcnow_iso()
    with get_connection(settings) as connection:
        cursor = connection.execute(
            """
            INSERT INTO search_runs (
                search_id, state_code, query_text, status, started_at, found_count
            ) VALUES (?, ?, ?, 'running', ?, 0)
            """,
            (search_id, state_code, query_text, now),
        )
        return int(cursor.lastrowid)


def finish_search_run(
    run_id: int,
    *,
    status: str,
    found_count: int,
    error_message: str | None = None,
    settings: Settings | None = None,
) -> None:
    now = utcnow_iso()
    with get_connection(settings) as connection:
        connection.execute(
            """
            UPDATE search_runs
            SET status = ?, finished_at = ?, found_count = ?, error_message = ?
            WHERE id = ?
            """,
            (status, now, found_count, error_message, run_id),
        )


def mark_stale_running_search_runs_failed(
    *,
    stale_after_minutes: int = 15,
    settings: Settings | None = None,
) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(1, stale_after_minutes))
    stale_run_ids: list[int] = []
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT id, started_at
            FROM search_runs
            WHERE status = 'running'
            """
        ).fetchall()
        for row in rows:
            started_at_raw = str(row["started_at"] or "").strip()
            if not started_at_raw:
                stale_run_ids.append(int(row["id"]))
                continue
            try:
                started_at = datetime.fromisoformat(started_at_raw)
            except ValueError:
                stale_run_ids.append(int(row["id"]))
                continue
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            if started_at <= cutoff:
                stale_run_ids.append(int(row["id"]))

        now = utcnow_iso()
        for run_id in stale_run_ids:
            connection.execute(
                """
                UPDATE search_runs
                SET status = 'failed',
                    finished_at = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    now,
                    "Marked failed automatically because the previous run appears stale.",
                    run_id,
                ),
            )
    return len(stale_run_ids)


def list_runs_for_search(search_id: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT * FROM search_runs
            WHERE search_id = ?
            ORDER BY started_at DESC
            """,
            (search_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows]


def clear_results_for_search(search_id: int, settings: Settings | None = None) -> None:
    with get_connection(settings) as connection:
        connection.execute("DELETE FROM search_results WHERE search_id = ?", (search_id,))


def upsert_post(
    *,
    external_id: str,
    permalink: str | None,
    author_name: str | None,
    author_profile_url: str | None,
    content_text: str,
    relative_time_text: str | None,
    absolute_posted_at: str | None,
    best_state_code: str | None,
    state_confidence: float,
    source_query: str | None,
    settings: Settings | None = None,
) -> int:
    now = utcnow_iso()
    with get_connection(settings) as connection:
        existing = connection.execute(
            """
            SELECT id, best_state_code, state_confidence, source_query
            FROM posts
            WHERE external_id = ?
            """,
            (external_id,),
        ).fetchone()
        if existing:
            existing_state_confidence = float(existing["state_confidence"] or 0.0)
            existing_best_state_code = existing["best_state_code"]
            existing_source_query = existing["source_query"]
            should_replace_state = not existing_best_state_code or state_confidence >= existing_state_confidence
            connection.execute(
                """
                UPDATE posts
                SET permalink = ?, author_name = ?, author_profile_url = ?, content_text = ?,
                    relative_time_text = ?, absolute_posted_at = ?, best_state_code = ?,
                    state_confidence = ?, source_query = ?, last_seen_at = ?
                WHERE external_id = ?
                """,
                (
                    permalink,
                    author_name,
                    author_profile_url,
                    content_text,
                    relative_time_text,
                    absolute_posted_at,
                    best_state_code if should_replace_state else existing_best_state_code,
                    state_confidence if should_replace_state else existing_state_confidence,
                    source_query if should_replace_state or not existing_source_query else existing_source_query,
                    now,
                    external_id,
                ),
            )
            return int(existing["id"])

        cursor = connection.execute(
            """
            INSERT INTO posts (
                external_id, permalink, author_name, author_profile_url, content_text,
                relative_time_text, absolute_posted_at, best_state_code, state_confidence,
                source_query, collected_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                external_id,
                permalink,
                author_name,
                author_profile_url,
                content_text,
                relative_time_text,
                absolute_posted_at,
                best_state_code,
                state_confidence,
                source_query,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def link_search_result(
    search_id: int,
    run_id: int,
    post_id: int,
    matched_state_code: str,
    score: float,
    settings: Settings | None = None,
) -> bool:
    with get_connection(settings) as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO search_results (
                search_id, run_id, post_id, matched_state_code, score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (search_id, run_id, post_id, matched_state_code, score, utcnow_iso()),
        )
    return cursor.rowcount > 0


def list_results_for_search(search_id: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        search_row = connection.execute(
            "SELECT keywords FROM searches WHERE id = ?",
            (search_id,),
        ).fetchone()
        rows = connection.execute(
            """
            SELECT
                sr.id AS result_id,
                sr.run_id,
                sr.matched_state_code,
                sr.score,
                p.*
            FROM search_results sr
            JOIN posts p ON p.id = sr.post_id
            WHERE sr.search_id = ?
            """,
            (search_id,),
        ).fetchall()
    keywords = search_row["keywords"] if search_row else ""
    results: list[dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(row)
        if not data:
            continue
        results.append(data)
    results.sort(
        key=lambda row: (
            row["matched_state_code"],
            -_linkedin_post_sort_timestamp(row),
            -(float(row.get("score") or 0.0)),
            -(int(row.get("id") or 0)),
        )
    )
    return results


def list_related_posts_for_search(
    search_id: int,
    *,
    keywords: str,
    state_codes: list[str] | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    clauses = [
        "p.best_state_code IS NOT NULL",
        "p.id NOT IN (SELECT post_id FROM search_results WHERE search_id = ?)",
    ]
    params: list[Any] = [search_id]

    normalized_state_codes = [str(code).upper() for code in (state_codes or []) if str(code).strip()]
    if normalized_state_codes:
        placeholders = ", ".join("?" for _ in normalized_state_codes)
        clauses.append(f"p.best_state_code IN ({placeholders})")
        params.extend(normalized_state_codes)

    where_sql = " AND ".join(clauses)
    with get_connection(settings) as connection:
        rows = connection.execute(
            f"""
            SELECT p.*
            FROM posts p
            WHERE {where_sql}
            """,
            params,
        ).fetchall()

    related_posts: list[dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(row)
        if not data:
            continue
        score = keyword_match_score(data.get("content_text", ""), keywords)
        if score <= 0:
            continue
        data["matched_state_code"] = data.get("best_state_code")
        data["score"] = score
        data["match_type"] = "related"
        related_posts.append(data)

    related_posts.sort(
        key=lambda row: (
            row["matched_state_code"],
            -_linkedin_post_sort_timestamp(row),
            -(float(row.get("score") or 0.0)),
            -(int(row.get("id") or 0)),
        )
    )
    if limit and limit > 0:
        return related_posts[:limit]
    return related_posts


def list_recent_posts(limit: int = 25, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT p.*
            FROM posts p
            WHERE EXISTS (
                SELECT 1
                FROM search_results sr
                WHERE sr.post_id = p.id
            )
            """,
        ).fetchall()
    posts = [row_to_dict(row) for row in rows]
    posts.sort(
        key=lambda row: (
            _linkedin_post_sort_timestamp(row),
            int(row.get("id") or 0),
        ),
        reverse=True,
    )
    return posts[:limit]


def get_post(post_id: int, settings: Settings | None = None) -> dict[str, Any] | None:
    with get_connection(settings) as connection:
        row = connection.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    return row_to_dict(row)


def mark_post_viewed(post_id: int, settings: Settings | None = None) -> str | None:
    now = utcnow_iso()
    with get_connection(settings) as connection:
        connection.execute(
            """
            UPDATE posts
            SET viewed_at = COALESCE(viewed_at, ?)
            WHERE id = ?
            """,
            (now, post_id),
        )
        row = connection.execute(
            "SELECT viewed_at FROM posts WHERE id = ?",
            (post_id,),
        ).fetchone()
    if row is None:
        return None
    return row["viewed_at"]


def purge_expired_posts(
    settings: Settings | None = None,
    *,
    max_age_hours: int = 24,
) -> int:
    with get_connection(settings) as connection:
        rows = connection.execute(
            "SELECT id, absolute_posted_at, relative_time_text, collected_at FROM posts"
        ).fetchall()
        expired_post_ids = [
            row["id"]
            for row in rows
            if not linkedin_post_is_within_hours(
                absolute_posted_at=row["absolute_posted_at"],
                relative_time_text=row["relative_time_text"],
                window_hours=max_age_hours,
                reference_now=row["collected_at"],
            )
        ]
        if not expired_post_ids:
            return 0

        placeholders = ", ".join("?" for _ in expired_post_ids)
        connection.execute(
            f"DELETE FROM posts WHERE id IN ({placeholders})",
            expired_post_ids,
        )
    return len(expired_post_ids)


def _linkedin_post_sort_timestamp(row: dict[str, Any]) -> float:
    posted_at = linkedin_posted_at(
        absolute_posted_at=row.get("absolute_posted_at"),
        relative_time_text=row.get("relative_time_text"),
        reference_now=row.get("collected_at"),
    )
    if posted_at is not None:
        return posted_at.timestamp()

    for fallback_key in ("collected_at", "last_seen_at"):
        fallback = linkedin_posted_at(
            absolute_posted_at=row.get(fallback_key),
            relative_time_text=None,
        )
        if fallback is not None:
            return fallback.timestamp()
    return 0.0


def list_post_matches(post_id: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT
                sr.search_id,
                sr.matched_state_code,
                sr.score,
                s.keywords
            FROM search_results sr
            JOIN searches s ON s.id = sr.search_id
            WHERE sr.post_id = ?
            ORDER BY sr.score DESC
            """,
            (post_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows]


# ---------------------------------------------------------------------------
#  Resume operations
# ---------------------------------------------------------------------------


def save_resume(
    filename: str,
    extracted_text: str,
    extracted_keywords: list[str],
    match_threshold: float = 0.05,
    settings: Settings | None = None,
) -> int:
    """Save a new resume.  Deactivates any previously active resume."""
    now = utcnow_iso()
    keywords_json = json.dumps(extracted_keywords)
    with get_connection(settings) as connection:
        # Deactivate previous resumes
        connection.execute("UPDATE resumes SET is_active = 0 WHERE is_active = 1")
        cursor = connection.execute(
            """
            INSERT INTO resumes (
                filename, extracted_text, extracted_keywords_json,
                match_threshold, uploaded_at, is_active
            ) VALUES (?, ?, ?, ?, ?, 1)
            """,
            (filename, extracted_text, keywords_json, match_threshold, now),
        )
        return int(cursor.lastrowid)


def get_active_resume(settings: Settings | None = None) -> dict[str, Any] | None:
    """Return the currently active resume, or None."""
    with get_connection(settings) as connection:
        row = connection.execute(
            "SELECT * FROM resumes WHERE is_active = 1 ORDER BY uploaded_at DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return None
    data = {key: row[key] for key in row.keys()}
    raw_keywords = data.get("extracted_keywords_json", "[]")
    try:
        data["keywords"] = json.loads(raw_keywords) if isinstance(raw_keywords, str) else []
    except json.JSONDecodeError:
        data["keywords"] = []
    return data


def delete_resume(resume_id: int, settings: Settings | None = None) -> None:
    with get_connection(settings) as connection:
        connection.execute("DELETE FROM resumes WHERE id = ?", (resume_id,))


def list_resume_matched_posts(
    resume_keywords: list[str],
    match_threshold: float = 0.30,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Return all posts that match > threshold of the resume keywords."""
    from .services.resume_parser import resume_match_score

    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT p.*
            FROM posts p
            WHERE EXISTS (
                SELECT 1 FROM search_results sr WHERE sr.post_id = p.id
            )
            """
        ).fetchall()

    matched: list[dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(row)
        if not data:
            continue
        score = resume_match_score(data.get("content_text", ""), resume_keywords)
        if score >= match_threshold:
            data["resume_match_score"] = round(score, 4)
            data["resume_match_pct"] = round(score * 100, 1)
            matched.append(data)

    matched.sort(key=lambda r: -r["resume_match_score"])
    return matched

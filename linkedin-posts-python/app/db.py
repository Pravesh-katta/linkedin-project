from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from .config import Settings, get_settings
from .post_age import linkedin_post_is_within_hours, linkedin_posted_at


SCHEMA = """
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keywords TEXT NOT NULL,
    state_scope TEXT NOT NULL DEFAULT 'all',
    enabled_states_json TEXT NOT NULL DEFAULT '[]',
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
        _ensure_post_view_tracking(connection)


def _ensure_post_view_tracking(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(posts)").fetchall()
    }
    if "viewed_at" not in columns:
        connection.execute("ALTER TABLE posts ADD COLUMN viewed_at TEXT")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_posts_viewed_at ON posts(viewed_at)")


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def create_search(
    keywords: str,
    *,
    state_scope: str = "all",
    enabled_states: list[str] | None = None,
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
                keywords, state_scope, enabled_states_json, window_hours,
                max_results_per_state, schedule_minutes, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                keywords.strip(),
                state_scope,
                enabled_states_json,
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
            "SELECT id FROM posts WHERE external_id = ?",
            (external_id,),
        ).fetchone()
        if existing:
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
                    best_state_code,
                    state_confidence,
                    source_query,
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
) -> None:
    with get_connection(settings) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO search_results (
                search_id, run_id, post_id, matched_state_code, score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (search_id, run_id, post_id, matched_state_code, score, utcnow_iso()),
        )


def list_results_for_search(search_id: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
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
    results = [row_to_dict(row) for row in rows]
    results.sort(
        key=lambda row: (
            row["matched_state_code"],
            -_linkedin_post_sort_timestamp(row),
            -(float(row.get("score") or 0.0)),
            -(int(row.get("id") or 0)),
        )
    )
    return results


def list_recent_posts(limit: int = 25, settings: Settings | None = None) -> list[dict[str, Any]]:
    with get_connection(settings) as connection:
        rows = connection.execute(
            """
            SELECT * FROM posts
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

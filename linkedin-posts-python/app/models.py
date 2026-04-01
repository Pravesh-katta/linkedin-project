from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


@dataclass(slots=True)
class Search:
    id: int
    keywords: str
    state_scope: str
    enabled_states: list[str]
    capture_mode: str
    window_hours: int
    max_results_per_state: int
    schedule_minutes: int
    is_active: bool
    created_at: str
    updated_at: str
    last_run_at: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Search":
        return cls(
            id=row["id"],
            keywords=row["keywords"],
            state_scope=row["state_scope"],
            enabled_states=_json_list(row["enabled_states_json"]),
            capture_mode=row["capture_mode"] if "capture_mode" in row.keys() else "standard",
            window_hours=row["window_hours"],
            max_results_per_state=row["max_results_per_state"],
            schedule_minutes=row["schedule_minutes"],
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_run_at=row["last_run_at"],
        )


@dataclass(slots=True)
class SearchRun:
    id: int
    search_id: int
    state_code: str
    query_text: str
    status: str
    started_at: str
    finished_at: str | None
    found_count: int
    error_message: str | None

    @classmethod
    def from_row(cls, row: Any) -> "SearchRun":
        return cls(
            id=row["id"],
            search_id=row["search_id"],
            state_code=row["state_code"],
            query_text=row["query_text"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            found_count=row["found_count"],
            error_message=row["error_message"],
        )


@dataclass(slots=True)
class Post:
    id: int
    external_id: str
    permalink: str | None
    author_name: str | None
    author_profile_url: str | None
    content_text: str
    relative_time_text: str | None
    absolute_posted_at: str | None
    best_state_code: str | None
    state_confidence: float
    source_query: str | None
    collected_at: str
    last_seen_at: str
    viewed_at: str | None

    @classmethod
    def from_row(cls, row: Any) -> "Post":
        return cls(
            id=row["id"],
            external_id=row["external_id"],
            permalink=row["permalink"],
            author_name=row["author_name"],
            author_profile_url=row["author_profile_url"],
            content_text=row["content_text"],
            relative_time_text=row["relative_time_text"],
            absolute_posted_at=row["absolute_posted_at"],
            best_state_code=row["best_state_code"],
            state_confidence=float(row["state_confidence"] or 0.0),
            source_query=row["source_query"],
            collected_at=row["collected_at"],
            last_seen_at=row["last_seen_at"],
            viewed_at=row["viewed_at"] if "viewed_at" in row.keys() else None,
        )

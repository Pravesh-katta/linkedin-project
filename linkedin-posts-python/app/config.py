from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_path(raw_value: str, *, default: str) -> Path:
    candidate = Path(raw_value or default)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _capture_mode(value: str | None, default: str = "balanced") -> str:
    normalized = (value or default).strip().lower()
    if normalized in {"standard", "balanced", "deep"}:
        return normalized
    return default


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_host: str
    app_port: int
    data_dir: Path
    database_path: Path
    linkedin_storage_state_path: Path
    linkedin_headless: bool
    default_capture_mode: str
    default_window_hours: int
    post_retention_hours: int
    default_max_results_per_state: int
    max_results_per_state_limit: int
    scraper_scroll_steps: int
    scraper_max_scroll_steps: int
    scraper_stable_rounds: int
    scraper_scroll_pause_seconds: float
    balanced_query_passes: int
    deep_query_passes: int
    balanced_detail_fetch_limit: int
    deep_detail_fetch_limit: int
    detail_fetch_char_threshold: int
    enable_scheduler: bool
    scheduler_poll_seconds: int
    templates_dir: Path
    static_dir: Path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    data_dir = _resolve_path(os.getenv("DATA_DIR", "data"), default="data")
    database_path = _resolve_path(os.getenv("DATABASE_PATH", "data/app.db"), default="data/app.db")
    linkedin_storage_state_path = _resolve_path(
        os.getenv("LINKEDIN_STORAGE_STATE_PATH", "data/linkedin_storage_state.json"),
        default="data/linkedin_storage_state.json",
    )
    return Settings(
        app_name=os.getenv("APP_NAME", "LinkedIn Posts Python"),
        app_host=os.getenv("APP_HOST", "127.0.0.1"),
        app_port=int(os.getenv("APP_PORT", "8000")),
        data_dir=data_dir,
        database_path=database_path,
        linkedin_storage_state_path=linkedin_storage_state_path,
        linkedin_headless=_to_bool(os.getenv("LINKEDIN_HEADLESS"), default=True),
        default_capture_mode=_capture_mode(os.getenv("DEFAULT_CAPTURE_MODE"), default="balanced"),
        default_window_hours=int(os.getenv("DEFAULT_WINDOW_HOURS", "24")),
        post_retention_hours=int(os.getenv("POST_RETENTION_HOURS", "24")),
        default_max_results_per_state=int(os.getenv("DEFAULT_MAX_RESULTS_PER_STATE", "40")),
        max_results_per_state_limit=int(os.getenv("MAX_RESULTS_PER_STATE_LIMIT", "100")),
        scraper_scroll_steps=int(os.getenv("SCRAPER_SCROLL_STEPS", "8")),
        scraper_max_scroll_steps=int(os.getenv("SCRAPER_MAX_SCROLL_STEPS", "40")),
        scraper_stable_rounds=int(os.getenv("SCRAPER_STABLE_ROUNDS", "5")),
        scraper_scroll_pause_seconds=float(os.getenv("SCRAPER_SCROLL_PAUSE_SECONDS", "1.0")),
        balanced_query_passes=int(os.getenv("BALANCED_QUERY_PASSES", "1")),
        deep_query_passes=int(os.getenv("DEEP_QUERY_PASSES", "3")),
        balanced_detail_fetch_limit=int(os.getenv("BALANCED_DETAIL_FETCH_LIMIT", "0")),
        deep_detail_fetch_limit=int(os.getenv("DEEP_DETAIL_FETCH_LIMIT", "12")),
        detail_fetch_char_threshold=int(os.getenv("DETAIL_FETCH_CHAR_THRESHOLD", "500")),
        enable_scheduler=_to_bool(os.getenv("ENABLE_SCHEDULER"), default=False),
        scheduler_poll_seconds=int(os.getenv("SCHEDULER_POLL_SECONDS", "60")),
        templates_dir=PROJECT_ROOT / "app" / "templates",
        static_dir=PROJECT_ROOT / "app" / "static",
    )

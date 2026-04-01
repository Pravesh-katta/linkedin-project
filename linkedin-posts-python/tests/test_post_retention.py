from __future__ import annotations

import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from app import db
from app.config import Settings, get_settings


def build_settings(root: Path) -> Settings:
    data_dir = root / "data"
    return Settings(
        app_name="LinkedIn Posts Python",
        app_host="127.0.0.1",
        app_port=8000,
        data_dir=data_dir,
        database_path=data_dir / "app.db",
        linkedin_storage_state_path=data_dir / "linkedin_storage_state.json",
        linkedin_headless=True,
        default_capture_mode="balanced",
        default_window_hours=24,
        post_retention_hours=24,
        default_max_results_per_state=20,
        max_results_per_state_limit=100,
        scraper_scroll_steps=8,
        scraper_max_scroll_steps=40,
        scraper_stable_rounds=5,
        scraper_scroll_pause_seconds=1.0,
        balanced_query_passes=2,
        deep_query_passes=3,
        balanced_detail_fetch_limit=6,
        deep_detail_fetch_limit=12,
        detail_fetch_char_threshold=500,
        enable_scheduler=False,
        scheduler_poll_seconds=60,
        templates_dir=root / "app" / "templates",
        static_dir=root / "app" / "static",
    )


class PurgeExpiredPostsTests(unittest.TestCase):
    def test_purge_expired_posts_respects_24_hour_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            db.init_db(settings)
            now = db.utcnow_iso()

            def hours_ago(value: int) -> str:
                baseline = db.linkedin_posted_at(absolute_posted_at=now, relative_time_text=None)
                assert baseline is not None
                return (baseline - timedelta(hours=value)).replace(microsecond=0).isoformat()

            with db.get_connection(settings) as connection:
                connection.executemany(
                    """
                    INSERT INTO posts (
                        external_id, permalink, author_name, author_profile_url, content_text,
                        relative_time_text, absolute_posted_at, best_state_code, state_confidence,
                        source_query, collected_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            "fresh-relative",
                            None,
                            "Fresh Relative",
                            None,
                            "Fresh post",
                            "3h",
                            None,
                            "TX",
                            0.9,
                            "python developer TX",
                            hours_ago(1),
                            hours_ago(1),
                        ),
                        (
                            "stale-relative",
                            None,
                            "Stale Relative",
                            None,
                            "Stale post",
                            "3h",
                            None,
                            "TX",
                            0.9,
                            "python developer TX",
                            hours_ago(22),
                            hours_ago(22),
                        ),
                        (
                            "stale-absolute",
                            None,
                            "Stale Absolute",
                            None,
                            "Older post",
                            None,
                            hours_ago(30),
                            "TX",
                            0.9,
                            "python developer TX",
                            hours_ago(29),
                            hours_ago(29),
                        ),
                    ],
                )

            deleted_count = db.purge_expired_posts(
                settings,
                max_age_hours=24,
            )

            self.assertEqual(deleted_count, 2)
            with db.get_connection(settings) as connection:
                remaining_rows = connection.execute(
                    "SELECT external_id FROM posts ORDER BY external_id ASC"
                ).fetchall()
            self.assertEqual([row["external_id"] for row in remaining_rows], ["fresh-relative"])


class SettingsDefaultsTests(unittest.TestCase):
    def test_post_retention_defaults_to_24_hours(self) -> None:
        get_settings.cache_clear()
        try:
            with patch.dict(os.environ, {}, clear=True):
                settings = get_settings()
            self.assertEqual(settings.post_retention_hours, 24)
        finally:
            get_settings.cache_clear()


if __name__ == "__main__":
    unittest.main()

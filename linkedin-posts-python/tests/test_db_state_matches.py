from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app import db
from app.config import Settings
from app.scoring import extract_state_match_scores


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
        balanced_query_passes=1,
        deep_query_passes=3,
        balanced_detail_fetch_limit=0,
        deep_detail_fetch_limit=12,
        detail_fetch_char_threshold=500,
        enable_scheduler=False,
        scheduler_poll_seconds=60,
        templates_dir=root / "app" / "templates",
        static_dir=root / "app" / "static",
    )


class PostStateMatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.settings = build_settings(Path(self.temp_dir.name))
        db.init_db(self.settings)

    def test_init_db_backfills_multi_state_matches_from_existing_post_text(self) -> None:
        post_id = db.upsert_post(
            external_id="post-1",
            permalink=None,
            author_name="Recruiter",
            author_profile_url=None,
            content_text="Job Title: Senior Python Developer Location: NC/TX (Onsite)",
            relative_time_text="2h",
            absolute_posted_at=None,
            best_state_code="NC",
            state_confidence=0.85,
            source_query='python developer, "NC"',
            settings=self.settings,
        )

        # Simulate an existing DB that predates multi-state indexing.
        db.init_db(self.settings)

        matches = db.list_post_state_matches(post_id, self.settings)
        self.assertEqual(
            [match["state_code"] for match in matches],
            ["NC", "TX"],
        )

    def test_related_posts_use_matched_state_codes_instead_of_single_best_state(self) -> None:
        post_id = db.upsert_post(
            external_id="post-2",
            permalink=None,
            author_name="Recruiter",
            author_profile_url=None,
            content_text="Role: Senior Python Developer Location: NC/TX (Onsite)",
            relative_time_text="1h",
            absolute_posted_at=None,
            best_state_code="NC",
            state_confidence=0.85,
            source_query='python developer, "NC"',
            settings=self.settings,
        )
        db.replace_post_state_matches(post_id, {"NC": 0.85, "TX": 0.85}, settings=self.settings)
        search_id = db.create_search(
            "python developer",
            enabled_states=["TX"],
            settings=self.settings,
        )

        related_posts = db.list_related_posts_for_search(
            search_id,
            keywords="python developer",
            state_codes=["TX"],
            limit=None,
            settings=self.settings,
        )

        self.assertEqual(len(related_posts), 1)
        self.assertEqual(related_posts[0]["id"], post_id)
        self.assertEqual(related_posts[0]["matched_state_code"], "TX")

    def test_extract_state_match_scores_ignores_ambiguous_or_in_plain_text(self) -> None:
        matches = extract_state_match_scores(
            "Bachelor's degree in Computer Science or a related field (or equivalent experience)."
        )
        self.assertNotIn("OR", matches)


if __name__ == "__main__":
    unittest.main()

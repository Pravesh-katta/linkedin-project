from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.config import Settings
from app.services.linkedin_scraper import CaptureProfile, LinkedInScraper, ScrapedPost


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


class LinkedInScraperHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.scraper = LinkedInScraper(build_settings(Path(self.temp_dir.name)))

    def test_normalize_linkedin_permalink_strips_tracking(self) -> None:
        normalized = self.scraper._normalize_linkedin_permalink(
            "https://www.linkedin.com/feed/update/urn:li:activity:123456/?trackingId=abc#frag"
        )
        self.assertEqual(
            normalized,
            "https://www.linkedin.com/feed/update/urn:li:activity:123456/",
        )

    def test_build_feed_update_permalink_from_urn(self) -> None:
        permalink = self.scraper._build_feed_update_permalink("urn:li:ugcPost:987654321")
        self.assertEqual(
            permalink,
            "https://www.linkedin.com/feed/update/urn:li:ugcPost:987654321/",
        )

    def test_build_content_results_url_uses_latest_and_past_24_hours(self) -> None:
        url = self.scraper._build_content_results_url('python developer, "NY"', window_hours=24)
        self.assertEqual(
            url,
            "https://www.linkedin.com/search/results/content/?keywords=python+developer%2C+%22NY%22&sortBy=%22date_posted%22&origin=FACETED_SEARCH&datePosted=%22past-24h%22",
        )

    def test_sanitize_content_results_url_strips_following_filter(self) -> None:
        url = self.scraper._sanitize_content_results_url(
            "https://www.linkedin.com/search/results/content/?keywords=python%20developer%2C%20%22NY%22&postedBy=%5B%22following%22%5D&sid=5n0",
            query='python developer, "NY"',
            window_hours=24,
        )
        self.assertEqual(
            url,
            "https://www.linkedin.com/search/results/content/?keywords=python+developer%2C+%22NY%22&sortBy=%22date_posted%22&origin=FACETED_SEARCH&datePosted=%22past-24h%22",
        )

    def test_select_post_permalink_prefers_canonical_candidate(self) -> None:
        permalink = self.scraper._select_post_permalink(
            {
                "permalink_candidates": [
                    "https://www.linkedin.com/feed/update/urn:li:activity:123456/?trackingId=abc",
                    "https://www.linkedin.com/posts/example-post/?foo=bar",
                ],
                "urn_candidates": ["urn:li:activity:999999"],
            }
        )
        self.assertEqual(
            permalink,
            "https://www.linkedin.com/feed/update/urn:li:activity:123456/",
        )

    def test_select_content_text_prefers_structured_candidate(self) -> None:
        content = self.scraper._select_content_text(
            content_candidates=["Need Python developer in Irving, TX"],
            full_text="Manish Chauhan 26m Need Python developer in Irving, TX Like Comment",
            author_name="Manish Chauhan",
            relative_time_text="26m",
        )
        self.assertEqual(content, "Need Python developer in Irving, TX")

    def test_select_content_text_falls_back_to_cleaned_full_text(self) -> None:
        content = self.scraper._select_content_text(
            content_candidates=[],
            full_text=(
                "Jitendra Kumar\n"
                "34m\n"
                "Hi,\n"
                "Looking for:-\n"
                "Python Developer\n"
                "Like\n"
                "Comment\n"
                "1 comment"
            ),
            author_name="Jitendra Kumar",
            relative_time_text="34m",
        )
        self.assertEqual(content, "Hi, Looking for:- Python Developer")

    def test_detail_fetch_budget_respects_configured_limit(self) -> None:
        budget = self.scraper._detail_fetch_budget(CaptureProfile(2, 6, 8), 20)
        self.assertEqual(budget, 6)

    def test_balanced_capture_profile_is_single_page(self) -> None:
        profile = self.scraper._capture_profile("balanced")
        self.assertEqual(profile, CaptureProfile(1, 0, 8))

    def test_balanced_search_stays_on_results_page(self) -> None:
        class FakeContext:
            def __init__(self) -> None:
                self.new_page_calls = 0

            def new_page(self) -> None:
                self.new_page_calls += 1
                raise AssertionError("balanced mode should not open a detail page")

        class FakePage:
            def __init__(self) -> None:
                self.url = "https://www.linkedin.com/search/results/content/?keywords=python"

            def wait_for_timeout(self, _milliseconds: int) -> None:
                return None

        fake_context = FakeContext()
        fake_page = FakePage()
        post = ScrapedPost(
            external_id="post-1",
            permalink="https://www.linkedin.com/feed/update/urn:li:activity:123456/",
            author_name="Recruiter",
            author_profile_url=None,
            content_text="Need Python developer in TX",
            relative_time_text="1h",
        )

        self.scraper._open_search = lambda page, query, *, window_hours: True
        self.scraper._assert_logged_in = lambda context, page: None
        self.scraper._ensure_manual_search_filters = lambda page, *, query, window_hours: {
            "content_page_opened": True,
            "latest_filter_clicked": True,
            "latest_filter_active": True,
            "date_filter_clicked": True,
            "date_filter_active": True,
            "visible_time_samples": ["1h"],
        }
        self.scraper._result_count = lambda page: 1
        self.scraper._scroll_results = lambda page, *, window_hours: None
        self.scraper._expand_see_more_js = lambda page: 1
        self.scraper._expand_result_cards = lambda page, *, max_cards: 2
        self.scraper._extract_posts = lambda page: [post]
        self.scraper._filter_posts_by_window = lambda posts, *, window_hours: posts

        result = self.scraper.search_posts_in_session(
            fake_context,
            fake_page,
            'python developer, "TX"',
            max_results=20,
            capture_mode="balanced",
        )

        self.assertEqual(fake_context.new_page_calls, 0)
        self.assertEqual(len(result.posts), 1)
        self.assertEqual(result.audit["query_passes_configured"], 1)
        self.assertEqual(result.audit["detail_fetch_budget"], 0)
        self.assertEqual(result.audit["detail_fetches"], 0)
        self.assertEqual(result.audit["attempts_completed"], 1)
        self.assertEqual(result.audit["inline_more_clicks_total"], 3)


if __name__ == "__main__":
    unittest.main()

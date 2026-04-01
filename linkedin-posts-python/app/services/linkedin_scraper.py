from __future__ import annotations

from contextlib import contextmanager
import re
from dataclasses import dataclass
from hashlib import sha1
from time import sleep
from typing import Any, Iterator
from urllib.parse import quote_plus

from ..config import Settings, get_settings
from ..logging_utils import get_rotating_file_logger
from ..post_age import linkedin_post_is_within_hours


@dataclass(slots=True)
class ScrapedPost:
    external_id: str
    permalink: str | None
    author_name: str | None
    author_profile_url: str | None
    content_text: str
    relative_time_text: str | None
    absolute_posted_at: str | None = None


@dataclass(frozen=True, slots=True)
class CaptureProfile:
    query_passes: int
    detail_fetch_limit: int
    target_buffer: int


@dataclass(slots=True)
class SearchSessionResult:
    posts: list[ScrapedPost]
    audit: dict[str, Any]


RESULT_SELECTORS = (
    "div.feed-shared-update-v2[role='article']",
    "div[data-view-name='feed-full-update']",
    "li.artdeco-card.mb2",
    "li.reusable-search__result-container",
    "div.reusable-search__result-container",
    "li.search-results__list-item",
)


class LinkedInScraper:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_rotating_file_logger(
            "linkedin_scraper",
            str(self.settings.data_dir / "logs" / "linkedin_scraper.log"),
        )

    def storage_state_exists(self) -> bool:
        return self.settings.linkedin_storage_state_path.exists()

    @staticmethod
    def _preview_text(value: str | None, *, limit: int = 180) -> str:
        normalized = " ".join((value or "").split())
        if len(normalized) <= limit:
            return normalized
        return f"{normalized[:limit - 1]}..."

    def bootstrap_session(self) -> str:
        sync_playwright = self._get_sync_playwright()
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=False, slow_mo=100)
            context = browser.new_context(viewport={"width": 1440, "height": 960})
            page = context.new_page()
            page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
            print(
                "\nA browser window is open for LinkedIn login.\n"
                "1. Complete login manually.\n"
                "2. Make sure you can see the LinkedIn feed.\n"
                "3. Return here and press ENTER to save the session.\n"
            )
            input("Press ENTER after LinkedIn is fully logged in: ")
            try:
                page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
            except Exception:
                pass
            if not self._has_authenticated_session(context, page):
                browser.close()
                raise RuntimeError(
                    "LinkedIn login did not complete successfully. "
                    f"LinkedIn ended on: {page.url}. "
                    "Please finish any checkpoint/challenge, make sure you can open "
                    "https://www.linkedin.com/feed/, then retry."
                )
            context.storage_state(path=str(self.settings.linkedin_storage_state_path))
            browser.close()
        return str(self.settings.linkedin_storage_state_path)

    def search_posts(
        self,
        query: str,
        *,
        max_results: int,
        window_hours: int = 24,
        capture_mode: str = "standard",
    ) -> list[ScrapedPost]:
        if not self.storage_state_exists():
            raise RuntimeError(
                "LinkedIn session state is missing. Run scripts/bootstrap_linkedin_session.py first."
            )

        self.logger.info(
            "search_posts_start query=%r max_results=%s window_hours=%s capture_mode=%s",
            query,
            max_results,
            window_hours,
            capture_mode,
        )
        with self.background_session() as (context, page):
            result = self.search_posts_in_session(
                context,
                page,
                query,
                max_results=max_results,
                window_hours=window_hours,
                capture_mode=capture_mode,
            )
        self.logger.info(
            "search_posts_complete query=%r collected=%s",
            query,
            len(result.posts),
        )
        return result.posts

    @contextmanager
    def background_session(self) -> Iterator[tuple[Any, Any]]:
        if not self.storage_state_exists():
            raise RuntimeError(
                "LinkedIn session state is missing. Run scripts/bootstrap_linkedin_session.py first."
            )

        sync_playwright = self._get_sync_playwright()
        with sync_playwright() as playwright:
            self.logger.debug(
                "background_session_launch headless=%s storage_state=%s",
                self.settings.linkedin_headless,
                self.settings.linkedin_storage_state_path,
            )
            browser = playwright.chromium.launch(headless=self.settings.linkedin_headless)
            context = browser.new_context(
                storage_state=str(self.settings.linkedin_storage_state_path),
                viewport={"width": 1440, "height": 960},
            )
            page = context.new_page()
            try:
                yield context, page
            finally:
                self.logger.debug("background_session_close current_url=%s", getattr(page, "url", ""))
                browser.close()

    def search_posts_in_session(
        self,
        context: Any,
        page: Any,
        query: str,
        *,
        max_results: int,
        window_hours: int = 24,
        capture_mode: str = "standard",
    ) -> SearchSessionResult:
        profile = self._capture_profile(capture_mode)
        audit: dict[str, Any] = {
            "query": query,
            "capture_mode": capture_mode,
            "query_passes_configured": profile.query_passes,
            "detail_fetch_limit": profile.detail_fetch_limit,
            "target_buffer": profile.target_buffer,
            "max_results": max_results,
            "window_hours": window_hours,
            "content_page_opened": False,
            "latest_filter_clicked": False,
            "latest_filter_active": False,
            "date_filter_clicked": False,
            "date_filter_active": False,
            "inline_more_clicks_total": 0,
            "detail_fetches": 0,
            "detail_page_more_clicks_total": 0,
            "extracted_posts_total": 0,
            "within_window_posts_total": 0,
            "cards_before_scroll_max": 0,
            "cards_after_scroll_max": 0,
            "attempts": [],
        }
        self.logger.info(
            "search_session_start query=%r passes=%s detail_fetch_limit=%s target_buffer=%s max_results=%s window_hours=%s",
            query,
            profile.query_passes,
            profile.detail_fetch_limit,
            profile.target_buffer,
            max_results,
            window_hours,
        )
        collected: dict[str, ScrapedPost] = {}
        detail_fetched_keys: set[str] = set()
        stagnant_passes = 0
        detail_fetches = 0
        detail_page = context.new_page() if profile.detail_fetch_limit > 0 else None

        try:
            for attempt in range(profile.query_passes):
                attempt_number = attempt + 1
                attempt_audit: dict[str, Any] = {
                    "attempt": attempt_number,
                    "content_page_opened": False,
                    "latest_filter_clicked": False,
                    "latest_filter_active": False,
                    "date_filter_clicked": False,
                    "date_filter_active": False,
                    "cards_before_scroll": 0,
                    "cards_after_scroll": 0,
                    "inline_more_clicks": 0,
                    "detail_fetches": 0,
                    "detail_page_more_clicks": 0,
                    "extracted_posts": 0,
                    "within_window_posts": 0,
                    "new_posts": 0,
                    "collected_total": len(collected),
                }
                self.logger.info(
                    "query_attempt_start query=%r attempt=%s/%s",
                    query,
                    attempt_number,
                    profile.query_passes,
                )
                attempt_audit["used_search_bar"] = self._open_search(page, query)
                attempt_audit["content_page_opened"] = "/search/results/content/" in page.url
                audit["content_page_opened"] = audit["content_page_opened"] or attempt_audit["content_page_opened"]
                self._assert_logged_in(context, page)
                sort_audit = self._apply_sort_latest(page)
                attempt_audit["latest_filter_clicked"] = sort_audit["clicked"]
                attempt_audit["latest_filter_active"] = sort_audit["active"]
                audit["latest_filter_clicked"] = audit["latest_filter_clicked"] or sort_audit["clicked"]
                audit["latest_filter_active"] = audit["latest_filter_active"] or sort_audit["active"]
                if window_hours <= 24:
                    date_audit = self._apply_date_filter(page, "Past 24 hours")
                    attempt_audit["date_filter_clicked"] = date_audit["clicked"]
                    attempt_audit["date_filter_active"] = date_audit["active"]
                    audit["date_filter_clicked"] = audit["date_filter_clicked"] or date_audit["clicked"]
                    audit["date_filter_active"] = audit["date_filter_active"] or date_audit["active"]
                pre_scroll_count = self._result_count(page)
                attempt_audit["cards_before_scroll"] = pre_scroll_count
                audit["cards_before_scroll_max"] = max(audit["cards_before_scroll_max"], pre_scroll_count)
                self.logger.debug(
                    "query_attempt_before_scroll query=%r attempt=%s url=%s result_count=%s",
                    query,
                    attempt_number,
                    page.url,
                    pre_scroll_count,
                )
                self._scroll_results(page, window_hours=window_hours)
                post_scroll_count = self._result_count(page)
                attempt_audit["cards_after_scroll"] = post_scroll_count
                audit["cards_after_scroll_max"] = max(audit["cards_after_scroll_max"], post_scroll_count)
                inline_more_clicks = self._expand_see_more_js(page)
                inline_more_clicks += self._expand_result_cards(page, max_cards=post_scroll_count)
                attempt_audit["inline_more_clicks"] = inline_more_clicks
                audit["inline_more_clicks_total"] += inline_more_clicks
                extracted_posts = self._extract_posts(page)
                pass_posts = self._filter_posts_by_window(extracted_posts, window_hours=window_hours)
                attempt_audit["extracted_posts"] = len(extracted_posts)
                attempt_audit["within_window_posts"] = len(pass_posts)
                audit["extracted_posts_total"] += len(extracted_posts)
                audit["within_window_posts_total"] += len(pass_posts)
                self.logger.info(
                    "query_attempt_extracted query=%r attempt=%s extracted=%s within_window=%s",
                    query,
                    attempt_number,
                    len(extracted_posts),
                    len(pass_posts),
                )

                new_posts = 0
                for post in pass_posts:
                    key = self._collection_key(post)
                    self.logger.debug(
                        "candidate_post query=%r attempt=%s key=%s permalink=%s author=%r chars=%s preview=%r",
                        query,
                        attempt_number,
                        key,
                        post.permalink,
                        post.author_name,
                        len((post.content_text or "").strip()),
                        self._preview_text(post.content_text),
                    )
                    if (
                        detail_page is not None
                        and detail_fetches < profile.detail_fetch_limit
                        and key not in detail_fetched_keys
                        and self._should_fetch_post_detail(post)
                    ):
                        self.logger.debug(
                            "detail_fetch_start query=%r attempt=%s key=%s chars=%s detail_fetches=%s/%s",
                            query,
                            attempt_number,
                            key,
                            len((post.content_text or "").strip()),
                            detail_fetches + 1,
                            profile.detail_fetch_limit,
                        )
                        detail_fetched_keys.add(key)
                        detail_fetches += 1
                        attempt_audit["detail_fetches"] += 1
                        audit["detail_fetches"] += 1
                        detail_post, detail_more_clicks = self._fetch_post_detail(detail_page, post)
                        attempt_audit["detail_page_more_clicks"] += detail_more_clicks
                        audit["detail_page_more_clicks_total"] += detail_more_clicks
                        post = self._merge_scraped_posts(post, detail_post)
                        self.logger.debug(
                            "detail_fetch_complete query=%r attempt=%s key=%s merged_chars=%s",
                            query,
                            attempt_number,
                            key,
                            len((post.content_text or "").strip()),
                        )

                    existing = collected.get(key)
                    if existing is None:
                        collected[key] = post
                        new_posts += 1
                        continue

                    merged = self._merge_scraped_posts(existing, post)
                    if merged != existing:
                        collected[key] = merged
                        self.logger.debug(
                            "candidate_post_merged query=%r attempt=%s key=%s old_chars=%s new_chars=%s",
                            query,
                            attempt_number,
                            key,
                            len((existing.content_text or "").strip()),
                            len((merged.content_text or "").strip()),
                        )

                if new_posts == 0:
                    stagnant_passes += 1
                else:
                    stagnant_passes = 0
                attempt_audit["new_posts"] = new_posts
                attempt_audit["collected_total"] = len(collected)
                attempt_audit["final_url"] = page.url
                audit["attempts"].append(attempt_audit)
                self.logger.info(
                    "query_attempt_complete query=%r attempt=%s new_posts=%s collected_total=%s stagnant_passes=%s",
                    query,
                    attempt_number,
                    new_posts,
                    len(collected),
                    stagnant_passes,
                )

                if stagnant_passes >= 2:
                    self.logger.info(
                        "query_attempt_stop query=%r reason=%r collected_total=%s",
                        query,
                        "stagnant_passes",
                        len(collected),
                    )
                    break
                if attempt + 1 < profile.query_passes:
                    page.wait_for_timeout(750 * (attempt + 1))

            final_posts = list(collected.values())
            audit["attempts_completed"] = len(audit["attempts"])
            audit["unique_posts_collected"] = len(collected)
            audit["returned_posts"] = len(final_posts)
            audit["final_url"] = getattr(page, "url", "")
            self.logger.info(
                "search_session_complete query=%r collected_total=%s returned=%s detail_fetches=%s",
                query,
                len(collected),
                len(final_posts),
                detail_fetches,
            )
            return SearchSessionResult(posts=final_posts, audit=audit)
        finally:
            if detail_page is not None:
                detail_page.close()

    def _capture_profile(self, capture_mode: str) -> CaptureProfile:
        normalized = (capture_mode or "standard").strip().lower()
        if normalized == "deep":
            return CaptureProfile(
                query_passes=max(1, self.settings.deep_query_passes),
                detail_fetch_limit=max(0, self.settings.deep_detail_fetch_limit),
                target_buffer=12,
            )
        if normalized == "balanced":
            return CaptureProfile(
                query_passes=max(1, self.settings.balanced_query_passes),
                detail_fetch_limit=max(0, self.settings.balanced_detail_fetch_limit),
                target_buffer=8,
            )
        return CaptureProfile(query_passes=1, detail_fetch_limit=0, target_buffer=0)

    def _filter_posts_by_window(self, posts: list[ScrapedPost], *, window_hours: int) -> list[ScrapedPost]:
        if window_hours <= 0:
            return posts
        return [
            post
            for post in posts
            if linkedin_post_is_within_hours(
                absolute_posted_at=post.absolute_posted_at,
                relative_time_text=post.relative_time_text,
                window_hours=window_hours,
            )
        ]

    def _collection_key(self, post: ScrapedPost) -> str:
        return post.permalink or post.external_id

    def _should_fetch_post_detail(self, post: ScrapedPost) -> bool:
        return bool(post.permalink) and (
            len((post.content_text or "").strip()) < self.settings.detail_fetch_char_threshold
            or self._appears_truncated(post.content_text)
        )

    @staticmethod
    def _appears_truncated(value: str | None) -> bool:
        normalized = " ".join((value or "").split()).lower()
        return any(
            marker in normalized
            for marker in (
                "... more",
                "… more",
                "...see more",
                "…see more",
                " see more",
            )
        )

    def _get_sync_playwright(self) -> Any:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed yet. Run `pip install -r requirements.txt` "
                "and `python -m playwright install chromium`."
            ) from exc
        return sync_playwright

    def _open_search(self, page: Any, query: str) -> bool:
        self.logger.debug("open_search_start query=%r current_url=%s", query, getattr(page, "url", ""))
        if self._open_search_from_search_bar(page, query):
            self.logger.debug("open_search_via_search_bar_success query=%r final_url=%s", query, page.url)
            return True

        encoded_query = quote_plus(query)
        page.goto(
            f"https://www.linkedin.com/search/results/content/?keywords={encoded_query}",
            wait_until="domcontentloaded",
        )
        page.wait_for_timeout(1500)
        self.logger.debug("open_search_direct_url query=%r final_url=%s", query, page.url)
        return False

    def _open_search_from_search_bar(self, page: Any, query: str) -> bool:
        try:
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
            page.wait_for_timeout(1500)

            search_input = page.locator(
                "input[placeholder*='Search'], input[aria-label*='Search'], input[role='combobox']"
            ).first
            search_input.wait_for(state="visible", timeout=5000)
            search_input.click(timeout=3000)
            search_input.fill(query, timeout=3000)
            search_input.press("Enter")
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(1500)
            self.logger.debug("search_bar_submit query=%r intermediate_url=%s", query, page.url)

            if self._open_content_results(page):
                self._normalize_content_results_url(page, query)
                return True
        except Exception as exc:
            self.logger.warning(
                "open_search_from_search_bar_failed query=%r error=%s",
                query,
                exc,
            )
            return False
        return "/search/results/content/" in page.url

    def _open_content_results(self, page: Any) -> bool:
        if "/search/results/content/" in page.url:
            self.logger.debug("open_content_results_already_on_content_page url=%s", page.url)
            return True

        selectors = [
            ("href_link", lambda: page.locator("a[href*='/search/results/content/']").first.click(timeout=3000)),
            ("role_link", lambda: page.get_by_role("link", name=re.compile(r"^(Posts|Content)$", re.I)).click(timeout=3000)),
            ("text_link", lambda: page.get_by_text(re.compile(r"^(Posts|Content)$", re.I)).click(timeout=3000)),
        ]
        for label, action in selectors:
            try:
                action()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1500)
                if "/search/results/content/" in page.url:
                    self.logger.debug("open_content_results_success method=%s url=%s", label, page.url)
                    return True
            except Exception as exc:
                self.logger.debug("open_content_results_method_failed method=%s error=%s", label, exc)
                continue
        self.logger.warning("open_content_results_failed final_url=%s", page.url)
        return False

    def _normalize_content_results_url(self, page: Any, query: str) -> None:
        encoded_query = quote_plus(query)
        clean_url = f"https://www.linkedin.com/search/results/content/?keywords={encoded_query}"
        if page.url == clean_url:
            self.logger.debug("normalize_content_results_url_skipped query=%r url=%s", query, page.url)
            return
        self.logger.debug("normalize_content_results_url query=%r from=%s to=%s", query, page.url, clean_url)
        page.goto(clean_url, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)

    def _assert_logged_in(self, context: Any, page: Any) -> None:
        current_url = page.url.lower()
        if ("login" in current_url or "checkpoint" in current_url) and not self._has_authenticated_session(
            context, page
        ):
            self.logger.error("assert_logged_in_failed current_url=%s", page.url)
            raise RuntimeError(
                "LinkedIn redirected to a login/checkpoint page. Refresh the saved session first."
            )

    def _has_authenticated_session(self, context: Any, page: Any) -> bool:
        try:
            cookies = context.cookies()
        except Exception:
            cookies = []

        has_li_at_cookie = any(
            cookie.get("name") == "li_at" and "linkedin.com" in cookie.get("domain", "")
            for cookie in cookies
        )
        if has_li_at_cookie:
            return True

        try:
            if page.locator("input[placeholder*='Search']").count():
                return True
        except Exception:
            pass

        return False

    def _apply_sort_latest(self, page: Any) -> dict[str, bool]:
        clicked = False
        try:
            if self._chip_is_active(page, "Latest"):
                self.logger.debug("apply_sort_latest_already_active url=%s", page.url)
                return {"clicked": False, "active": True}
            page.get_by_role("button", name=re.compile("Sort by", re.I)).click(timeout=3000)
            page.get_by_text(re.compile("^Latest$", re.I)).click(timeout=3000)
            page.wait_for_timeout(1000)
            clicked = True
        except Exception:
            # LinkedIn sometimes keeps the previous selection or changes the control markup.
            self.logger.debug("apply_sort_latest_skipped url=%s", page.url)
        active = self._chip_is_active(page, "Latest")
        self.logger.debug("apply_sort_latest_result clicked=%s active=%s url=%s", clicked, active, page.url)
        return {"clicked": clicked, "active": active}

    def _apply_date_filter(self, page: Any, label: str) -> dict[str, bool]:
        clicked = False
        try:
            if self._chip_is_active(page, label):
                self.logger.debug("apply_date_filter_already_active label=%r url=%s", label, page.url)
                return {"clicked": False, "active": True}
            page.get_by_role("button", name=re.compile("Date posted", re.I)).click(timeout=3000)
            page.get_by_label(re.compile(label, re.I)).check(timeout=3000)
            try:
                page.get_by_role("button", name=re.compile("Apply", re.I)).click(timeout=3000)
            except Exception:
                page.keyboard.press("Escape")
            page.wait_for_timeout(1000)
            clicked = True
        except Exception:
            self.logger.debug("apply_date_filter_skipped label=%r url=%s", label, page.url)
        active = self._chip_is_active(page, label)
        self.logger.debug("apply_date_filter_result label=%r clicked=%s active=%s url=%s", label, clicked, active, page.url)
        return {"clicked": clicked, "active": active}

    def _chip_is_active(self, page: Any, label: str) -> bool:
        try:
            return bool(
                page.evaluate(
                    """
                    (targetLabel) => {
                      const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
                      const target = normalize(targetLabel);
                      const candidates = Array.from(
                        document.querySelectorAll('button, a, div[role="button"], span[role="button"], li, span')
                      );
                      const isActive = (element) => {
                        if (!element) return false;
                        const related = [
                          element,
                          element.parentElement,
                          element.closest('[class], [aria-pressed], [aria-selected], [aria-current], [data-active]')
                        ].filter(Boolean);
                        return related.some((node) => {
                          const className = String(node.className || "").toLowerCase();
                          const ariaPressed = node.getAttribute && node.getAttribute('aria-pressed');
                          const ariaSelected = node.getAttribute && node.getAttribute('aria-selected');
                          const ariaCurrent = node.getAttribute && node.getAttribute('aria-current');
                          const dataActive = node.getAttribute && node.getAttribute('data-active');
                          return ariaPressed === 'true' ||
                            ariaSelected === 'true' ||
                            ariaCurrent === 'true' ||
                            ariaCurrent === 'page' ||
                            dataActive === 'true' ||
                            className.includes('selected') ||
                            className.includes('active');
                        });
                      };
                      return candidates.some((element) => normalize(element.innerText || element.textContent) === target && isActive(element));
                    }
                    """,
                    label,
                )
            )
        except Exception:
            return False

    def _scroll_results(self, page: Any, *, window_hours: int = 24, target_results: int = 0) -> None:
        """Scroll results, stopping as soon as we see posts older than window_hours.

        LinkedIn sorts by Latest — once we see posts > 24h old at the bottom,
        there are no more fresh posts to find. We stop scrolling immediately
        instead of continuing through months of old content.
        """
        stable_steps = 0
        max_steps = 30  # 30 steps × ~1s each = 30s max per state
        last_count = self._result_count(page)
        last_height = self._page_height(page)
        self.logger.debug(
            "scroll_results_start max_steps=%s initial_count=%s initial_height=%s window_hours=%s",
            max_steps,
            last_count,
            last_height,
            window_hours,
        )

        for step in range(max_steps):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(int(self.settings.scraper_scroll_pause_seconds * 1000))
            current_count = self._result_count(page)
            current_height = self._page_height(page)
            self.logger.debug(
                "scroll_results_step step=%s/%s current_count=%s current_height=%s stable_steps=%s",
                step + 1,
                max_steps,
                current_count,
                current_height,
                stable_steps,
            )

            # Smart stop: check if newest cards loaded are already old
            # If we see posts older than window_hours appearing, stop — no more fresh posts
            if step > 2 and self._last_cards_are_old(page, window_hours=window_hours):
                self.logger.debug(
                    "scroll_results_stop reason=old_posts_detected step=%s current_count=%s",
                    step + 1,
                    current_count,
                )
                break

            if current_count == last_count and current_height == last_height:
                stable_steps += 1
                if stable_steps >= 3:
                    self.logger.debug("scroll_results_stop reason=stable current_count=%s", current_count)
                    break
            else:
                stable_steps = 0
                last_count = current_count
                last_height = current_height
        sleep(0.3)
        self.logger.debug("scroll_results_complete final_count=%s final_height=%s", self._result_count(page), self._page_height(page))

    def _result_count(self, page: Any) -> int:
        locator = self._result_locator(page)
        return locator.count() if locator is not None else 0

    def _page_height(self, page: Any) -> int:
        try:
            return int(page.evaluate("() => document.scrollingElement?.scrollHeight || document.body.scrollHeight || 0"))
        except Exception:
            return 0

    def _last_cards_are_old(self, page: Any, *, window_hours: int = 24) -> bool:
        """Check if the last few visible cards have timestamps older than window_hours.

        Used to stop scrolling early — if the bottom cards are already old,
        LinkedIn has no more fresh posts to load.
        """
        try:
            cards = self._result_cards(page)
            if not cards:
                return False
            # Check the last 5 cards at the bottom of the page
            check_cards = cards[-5:] if len(cards) >= 5 else cards
            old_count = 0
            for card in check_cards:
                try:
                    time_text = card.inner_text(timeout=300).lower()
                    # If text contains days/weeks/months markers beyond window, it's old
                    if window_hours <= 24:
                        # Look for patterns like '2d', '3d', '1w', '2w', '1mo', '3mo' etc.
                        import re
                        if re.search(r'\b[2-9]d\b|\b\d+w\b|\b\d+mo\b|\b\d+ (days|weeks|months)', time_text):
                            old_count += 1
                except Exception:
                    continue
            # If majority of last cards are old, stop
            return old_count >= max(1, len(check_cards) // 2)
        except Exception:
            return False

    def _result_cards(self, page: Any) -> list[Any]:
        locator = self._result_locator(page)
        if locator is None:
            return []

        selector = ", ".join(RESULT_SELECTORS)
        cards: list[Any] = []
        for index in range(locator.count()):
            card = locator.nth(index)
            try:
                is_nested = bool(
                    card.evaluate(
                        "(node, selector) => !!(node.parentElement && node.parentElement.closest(selector))",
                        selector,
                    )
                )
            except Exception:
                is_nested = False
            if is_nested:
                continue
            cards.append(card)
        return cards

    def _result_locator(self, page: Any) -> Any | None:
        selector = ", ".join(RESULT_SELECTORS)
        locator = page.locator(selector)
        return locator if locator.count() else None

    def _extract_posts(self, page: Any) -> list[ScrapedPost]:
        cards = self._result_cards(page)
        if not cards:
            self.logger.warning("extract_posts_no_selector url=%s", page.url)
            return []

        self.logger.debug(
            "extract_posts_start selectors=%r card_count=%s url=%s",
            RESULT_SELECTORS,
            len(cards),
            page.url,
        )
        posts: list[ScrapedPost] = []
        for index, card in enumerate(cards):
            try:
                payload = card.evaluate(
                    """
                    (node) => {
                      const textFrom = (selectors) => {
                        for (const selector of selectors) {
                          const element = node.querySelector(selector);
                          if (element && element.innerText && element.innerText.trim()) {
                            return element.innerText.trim();
                          }
                        }
                        return "";
                      };

                      const linkSelectors = [
                        'a[href*="/feed/update/"]',
                        'a[href*="/posts/"]',
                        'a[href*="/activity/"]'
                      ];
                      let permalink = "";
                      for (const selector of linkSelectors) {
                        const link = node.querySelector(selector);
                        if (link && link.href) {
                          permalink = link.href;
                          break;
                        }
                      }
                      if (!permalink) {
                        const urn = node.getAttribute('data-urn');
                        if (urn) {
                          permalink = `https://www.linkedin.com/feed/update/${urn}/`;
                        }
                      }

                      const authorSelectors = [
                        '.update-components-actor__name span[dir="ltr"]',
                        '.entity-result__title-text',
                        'span[dir="ltr"]'
                      ];

                      const authorProfileSelectors = [
                        'a[href*="/in/"]',
                        'a[href*="/company/"]'
                      ];

                      let authorProfileUrl = "";
                      for (const selector of authorProfileSelectors) {
                        const link = node.querySelector(selector);
                        if (link && link.href) {
                          authorProfileUrl = link.href;
                          break;
                        }
                      }

                      return {
                        permalink,
                        author_name: textFrom(authorSelectors),
                        author_profile_url: authorProfileUrl,
                        content_text: textFrom([
                          '.update-components-text',
                          '.feed-shared-update-v2__description',
                          '.entity-result__summary',
                          'div[dir="ltr"]'
                        ]),
                        full_text: node.innerText ? node.innerText.trim() : "",
                        relative_time_text: textFrom([
                          '.update-components-actor__sub-description',
                          '.entity-result__secondary-subtitle'
                        ]),
                      };
                    }
                    """
                )
            except Exception:
                continue

            content_text = " ".join((payload.get("content_text") or "").split())
            full_text = " ".join((payload.get("full_text") or "").split())
            if len(full_text) > len(content_text):
                content_text = full_text
            author_name = self._clean_author_name(payload.get("author_name"))
            permalink = payload.get("permalink") or None
            author_profile_url = payload.get("author_profile_url") or None
            relative_time_text = payload.get("relative_time_text") or None

            if not content_text and not permalink:
                self.logger.debug("extract_posts_skip_empty_card index=%s", index)
                continue

            external_id = self._build_external_id(permalink, author_name, content_text)
            post = ScrapedPost(
                external_id=external_id,
                permalink=permalink,
                author_name=author_name or None,
                author_profile_url=author_profile_url,
                content_text=content_text,
                relative_time_text=relative_time_text,
            )
            posts.append(post)
            self.logger.debug(
                "extract_posts_card index=%s permalink=%s author=%r chars=%s relative_time=%r preview=%r",
                index,
                post.permalink,
                post.author_name,
                len((post.content_text or "").strip()),
                post.relative_time_text,
                self._preview_text(post.content_text),
            )
        self.logger.debug("extract_posts_complete extracted=%s url=%s", len(posts), page.url)
        return posts

    def _expand_see_more_js(self, page: Any) -> int:
        """Use JavaScript to click ALL 'See more' / '…more' buttons at once.

        This is much faster than iterating through each card with Playwright
        locators, and actually catches buttons that the old method missed.
        LinkedIn uses various patterns for truncated content.
        """
        try:
            expanded_total = 0
            for _ in range(3):
                expanded = int(page.evaluate("""
                    () => {
                        let count = 0;
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        const allClickables = document.querySelectorAll(
                            'button, a[role="button"], [role="button"], span.feed-shared-inline-show-more-text'
                        );
                        for (const el of allClickables) {
                            const text = normalize(el.innerText || el.textContent);
                            const ariaLabel = normalize(el.getAttribute && el.getAttribute('aria-label'));
                            const looksExpandable =
                                text === 'see more' ||
                                text === '…see more' ||
                                text === '...see more' ||
                                text === '…more' ||
                                text === '...more' ||
                                text.endsWith(' see more') ||
                                text.endsWith('... more') ||
                                text.endsWith('… more') ||
                                ariaLabel.includes('see more');
                            if (!looksExpandable) {
                                continue;
                            }
                            try { el.click(); count++; } catch(e) {}
                        }
                        return count;
                    }
                """))
                expanded_total += expanded
                if expanded <= 0:
                    break
                page.wait_for_timeout(600)
            self.logger.debug("expand_see_more_js_complete expanded=%s", expanded_total)
            return expanded_total
        except Exception as exc:
            self.logger.debug("expand_see_more_js_error error=%s", exc)
            return 0

    def _expand_result_cards(self, page: Any, *, max_cards: int = 0) -> int:
        """Legacy card-by-card expansion (kept as fallback)."""
        cards = self._result_cards(page)
        if not cards:
            return 0

        limit = max_cards if max_cards > 0 else len(cards)
        expanded_count = 0
        for index, card in enumerate(cards[:limit]):
            try:
                controls = card.locator("button, a[role='button'], span[role='button']")
                for button_index in range(controls.count()):
                    control = controls.nth(button_index)
                    try:
                        label = " ".join(control.inner_text(timeout=500).split()).lower()
                    except Exception:
                        continue
                    if "see more" not in label:
                        continue
                    try:
                        control.click(timeout=500)
                        expanded_count += 1
                    except Exception:
                        try:
                            control.evaluate("(node) => node.click()")
                            expanded_count += 1
                        except Exception:
                            continue
            except Exception:
                continue
        self.logger.debug("expand_result_cards_complete cards_checked=%s/%s expanded=%s", min(limit, len(cards)), len(cards), expanded_count)
        return expanded_count

    def _fetch_post_detail(self, page: Any, post: ScrapedPost) -> tuple[ScrapedPost | None, int]:
        if not post.permalink:
            self.logger.debug("fetch_post_detail_skipped_missing_permalink")
            return None, 0

        try:
            self.logger.debug("fetch_post_detail_start permalink=%s", post.permalink)
            page.goto(post.permalink, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            current_url = page.url.lower()
            if "login" in current_url or "checkpoint" in current_url:
                self.logger.warning("fetch_post_detail_login_redirect permalink=%s final_url=%s", post.permalink, page.url)
                return None, 0
            detail_more_clicks = self._expand_page_see_more(page)
            payload = page.evaluate(
                """
                () => {
                  const textFrom = (selectors) => {
                    for (const selector of selectors) {
                      const element = document.querySelector(selector);
                      if (element && element.innerText && element.innerText.trim()) {
                        return element.innerText.trim();
                      }
                    }
                    return "";
                  };

                  const firstHref = (selectors) => {
                    for (const selector of selectors) {
                      const element = document.querySelector(selector);
                      if (element && element.href) {
                        return element.href;
                      }
                    }
                    return "";
                  };

                  const timeElement = document.querySelector("time");
                  return {
                    permalink: window.location.href,
                    author_name: textFrom([
                      '.update-components-actor__name span[dir="ltr"]',
                      '.feed-shared-actor__name',
                      'span[dir="ltr"]'
                    ]),
                    author_profile_url: firstHref([
                      'a[href*="/in/"]',
                      'a[href*="/company/"]'
                    ]),
                    content_text: textFrom([
                      '.update-components-text',
                      '.feed-shared-update-v2__description',
                      '.break-words',
                      'main div[dir="ltr"]'
                    ]),
                    relative_time_text: textFrom([
                      '.update-components-actor__sub-description',
                      '.feed-shared-actor__sub-description'
                    ]),
                    absolute_posted_at: timeElement ? timeElement.getAttribute('datetime') || "" : ""
                  };
                }
                """
                )
        except Exception as exc:
            self.logger.warning("fetch_post_detail_failed permalink=%s error=%s", post.permalink, exc)
            return None, 0

        content_text = " ".join((payload.get("content_text") or "").split())
        if not content_text:
            self.logger.debug("fetch_post_detail_empty_content permalink=%s", post.permalink)
            return None, detail_more_clicks

        author_name = self._clean_author_name(payload.get("author_name")) or post.author_name
        permalink = payload.get("permalink") or post.permalink
        author_profile_url = payload.get("author_profile_url") or post.author_profile_url
        relative_time_text = payload.get("relative_time_text") or post.relative_time_text
        absolute_posted_at = payload.get("absolute_posted_at") or post.absolute_posted_at

        detail_post = ScrapedPost(
            external_id=self._build_external_id(permalink, author_name, content_text),
            permalink=permalink,
            author_name=author_name,
            author_profile_url=author_profile_url,
            content_text=content_text,
            relative_time_text=relative_time_text,
            absolute_posted_at=absolute_posted_at,
        )
        self.logger.debug(
            "fetch_post_detail_complete permalink=%s chars=%s relative_time=%r preview=%r",
            detail_post.permalink,
            len((detail_post.content_text or "").strip()),
            detail_post.relative_time_text,
            self._preview_text(detail_post.content_text),
        )
        return detail_post, detail_more_clicks

    def _expand_page_see_more(self, page: Any) -> int:
        try:
            controls = page.locator("button, a[role='button'], span[role='button']")
            expanded_count = 0
            for index in range(controls.count()):
                control = controls.nth(index)
                try:
                    label = " ".join(control.inner_text(timeout=400).split()).lower()
                except Exception:
                    continue
                if "see more" not in label:
                    continue
                try:
                    control.click(timeout=400)
                    expanded_count += 1
                except Exception:
                    try:
                        control.evaluate("(node) => node.click()")
                        expanded_count += 1
                    except Exception:
                        continue
            self.logger.debug("expand_page_see_more_complete url=%s expanded=%s", page.url, expanded_count)
            return expanded_count
        except Exception:
            self.logger.debug("expand_page_see_more_skipped url=%s", page.url)
            return 0

    def _merge_scraped_posts(self, base: ScrapedPost, extra: ScrapedPost | None) -> ScrapedPost:
        if extra is None:
            return base

        base_text = (base.content_text or "").strip()
        extra_text = (extra.content_text or "").strip()
        if len(extra_text) > len(base_text):
            content_text = extra.content_text
            relative_time_text = extra.relative_time_text or base.relative_time_text
            absolute_posted_at = extra.absolute_posted_at or base.absolute_posted_at
        else:
            content_text = base.content_text
            relative_time_text = base.relative_time_text or extra.relative_time_text
            absolute_posted_at = base.absolute_posted_at or extra.absolute_posted_at

        permalink = extra.permalink or base.permalink
        author_name = extra.author_name or base.author_name
        author_profile_url = extra.author_profile_url or base.author_profile_url
        external_id = self._build_external_id(permalink, author_name, content_text)
        return ScrapedPost(
            external_id=external_id,
            permalink=permalink,
            author_name=author_name,
            author_profile_url=author_profile_url,
            content_text=content_text,
            relative_time_text=relative_time_text,
            absolute_posted_at=absolute_posted_at,
        )

    def _build_external_id(self, permalink: str | None, author_name: str | None, content_text: str) -> str:
        if permalink:
            return permalink
        raw = f"{author_name or ''}|{content_text}"
        return sha1(raw.encode("utf-8")).hexdigest()

    def _clean_author_name(self, value: str | None) -> str:
        parts = [" ".join(part.split()) for part in (value or "").splitlines()]
        parts = [part for part in parts if part]
        seen: set[str] = set()
        ordered: list[str] = []
        for part in parts:
            key = part.casefold()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(part)
        return ordered[0] if ordered else ""

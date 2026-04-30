from __future__ import annotations

from contextlib import contextmanager
import re
from dataclasses import dataclass
from hashlib import sha1
from time import sleep
from typing import Any, Iterator
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

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
    "div[role='listitem']:has(h2:has-text('Feed post'))",
)

FILTER_RETRY_ATTEMPTS = 1
FILTER_WAIT_MS = 900


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
        collection_target = self._collection_target(profile, max_results)
        detail_fetch_budget = self._detail_fetch_budget(profile, max_results)
        audit: dict[str, Any] = {
            "query": query,
            "capture_mode": capture_mode,
            "query_passes_configured": profile.query_passes,
            "collection_target": collection_target,
            "detail_fetch_limit": profile.detail_fetch_limit,
            "detail_fetch_budget": detail_fetch_budget,
            "target_buffer": profile.target_buffer,
            "max_results": max_results,
            "window_hours": window_hours,
            "content_page_opened": False,
            "latest_filter_clicked": False,
            "latest_filter_active": False,
            "date_filter_clicked": False,
            "date_filter_active": False,
            "visible_time_samples": [],
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
        detail_page = context.new_page() if detail_fetch_budget > 0 else None

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
                    "visible_time_samples": [],
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
                attempt_audit["used_search_bar"] = self._open_search(page, query, window_hours=window_hours)
                self._assert_logged_in(context, page)
                filter_audit = self._ensure_manual_search_filters(page, query=query, window_hours=window_hours)
                attempt_audit["content_page_opened"] = filter_audit["content_page_opened"]
                attempt_audit["latest_filter_clicked"] = filter_audit["latest_filter_clicked"]
                attempt_audit["latest_filter_active"] = filter_audit["latest_filter_active"]
                attempt_audit["date_filter_clicked"] = filter_audit["date_filter_clicked"]
                attempt_audit["date_filter_active"] = filter_audit["date_filter_active"]
                attempt_audit["visible_time_samples"] = filter_audit["visible_time_samples"]
                audit["content_page_opened"] = audit["content_page_opened"] or filter_audit["content_page_opened"]
                audit["latest_filter_clicked"] = audit["latest_filter_clicked"] or filter_audit["latest_filter_clicked"]
                audit["latest_filter_active"] = audit["latest_filter_active"] or filter_audit["latest_filter_active"]
                audit["date_filter_clicked"] = audit["date_filter_clicked"] or filter_audit["date_filter_clicked"]
                audit["date_filter_active"] = audit["date_filter_active"] or filter_audit["date_filter_active"]
                if filter_audit["visible_time_samples"]:
                    audit["visible_time_samples"] = filter_audit["visible_time_samples"]
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
                self._scroll_results(page, window_hours=window_hours, target_results=collection_target)
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
                        and detail_fetches < detail_fetch_budget
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
                            detail_fetch_budget,
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
            # Balanced mode stays on the search results page for speed.
            return CaptureProfile(query_passes=1, detail_fetch_limit=0, target_buffer=8)
        return CaptureProfile(query_passes=1, detail_fetch_limit=0, target_buffer=0)

    @staticmethod
    def _detail_fetch_budget(profile: CaptureProfile, max_results: int) -> int:
        del max_results
        return max(0, profile.detail_fetch_limit)

    def _collection_target(self, profile: CaptureProfile, max_results: int) -> int:
        requested = max(1, int(max_results))
        minimum_target = min(max(50, requested * 2), 75)
        return max(requested + max(0, profile.target_buffer), minimum_target)

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
            not post.absolute_posted_at
            or len((post.content_text or "").strip()) < self.settings.detail_fetch_char_threshold
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

    @staticmethod
    def _normalize_space(value: Any) -> str:
        return " ".join(str(value or "").split())

    @staticmethod
    def _label_pattern(label: str, *, exact: bool) -> re.Pattern[str]:
        escaped = re.escape(label)
        if exact:
            return re.compile(rf"^{escaped}$", re.I)
        return re.compile(escaped, re.I)

    def _ensure_manual_search_filters(self, page: Any, *, query: str, window_hours: int) -> dict[str, Any]:
        content_page_opened = "/search/results/content/" in page.url or self._open_content_results(page)
        if not content_page_opened:
            raise RuntimeError("LinkedIn did not open the Posts results page.")
        self._normalize_content_results_url(page, query, window_hours=window_hours)

        latest_audit = self._ensure_filter_active(
            page,
            active_label="Latest",
            option_label="Latest",
            trigger_labels=("Sort by", "Latest"),
        )
        visible_samples = self._sample_visible_times(page)
        if not latest_audit["active"]:
            # New LinkedIn DOM uses obfuscated a11y attrs — the chip-active check
            # often fails even when sortBy=date_posted is applied via URL. Posts
            # are still rendering (visible_samples non-empty), so log and continue.
            self.logger.warning(
                "filter_guard_latest_unconfirmed query=%r visible_times=%s",
                query,
                visible_samples,
            )

        # Date filter UI is skipped entirely: the URL no longer carries
        # datePosted (LinkedIn returns "No results found" for past-24h on the
        # new DOM) and date filtering happens in Python via _filter_posts_by_window.
        # Trying to click the chip wastes ~80s per run on buttons whose
        # selectors no longer match.
        date_audit = {"clicked": False, "active": True}

        return {
            "content_page_opened": content_page_opened,
            "latest_filter_clicked": latest_audit["clicked"],
            "latest_filter_active": latest_audit["active"],
            "date_filter_clicked": date_audit["clicked"],
            "date_filter_active": date_audit["active"],
            "visible_time_samples": self._sample_visible_times(page),
        }

    def _ensure_filter_active(
        self,
        page: Any,
        *,
        active_label: str,
        option_label: str,
        trigger_labels: tuple[str, ...],
    ) -> dict[str, bool]:
        clicked = False
        for _ in range(FILTER_RETRY_ATTEMPTS):
            if self._chip_is_active(page, active_label):
                return {"clicked": clicked, "active": True}

            opened = self._open_filter_menu(page, trigger_labels)
            selected = self._select_filter_option(page, option_label)
            clicked = clicked or opened or selected
            self._apply_filter_panel(page)
            page.wait_for_timeout(FILTER_WAIT_MS)

            if self._chip_is_active(page, active_label):
                return {"clicked": clicked, "active": True}

            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
            page.wait_for_timeout(150)

        return {"clicked": clicked, "active": self._chip_is_active(page, active_label)}

    def _open_filter_menu(self, page: Any, labels: tuple[str, ...]) -> bool:
        for label in labels:
            if self._click_labeled_control(page, label, exact=False):
                page.wait_for_timeout(250)
                return True
        return False

    def _select_filter_option(self, page: Any, label: str) -> bool:
        return self._check_labeled_option(page, label) or self._click_labeled_control(page, label, exact=True)

    def _apply_filter_panel(self, page: Any) -> bool:
        for label in ("Apply", "Show results", "Done"):
            if self._click_labeled_control(page, label, exact=False):
                page.wait_for_timeout(250)
                return True
        return False

    def _check_labeled_option(self, page: Any, label: str) -> bool:
        try:
            page.get_by_label(self._label_pattern(label, exact=True)).first.check(timeout=1500)
            return True
        except Exception:
            return False

    def _click_labeled_control(self, page: Any, label: str, *, exact: bool) -> bool:
        pattern = self._label_pattern(label, exact=exact)
        locators = [
            page.get_by_role("button", name=pattern),
            page.get_by_role("link", name=pattern),
            page.get_by_role("option", name=pattern),
            page.get_by_role("menuitemradio", name=pattern),
            page.get_by_role("menuitemcheckbox", name=pattern),
            page.get_by_text(pattern),
        ]
        for locator in locators:
            if self._safe_click(locator):
                return True
        return self._click_text_control_js(page, label, exact=exact)

    def _safe_click(self, locator: Any) -> bool:
        try:
            target = locator.first
            target.scroll_into_view_if_needed(timeout=750)
            target.click(timeout=1500)
            return True
        except Exception:
            try:
                locator.first.evaluate("(node) => node.click()")
                return True
            except Exception:
                return False

    def _click_text_control_js(self, page: Any, label: str, *, exact: bool) -> bool:
        try:
            return bool(
                page.evaluate(
                    """
                    ({ label, exact }) => {
                      const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
                      const target = normalize(label);
                      const isVisible = (element) => {
                        if (!element) return false;
                        const style = window.getComputedStyle(element);
                        const rect = element.getBoundingClientRect();
                        return style.visibility !== "hidden" &&
                          style.display !== "none" &&
                          rect.width > 0 &&
                          rect.height > 0;
                      };
                      const matches = (value) => {
                        const normalized = normalize(value);
                        return exact ? normalized === target : normalized.includes(target);
                      };
                      const candidates = Array.from(
                        document.querySelectorAll("button, a, label, div[role='button'], span[role='button'], li, span")
                      );
                      for (const element of candidates) {
                        if (!isVisible(element)) continue;
                        const ariaLabel = element.getAttribute ? element.getAttribute("aria-label") : "";
                        const text = element.innerText || element.textContent || "";
                        if (!matches(text) && !matches(ariaLabel)) continue;
                        try {
                          element.click();
                          return true;
                        } catch (error) {
                          continue;
                        }
                      }
                      return false;
                    }
                    """,
                    {"label": label, "exact": exact},
                )
            )
        except Exception:
            return False

    def _sample_visible_times(self, page: Any, *, limit: int = 6) -> list[str]:
        samples: list[str] = []
        pattern = re.compile(
            r"\b(?:\d+\s*(?:m|h|d|w|mo|y)|\d+\s+(?:minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)|yesterday)\b",
            re.I,
        )
        for card in self._result_cards(page)[:limit]:
            try:
                text = " ".join(card.inner_text(timeout=300).split())
            except Exception:
                continue
            match = pattern.search(text)
            if match:
                samples.append(match.group(0))
        return samples

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

    def _open_search(self, page: Any, query: str, *, window_hours: int) -> bool:
        self.logger.debug("open_search_start query=%r current_url=%s", query, getattr(page, "url", ""))
        if self._open_search_from_search_bar(page, query, window_hours=window_hours):
            self.logger.debug("open_search_via_search_bar_success query=%r final_url=%s", query, page.url)
            return True

        page.goto(self._build_content_results_url(query, window_hours=window_hours), wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        self.logger.debug("open_search_direct_url query=%r final_url=%s", query, page.url)
        return False

    def _open_search_from_search_bar(self, page: Any, query: str, *, window_hours: int) -> bool:
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
                self._normalize_content_results_url(page, query, window_hours=window_hours)
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
            ("role_link", lambda: page.get_by_role("link", name=re.compile(r"^(Posts|Content)$", re.I)).first.click(timeout=3000)),
            ("text_link", lambda: page.get_by_text(re.compile(r"^(Posts|Content)$", re.I)).first.click(timeout=3000)),
            (
                "href_link",
                lambda: page.locator("a[href*='/search/results/content/']:not([href*='postedBy='])").first.click(
                    timeout=3000
                ),
            ),
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

    def _build_content_results_url(self, query: str, *, window_hours: int) -> str:
        # NOTE: omit datePosted=past-24h — the new LinkedIn DOM returns
        # "No results found" for that param value. The scraper still applies a
        # window-hours filter in Python via _filter_posts_by_window, so this
        # only affects the URL, not which posts are kept.
        del window_hours
        params: list[tuple[str, str]] = [
            ("keywords", self._normalize_space(query)),
            ("sortBy", '"date_posted"'),
            ("origin", "FACETED_SEARCH"),
        ]
        return f"https://www.linkedin.com/search/results/content/?{urlencode(params)}"

    def _sanitize_content_results_url(self, url: str, *, query: str, window_hours: int) -> str:
        del url
        return self._build_content_results_url(query, window_hours=window_hours)

    def _normalize_content_results_url(self, page: Any, query: str, *, window_hours: int) -> None:
        clean_url = self._sanitize_content_results_url(page.url, query=query, window_hours=window_hours)
        if page.url == clean_url:
            self.logger.debug("normalize_content_results_url_skipped query=%r url=%s", query, page.url)
            return
        self.logger.debug("normalize_content_results_url query=%r from=%s to=%s", query, page.url, clean_url)
        try:
            page.goto(clean_url, wait_until="commit", timeout=8000)
        except Exception as exc:
            self.logger.warning("normalize_content_results_url_goto_failed query=%r error=%s", query, exc)
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
        # URL-based fast-path: trust query-string params over DOM state. The new
        # LinkedIn DOM uses obfuscated CSS classes for the "active" chip styling
        # and exposes none of the standard a11y attributes the JS check below
        # looks for, so the only reliable signal is the URL we navigated to.
        url = (page.url or "").lower()
        label_norm = (label or "").strip().lower()
        if label_norm == "latest" and "sortby=" in url and "date_posted" in url:
            return True
        if label_norm == "past 24 hours" and "dateposted=" in url and "past-24h" in url:
            return True
        try:
            return bool(
                page.evaluate(
                    """
                    (targetLabel) => {
                      const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
                      const target = normalize(targetLabel);
                      const matches = (value) => {
                        const normalized = normalize(value);
                        return normalized === target ||
                          normalized.startsWith(target + " ") ||
                          normalized.endsWith(" " + target);
                      };
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
                      return candidates.some((element) => matches(element.innerText || element.textContent) && isActive(element));
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

        LinkedIn's content search uses a virtualized container, so a viewport-level
        wheel event won't trigger lazy loading. We scroll the real container via JS,
        press `End` as a fallback, click any "Show more results" button, and wait
        for either new cards or new scroll height before declaring stable.
        """
        stable_steps = 0
        max_steps = max(1, int(self.settings.scraper_max_scroll_steps))
        stable_rounds = max(2, int(self.settings.scraper_stable_rounds))
        min_steps_before_stop = min(max_steps, max(3, int(self.settings.scraper_scroll_steps)))
        pause_ms = int(self.settings.scraper_scroll_pause_seconds * 1000)
        last_count = self._result_count(page)
        last_height = self._scroll_height(page)
        self.logger.debug(
            "scroll_results_start max_steps=%s stable_rounds=%s min_steps_before_stop=%s initial_count=%s initial_height=%s window_hours=%s target_results=%s",
            max_steps,
            stable_rounds,
            min_steps_before_stop,
            last_count,
            last_height,
            window_hours,
            target_results,
        )

        for step in range(max_steps):
            self._scroll_to_bottom(page)
            try:
                page.keyboard.press("End")
            except Exception:
                pass
            self._click_show_more(page)

            try:
                page.wait_for_function(
                    """([prevCount, prevHeight, sel]) => {
                        const cards = document.querySelectorAll(sel);
                        const el = document.scrollingElement || document.body;
                        const heights = [el ? el.scrollHeight : 0];
                        document.querySelectorAll('main, [role="main"]').forEach((node) => heights.push(node.scrollHeight || 0));
                        return cards.length > prevCount || Math.max(...heights) > prevHeight;
                    }""",
                    arg=[last_count, last_height, ", ".join(RESULT_SELECTORS)],
                    timeout=max(pause_ms * 2, 2500),
                )
            except Exception:
                page.wait_for_timeout(pause_ms)

            current_count = self._result_count(page)
            current_height = self._scroll_height(page)
            self.logger.debug(
                "scroll_results_step step=%s/%s current_count=%s current_height=%s stable_steps=%s",
                step + 1,
                max_steps,
                current_count,
                current_height,
                stable_steps,
            )

            target_reached = target_results > 0 and current_count >= target_results
            if current_count == last_count and current_height == last_height:
                stable_steps += 1
            else:
                stable_steps = 0
                last_count = current_count
                last_height = current_height

            if step + 1 < min_steps_before_stop:
                continue

            if target_reached and self._last_cards_are_old(page, window_hours=window_hours):
                self.logger.debug(
                    "scroll_results_stop reason=old_posts_detected step=%s current_count=%s target_results=%s",
                    step + 1,
                    current_count,
                    target_results,
                )
                break

            if self._last_cards_are_old(page, window_hours=window_hours) and current_count >= 8:
                self.logger.debug(
                    "scroll_results_stop reason=window_exhausted step=%s current_count=%s",
                    step + 1,
                    current_count,
                )
                break

            if stable_steps >= stable_rounds:
                self.logger.debug(
                    "scroll_results_stop reason=stable current_count=%s stable_steps=%s target_results=%s target_reached=%s",
                    current_count,
                    stable_steps,
                    target_results,
                    target_reached,
                )
                break
        sleep(0.3)
        self.logger.debug("scroll_results_complete final_count=%s final_height=%s", self._result_count(page), self._scroll_height(page))

    def _scroll_to_bottom(self, page: Any) -> None:
        """Scroll every plausible scroll container to its bottom.

        LinkedIn renders the feed inside a virtualized container; the document
        body itself often has fixed height, so `window.scrollTo` alone does
        nothing. We walk likely scroll roots and push each to its scrollHeight.
        """
        try:
            page.evaluate(
                """
                () => {
                  const targets = new Set();
                  const root = document.scrollingElement || document.documentElement || document.body;
                  if (root) targets.add(root);
                  document.querySelectorAll('main, [role="main"], .scaffold-finite-scroll, .scaffold-finite-scroll__content').forEach((el) => targets.add(el));
                  document.querySelectorAll('*').forEach((el) => {
                    if (targets.has(el)) return;
                    const style = window.getComputedStyle(el);
                    const overflowY = style && style.overflowY;
                    if ((overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 80) {
                      targets.add(el);
                    }
                  });
                  for (const el of targets) {
                    try { el.scrollTop = el.scrollHeight; } catch (_) { /* noop */ }
                  }
                  try { window.scrollTo(0, document.body ? document.body.scrollHeight : 0); } catch (_) { /* noop */ }
                }
                """
            )
        except Exception as exc:
            self.logger.debug("scroll_to_bottom_failed error=%r", exc)

    def _scroll_height(self, page: Any) -> int:
        try:
            return int(
                page.evaluate(
                    """
                    () => {
                      const heights = [];
                      const root = document.scrollingElement || document.body;
                      if (root) heights.push(root.scrollHeight || 0);
                      document.querySelectorAll('main, [role="main"]').forEach((el) => heights.push(el.scrollHeight || 0));
                      return heights.length ? Math.max(...heights) : 0;
                    }
                    """
                )
            )
        except Exception:
            return 0

    def _click_show_more(self, page: Any) -> None:
        try:
            button = page.locator(
                "button:has-text('Show more results'), button:has-text('See more results'), button:has-text('Load more')"
            ).first
            if button and button.count():
                button.scroll_into_view_if_needed(timeout=400)
                button.click(timeout=600)
                self.logger.debug("scroll_results_show_more_clicked")
        except Exception:
            pass

    def _result_count(self, page: Any) -> int:
        return len(self._result_cards(page))

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
                      const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
                      const unique = (values) => {
                        const ordered = [];
                        const seen = new Set();
                        for (const value of values) {
                          const normalized = normalize(value);
                          if (!normalized) continue;
                          const key = normalized.toLowerCase();
                          if (seen.has(key)) continue;
                          seen.add(key);
                          ordered.push(normalized);
                        }
                        return ordered;
                      };
                      const textFrom = (selectors) => {
                        for (const selector of selectors) {
                          const element = node.querySelector(selector);
                          if (element && element.innerText && element.innerText.trim()) {
                            return normalize(element.innerText);
                          }
                        }
                        return "";
                      };
                      const textsFrom = (selectors) => {
                        const collected = [];
                        for (const selector of selectors) {
                          for (const element of Array.from(node.querySelectorAll(selector))) {
                            if (element && element.innerText && element.innerText.trim()) {
                              collected.push(element.innerText);
                            }
                          }
                        }
                        return unique(collected);
                      };

                      const permalinkCandidates = [];
                      for (const link of Array.from(node.querySelectorAll('a[href]'))) {
                        const href = link.href || link.getAttribute('href') || "";
                        if (!href) continue;
                        if (
                          href.includes('/feed/update/') ||
                          href.includes('/posts/') ||
                          href.includes('/activity/')
                        ) {
                          permalinkCandidates.push(href);
                        }
                      }

                      const urnCandidates = unique([
                        node.getAttribute('data-urn'),
                        node.getAttribute('data-id'),
                        ...Array.from(node.querySelectorAll('[data-urn], [data-id]')).flatMap((element) => [
                          element.getAttribute('data-urn'),
                          element.getAttribute('data-id'),
                        ]),
                      ]);

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
                      let authorNameFromLink = "";
                      for (const selector of authorProfileSelectors) {
                        const link = node.querySelector(selector);
                        if (link && link.href && !authorProfileUrl) {
                          authorProfileUrl = link.href;
                        }
                        // Walk all matching links — pick the first whose innerText starts with a real name
                        for (const candidate of Array.from(node.querySelectorAll(selector))) {
                          const lines = (candidate.innerText || "").split("\\n").map((s) => s.trim()).filter(Boolean);
                          for (const line of lines) {
                            if (line.startsWith("•")) continue;
                            if (/^\d[\d,]*\s+followers?$/i.test(line)) continue;
                            if (/^(?:[123]rd\+?|1st|2nd)$/i.test(line)) continue;
                            authorNameFromLink = line;
                            break;
                          }
                          if (authorNameFromLink) break;
                        }
                        if (authorProfileUrl && authorNameFromLink) break;
                      }

                      const timeElement = node.querySelector('time');

                      // Fallback: scan card text for the relative-time pattern LinkedIn shows
                      // ("3h", "22m", "2d", "1w", "yesterday", etc.) since the new DOM no
                      // longer wraps it in a <time> element with a stable class hook.
                      let relativeTimeFromText = "";
                      const fullCardText = node.innerText || "";
                      const timePattern = /\\b(\\d+\\s*(?:m|h|d|w|mo|y)|\\d+\\s+(?:minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)|yesterday)\\b/i;
                      const timeMatch = fullCardText.match(timePattern);
                      if (timeMatch) {
                        relativeTimeFromText = timeMatch[0];
                      }

                      return {
                        permalink_candidates: unique(permalinkCandidates),
                        urn_candidates: urnCandidates,
                        author_name: textFrom(authorSelectors) || authorNameFromLink,
                        author_profile_url: authorProfileUrl,
                        content_candidates: textsFrom([
                          '.update-components-update-v2__commentary',
                          '.feed-shared-inline-show-more-text',
                          '.update-components-text',
                          '.feed-shared-update-v2__description',
                          '.entity-result__summary',
                          '.feed-shared-text-view',
                          '.attributed-text-segment-list__content',
                          '.break-words'
                        ]),
                        full_text: node.innerText ? normalize(node.innerText) : "",
                        relative_time_text: ((timeElement && timeElement.innerText) ? normalize(timeElement.innerText) : textFrom([
                          '.update-components-actor__sub-description',
                          '.entity-result__secondary-subtitle'
                        ])) || relativeTimeFromText,
                        absolute_posted_at: timeElement ? timeElement.getAttribute('datetime') || "" : "",
                      };
                    }
                    """
                )
            except Exception:
                continue

            author_name = self._clean_author_name(payload.get("author_name"))
            permalink = self._select_post_permalink(payload)
            author_profile_url = self._normalize_linkedin_permalink(payload.get("author_profile_url"))
            relative_time_text = self._normalize_space(payload.get("relative_time_text")) or None
            absolute_posted_at = self._normalize_space(payload.get("absolute_posted_at")) or None
            content_text = self._select_content_text(
                content_candidates=payload.get("content_candidates"),
                full_text=payload.get("full_text"),
                author_name=author_name or None,
                relative_time_text=relative_time_text,
            )

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
                absolute_posted_at=absolute_posted_at,
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
                  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
                  const unique = (values) => {
                    const ordered = [];
                    const seen = new Set();
                    for (const value of values) {
                      const normalized = normalize(value);
                      if (!normalized) continue;
                      const key = normalized.toLowerCase();
                      if (seen.has(key)) continue;
                      seen.add(key);
                      ordered.push(normalized);
                    }
                    return ordered;
                  };
                  const textFrom = (selectors) => {
                    for (const selector of selectors) {
                      const element = document.querySelector(selector);
                      if (element && element.innerText && element.innerText.trim()) {
                        return normalize(element.innerText);
                      }
                    }
                    return "";
                  };
                  const textsFrom = (selectors) => {
                    const collected = [];
                    for (const selector of selectors) {
                      for (const element of Array.from(document.querySelectorAll(selector))) {
                        if (element && element.innerText && element.innerText.trim()) {
                          collected.push(element.innerText);
                        }
                      }
                    }
                    return unique(collected);
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
                    content_candidates: textsFrom([
                      '.update-components-update-v2__commentary',
                      '.feed-shared-inline-show-more-text',
                      '.update-components-text',
                      '.feed-shared-update-v2__description',
                      '.break-words',
                      '.attributed-text-segment-list__content',
                      'main div[dir="ltr"]'
                    ]),
                    full_text: document.querySelector('main') && document.querySelector('main').innerText
                      ? normalize(document.querySelector('main').innerText)
                      : normalize(document.body && document.body.innerText),
                    relative_time_text: (timeElement && timeElement.innerText) ? normalize(timeElement.innerText) : textFrom([
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

        content_text = self._select_content_text(
            content_candidates=payload.get("content_candidates"),
            full_text=payload.get("full_text"),
            author_name=post.author_name,
            relative_time_text=payload.get("relative_time_text") or post.relative_time_text,
        )
        if not content_text:
            self.logger.debug("fetch_post_detail_empty_content permalink=%s", post.permalink)
            return None, detail_more_clicks

        author_name = self._clean_author_name(payload.get("author_name")) or post.author_name
        permalink = self._normalize_linkedin_permalink(payload.get("permalink")) or post.permalink
        author_profile_url = (
            self._normalize_linkedin_permalink(payload.get("author_profile_url")) or post.author_profile_url
        )
        relative_time_text = self._normalize_space(payload.get("relative_time_text")) or post.relative_time_text
        absolute_posted_at = self._normalize_space(payload.get("absolute_posted_at")) or post.absolute_posted_at

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
            expanded_count = 0
            for _ in range(3):
                expanded = int(
                    page.evaluate(
                        """
                        () => {
                          let count = 0;
                          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                          const controls = document.querySelectorAll(
                            'button, a[role="button"], [role="button"], span.feed-shared-inline-show-more-text'
                          );
                          for (const control of controls) {
                            const text = normalize(control.innerText || control.textContent);
                            if (
                              text === 'see more' ||
                              text === '…see more' ||
                              text === '...see more' ||
                              text === '…more' ||
                              text === '...more' ||
                              text.endsWith(' see more') ||
                              text.endsWith('... more') ||
                              text.endsWith('… more')
                            ) {
                              try {
                                control.click();
                                count += 1;
                              } catch (error) {
                              }
                            }
                          }
                          return count;
                        }
                        """
                    )
                )
                expanded_count += expanded
                if expanded <= 0:
                    break
                page.wait_for_timeout(400)
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

    def _select_post_permalink(self, payload: dict[str, Any]) -> str | None:
        for raw in payload.get("permalink_candidates") or []:
            normalized = self._normalize_linkedin_permalink(raw)
            if normalized and self._looks_like_post_permalink(normalized):
                return normalized
        for raw in payload.get("urn_candidates") or []:
            permalink = self._build_feed_update_permalink(raw)
            if permalink:
                return permalink
        return None

    def _select_content_text(
        self,
        *,
        content_candidates: list[Any] | None,
        full_text: str | None,
        author_name: str | None,
        relative_time_text: str | None,
    ) -> str:
        cleaned_candidates: list[str] = []
        seen: set[str] = set()
        for raw in content_candidates or []:
            text = self._normalize_space(raw)
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            cleaned_candidates.append(text)

        best_candidate = max(cleaned_candidates, key=len, default="")
        fallback_text = self._fallback_card_text(
            full_text,
            author_name=author_name,
            relative_time_text=relative_time_text,
        )
        if fallback_text and (
            not best_candidate
            or self._appears_truncated(best_candidate)
            or len(fallback_text) > (len(best_candidate) + 80)
        ):
            return fallback_text
        return best_candidate or fallback_text

    def _fallback_card_text(
        self,
        full_text: str | None,
        *,
        author_name: str | None,
        relative_time_text: str | None,
    ) -> str:
        ignored_exact = {
            "like",
            "comment",
            "repost",
            "send",
            "follow",
            "message",
            "promoted",
        }
        ignored_pattern = re.compile(
            r"^(?:\d+\s+)?(?:comment|comments|repost|reposts|like|likes|reaction|reactions)$",
            re.I,
        )
        author_key = self._normalize_space(author_name).casefold()
        relative_key = self._normalize_space(relative_time_text).casefold()
        lines = [self._normalize_space(line) for line in str(full_text or "").splitlines()]
        cleaned_lines: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if not line:
                continue
            lowered = line.casefold()
            if lowered in ignored_exact or ignored_pattern.match(line):
                continue
            if author_key and lowered == author_key:
                continue
            if relative_key and lowered == relative_key:
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned_lines.append(line)
        return " ".join(cleaned_lines)

    @staticmethod
    def _build_feed_update_permalink(value: Any) -> str | None:
        normalized = LinkedInScraper._normalize_space(value)
        if not normalized:
            return None
        urn_match = re.search(r"(urn:li:(?:activity|ugcPost):[A-Za-z0-9_-]+)", normalized)
        if not urn_match:
            return None
        return f"https://www.linkedin.com/feed/update/{urn_match.group(1)}/"

    @staticmethod
    def _looks_like_post_permalink(value: str) -> bool:
        lowered = value.lower()
        return any(part in lowered for part in ("/feed/update/", "/posts/", "/activity/"))

    @staticmethod
    def _normalize_linkedin_permalink(value: Any) -> str | None:
        normalized = LinkedInScraper._normalize_space(value)
        if not normalized:
            return None
        absolute = urljoin("https://www.linkedin.com", normalized)
        parsed = urlsplit(absolute)
        if parsed.scheme not in {"http", "https"}:
            return None
        path = parsed.path or "/"
        clean_path = path.rstrip("/") or "/"
        if clean_path != "/":
            clean_path = f"{clean_path}/"
        return urlunsplit((parsed.scheme, parsed.netloc, clean_path, "", ""))

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

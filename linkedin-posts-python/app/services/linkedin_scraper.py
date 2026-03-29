from __future__ import annotations

from contextlib import contextmanager
import re
from dataclasses import dataclass
from hashlib import sha1
from time import sleep
from typing import Any, Iterator
from urllib.parse import quote_plus

from ..config import Settings, get_settings
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


class LinkedInScraper:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def storage_state_exists(self) -> bool:
        return self.settings.linkedin_storage_state_path.exists()

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

    def search_posts(self, query: str, *, max_results: int, window_hours: int = 24) -> list[ScrapedPost]:
        if not self.storage_state_exists():
            raise RuntimeError(
                "LinkedIn session state is missing. Run scripts/bootstrap_linkedin_session.py first."
            )

        with self.background_session() as (context, page):
            return self.search_posts_in_session(
                context,
                page,
                query,
                max_results=max_results,
                window_hours=window_hours,
            )

    @contextmanager
    def background_session(self) -> Iterator[tuple[Any, Any]]:
        if not self.storage_state_exists():
            raise RuntimeError(
                "LinkedIn session state is missing. Run scripts/bootstrap_linkedin_session.py first."
            )

        sync_playwright = self._get_sync_playwright()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.settings.linkedin_headless)
            context = browser.new_context(
                storage_state=str(self.settings.linkedin_storage_state_path),
                viewport={"width": 1440, "height": 960},
            )
            page = context.new_page()
            try:
                yield context, page
            finally:
                browser.close()

    def search_posts_in_session(
        self,
        context: Any,
        page: Any,
        query: str,
        *,
        max_results: int,
        window_hours: int = 24,
    ) -> list[ScrapedPost]:
        self._open_search(page, query)
        self._assert_logged_in(context, page)
        self._apply_sort_latest(page)
        if window_hours <= 24:
            self._apply_date_filter(page, "Past 24 hours")
        self._scroll_results(page, target_results=max_results)
        posts = self._extract_posts(page)
        if window_hours > 0:
            posts = [
                post
                for post in posts
                if linkedin_post_is_within_hours(
                    absolute_posted_at=post.absolute_posted_at,
                    relative_time_text=post.relative_time_text,
                    window_hours=window_hours,
                )
            ]
        return posts[:max_results]

    def _get_sync_playwright(self) -> Any:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed yet. Run `pip install -r requirements.txt` "
                "and `python -m playwright install chromium`."
            ) from exc
        return sync_playwright

    def _open_search(self, page: Any, query: str) -> None:
        encoded_query = quote_plus(query)
        page.goto(
            f"https://www.linkedin.com/search/results/content/?keywords={encoded_query}",
            wait_until="domcontentloaded",
        )
        page.wait_for_timeout(1500)

    def _assert_logged_in(self, context: Any, page: Any) -> None:
        current_url = page.url.lower()
        if ("login" in current_url or "checkpoint" in current_url) and not self._has_authenticated_session(
            context, page
        ):
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

    def _apply_sort_latest(self, page: Any) -> None:
        try:
            page.get_by_role("button", name=re.compile("Sort by", re.I)).click(timeout=3000)
            page.get_by_text(re.compile("^Latest$", re.I)).click(timeout=3000)
            page.wait_for_timeout(1000)
        except Exception:
            # LinkedIn sometimes keeps the previous selection or changes the control markup.
            return

    def _apply_date_filter(self, page: Any, label: str) -> None:
        try:
            page.get_by_role("button", name=re.compile("Date posted", re.I)).click(timeout=3000)
            page.get_by_label(re.compile(label, re.I)).check(timeout=3000)
            try:
                page.get_by_role("button", name=re.compile("Apply", re.I)).click(timeout=3000)
            except Exception:
                page.keyboard.press("Escape")
            page.wait_for_timeout(1000)
        except Exception:
            return

    def _scroll_results(self, page: Any, *, target_results: int) -> None:
        for _ in range(self.settings.scraper_scroll_steps):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(int(self.settings.scraper_scroll_pause_seconds * 1000))
            if self._result_count(page) >= target_results:
                break
        sleep(0.5)

    def _result_count(self, page: Any) -> int:
        selector = self._result_selector(page)
        return page.locator(selector).count() if selector else 0

    def _result_selector(self, page: Any) -> str | None:
        selectors = [
            "div.feed-shared-update-v2[role='article']",
            "div[data-view-name='feed-full-update']",
            "li.artdeco-card.mb2",
            "li.reusable-search__result-container",
            "div.reusable-search__result-container",
            "li.search-results__list-item",
        ]
        for selector in selectors:
            if page.locator(selector).count():
                return selector
        return None

    def _extract_posts(self, page: Any) -> list[ScrapedPost]:
        selector = self._result_selector(page)
        if not selector:
            return []

        cards = page.locator(selector)
        posts: list[ScrapedPost] = []
        for index in range(cards.count()):
            card = cards.nth(index)
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
            author_name = self._clean_author_name(payload.get("author_name"))
            permalink = payload.get("permalink") or None
            author_profile_url = payload.get("author_profile_url") or None
            relative_time_text = payload.get("relative_time_text") or None

            if not content_text and not permalink:
                continue

            external_id = self._build_external_id(permalink, author_name, content_text)
            posts.append(
                ScrapedPost(
                    external_id=external_id,
                    permalink=permalink,
                    author_name=author_name or None,
                    author_profile_url=author_profile_url,
                    content_text=content_text,
                    relative_time_text=relative_time_text,
                )
            )
        return posts

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

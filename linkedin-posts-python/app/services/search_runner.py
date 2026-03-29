from __future__ import annotations

from collections import defaultdict
from typing import Any

from .. import db
from ..config import Settings, get_settings
from ..scoring import dedupe_fingerprint, overall_result_score, state_match_score
from ..state_catalog import build_state_query_variants, resolve_enabled_states
from .linkedin_scraper import LinkedInScraper


class SearchRunner:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.scraper = LinkedInScraper(self.settings)

    def run_search(self, search_id: int) -> dict[str, Any]:
        db.purge_expired_posts(
            self.settings,
            max_age_hours=self.settings.post_retention_hours,
        )
        search = db.get_search(search_id, self.settings)
        if not search:
            raise ValueError(f"Search {search_id} was not found.")

        states = resolve_enabled_states(search["state_scope"], self._enabled_state_codes(search))
        seen_fingerprints: set[str] = set()
        totals_by_state: dict[str, int] = defaultdict(int)

        db.clear_results_for_search(search_id, self.settings)
        with self.scraper.background_session() as (context, page):
            for state in states:
                for query_variant in build_state_query_variants(search["keywords"], state):
                    run_id = db.create_search_run(search_id, state.code, query_variant, self.settings)
                    found_count = 0
                    try:
                        scraped_posts = self.scraper.search_posts_in_session(
                            context,
                            page,
                            query_variant,
                            max_results=int(search["max_results_per_state"]),
                            window_hours=int(search["window_hours"]),
                        )
                        for scraped_post in scraped_posts:
                            fingerprint = dedupe_fingerprint(
                                scraped_post.permalink,
                                scraped_post.author_name,
                                scraped_post.content_text,
                            )
                            if fingerprint in seen_fingerprints:
                                continue

                            state_confidence = max(
                                state_match_score(scraped_post.content_text, state),
                                0.5 if query_variant.endswith(state.name) or state.code in query_variant else 0.0,
                            )
                            score = overall_result_score(scraped_post.content_text, search["keywords"], state)
                            if score <= 0:
                                continue

                            post_id = db.upsert_post(
                                external_id=scraped_post.external_id,
                                permalink=scraped_post.permalink,
                                author_name=scraped_post.author_name,
                                author_profile_url=scraped_post.author_profile_url,
                                content_text=scraped_post.content_text,
                                relative_time_text=scraped_post.relative_time_text,
                                absolute_posted_at=scraped_post.absolute_posted_at,
                                best_state_code=state.code,
                                state_confidence=state_confidence,
                                source_query=query_variant,
                                settings=self.settings,
                            )
                            db.link_search_result(
                                search_id,
                                run_id,
                                post_id,
                                state.code,
                                score,
                                self.settings,
                            )
                            seen_fingerprints.add(fingerprint)
                            found_count += 1
                        db.finish_search_run(
                            run_id,
                            status="completed",
                            found_count=found_count,
                            settings=self.settings,
                        )
                        totals_by_state[state.code] += found_count
                    except Exception as exc:
                        db.finish_search_run(
                            run_id,
                            status="failed",
                            found_count=0,
                            error_message=str(exc),
                            settings=self.settings,
                        )

        db.update_search_last_run(search_id, self.settings)
        db.purge_expired_posts(
            self.settings,
            max_age_hours=self.settings.post_retention_hours,
        )
        return {
            "search_id": search_id,
            "states_processed": len(states),
            "results_by_state": dict(totals_by_state),
            "total_results": sum(totals_by_state.values()),
        }

    @staticmethod
    def _enabled_state_codes(search: dict[str, Any]) -> list[str]:
        raw = search.get("enabled_states_json") or "[]"
        try:
            import json

            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return []

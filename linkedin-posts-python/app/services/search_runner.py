from __future__ import annotations

from collections import defaultdict
from typing import Any

from .. import db
from ..config import Settings, get_settings
from ..logging_utils import get_rotating_file_logger
from ..role_matching import analyze_post_for_query
from ..scoring import (
    dedupe_fingerprint,
    extract_state_match_scores,
    keyword_focus_terms,
    keyword_match_score,
    overall_result_score,
    state_match_score,
)
from ..state_catalog import build_state_query_variants, resolve_enabled_states
from .linkedin_scraper import LinkedInScraper


class SearchRunner:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.scraper = LinkedInScraper(self.settings)
        self.logger = get_rotating_file_logger(
            "search_runner",
            str(self.settings.data_dir / "logs" / "search_runner.log"),
        )

    @staticmethod
    def _preview_text(value: str | None, *, limit: int = 180) -> str:
        normalized = " ".join((value or "").split())
        if len(normalized) <= limit:
            return normalized
        return f"{normalized[:limit - 1]}..."

    def run_search(self, search_id: int) -> dict[str, Any]:
        db.purge_expired_posts(
            self.settings,
            max_age_hours=self.settings.post_retention_hours,
        )
        search = db.get_search(search_id, self.settings)
        if not search:
            raise ValueError(f"Search {search_id} was not found.")

        capture_mode = self._capture_mode(search)
        states = resolve_enabled_states(search["state_scope"], self._enabled_state_codes(search))
        core_terms = keyword_focus_terms(search["keywords"])
        self.logger.info(
            "run_search_start search_id=%s keywords=%r capture_mode=%s states=%s max_results_per_state=%s window_hours=%s core_terms=%s",
            search_id,
            search["keywords"],
            capture_mode,
            [state.code for state in states],
            search["max_results_per_state"],
            search["window_hours"],
            core_terms,
        )
        seen_state_fingerprints: set[tuple[str, str]] = set()
        totals_by_state: dict[str, int] = defaultdict(int)

        db.clear_results_for_search(search_id, self.settings)
        with self.scraper.background_session() as (context, page):
            for state in states:
                for query_variant in build_state_query_variants(search["keywords"], state):
                    run_id = db.create_search_run(search_id, state.code, query_variant, self.settings)
                    found_count = 0
                    run_audit: dict[str, Any] = {
                        "query_variant": query_variant,
                        "search_keywords": search["keywords"],
                        "core_terms": core_terms,
                        "matched_core_posts": 0,
                        "stored_posts": 0,
                        "duplicates_skipped": 0,
                        "skipped_missing_core_terms": 0,
                        "skipped_hidden_from_frontend": 0,
                    }
                    self.logger.info(
                        "query_run_start search_id=%s run_id=%s state=%s query=%r",
                        search_id,
                        run_id,
                        state.code,
                        query_variant,
                    )
                    try:
                        session_result = self.scraper.search_posts_in_session(
                            context,
                            page,
                            query_variant,
                            max_results=int(search["max_results_per_state"]),
                            window_hours=int(search["window_hours"]),
                            capture_mode=capture_mode,
                        )
                        scraped_posts = session_result.posts
                        run_audit.update(session_result.audit)
                        run_audit["query_variant"] = query_variant
                        run_audit["search_keywords"] = search["keywords"]
                        run_audit["core_terms"] = core_terms
                        run_audit.setdefault("matched_core_posts", 0)
                        run_audit.setdefault("stored_posts", 0)
                        run_audit.setdefault("duplicates_skipped", 0)
                        run_audit.setdefault("skipped_missing_core_terms", 0)
                        run_audit.setdefault("skipped_hidden_from_frontend", 0)

                        self.logger.info(
                            "query_run_scraped search_id=%s run_id=%s state=%s query=%r scraped_posts=%s",
                            search_id,
                            run_id,
                            state.code,
                            query_variant,
                            len(scraped_posts),
                        )
                        for scraped_post in scraped_posts:
                            keyword_score = keyword_match_score(scraped_post.content_text, search["keywords"])
                            if keyword_score <= 0:
                                run_audit["skipped_missing_core_terms"] += 1
                                self.logger.debug(
                                    "post_skip_missing_core_terms search_id=%s run_id=%s state=%s query=%r permalink=%s author=%r core_terms=%s preview=%r",
                                    search_id,
                                    run_id,
                                    state.code,
                                    query_variant,
                                    scraped_post.permalink,
                                    scraped_post.author_name,
                                    core_terms,
                                    self._preview_text(scraped_post.content_text),
                                )
                                continue

                            run_audit["matched_core_posts"] += 1
                            fingerprint = dedupe_fingerprint(
                                scraped_post.permalink,
                                scraped_post.author_name,
                                scraped_post.content_text,
                            )
                            state_fingerprint = (fingerprint, state.code)
                            if state_fingerprint in seen_state_fingerprints:
                                run_audit["duplicates_skipped"] += 1
                                self.logger.debug(
                                    "post_skip_duplicate search_id=%s run_id=%s state=%s query=%r permalink=%s author=%r preview=%r",
                                    search_id,
                                    run_id,
                                    state.code,
                                    query_variant,
                                    scraped_post.permalink,
                                    scraped_post.author_name,
                                    self._preview_text(scraped_post.content_text),
                                )
                                continue

                            state_confidence = max(
                                state_match_score(scraped_post.content_text, state),
                                0.5 if query_variant.endswith(state.name) or state.code in query_variant else 0.0,
                            )
                            score = overall_result_score(scraped_post.content_text, search["keywords"], state)
                            if score <= 0:
                                score = max(0.01, keyword_score)
                            role_analysis = analyze_post_for_query(scraped_post.content_text, search["keywords"])
                            score = round(max(score, float(role_analysis.relevance_score or 0.0)), 4)

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
                            db.replace_post_state_matches(
                                post_id,
                                extract_state_match_scores(scraped_post.content_text),
                                settings=self.settings,
                            )
                            seen_state_fingerprints.add(state_fingerprint)
                            if role_analysis.hidden_from_frontend:
                                run_audit["skipped_hidden_from_frontend"] += 1
                                self.logger.info(
                                    "post_saved_hidden search_id=%s run_id=%s state=%s query=%r post_id=%s reason=%s match_type=%s permalink=%s author=%r preview=%r",
                                    search_id,
                                    run_id,
                                    state.code,
                                    query_variant,
                                    post_id,
                                    role_analysis.hidden_reason,
                                    role_analysis.match_type,
                                    scraped_post.permalink,
                                    scraped_post.author_name,
                                    self._preview_text(scraped_post.content_text),
                                )
                                continue

                            inserted = db.link_search_result(
                                search_id,
                                run_id,
                                post_id,
                                state.code,
                                score,
                                matched_opening_text=role_analysis.matched_opening,
                                match_type=role_analysis.match_type,
                                role_family=role_analysis.role_family,
                                relevance_score=role_analysis.relevance_score,
                                settings=self.settings,
                            )
                            if inserted:
                                found_count += 1
                                run_audit["stored_posts"] += 1
                            self.logger.info(
                                "post_saved search_id=%s run_id=%s state=%s query=%r post_id=%s score=%s state_confidence=%s match_type=%s matched_opening=%r role_family=%s permalink=%s author=%r preview=%r",
                                search_id,
                                run_id,
                                state.code,
                                query_variant,
                                post_id,
                                score,
                                state_confidence,
                                role_analysis.match_type,
                                role_analysis.matched_opening,
                                role_analysis.role_family,
                                scraped_post.permalink,
                                scraped_post.author_name,
                                self._preview_text(scraped_post.content_text),
                            )

                        self.logger.info(
                            "query_run_audit search_id=%s run_id=%s state=%s query=%r audit=%s",
                            search_id,
                            run_id,
                            state.code,
                            query_variant,
                            run_audit,
                        )
                        db.finish_search_run(
                            run_id,
                            status="completed",
                            found_count=found_count,
                            settings=self.settings,
                        )
                        totals_by_state[state.code] += found_count
                        self.logger.info(
                            "query_run_complete search_id=%s run_id=%s state=%s query=%r found_count=%s",
                            search_id,
                            run_id,
                            state.code,
                            query_variant,
                            found_count,
                        )
                    except Exception as exc:
                        db.finish_search_run(
                            run_id,
                            status="failed",
                            found_count=0,
                            error_message=str(exc),
                            settings=self.settings,
                        )
                        self.logger.exception(
                            "query_run_failed search_id=%s run_id=%s state=%s query=%r error=%s",
                            search_id,
                            run_id,
                            state.code,
                            query_variant,
                            exc,
                        )

        db.update_search_last_run(search_id, self.settings)
        db.purge_expired_posts(
            self.settings,
            max_age_hours=self.settings.post_retention_hours,
        )
        summary = {
            "search_id": search_id,
            "states_processed": len(states),
            "results_by_state": dict(totals_by_state),
            "total_results": sum(totals_by_state.values()),
        }
        self.logger.info("run_search_complete summary=%s", summary)
        return summary

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

    @staticmethod
    def _capture_mode(search: dict[str, Any]) -> str:
        capture_mode = str(search.get("capture_mode") or "standard").lower()
        if capture_mode in {"standard", "balanced", "deep"}:
            return capture_mode
        return "standard"

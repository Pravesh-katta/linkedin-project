from __future__ import annotations

from collections import OrderedDict
from contextlib import asynccontextmanager
from pathlib import Path
import re
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db
from .config import Settings, get_settings
from .post_age import linkedin_posted_at
from .role_matching import analyze_post_for_query, strip_ephemeral_engagement, strip_query_state_suffix
from .scoring import keyword_focus_terms
from .scheduler import SearchScheduler
from .services.contact_extractor import extract_emails
from .services.resume_parser import resume_match_score
from .services.search_runner import SearchRunner
from .state_catalog import ALL_STATES, STATE_BY_CODE


settings = get_settings()
templates = Jinja2Templates(directory=str(settings.templates_dir))
scheduler = SearchScheduler(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "logs").mkdir(parents=True, exist_ok=True)
    db.init_db(settings)
    db.mark_stale_running_search_runs_failed(settings=settings)
    db.purge_expired_posts(
        settings,
        max_age_hours=settings.post_retention_hours,
    )
    if settings.enable_scheduler:
        scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")


def _template_context(request: Request, **extra: Any) -> dict[str, Any]:
    styles_path = settings.static_dir / "styles.css"
    try:
        static_version = str(styles_path.stat().st_mtime_ns)
    except FileNotFoundError:
        static_version = "1"
    return {
        "request": request,
        "app_name": settings.app_name,
        "session_ready": Path(settings.linkedin_storage_state_path).exists(),
        "static_version": static_version,
        **extra,
    }


def _default_search_form_state(**overrides: Any) -> dict[str, Any]:
    state = {
        "keywords": "",
        "state_scope": "custom",
        "enabled_states": [],
        "max_results_per_state": settings.default_max_results_per_state,
        "capture_mode": settings.default_capture_mode,
        "schedule_minutes": 0,
    }
    state.update(overrides)
    return state


def _group_results(results: list[dict[str, Any]]) -> OrderedDict[str, list[dict[str, Any]]]:
    grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for row in results:
        state_code = row["matched_state_code"]
        grouped.setdefault(state_code, []).append(row)
    return grouped


def _partition_results_by_seen(
    results: list[dict[str, Any]],
) -> tuple[OrderedDict[str, list[dict[str, Any]]], OrderedDict[str, list[dict[str, Any]]]]:
    new_posts = [row for row in results if not row.get("viewed_at")]
    seen_posts = [row for row in results if row.get("viewed_at")]
    return _group_results(new_posts), _group_results(seen_posts)


def _result_sort_timestamp(row: dict[str, Any]) -> float:
    posted_at = linkedin_posted_at(
        absolute_posted_at=row.get("absolute_posted_at"),
        relative_time_text=row.get("relative_time_text"),
        reference_now=row.get("collected_at"),
    )
    if posted_at is not None:
        return posted_at.timestamp()
    return 0.0


def _normalize_display_text(value: Any) -> str:
    return " ".join(str(value or "").split())


DISPLAY_HIGHLIGHT_PREFIXES: dict[str, tuple[str, ...]] = {
    "display_role": ("Hiring:", "Role:", "Job Title:", "Position:"),
    "display_location": ("Location:",),
    "display_duration": ("Duration:", "Type:", "Employment Type:"),
}

DISPLAY_SECTION_LABELS = (
    "Role:",
    "Hiring:",
    "Job Title:",
    "Position:",
    "Location:",
    "Duration:",
    "Job Description:",
    "Key Responsibilities:",
    "Responsibilities:",
    "Required Skills:",
    "Required Qualifications:",
    "Preferred Skills:",
    "Nice to Have:",
    "What We're Looking For:",
    "What We’re Looking For:",
    "Must have:",
    "Must Have:",
    "Plus:",
    "Role Overview:",
    "Job Overview:",
    "Role Summary",
    "Summary:",
    "Core Skills Required:",
    "Key Highlights:",
    "Email:",
)

DISPLAY_BULLET_MARKERS = (
    "•",
    "✔",
    "🔹",
    "📍",
    "📅",
    "💡",
    "🛠️",
    "✨",
    "🚀",
    "📄",
    "⏳",
    "🏦",
    "📝",
    "🔑",
    "⭐",
    "🎯",
)

DISPLAY_JOB_SIGNAL_SCORES = {
    "we are hiring": 4,
    "we're hiring": 4,
    "hiring for": 4,
    "opening for": 4,
    "job opening": 4,
    "job openings": 4,
    "open position": 4,
    "open positions": 4,
    "position available": 3,
    "positions available": 3,
    "roles available": 3,
    "requirements available": 3,
    "looking for": 3,
    "role:": 3,
    "position:": 3,
    "job title:": 3,
    "location:": 2,
    "must have": 2,
    "required skills": 3,
    "required qualifications": 3,
    "responsibilities": 3,
    "job description": 3,
    "benefits:": 2,
    "interview": 2,
    "send resume": 2,
    "share your resume": 2,
    "full-time": 2,
    "full time": 2,
}

DISPLAY_SUPPLY_SIGNAL_SCORES = {
    "dear hiring manager": 6,
    "dear hiring managers": 6,
    "i have consultants": 6,
    "we have consultants": 6,
    "my consultants": 5,
    "our consultants": 5,
    "i have candidates": 6,
    "we have candidates": 6,
    "my candidates": 5,
    "our candidates": 5,
    "available for immediate deployment": 6,
    "hot profiles": 6,
    "available candidates": 5,
    "available consultants": 5,
    "consultants available": 5,
    "candidates available": 5,
    "share your current requirements": 6,
    "share your requirements": 5,
    "vendor distribution list": 6,
    "vendor list": 5,
    "vendor partnerships": 5,
    "bench sales": 4,
    "immediate joiners": 4,
}

DISPLAY_JOB_SIGNAL_PATTERNS: tuple[tuple[str, int, str], ...] = (
    (r"\b(hotlist|hot list)\b.{0,40}\b(positions?|roles?|openings?|requirements?)\b", 4, "hotlist_positions"),
    (r"\b(positions?|roles?|openings?|requirements?)\b.{0,24}\bavailable\b", 3, "positions_available"),
)

DISPLAY_SUPPLY_SIGNAL_PATTERNS: tuple[tuple[str, int, str], ...] = (
    (r"\b(hotlist|hot list)\b.{0,40}\b(consultants?|candidates?|profiles?|resources?)\b", 6, "hotlist_consultants"),
    (r"\b(consultants?|candidates?|profiles?|resources?)\b.{0,24}\bavailable\b", 5, "consultants_available"),
    (r"\bavailable\b.{0,24}\b(consultants?|candidates?|profiles?|resources?)\b", 5, "available_consultants"),
    (r"\badd me to (?:your|ur) vendor (?:distribution )?list\b", 6, "vendor_list"),
    (r"\bshare (?:your|ur|current) requirements\b", 5, "share_requirements"),
    (r"\bopen to new vendor partnerships?\b", 5, "vendor_partnerships"),
)

DISPLAY_STRUCTURED_JOB_LABELS = (
    "role:",
    "position:",
    "location:",
    "must have",
    "required skills",
    "responsibilities",
    "job description",
    "benefits:",
)

DISPLAY_SUPPLY_NOUNS = (
    "consultant",
    "consultants",
    "candidate",
    "candidates",
    "profile",
    "profiles",
    "resource",
    "resources",
)

DISPLAY_VISA_TERMS = ("h1b", "gc", "opt", "cpt", "ead")


def _extract_group_post_title(content_text: str | None, author_name: str | None) -> str | None:
    content = _normalize_display_text(content_text)
    author = _normalize_display_text(author_name)
    if not content:
        return None
    if content.lower().startswith("feed post "):
        content = content[len("Feed post ") :].strip()
    if not author:
        return None
    author_index = content.find(author)
    if author_index <= 0:
        return None
    title = content[:author_index].strip(" -•")
    if title.lower().startswith("new post in "):
        title = title[len("New post in ") :].strip()
    return title or None


def _strip_post_display_scaffolding(content_text: str | None) -> str:
    content = _normalize_display_text(content_text)
    if not content:
        return ""
    if content.lower().startswith("feed post "):
        content = content[len("Feed post ") :].strip()
    if " Follow " in content:
        trailing = content.split(" Follow ", 1)[1].strip()
        if trailing:
            content = trailing
    for marker in (
        " Only group members can comment on this post.",
        " Activate to view larger image",
        " See content credentials",
        " Like Comment Repost Send",
        " Like Comment",
    ):
        index = content.find(marker)
        if index > 0:
            content = content[:index].strip()
    return content.strip()


def _format_post_display_text(content_text: str | None) -> str:
    formatted = _strip_post_display_scaffolding(content_text)
    if not formatted:
        return ""
    section_pattern = "|".join(
        re.escape(label) for label in sorted(DISPLAY_SECTION_LABELS, key=len, reverse=True)
    )
    formatted = re.sub(rf"\s+({section_pattern})", r"\n\n\1", formatted)
    for marker in DISPLAY_BULLET_MARKERS:
        formatted = formatted.replace(f" {marker} ", f"\n{marker} ")
    formatted = re.sub(r"\s+(?=hashtag\s+#)", "\n", formatted, flags=re.I)
    formatted = re.sub(r"\n{3,}", "\n\n", formatted)
    return formatted.strip()


def _extract_display_highlights(formatted_text: str) -> dict[str, str | None]:
    highlights: dict[str, str | None] = {key: None for key in DISPLAY_HIGHLIGHT_PREFIXES}
    for line in [segment.strip() for segment in formatted_text.splitlines() if segment.strip()]:
        normalized = _normalize_display_text(line)
        for key, prefixes in DISPLAY_HIGHLIGHT_PREFIXES.items():
            if highlights[key]:
                continue
            for prefix in prefixes:
                if normalized.lower().startswith(prefix.lower()):
                    highlights[key] = normalized[len(prefix) :].strip(" -")
                    break
    return highlights


def _remove_highlight_lines(formatted_text: str, highlights: dict[str, str | None]) -> str:
    removable_lines = {
        _normalize_display_text(f"{prefix} {value}")
        for key, value in highlights.items()
        if value
        for prefix in DISPLAY_HIGHLIGHT_PREFIXES.get(key, ())
    }
    kept_lines: list[str] = []
    for raw_line in formatted_text.splitlines():
        line = raw_line.strip()
        if not line:
            kept_lines.append("")
            continue
        normalized = _normalize_display_text(line)
        if normalized in removable_lines:
            continue
        kept_lines.append(line)
    body = "\n".join(kept_lines)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _classify_frontend_post_intent(content_text: str | None) -> dict[str, Any]:
    normalized = _normalize_display_text(content_text).lower()
    if not normalized:
        return {
            "display_post_intent": "unclear",
            "display_job_intent_score": 0,
            "display_supply_intent_score": 0,
            "display_hidden_from_frontend": False,
            "display_intent_reason": None,
        }

    job_score = 0
    supply_score = 0
    job_hits: list[str] = []
    supply_hits: list[str] = []

    for phrase, score in DISPLAY_JOB_SIGNAL_SCORES.items():
        if phrase in normalized:
            job_score += score
            job_hits.append(phrase)

    for phrase, score in DISPLAY_SUPPLY_SIGNAL_SCORES.items():
        if phrase in normalized:
            supply_score += score
            supply_hits.append(phrase)

    for pattern, score, label in DISPLAY_JOB_SIGNAL_PATTERNS:
        if re.search(pattern, normalized):
            job_score += score
            job_hits.append(label)

    for pattern, score, label in DISPLAY_SUPPLY_SIGNAL_PATTERNS:
        if re.search(pattern, normalized):
            supply_score += score
            supply_hits.append(label)

    structured_job_sections = sum(1 for label in DISPLAY_STRUCTURED_JOB_LABELS if label in normalized)
    if structured_job_sections >= 2:
        job_score += min(4, structured_job_sections)
        job_hits.append("structured_job_sections")

    if any(noun in normalized for noun in DISPLAY_SUPPLY_NOUNS):
        visa_hits = {term for term in DISPLAY_VISA_TERMS if re.search(rf"\b{re.escape(term)}\b", normalized)}
        if len(visa_hits) >= 2:
            supply_score += 2
            supply_hits.append("visa_inventory")

    hidden_from_frontend = False
    intent = "unclear"
    if supply_score >= 6 and supply_score >= job_score + 2:
        hidden_from_frontend = True
        intent = "consultant_supply"
    elif job_score >= supply_score + 1:
        intent = "job_post"

    reason = None
    if hidden_from_frontend and supply_hits:
        reason = supply_hits[0]
    elif job_hits:
        reason = job_hits[0]

    return {
        "display_post_intent": intent,
        "display_job_intent_score": job_score,
        "display_supply_intent_score": supply_score,
        "display_hidden_from_frontend": hidden_from_frontend,
        "display_intent_reason": reason,
    }


def _annotate_post_for_display(post: dict[str, Any]) -> dict[str, Any]:
    author_name = _normalize_display_text(post.get("author_name")) or None
    display_title = _extract_group_post_title(post.get("content_text"), author_name)
    formatted_text = _format_post_display_text(post.get("content_text"))
    highlights = _extract_display_highlights(formatted_text)
    display_body = _remove_highlight_lines(formatted_text, highlights) or formatted_text
    post.update(_classify_frontend_post_intent(post.get("content_text")))
    post["display_author_name"] = author_name or "Unknown author"
    post["display_title"] = display_title or post["display_author_name"]
    post["display_has_group_title"] = bool(display_title and display_title != author_name)
    post["display_body"] = display_body
    post["display_excerpt"] = _normalize_display_text(display_body or formatted_text or post.get("content_text"))
    post.update(highlights)
    return post


def _annotate_posts_for_display(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for post in posts:
        _annotate_post_for_display(post)
    return posts


def _apply_role_match_display(post: dict[str, Any], *, keywords: str | None = None) -> dict[str, Any]:
    query_text = (keywords or "").strip()
    if not query_text:
        query_text = strip_query_state_suffix(post.get("source_query"))
    if not query_text:
        post.setdefault("display_matched_opening", None)
        post.setdefault("display_match_type", None)
        post.setdefault("display_role_family", None)
        post.setdefault("display_role_relevance_score", 0.0)
        return post
    analysis = analyze_post_for_query(post.get("content_text"), query_text)
    post["display_matched_opening"] = analysis.matched_opening
    post["display_match_type"] = analysis.match_type
    post["display_role_family"] = analysis.role_family
    post["display_role_relevance_score"] = analysis.relevance_score
    post["display_hidden_from_frontend"] = analysis.hidden_from_frontend
    post["display_intent_reason"] = analysis.hidden_reason
    post["display_post_intent"] = analysis.post_intent
    return post


def _post_dedup_key(post: dict[str, Any]) -> str | None:
    author_url = (post.get("author_profile_url") or "").strip().lower()
    author_name_norm = " ".join((post.get("author_name") or "").lower().split())
    author_id = author_url or author_name_norm
    content_source = post.get("display_excerpt") or post.get("content_text") or ""
    fingerprint = strip_ephemeral_engagement(content_source).lower()[:400]
    if not author_id and not fingerprint:
        return None
    return f"{author_id}|{fingerprint}"


def _dedupe_posts_by_author_content(
    posts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    keyed: OrderedDict[str, dict[str, Any]] = OrderedDict()
    unkeyed: list[dict[str, Any]] = []
    for post in posts:
        key = _post_dedup_key(post)
        if key is None:
            unkeyed.append(post)
            continue
        existing = keyed.get(key)
        if existing is None:
            keyed[key] = post
            continue
        # Propagate viewed_at across the duplicate group so marking one
        # variant as seen collapses the whole group into "Seen posts".
        existing_viewed = existing.get("viewed_at")
        current_viewed = post.get("viewed_at")
        if existing_viewed and not current_viewed:
            post["viewed_at"] = existing_viewed
        elif current_viewed and not existing_viewed:
            existing["viewed_at"] = current_viewed
        existing_score = float(existing.get("score") or 0.0)
        current_score = float(post.get("score") or 0.0)
        if current_score > existing_score or (
            current_score == existing_score
            and int(post.get("id") or 0) > int(existing.get("id") or 0)
        ):
            keyed[key] = post
    return list(keyed.values()) + unkeyed


def _filter_posts_for_frontend(
    posts: list[dict[str, Any]],
    *,
    keywords: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    visible_posts: list[dict[str, Any]] = []
    for post in posts:
        _annotate_post_for_display(post)
        _apply_role_match_display(post, keywords=keywords)
        if post.get("display_hidden_from_frontend"):
            continue
        visible_posts.append(post)
    deduped = _dedupe_posts_by_author_content(visible_posts)
    if limit is not None:
        return deduped[:limit]
    return deduped


def _merge_results(
    primary_results: list[dict[str, Any]],
    additional_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged_by_post_id: OrderedDict[int, dict[str, Any]] = OrderedDict()
    for row in [*primary_results, *additional_results]:
        post_id = int(row.get("id") or 0)
        if not post_id:
            continue
        existing = merged_by_post_id.get(post_id)
        if existing is None:
            merged_by_post_id[post_id] = row
            continue
        existing_score = float(existing.get("score") or 0.0)
        current_score = float(row.get("score") or 0.0)
        if current_score > existing_score:
            merged_by_post_id[post_id] = row

    merged = list(merged_by_post_id.values())
    merged.sort(
        key=lambda row: (
            row["matched_state_code"],
            -_result_sort_timestamp(row),
            -(float(row.get("score") or 0.0)),
            -(int(row.get("id") or 0)),
        )
    )
    return merged


def _selected_state_codes(search: dict[str, Any]) -> list[str]:
    selected_codes: list[str] = []
    seen_codes: set[str] = set()
    for code in search.get("enabled_states", []):
        upper_code = str(code).upper()
        if upper_code in STATE_BY_CODE and upper_code not in seen_codes:
            selected_codes.append(upper_code)
            seen_codes.add(upper_code)
    return selected_codes


def _state_scope_label(search: dict[str, Any], *, use_names: bool = False) -> str:
    selected_codes = _selected_state_codes(search)
    if search.get("state_scope") == "all" or len(selected_codes) == len(ALL_STATES):
        return "All states"
    if not selected_codes:
        return "No states selected"
    if use_names:
        return ", ".join(STATE_BY_CODE[code].name for code in selected_codes)
    return ", ".join(selected_codes)


def _capture_mode_label(capture_mode: str) -> str:
    labels = {
        "standard": "Standard",
        "balanced": "Balanced",
        "deep": "Deep",
    }
    return labels.get((capture_mode or "").lower(), "Standard")


def _run_search_task(search_id: int) -> None:
    SearchRunner(settings).run_search(search_id)


def _purge_expired_posts() -> None:
    db.purge_expired_posts(
        settings,
        max_age_hours=settings.post_retention_hours,
    )


def _render_index(
    request: Request,
    *,
    message: str | None = None,
    search_form: dict[str, Any] | None = None,
    status_code: int = 200,
):
    db.mark_stale_running_search_runs_failed(settings=settings)
    _purge_expired_posts()
    active_resume = db.get_active_resume(settings)

    return templates.TemplateResponse(
        request,
        "index.html",
        _template_context(
            request,
            all_states=ALL_STATES,
            default_capture_mode=settings.default_capture_mode,
            message=message,
            search_form=search_form or _default_search_form_state(),
            active_resume=active_resume,
        ),
        status_code=status_code,
    )


@app.get("/")
async def index(request: Request):
    return _render_index(request, message=request.query_params.get("message"))


@app.post("/searches")
async def create_search(
    request: Request,
    keywords: str = Form(...),
    max_results_per_state: int = Form(...),
    schedule_minutes: int = Form(0),
    state_scope: str = Form("custom"),
    enabled_states: list[str] = Form([]),
    capture_mode: str = Form("balanced"),
):
    cleaned_keywords = " ".join(keywords.split())
    normalized_state_scope = state_scope if state_scope in {"all", "custom"} else "custom"
    normalized_enabled_states: list[str] = []
    seen_codes: set[str] = set()
    for code in enabled_states:
        upper_code = code.upper()
        if upper_code in STATE_BY_CODE and upper_code not in seen_codes:
            normalized_enabled_states.append(upper_code)
            seen_codes.add(upper_code)

    normalized_capture_mode = capture_mode.lower()
    if normalized_capture_mode not in {"standard", "balanced", "deep"}:
        normalized_capture_mode = settings.default_capture_mode
    normalized_max_results = max(1, min(max_results_per_state, settings.max_results_per_state_limit))
    normalized_schedule_minutes = max(0, schedule_minutes)
    form_state = _default_search_form_state(
        keywords=cleaned_keywords,
        state_scope=normalized_state_scope,
        enabled_states=normalized_enabled_states,
        max_results_per_state=normalized_max_results,
        capture_mode=normalized_capture_mode,
        schedule_minutes=normalized_schedule_minutes,
    )

    if not cleaned_keywords:
        return _render_index(
            request,
            message="Enter keywords to create a search.",
            search_form=form_state,
            status_code=400,
        )

    if normalized_state_scope == "custom" and not normalized_enabled_states:
        return _render_index(
            request,
            message="Select at least one state below, or switch State scope to All states.",
            search_form=form_state,
            status_code=400,
        )

    search_id = db.create_search(
        cleaned_keywords,
        state_scope=normalized_state_scope,
        enabled_states=normalized_enabled_states,
        capture_mode=normalized_capture_mode,
        window_hours=settings.default_window_hours,
        max_results_per_state=normalized_max_results,
        schedule_minutes=normalized_schedule_minutes,
        settings=settings,
    )
    return RedirectResponse(
        url=request.url_for("search_detail", search_id=search_id),
        status_code=303,
    )


@app.post("/searches/{search_id}/run")
async def run_search(request: Request, search_id: int, background_tasks: BackgroundTasks):
    if not db.get_search(search_id, settings):
        raise HTTPException(status_code=404, detail="Search not found.")
    background_tasks.add_task(_run_search_task, search_id)
    return RedirectResponse(
        url=f"{request.url_for('search_detail', search_id=search_id)}?message=Search+started",
        status_code=303,
    )


@app.get("/searches/{search_id}", name="search_detail")
async def search_detail(request: Request, search_id: int):
    db.mark_stale_running_search_runs_failed(settings=settings)
    _purge_expired_posts()
    search = db.get_search(search_id, settings)
    if not search:
        raise HTTPException(status_code=404, detail="Search not found.")
    search["state_scope_label"] = _state_scope_label(search, use_names=True)
    search["capture_mode_label"] = _capture_mode_label(search.get("capture_mode", "standard"))
    runs = db.list_runs_for_search(search_id, settings)
    latest_run = runs[0] if runs else None

    results = db.list_results_for_search(search_id, settings)
    selected_codes = _selected_state_codes(search)
    related_posts = db.list_related_posts_for_search(
        search_id,
        keywords=search["keywords"],
        state_codes=None if search.get("state_scope") == "all" else selected_codes,
        limit=None,
        settings=settings,
    )
    merged_results = _merge_results(results, related_posts)

    # Resume matching — score every post, then filter to only show resume matches when active
    active_resume = db.get_active_resume(settings)
    resume_keywords = active_resume["keywords"] if active_resume else []
    resume_threshold = float(active_resume["match_threshold"]) if active_resume else 0.05

    for post in merged_results:
        if resume_keywords:
            score = resume_match_score(post.get("content_text", ""), resume_keywords)
            post["resume_match_pct"] = round(score * 100, 1) if score >= resume_threshold else 0
        else:
            post["resume_match_pct"] = 0
    merged_results = _filter_posts_for_frontend(merged_results, keywords=search["keywords"])

    # When a resume is active: score posts but show ALL of them
    # (badge shows match %, but nothing is hidden)
    # if resume_keywords:
    #     merged_results = [p for p in merged_results if p.get("resume_match_pct", 0) > 0]

    new_grouped_results, seen_grouped_results = _partition_results_by_seen(merged_results)
    all_group_codes = [*new_grouped_results.keys(), *seen_grouped_results.keys()]
    state_names = {code: STATE_BY_CODE[code].name for code in all_group_codes if code in STATE_BY_CODE}
    search_core_terms = keyword_focus_terms(search["keywords"])

    return templates.TemplateResponse(
        request,
        "search_detail.html",
        _template_context(
            request,
            search=search,
            new_grouped_results=new_grouped_results,
            seen_grouped_results=seen_grouped_results,
            new_results_count=sum(len(posts) for posts in new_grouped_results.values()),
            seen_results_count=sum(len(posts) for posts in seen_grouped_results.values()),
            state_names=state_names,
            message=request.query_params.get("message"),
            latest_attempt=latest_run,
            search_core_terms=search_core_terms,
            auto_refresh_seconds=5 if latest_run and latest_run.get("status") == "running" else None,
            active_resume=active_resume,
        ),
    )


@app.get("/posts/{post_id}", name="post_detail")
async def post_detail(request: Request, post_id: int):
    _purge_expired_posts()
    viewed_at = db.mark_post_viewed(post_id, settings)
    post = db.get_post(post_id, settings)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found.")
    post["viewed_at"] = viewed_at
    matches = db.list_post_matches(post_id, settings)
    emails = extract_emails(post["content_text"])

    # Resume matching
    active_resume = db.get_active_resume(settings)
    resume_keywords = active_resume["keywords"] if active_resume else []
    resume_threshold = float(active_resume["match_threshold"]) if active_resume else 0.05
    if resume_keywords:
        score = resume_match_score(post.get("content_text", ""), resume_keywords)
        post["resume_match_pct"] = round(score * 100, 1) if score >= resume_threshold else 0
        post["resume_matched_keywords"] = _find_matched_keywords(post.get("content_text", ""), resume_keywords)
    else:
        post["resume_match_pct"] = 0
        post["resume_matched_keywords"] = []
    _annotate_post_for_display(post)
    display_query = matches[0]["keywords"] if matches else strip_query_state_suffix(post.get("source_query"))
    _apply_role_match_display(post, keywords=display_query)

    return templates.TemplateResponse(
        request,
        "post_detail.html",
        _template_context(
            request,
            post=post,
            matches=matches,
            emails=emails,
            active_resume=active_resume,
        ),
    )


def _find_matched_keywords(text: str, keywords: list[str]) -> list[str]:
    """Return which resume keywords appear in the post text."""
    import re as _re
    lowered = text.lower()
    words = {w.lower().strip(".-") for w in _re.findall(r"[a-zA-Z0-9#+.\-/]+", lowered)}
    found = []
    for kw in keywords:
        if " " in kw or "." in kw or "/" in kw:
            if kw in lowered:
                found.append(kw)
        else:
            if kw in words:
                found.append(kw)
    return found


@app.post("/resume/upload", name="upload_resume")
async def upload_resume(request: Request, resume_file: UploadFile = File(...)):
    if not resume_file.filename:
        return RedirectResponse(
            url=f"{request.url_for('index')}?message=No+file+selected",
            status_code=303,
        )

    suffix = Path(resume_file.filename).suffix.lower()
    if suffix not in {".docx", ".pdf", ".txt", ".text", ".md"}:
        return RedirectResponse(
            url=f"{request.url_for('index')}?message=Unsupported+format.+Use+DOCX,+PDF,+or+TXT",
            status_code=303,
        )

    # Save uploaded file to data/resumes/
    from .services.resume_parser import parse_and_extract

    resumes_dir = settings.data_dir / "resumes"
    resumes_dir.mkdir(parents=True, exist_ok=True)
    dest = resumes_dir / resume_file.filename
    content = await resume_file.read()
    dest.write_bytes(content)

    try:
        result = parse_and_extract(dest)
    except Exception as exc:
        return RedirectResponse(
            url=f"{request.url_for('index')}?message=Failed+to+parse+resume:+{exc}",
            status_code=303,
        )

    keyword_count = len(result["keywords"])
    db.save_resume(
        filename=resume_file.filename,
        extracted_text=result["text"],
        extracted_keywords=result["keywords"],
        settings=settings,
    )

    return RedirectResponse(
        url=f"{request.url_for('index')}?message=Resume+uploaded!+Extracted+{keyword_count}+skills",
        status_code=303,
    )


@app.post("/resume/delete", name="delete_resume")
async def delete_resume_route(request: Request):
    active = db.get_active_resume(settings)
    if active:
        db.delete_resume(int(active["id"]), settings)
    return RedirectResponse(
        url=f"{request.url_for('index')}?message=Resume+removed",
        status_code=303,
    )


@app.get("/resume/matches", name="resume_matches")
async def resume_matches(request: Request):
    _purge_expired_posts()
    active_resume = db.get_active_resume(settings)
    if not active_resume:
        return RedirectResponse(
            url=f"{request.url_for('index')}?message=Upload+a+resume+first",
            status_code=303,
        )

    resume_keywords = active_resume["keywords"]
    threshold = float(active_resume["match_threshold"])
    matched_posts = db.list_resume_matched_posts(
        resume_keywords,
        match_threshold=threshold,
        settings=settings,
    )
    matched_posts = _filter_posts_for_frontend(matched_posts)

    return templates.TemplateResponse(
        request,
        "resume_matches.html",
        _template_context(
            request,
            active_resume=active_resume,
            matched_posts=matched_posts,
            total_matched=len(matched_posts),
        ),
    )


@app.get("/health")
async def health():
    return {"status": "ok"}

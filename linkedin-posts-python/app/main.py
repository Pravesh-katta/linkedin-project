from __future__ import annotations

from collections import OrderedDict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db
from .config import Settings, get_settings
from .scheduler import SearchScheduler
from .services.contact_extractor import extract_emails
from .services.search_runner import SearchRunner
from .state_catalog import ALL_STATES, STATE_BY_CODE


settings = get_settings()
templates = Jinja2Templates(directory=str(settings.templates_dir))
scheduler = SearchScheduler(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(settings)
    db.purge_expired_posts(
        settings,
        max_age_hours=settings.post_retention_hours,
    )
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    if settings.enable_scheduler:
        scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")


def _template_context(request: Request, **extra: Any) -> dict[str, Any]:
    return {
        "request": request,
        "app_name": settings.app_name,
        "session_ready": Path(settings.linkedin_storage_state_path).exists(),
        **extra,
    }


def _group_results(results: list[dict[str, Any]]) -> OrderedDict[str, list[dict[str, Any]]]:
    grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for row in results:
        state_code = row["matched_state_code"]
        grouped.setdefault(state_code, []).append(row)
    return grouped


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


def _run_search_task(search_id: int) -> None:
    SearchRunner(settings).run_search(search_id)


def _purge_expired_posts() -> None:
    db.purge_expired_posts(
        settings,
        max_age_hours=settings.post_retention_hours,
    )


@app.get("/")
async def index(request: Request):
    _purge_expired_posts()
    searches = db.list_searches(settings)
    for search in searches:
        search["state_scope_label"] = _state_scope_label(search)
    recent_posts = db.list_recent_posts(15, settings)
    return templates.TemplateResponse(
        request,
        "index.html",
        _template_context(
            request,
            searches=searches,
            recent_posts=recent_posts,
            all_states=ALL_STATES,
            message=request.query_params.get("message"),
        ),
    )


@app.post("/searches")
async def create_search(
    request: Request,
    keywords: str = Form(...),
    max_results_per_state: int = Form(...),
    schedule_minutes: int = Form(0),
    state_scope: str = Form("custom"),
    enabled_states: list[str] = Form([]),
):
    cleaned_keywords = " ".join(keywords.split())
    if not cleaned_keywords:
        raise HTTPException(status_code=400, detail="Keywords are required.")

    normalized_state_scope = state_scope if state_scope in {"all", "custom"} else "custom"
    normalized_enabled_states: list[str] = []
    seen_codes: set[str] = set()
    for code in enabled_states:
        upper_code = code.upper()
        if upper_code in STATE_BY_CODE and upper_code not in seen_codes:
            normalized_enabled_states.append(upper_code)
            seen_codes.add(upper_code)

    if normalized_state_scope == "custom" and not normalized_enabled_states:
        raise HTTPException(status_code=400, detail="Choose at least one state for a custom search.")

    search_id = db.create_search(
        cleaned_keywords,
        state_scope=normalized_state_scope,
        enabled_states=normalized_enabled_states,
        window_hours=settings.default_window_hours,
        max_results_per_state=max(1, min(max_results_per_state, 50)),
        schedule_minutes=max(0, schedule_minutes),
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
    _purge_expired_posts()
    search = db.get_search(search_id, settings)
    if not search:
        raise HTTPException(status_code=404, detail="Search not found.")
    search["state_scope_label"] = _state_scope_label(search, use_names=True)

    results = db.list_results_for_search(search_id, settings)
    grouped_results = _group_results(results)
    runs = db.list_runs_for_search(search_id, settings)
    state_names = {code: STATE_BY_CODE[code].name for code in grouped_results if code in STATE_BY_CODE}

    return templates.TemplateResponse(
        request,
        "search_detail.html",
        _template_context(
            request,
            search=search,
            grouped_results=grouped_results,
            runs=runs,
            state_names=state_names,
            message=request.query_params.get("message"),
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
    return templates.TemplateResponse(
        request,
        "post_detail.html",
        _template_context(
            request,
            post=post,
            matches=matches,
            emails=emails,
        ),
    )


@app.get("/health")
async def health():
    return {"status": "ok"}

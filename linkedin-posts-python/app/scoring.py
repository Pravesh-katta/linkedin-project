from __future__ import annotations

import hashlib
import re

from .state_catalog import State


WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-zA-Z0-9]+")
ROLE_STOPWORDS = {
    "developer",
    "developers",
    "engineer",
    "engineers",
    "architect",
    "architects",
    "analyst",
    "analysts",
    "lead",
    "leads",
    "senior",
    "sr",
    "junior",
    "jr",
    "principal",
    "staff",
    "software",
    "application",
    "applications",
    "app",
    "apps",
    "backend",
    "frontend",
    "fullstack",
    "full",
    "stack",
    "specialist",
    "manager",
    "consultant",
    "consultants",
    "programmer",
    "programmers",
    "admin",
    "administrator",
    "admins",
    "qa",
    "tester",
    "testing",
    "role",
    "roles",
    "position",
    "positions",
    "job",
    "jobs",
    "opening",
    "openings",
    "hiring",
    "opportunity",
    "opportunities",
    "contract",
    "contracts",
    "remote",
    "onsite",
    "hybrid",
}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_RE.sub(" ", value).strip()


def _normalize_keyword_token(token: str) -> str:
    value = token.lower().strip()
    if len(value) > 4 and value.endswith("ies"):
        return f"{value[:-3]}y"
    if len(value) > 4 and value.endswith("es"):
        return value[:-2]
    if len(value) > 3 and value.endswith("s"):
        return value[:-1]
    return value


def _token_set(value: str | None) -> set[str]:
    normalized = normalize_text(value)
    return {_normalize_keyword_token(token) for token in WORD_RE.findall(normalized) if token}


def _ordered_tokens(value: str | None) -> list[str]:
    normalized = normalize_text(value)
    seen: set[str] = set()
    ordered: list[str] = []
    for token in WORD_RE.findall(normalized):
        normalized_token = _normalize_keyword_token(token)
        if not normalized_token or normalized_token in seen:
            continue
        seen.add(normalized_token)
        ordered.append(normalized_token)
    return ordered


def keyword_focus_terms(keywords: str) -> list[str]:
    ordered_tokens = [token for token in _ordered_tokens(keywords) if len(token) > 1]
    focused = [token for token in ordered_tokens if token not in ROLE_STOPWORDS]
    return focused or ordered_tokens


def dedupe_fingerprint(permalink: str | None, author_name: str | None, content_text: str | None) -> str:
    if permalink:
        return permalink.strip()
    raw = "|".join(
        [
            normalize_text(author_name).lower(),
            normalize_text(content_text).lower(),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def keyword_match_score(content_text: str, keywords: str) -> float:
    content_words = _token_set(content_text)
    keyword_words = keyword_focus_terms(keywords)
    if not keyword_words:
        return 0.0
    matched = sum(1 for word in keyword_words if word in content_words)
    return round(matched / len(keyword_words), 2)


def state_match_score(content_text: str, state: State) -> float:
    normalized = normalize_text(content_text)
    lowered = normalized.lower()
    uppered = normalized.upper()
    if state.name.lower() in lowered:
        return 1.0
    if re.search(rf"\b{re.escape(state.code)}\b", uppered):
        return 0.85
    return 0.0


def overall_result_score(content_text: str, keywords: str, state: State) -> float:
    keyword_score = keyword_match_score(content_text, keywords)
    state_score = state_match_score(content_text, state)
    if state_score == 0 and keyword_score == 0:
        return 0.0
    if state_score == 0:
        return round(min(0.35, 0.2 + (keyword_score * 0.15)), 2)
    return round(min(1.0, (state_score * 0.7) + (keyword_score * 0.3)), 2)

from __future__ import annotations

import hashlib
import re

from .state_catalog import State


WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-zA-Z0-9]+")


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return WHITESPACE_RE.sub(" ", value).strip()


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
    content_words = set(WORD_RE.findall(normalize_text(content_text).lower()))
    keyword_words = [word for word in WORD_RE.findall(normalize_text(keywords).lower()) if len(word) > 1]
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

from __future__ import annotations

from dataclasses import dataclass
import re


WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-z0-9+/#.-]+")
STATE_SUFFIX_RE = re.compile(r',\s*"?(?:[A-Z]{2})"?\s*$')
HASHTAG_RE = re.compile(r"#([a-z0-9_+-]+)", re.I)
ROLE_MARKER_RE = re.compile(
    r"\b(developer|engineer|scientist|analyst|architect|administrator|tester|qa|devops|sre|specialist|consultant|programmer)\b",
    re.I,
)
OPENING_LABELS = (
    "role",
    "position",
    "job title",
    "hiring",
    "opening",
    "requirement",
    "job description",
    "job role",
    "working title",
    "title",
)
OPENING_LABEL_PATTERN = "|".join(re.escape(label) for label in OPENING_LABELS)

SENIORITY_WORDS = {
    "senior",
    "sr",
    "lead",
    "principal",
    "staff",
    "junior",
    "jr",
    "mid",
    "level",
}

GENERIC_QUERY_WORDS = {
    "developer",
    "developers",
    "engineer",
    "engineers",
    "analyst",
    "analysts",
    "architect",
    "architects",
    "scientist",
    "scientists",
    "consultant",
    "consultants",
    "role",
    "roles",
    "opening",
    "openings",
    "job",
    "jobs",
    "hiring",
    "position",
    "positions",
}

JOB_SIGNAL_SCORES = {
    "we are hiring": 4,
    "we're hiring": 4,
    "hiring for": 4,
    "opening for": 4,
    "job opening": 4,
    "job openings": 4,
    "open position": 4,
    "open positions": 4,
    "positions available": 3,
    "requirements available": 3,
    "roles available": 3,
    "role:": 3,
    "position:": 3,
    "job title:": 3,
    "location:": 2,
    "must have": 2,
    "required skills": 3,
    "responsibilities": 3,
    "job description": 3,
    "benefits:": 2,
    "interview": 2,
    "send resume": 2,
    "share your resume": 2,
    "full-time": 2,
    "full time": 2,
}

SUPPLY_SIGNAL_SCORES = {
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

JOB_SIGNAL_PATTERNS: tuple[tuple[str, int], ...] = (
    (r"\b(hotlist|hot list)\b.{0,40}\b(positions?|roles?|openings?|requirements?)\b", 4),
    (r"\b(positions?|roles?|openings?|requirements?)\b.{0,24}\bavailable\b", 3),
)

SUPPLY_SIGNAL_PATTERNS: tuple[tuple[str, int], ...] = (
    (r"\b(hotlist|hot list)\b.{0,40}\b(consultants?|candidates?|profiles?|resources?)\b", 6),
    (r"\b(consultants?|candidates?|profiles?|resources?)\b.{0,24}\bavailable\b", 5),
    (r"\bavailable\b.{0,24}\b(consultants?|candidates?|profiles?|resources?)\b", 5),
    (r"\badd me to (?:your|ur) vendor (?:distribution )?list\b", 6),
    (r"\bshare (?:your|ur|current) requirements\b", 5),
)

STRUCTURED_JOB_LABELS = (
    "role:",
    "position:",
    "job role:",
    "location:",
    "must have",
    "required skills",
    "responsibilities",
    "job description",
    "working title:",
    "benefits:",
)

SUPPLY_NOUNS = (
    "consultant",
    "consultants",
    "candidate",
    "candidates",
    "profile",
    "profiles",
    "resource",
    "resources",
)

RELATED_TITLE_HINTS = {
    "python": (
        "ai/ml",
        "ai engineer",
        "ai developer",
        "generative ai",
        "ml engineer",
        "machine learning",
        "data engineer",
        "data scientist",
        "software engineer",
        "backend",
        "full stack",
    ),
    "java": ("backend", "software engineer", "microservices", "spring", "full stack"),
    "pyspark": ("data engineer", "big data", "spark", "etl", "analytics"),
    "spark": ("data engineer", "big data", "etl", "analytics"),
    "snowflake": ("data engineer", "etl", "analytics", "data analyst"),
    "sql": ("data engineer", "data analyst", "database", "etl", "analytics"),
}

FRONTEND_RELEVANCE_THRESHOLD = 0.34


@dataclass(frozen=True, slots=True)
class QueryIntent:
    raw_query: str
    normalized_query: str
    anchor_tokens: tuple[str, ...]
    anchor_count: int
    requires_full_stack: bool
    broad_single_anchor: bool


@dataclass(frozen=True, slots=True)
class PostOpening:
    title: str
    body: str
    role_family: str


@dataclass(frozen=True, slots=True)
class PostMatchAnalysis:
    post_intent: str
    hidden_from_frontend: bool
    hidden_reason: str | None
    matched_opening: str | None
    match_type: str
    role_family: str | None
    relevance_score: float
    extracted_opening_count: int


def normalize_matching_text(value: str | None) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "")).strip()


def strip_query_state_suffix(query: str | None) -> str:
    normalized = normalize_matching_text(query)
    return STATE_SUFFIX_RE.sub("", normalized)


def clean_post_text_for_matching(content_text: str | None) -> str:
    content = normalize_matching_text(content_text)
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


def classify_post_intent(content_text: str | None) -> tuple[str, bool, str | None, int, int]:
    normalized = clean_post_text_for_matching(content_text).lower()
    if not normalized:
        return "unclear", False, None, 0, 0

    job_score = 0
    supply_score = 0
    hidden_reason: str | None = None

    for phrase, score in JOB_SIGNAL_SCORES.items():
        if phrase in normalized:
            job_score += score

    for phrase, score in SUPPLY_SIGNAL_SCORES.items():
        if phrase in normalized:
            supply_score += score
            hidden_reason = hidden_reason or phrase

    for pattern, score in JOB_SIGNAL_PATTERNS:
        if re.search(pattern, normalized):
            job_score += score

    for pattern, score in SUPPLY_SIGNAL_PATTERNS:
        if re.search(pattern, normalized):
            supply_score += score
            hidden_reason = hidden_reason or "consultant_supply_pattern"

    structured_job_sections = sum(1 for label in STRUCTURED_JOB_LABELS if label in normalized)
    if structured_job_sections >= 2:
        job_score += min(4, structured_job_sections)

    if any(noun in normalized for noun in SUPPLY_NOUNS):
        visa_hits = {term for term in ("h1b", "gc", "opt", "cpt", "ead") if re.search(rf"\b{re.escape(term)}\b", normalized)}
        if len(visa_hits) >= 2:
            supply_score += 2
            hidden_reason = hidden_reason or "visa_inventory"

    hidden = supply_score >= 6 and supply_score >= job_score + 2
    intent = "consultant_supply" if hidden else ("job_post" if job_score > 0 else "unclear")
    return intent, hidden, hidden_reason, job_score, supply_score


def parse_query_intent(query: str) -> QueryIntent:
    normalized_query = strip_query_state_suffix(query).lower()
    normalized_query = normalized_query.replace("ai/ml", "ai ml")
    tokens = [token for token in WORD_RE.findall(normalized_query) if token]
    anchor_tokens = tuple(
        token
        for token in tokens
        if token not in GENERIC_QUERY_WORDS and token not in SENIORITY_WORDS and len(token) > 1
    )
    requires_full_stack = "full stack" in normalized_query
    broad_single_anchor = len(anchor_tokens) == 1 and not requires_full_stack
    return QueryIntent(
        raw_query=query,
        normalized_query=normalized_query,
        anchor_tokens=anchor_tokens,
        anchor_count=len(anchor_tokens),
        requires_full_stack=requires_full_stack,
        broad_single_anchor=broad_single_anchor,
    )


def extract_openings(content_text: str | None) -> list[PostOpening]:
    cleaned = clean_post_text_for_matching(content_text)
    if not cleaned:
        return []

    working = cleaned
    working = re.sub(r"\s+(?=\d+\.)", "\n", working)
    working = re.sub(r"\s+(?=[-•]\s+[A-Z])", "\n", working)
    working = re.sub(
        rf"\s+(?=(?:{OPENING_LABEL_PATTERN})\s*:)",
        "\n",
        working,
        flags=re.I,
    )

    lines = [segment.strip() for segment in working.splitlines() if segment.strip()]
    openings: list[PostOpening] = []

    for line in lines:
        title = _extract_opening_title(line)
        if not title:
            continue
        openings.append(
            PostOpening(
                title=title,
                body=line,
                role_family=_infer_role_family(title),
            )
        )

    if openings:
        return openings

    openings = _extract_openings_from_label_hits(cleaned)
    if openings:
        return openings

    phrase_opening = _extract_phrase_based_opening(cleaned)
    if phrase_opening is not None:
        return [phrase_opening]

    fallback_title = _extract_opening_title(cleaned)
    if not fallback_title:
        return []
    return [PostOpening(title=fallback_title, body=cleaned, role_family=_infer_role_family(fallback_title))]


def analyze_post_for_query(content_text: str | None, query: str | None) -> PostMatchAnalysis:
    post_intent, hidden_intent, hidden_reason, _, _ = classify_post_intent(content_text)
    if hidden_intent:
        return PostMatchAnalysis(
            post_intent=post_intent,
            hidden_from_frontend=True,
            hidden_reason=hidden_reason,
            matched_opening=None,
            match_type="hidden",
            role_family=None,
            relevance_score=0.0,
            extracted_opening_count=0,
        )

    cleaned_query = strip_query_state_suffix(query)
    if not cleaned_query:
        return PostMatchAnalysis(
            post_intent=post_intent,
            hidden_from_frontend=False,
            hidden_reason=None,
            matched_opening=None,
            match_type="possible",
            role_family=None,
            relevance_score=0.0,
            extracted_opening_count=0,
        )

    openings = extract_openings(content_text)
    if not openings:
        return PostMatchAnalysis(
            post_intent=post_intent,
            hidden_from_frontend=True,
            hidden_reason="no_opening_detected",
            matched_opening=None,
            match_type="hidden",
            role_family=None,
            relevance_score=0.0,
            extracted_opening_count=0,
        )

    intent = parse_query_intent(cleaned_query)
    scored = sorted(
        (_score_opening(opening, intent) for opening in openings),
        key=lambda item: (item["score"], item["title_score"], item["anchor_hits_title"]),
        reverse=True,
    )
    best = scored[0]
    hidden_from_frontend = best["score"] < FRONTEND_RELEVANCE_THRESHOLD
    hidden_reason = "low_relevance" if hidden_from_frontend else None
    return PostMatchAnalysis(
        post_intent=post_intent,
        hidden_from_frontend=hidden_from_frontend,
        hidden_reason=hidden_reason,
        matched_opening=best["opening"].title if not hidden_from_frontend else None,
        match_type=best["match_type"] if not hidden_from_frontend else "hidden",
        role_family=best["opening"].role_family if not hidden_from_frontend else None,
        relevance_score=round(best["score"], 4),
        extracted_opening_count=len(openings),
    )


def _extract_opening_title(segment: str) -> str | None:
    working = normalize_matching_text(segment)
    if not working:
        return None
    working = re.sub(r"\s+:\s*", ": ", working)
    working = re.sub(r"^(?:\d+\.\s*|[-•]\s*)", "", working)
    working = re.sub(rf"^(?:{OPENING_LABEL_PATTERN})\s*:\s*", "", working, flags=re.I)
    working = re.sub(r"^hashtag\s+#hiring\b", "", working, flags=re.I).strip(" -:")
    if not working:
        return None

    stop_markers = (
        " Location:",
        " Location :",
        " Skills:",
        " Skills :",
        " Must Have",
        " Required",
        " Responsibilities",
        " Responsibilities:",
        " Job Description",
        " Job Role:",
        " Role:",
        " Position:",
        " Title:",
        " Working Title:",
        " Duration:",
        " Experience:",
        " Experince:",
        " Work type:",
        " Length:",
        " Interview",
        " Share resume",
        " Please send",
        " Please share",
        " Interested?",
        " DM me",
        " 📍",
        " | ",
        " || ",
    )
    end_index = len(working)
    for marker in stop_markers:
        marker_index = working.find(marker)
        if marker_index != -1:
            end_index = min(end_index, marker_index)
    title = working[:end_index].strip(" -:|,")
    title = re.sub(r"\s+\([^)]{1,40}\)$", "", title)
    if not title:
        return None
    if not ROLE_MARKER_RE.search(title):
        return None
    if len(title.split()) > 16:
        return None
    lowered = title.lower()
    if lowered.startswith(("location:", "length:", "duration:", "please send", "interview")):
        return None
    return title


def _extract_openings_from_label_hits(cleaned: str) -> list[PostOpening]:
    label_matches = list(re.finditer(rf"(?:{OPENING_LABEL_PATTERN})\s*:", cleaned, flags=re.I))
    if not label_matches:
        return []

    openings: list[PostOpening] = []
    for index, match in enumerate(label_matches):
        start = match.start()
        end = label_matches[index + 1].start() if index + 1 < len(label_matches) else len(cleaned)
        segment = cleaned[start:end].strip()
        title = _extract_opening_title(segment)
        if not title:
            continue
        openings.append(
            PostOpening(
                title=title,
                body=segment,
                role_family=_infer_role_family(title),
            )
        )
    return openings


def _extract_phrase_based_opening(cleaned: str) -> PostOpening | None:
    phrase_patterns = (
        r"\b(?:we are looking for|we're looking for|looking for)\s+(?:an?\s+|the\s+)?(?P<title>[^.!?\n]{0,120})",
        r"\b(?:we are hiring|we're hiring|hiring for)\s+(?:an?\s+|the\s+)?(?P<title>[^.!?\n]{0,120})",
    )
    for pattern in phrase_patterns:
        for match in re.finditer(pattern, cleaned, flags=re.I):
            candidate = normalize_matching_text(match.group("title"))
            candidate = re.split(
                r"\b(?:to join|with|in|for|on|at|location|duration|hybrid)\b",
                candidate,
                maxsplit=1,
                flags=re.I,
            )[0].strip(" -:|,")
            if not candidate or not ROLE_MARKER_RE.search(candidate):
                continue
            if len(candidate.split()) > 16:
                continue
            return PostOpening(
                title=candidate,
                body=cleaned,
                role_family=_infer_role_family(candidate),
            )
    return None


def _infer_role_family(title: str) -> str:
    lowered = normalize_matching_text(title).lower()
    if "snowflake" in lowered:
        return "snowflake_data"
    if "pyspark" in lowered or ("spark" in lowered and "developer" in lowered):
        return "spark_data"
    if "python" in lowered and "full stack" in lowered:
        return "python_fullstack"
    if "java" in lowered and "full stack" in lowered:
        return "java_fullstack"
    if "python" in lowered:
        return "python_core"
    if "java" in lowered:
        return "java_core"
    if "ai" in lowered or "ml" in lowered or "machine learning" in lowered:
        return "ai_ml"
    if "data engineer" in lowered or "data scientist" in lowered or "analytics" in lowered:
        return "data"
    if "full stack" in lowered:
        return "full_stack"
    if "backend" in lowered:
        return "backend"
    return "general"


def _score_opening(opening: PostOpening, intent: QueryIntent) -> dict[str, object]:
    title_normalized = normalize_matching_text(opening.title).lower()
    title_comparable = _comparable_title(title_normalized)
    body_clean = clean_post_text_for_matching(opening.body).lower()
    body_without_hashtags = HASHTAG_RE.sub(" ", body_clean)
    hashtag_tokens = {match.group(1).lower() for match in HASHTAG_RE.finditer(body_clean)}

    anchor_hits_title = sum(1 for token in intent.anchor_tokens if _contains_token(title_comparable, token))
    anchor_hits_body = sum(1 for token in intent.anchor_tokens if _contains_token(body_without_hashtags, token))
    anchor_hits_hashtags_only = sum(
        1
        for token in intent.anchor_tokens
        if token in hashtag_tokens and not _contains_token(body_without_hashtags, token)
    )

    exact_query = _comparable_title(intent.normalized_query)
    title_exact = bool(exact_query and exact_query in title_comparable)
    all_anchors_in_title = bool(intent.anchor_tokens) and anchor_hits_title == intent.anchor_count
    all_anchors_in_body = bool(intent.anchor_tokens) and anchor_hits_body == intent.anchor_count

    raw_score = 0.0
    if title_exact:
        raw_score += 9.0
    if all_anchors_in_title:
        raw_score += 6.0
    raw_score += anchor_hits_title * 3.5
    raw_score += anchor_hits_body * 2.0
    raw_score += anchor_hits_hashtags_only * 0.2

    if intent.requires_full_stack and "full stack" in title_comparable:
        raw_score += 2.0
    if _has_related_title_family(intent.anchor_tokens, title_comparable) and anchor_hits_body > 0:
        raw_score += 2.0
    if intent.broad_single_anchor and anchor_hits_body > 0 and ROLE_MARKER_RE.search(title_comparable):
        raw_score += 1.5

    if anchor_hits_body == 0 and anchor_hits_title == 0 and anchor_hits_hashtags_only > 0:
        raw_score -= 3.5

    if intent.anchor_count > 1:
        non_hashtag_hits = max(anchor_hits_title, anchor_hits_body)
        if non_hashtag_hits < intent.anchor_count:
            raw_score -= float(intent.anchor_count - non_hashtag_hits) * 1.75
        if all_anchors_in_body or all_anchors_in_title:
            raw_score += 2.0

    normalized_score = max(0.0, min(1.0, raw_score / 12.0))

    if title_exact or all_anchors_in_title:
        match_type = "exact"
    elif normalized_score >= 0.62:
        match_type = "related"
    elif normalized_score >= FRONTEND_RELEVANCE_THRESHOLD:
        match_type = "possible"
    else:
        match_type = "hidden"

    return {
        "opening": opening,
        "score": normalized_score,
        "match_type": match_type,
        "title_score": raw_score,
        "anchor_hits_title": anchor_hits_title,
    }


def _comparable_title(value: str) -> str:
    tokens = [token for token in WORD_RE.findall(value) if token not in SENIORITY_WORDS]
    return " ".join(tokens)


def _contains_token(value: str, token: str) -> bool:
    return bool(re.search(rf"\b{re.escape(token)}\b", value))


def _has_related_title_family(anchor_tokens: tuple[str, ...], title: str) -> bool:
    for token in anchor_tokens:
        hints = RELATED_TITLE_HINTS.get(token, ())
        if any(hint in title for hint in hints):
            return True
    return False

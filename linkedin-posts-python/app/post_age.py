from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re


def linkedin_posted_at(
    *,
    absolute_posted_at: str | None,
    relative_time_text: str | None,
    reference_now: datetime | str | None = None,
) -> datetime | None:
    posted_at = _parse_absolute_posted_at(absolute_posted_at)
    if posted_at is not None:
        return posted_at

    age_hours = _relative_time_to_hours(relative_time_text)
    if age_hours is None:
        return None

    baseline = _coerce_datetime(reference_now) or datetime.now(timezone.utc)
    return baseline - timedelta(hours=age_hours)


def linkedin_post_is_within_hours(
    *,
    absolute_posted_at: str | None,
    relative_time_text: str | None,
    window_hours: int,
    now: datetime | None = None,
    reference_now: datetime | str | None = None,
) -> bool:
    if window_hours <= 0:
        return True

    current_now = now or datetime.now(timezone.utc)
    posted_at = linkedin_posted_at(
        absolute_posted_at=absolute_posted_at,
        relative_time_text=relative_time_text,
        reference_now=reference_now or current_now,
    )
    if posted_at is None:
        return True
    age_seconds = (current_now - posted_at).total_seconds()
    return age_seconds < (window_hours * 3600)


def _parse_absolute_posted_at(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return _parse_absolute_posted_at(value)


def _relative_time_to_hours(relative_time_text: str | None) -> float | None:
    if not relative_time_text:
        return None

    normalized = " ".join(relative_time_text.lower().split())

    if normalized in {"now", "just now"}:
        return 0.0

    compact_match = re.search(r"\b(\d+)\s*(m|h|d|w|mo|y)\b", normalized)
    if compact_match:
        return _unit_value_to_hours(int(compact_match.group(1)), compact_match.group(2))

    verbose_match = re.search(
        r"\b(\d+)\s*(minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\b",
        normalized,
    )
    if verbose_match:
        unit_map = {
            "minute": "m",
            "minutes": "m",
            "hour": "h",
            "hours": "h",
            "day": "d",
            "days": "d",
            "week": "w",
            "weeks": "w",
            "month": "mo",
            "months": "mo",
            "year": "y",
            "years": "y",
        }
        return _unit_value_to_hours(int(verbose_match.group(1)), unit_map[verbose_match.group(2)])

    if "yesterday" in normalized:
        return 24.0

    return None


def _unit_value_to_hours(value: int, unit: str) -> float:
    multipliers = {
        "m": 1 / 60,
        "h": 1,
        "d": 24,
        "w": 24 * 7,
        "mo": 24 * 30,
        "y": 24 * 365,
    }
    return value * multipliers[unit]

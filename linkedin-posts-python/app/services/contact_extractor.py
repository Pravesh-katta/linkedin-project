from __future__ import annotations

import re


EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")


def extract_emails(text: str | None) -> list[str]:
    if not text:
        return []
    seen: set[str] = set()
    emails: list[str] = []
    for match in EMAIL_RE.findall(text):
        email = match.lower()
        if email not in seen:
            seen.add(email)
            emails.append(email)
    return emails

from __future__ import annotations


def build_draft_outreach(author_name: str | None, post_excerpt: str) -> str:
    recipient = author_name or "there"
    excerpt = (post_excerpt or "").strip()
    return (
        f"Hi {recipient},\n\n"
        "I came across your LinkedIn post and thought it was relevant to the roles I am tracking.\n\n"
        f"Context: {excerpt[:280]}\n\n"
        "If it makes sense, I would love to connect and learn more.\n\n"
        "Best,\n"
        "Your Name"
    )

from __future__ import annotations

import unittest

from app.main import (
    _annotate_post_for_display,
    _classify_frontend_post_intent,
    _extract_group_post_title,
    _filter_posts_for_frontend,
    _format_post_display_text,
    _strip_post_display_scaffolding,
)


class PostDisplayTests(unittest.TestCase):
    def test_extract_group_post_title_uses_group_header_before_author(self) -> None:
        title = _extract_group_post_title(
            (
                "Feed post New post in C2C and W2 positions/ Direct clients/ Implementation Partners/All Over USA "
                "Sampath K Sampath K • 3rd+ Director at Gen2soft 22m • 22 minutes ago Follow "
                "Role: Sr. Python Developer"
            ),
            "Sampath K Sampath K",
        )
        self.assertEqual(
            title,
            "C2C and W2 positions/ Direct clients/ Implementation Partners/All Over USA",
        )

    def test_extract_group_post_title_returns_none_for_regular_person_post(self) -> None:
        title = _extract_group_post_title(
            (
                "Feed post Mohit Pal Mohit Pal • 3rd+ Senior Talent Acquisition/ Lead 38m • 38 minutes ago "
                "Follow Hiring: Python Developer (async programming (asyncio))"
            ),
            "Mohit Pal Mohit Pal",
        )
        self.assertIsNone(title)

    def test_annotate_post_for_display_prefers_group_title(self) -> None:
        post = {
            "author_name": "Sampath K Sampath K",
            "content_text": (
                "Feed post New post in C2C and W2 positions/ Direct clients/ Implementation Partners/All Over USA "
                "Sampath K Sampath K • 3rd+ Director at Gen2soft Follow Role: Sr. Python Developer"
            ),
        }
        _annotate_post_for_display(post)
        self.assertEqual(
            post["display_title"],
            "C2C and W2 positions/ Direct clients/ Implementation Partners/All Over USA",
        )
        self.assertEqual(post["display_author_name"], "Sampath K Sampath K")
        self.assertTrue(post["display_has_group_title"])

    def test_strip_post_display_scaffolding_removes_linkedin_chrome(self) -> None:
        cleaned = _strip_post_display_scaffolding(
            (
                "Feed post New post in C2C and W2 positions/ Direct clients/ Implementation Partners/All Over USA "
                "Sampath K Sampath K • 3rd+ Director at Gen2soft 22m • 22 minutes ago Visible to anyone on or off LinkedIn "
                "Follow Role: Sr. Python Developer Location: Plano, TX Like Comment Only group members can comment on this post."
            )
        )
        self.assertEqual(cleaned, "Role: Sr. Python Developer Location: Plano, TX")

    def test_format_post_display_text_breaks_sections_for_readability(self) -> None:
        formatted = _format_post_display_text(
            (
                "Role: Sr. Python Developer Location: Plano, TX Duration: 1 year open-ended Contract "
                "Key Responsibilities: Develop and maintain applications using Python • Work with AWS services "
                "Required Skills: Strong experience in Python development"
            )
        )
        self.assertEqual(
            formatted,
            "Role: Sr. Python Developer\n\nLocation: Plano, TX\n\nDuration: 1 year open-ended Contract\n\n"
            "Key Responsibilities: Develop and maintain applications using Python\n• Work with AWS services\n\n"
            "Required Skills: Strong experience in Python development",
        )

    def test_classify_frontend_post_intent_hides_consultant_supply_posts(self) -> None:
        classification = _classify_frontend_post_intent(
            (
                "Dear Hiring Managers, Greetings! I have highly skilled consultants available for immediate deployment "
                "and ready to relocate across the US. Available Skill Sets & Hot Profiles: "
                "Python Developer - H1B - TX (F2F Ready). "
                "Please feel free to share your current requirements or add me to your vendor distribution list."
            )
        )
        self.assertEqual(classification["display_post_intent"], "consultant_supply")
        self.assertTrue(classification["display_hidden_from_frontend"])
        self.assertGreater(
            classification["display_supply_intent_score"],
            classification["display_job_intent_score"],
        )

    def test_classify_frontend_post_intent_keeps_hotlist_positions_posts(self) -> None:
        classification = _classify_frontend_post_intent(
            (
                "Hotlist positions available. We are hiring for below roles. "
                "Role: Python Developer Location: Pennington, NJ Must Have Skills: Python, Flask, GraphQL. "
                "Send resume to recruiter@example.com."
            )
        )
        self.assertEqual(classification["display_post_intent"], "job_post")
        self.assertFalse(classification["display_hidden_from_frontend"])

    def test_filter_posts_for_frontend_removes_supply_posts_only(self) -> None:
        posts = [
            {
                "author_name": "Bhavani Prasad",
                "content_text": (
                    "Dear Hiring Managers, I have consultants available for immediate deployment. "
                    "Hot Profiles: Python Developer - H1B. Share your requirements."
                ),
            },
            {
                "author_name": "Harshit Gupta",
                "content_text": (
                    "We are hiring. Role: Full Stack Python Developer Location: Pennington, NJ "
                    "Must Have Skills: Python, Flask, FastAPI, GraphQL. Send resume."
                ),
            },
        ]

        visible_posts = _filter_posts_for_frontend(posts)

        self.assertEqual(len(visible_posts), 1)
        self.assertEqual(visible_posts[0]["author_name"], "Harshit Gupta")
        self.assertFalse(visible_posts[0]["display_hidden_from_frontend"])


if __name__ == "__main__":
    unittest.main()

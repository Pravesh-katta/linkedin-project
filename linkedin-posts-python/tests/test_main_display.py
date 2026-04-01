from __future__ import annotations

import unittest

from app.main import _annotate_post_for_display, _extract_group_post_title


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


if __name__ == "__main__":
    unittest.main()

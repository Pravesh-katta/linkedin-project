from __future__ import annotations

import unittest

from app.role_matching import analyze_post_for_query, extract_openings


class RoleMatchingTests(unittest.TestCase):
    def test_extract_openings_splits_multi_role_posts(self) -> None:
        openings = extract_openings(
            (
                "My HOTTEST roles for today! : "
                "- Python Developer "
                "- AWS Data Engineer "
                "- AI/ML Data Scientist "
                "- Data Analytics Engineer (Python, SQL, Tableau, AWS) "
                "Location: Malvern, PA"
            )
        )
        self.assertEqual(
            [opening.title for opening in openings],
            [
                "Python Developer",
                "AWS Data Engineer",
                "AI/ML Data Scientist",
                "Data Analytics Engineer",
            ],
        )

    def test_analyze_post_for_query_matches_senior_python_developer(self) -> None:
        analysis = analyze_post_for_query(
            "Role: Senior Python Developer Location: Chicago, IL Must Have Skills: Python, Flask, AWS",
            "python developer",
        )
        self.assertFalse(analysis.hidden_from_frontend)
        self.assertEqual(analysis.matched_opening, "Senior Python Developer")
        self.assertEqual(analysis.match_type, "exact")

    def test_analyze_post_for_query_keeps_related_ai_ml_roles(self) -> None:
        analysis = analyze_post_for_query(
            "1. AI/ML Engineer Skills: Python, TensorFlow, PyTorch, AWS",
            "python developer",
        )
        self.assertFalse(analysis.hidden_from_frontend)
        self.assertEqual(analysis.matched_opening, "AI/ML Engineer")
        self.assertIn(analysis.match_type, {"related", "possible"})

    def test_analyze_post_for_query_keeps_job_description_full_stack_python_roles(self) -> None:
        analysis = analyze_post_for_query(
            (
                "Hi, We are Hiring on W2. "
                "JOB DESCRIPTION: Full Stack Developer "
                "Location: Plano TX "
                "Key Responsibilities: Build backend services using Python (FastAPI/Flask/Django)"
            ),
            "python developer",
        )
        self.assertFalse(analysis.hidden_from_frontend)
        self.assertEqual(analysis.matched_opening, "Full Stack Developer")
        self.assertIn(analysis.match_type, {"related", "possible"})

    def test_analyze_post_for_query_keeps_job_role_with_spaced_colon(self) -> None:
        analysis = analyze_post_for_query(
            (
                "We are looking for a Senior Generative AI Developer. "
                "Job Role : Senior Generative AI Developer "
                "Location : Irving, TX "
                "Required Skills: Python, FastAPI, RAG"
            ),
            "python developer",
        )
        self.assertFalse(analysis.hidden_from_frontend)
        self.assertEqual(analysis.matched_opening, "Senior Generative AI Developer")
        self.assertIn(analysis.match_type, {"related", "possible"})

    def test_analyze_post_for_query_hides_hashtag_only_python_mentions(self) -> None:
        analysis = analyze_post_for_query(
            (
                "Direct client : Big Data Developer (Onsite - USA)- W2 only Charlotte, NC "
                "Strong SQL Hadoop / Big Data Share resume: praveenl@example.com "
                "hashtag #Hiring hashtag #BigData hashtag #Spark hashtag #python"
            ),
            "python developer",
        )
        self.assertTrue(analysis.hidden_from_frontend)
        self.assertEqual(analysis.match_type, "hidden")

    def test_analyze_post_for_query_prefers_exact_opening_in_multi_role_post(self) -> None:
        analysis = analyze_post_for_query(
            (
                "1. AI/ML Engineer Skills: Python, TensorFlow "
                "2. Java Developer Skills: Core Java, Spring Boot "
                "3. Python PySpark Developer Skills: Python, PySpark, Spark, SQL"
            ),
            "python pyspark developer",
        )
        self.assertFalse(analysis.hidden_from_frontend)
        self.assertEqual(analysis.matched_opening, "Python PySpark Developer")
        self.assertEqual(analysis.match_type, "exact")

    def test_analyze_post_for_query_hides_consultant_supply_posts(self) -> None:
        analysis = analyze_post_for_query(
            (
                "Dear Hiring Managers, I have highly skilled consultants available for immediate deployment. "
                "Hot Profiles: Python Developer, Java Developer. Share your current requirements."
            ),
            "python developer",
        )
        self.assertTrue(analysis.hidden_from_frontend)
        self.assertEqual(analysis.post_intent, "consultant_supply")


if __name__ == "__main__":
    unittest.main()

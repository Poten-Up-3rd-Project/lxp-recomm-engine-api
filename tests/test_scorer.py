import pandas as pd

from app.core.scorer import TfidfScorer


class TestTfidfScorer:
    def test_score_returns_expected_columns(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        scorer = TfidfScorer()
        result = scorer.score(sample_users, sample_courses)

        assert "user_id" in result.columns
        assert "course_id" in result.columns
        assert "score" in result.columns

    def test_score_values_between_0_and_1(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        scorer = TfidfScorer()
        result = scorer.score(sample_users, sample_courses)

        assert (result["score"] >= 0).all()
        assert (result["score"] <= 1).all()

    def test_score_only_positive_similarity(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        scorer = TfidfScorer()
        result = scorer.score(sample_users, sample_courses)

        assert (result["score"] > 0).all()

    def test_score_no_results_for_disjoint_tags(self):
        users = pd.DataFrame([{"id": "u1", "interest_tags": [100, 200], "level": 0,
                                "purchased_course_ids": [], "created_course_ids": []}])
        courses = pd.DataFrame([{"id": "c1", "tags": [300, 400], "level": 0}])

        scorer = TfidfScorer()
        result = scorer.score(users, courses)

        assert len(result) == 0

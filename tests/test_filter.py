import pandas as pd

from app.core.filter import ExclusionFilter


class TestExclusionFilter:
    def test_removes_purchased_courses(self, sample_users: pd.DataFrame):
        scores = pd.DataFrame([
            {"user_id": "user_001", "course_id": "course_001", "score": 0.9},
            {"user_id": "user_001", "course_id": "course_002", "score": 0.8},
        ])

        f = ExclusionFilter()
        result = f.apply(scores, sample_users)

        assert "course_001" not in result["course_id"].values
        assert "course_002" in result["course_id"].values

    def test_removes_created_courses(self, sample_users: pd.DataFrame):
        scores = pd.DataFrame([
            {"user_id": "user_002", "course_id": "course_003", "score": 0.7},
            {"user_id": "user_002", "course_id": "course_004", "score": 0.6},
        ])

        f = ExclusionFilter()
        result = f.apply(scores, sample_users)

        assert "course_003" not in result["course_id"].values
        assert "course_004" in result["course_id"].values

    def test_no_exclusion_for_clean_user(self, sample_users: pd.DataFrame):
        scores = pd.DataFrame([
            {"user_id": "user_003", "course_id": "course_001", "score": 0.5},
            {"user_id": "user_003", "course_id": "course_002", "score": 0.4},
        ])

        f = ExclusionFilter()
        result = f.apply(scores, sample_users)

        assert len(result) == 2

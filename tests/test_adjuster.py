import pandas as pd

from app.core.adjuster import LevelWeightAdjuster


class TestLevelWeightAdjuster:
    def test_no_penalty_for_same_level(self):
        scores = pd.DataFrame([{"user_id": "u1", "course_id": "c1", "score": 1.0}])
        users = pd.DataFrame([{"id": "u1", "level": 1}])
        courses = pd.DataFrame([{"id": "c1", "level": 1}])

        adjuster = LevelWeightAdjuster()
        result = adjuster.adjust(scores, users, courses)

        assert result.iloc[0]["score"] == 1.0

    def test_penalty_for_diff_1(self):
        scores = pd.DataFrame([{"user_id": "u1", "course_id": "c1", "score": 1.0}])
        users = pd.DataFrame([{"id": "u1", "level": 1}])
        courses = pd.DataFrame([{"id": "c1", "level": 2}])

        adjuster = LevelWeightAdjuster()
        result = adjuster.adjust(scores, users, courses)

        assert abs(result.iloc[0]["score"] - 0.85) < 1e-9

    def test_penalty_for_diff_3(self):
        scores = pd.DataFrame([{"user_id": "u1", "course_id": "c1", "score": 1.0}])
        users = pd.DataFrame([{"id": "u1", "level": 0}])
        courses = pd.DataFrame([{"id": "c1", "level": 3}])

        adjuster = LevelWeightAdjuster()
        result = adjuster.adjust(scores, users, courses)

        assert abs(result.iloc[0]["score"] - 0.15) < 1e-9

    def test_custom_penalty_weights(self):
        scores = pd.DataFrame([{"user_id": "u1", "course_id": "c1", "score": 1.0}])
        users = pd.DataFrame([{"id": "u1", "level": 0}])
        courses = pd.DataFrame([{"id": "c1", "level": 1}])

        adjuster = LevelWeightAdjuster(penalty_weights=[0.0, 0.30, 0.60, 0.90])
        result = adjuster.adjust(scores, users, courses)

        assert abs(result.iloc[0]["score"] - 0.70) < 1e-9

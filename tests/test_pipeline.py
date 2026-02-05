import pandas as pd

from app.core.adjuster import LevelWeightAdjuster
from app.core.filter import ExclusionFilter
from app.core.pipeline import RecommendationPipeline
from app.core.scorer import TfidfScorer


class TestRecommendationPipeline:
    def test_pipeline_returns_expected_columns(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
        )
        result = pipeline.run(sample_users, sample_courses, top_k=3)

        assert set(result.columns) == {"user_id", "course_id", "score", "rank"}

    def test_pipeline_respects_top_k(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
        )
        result = pipeline.run(sample_users, sample_courses, top_k=2)

        counts = result.groupby("user_id").size()
        assert (counts <= 2).all()

    def test_pipeline_excludes_purchased(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
        )
        result = pipeline.run(sample_users, sample_courses, top_k=5)

        user1_courses = result.loc[result["user_id"] == "user_001", "course_id"].values
        assert "course_001" not in user1_courses

    def test_pipeline_with_adjuster(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
            adjuster=LevelWeightAdjuster(),
        )
        result = pipeline.run(sample_users, sample_courses, top_k=3)

        assert not result.empty
        assert (result["score"] >= 0).all()

    def test_pipeline_rank_starts_from_1(self, sample_users: pd.DataFrame, sample_courses: pd.DataFrame):
        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
        )
        result = pipeline.run(sample_users, sample_courses, top_k=3)

        for _, group in result.groupby("user_id"):
            assert group["rank"].min() == 1

    def test_fallback_fills_missing_recommendations(self):
        users = pd.DataFrame([
            {"id": "u1", "interest_tags": [999], "level": 0,
             "purchased_course_ids": [], "created_course_ids": []},
        ])
        courses = pd.DataFrame([
            {"id": "c1", "tags": [1], "level": 0},
            {"id": "c2", "tags": [2], "level": 0},
            {"id": "c3", "tags": [3], "level": 0},
        ])

        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
        )
        result = pipeline.run(users, courses, top_k=3)

        user_recs = result[result["user_id"] == "u1"]
        assert len(user_recs) == 3

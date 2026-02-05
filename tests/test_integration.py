"""통합 테스트: /engine/process 엔드포인트부터 파이프라인 실행까지."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_parquet_files():
    """임시 디렉토리에 테스트용 Parquet 파일을 생성한다."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        users_df = pd.DataFrame([
            {"id": "u1", "interest_tags": [1, 2], "level": 1,
             "purchased_course_ids": [], "created_course_ids": []},
            {"id": "u2", "interest_tags": [2, 3], "level": 0,
             "purchased_course_ids": ["c1"], "created_course_ids": []},
        ])
        courses_df = pd.DataFrame([
            {"id": "c1", "tags": [1, 2], "level": 1},
            {"id": "c2", "tags": [2, 3], "level": 0},
            {"id": "c3", "tags": [1, 3], "level": 2},
        ])
        users_path = Path(tmp_dir) / "users.parquet"
        courses_path = Path(tmp_dir) / "courses.parquet"
        users_df.to_parquet(users_path, index=False)
        courses_df.to_parquet(courses_path, index=False)
        yield users_path, courses_path


class TestEngineProcessEndpoint:
    @patch("app.api.endpoints.engine.run_recommendation_process", new_callable=AsyncMock)
    def test_process_returns_202(self, mock_run, client):
        response = client.post("/engine/process", json={
            "batch_id": "test_batch_001",
            "users_file_path": "exports/users.parquet",
            "courses_file_path": "exports/courses.parquet",
        })

        assert response.status_code == 202
        body = response.json()
        assert body["batch_id"] == "test_batch_001"
        assert body["status"] == "ACCEPTED"

    def test_process_missing_required_fields(self, client):
        response = client.post("/engine/process", json={
            "batch_id": "test_batch_002",
        })

        assert response.status_code == 422


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        response = client.get("/health")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "UP"
        assert body["version"] == "0.1.0"


class TestFullPipelineIntegration:
    def test_full_pipeline_with_mock_data(self, mock_parquet_files):
        """StorageClient를 Mock하고 실제 파이프라인을 로컬 파일로 실행한다."""
        users_path, courses_path = mock_parquet_files

        mock_storage = MagicMock()
        mock_storage.download_file = MagicMock(side_effect=[users_path, courses_path])
        mock_storage.upload_file = MagicMock(return_value="results/recommendations.parquet")

        mock_callback = AsyncMock()

        from app.infra.loader import DatasetLoader

        loader = DatasetLoader()
        users_df = loader.load_users(users_path)
        courses_df = loader.load_courses(courses_path)

        from app.core.adjuster import LevelWeightAdjuster
        from app.core.filter import ExclusionFilter
        from app.core.pipeline import RecommendationPipeline
        from app.core.scorer import TfidfScorer

        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
            adjuster=LevelWeightAdjuster(),
        )
        result = pipeline.run(users_df, courses_df, top_k=2)

        assert not result.empty
        assert set(result.columns) == {"user_id", "course_id", "score", "rank"}

        # u2는 c1을 구매했으므로 추천에서 제외되어야 함
        u2_courses = result.loc[result["user_id"] == "u2", "course_id"].values
        assert "c1" not in u2_courses

        # 각 사용자당 최대 2개
        counts = result.groupby("user_id").size()
        assert (counts <= 2).all()

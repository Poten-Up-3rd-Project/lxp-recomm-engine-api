import logging
import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import JSONResponse

from app.config import settings
from app.core.adjuster import LevelWeightAdjuster
from app.core.filter import ExclusionFilter
from app.core.pipeline import RecommendationPipeline
from app.core.scorer import TfidfScorer
from app.infra.loader import DatasetLoader

router = APIRouter(prefix="/engine", tags=["engine-test"])
logger = logging.getLogger(__name__)


@router.post("/test")
async def test_pipeline(
    users_file: UploadFile = File(..., description="users parquet/csv 파일"),
    courses_file: UploadFile = File(..., description="courses parquet/csv 파일"),
    top_k: int = Query(default=settings.DEFAULT_TOP_K, description="사용자당 추천 개수"),
):
    """R2 없이 파일을 직접 업로드하여 추천 결과를 확인하는 테스트 엔드포인트."""
    loader = DatasetLoader()

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        users_path = tmp_path / users_file.filename
        users_path.write_bytes(await users_file.read())

        courses_path = tmp_path / courses_file.filename
        courses_path.write_bytes(await courses_file.read())

        users_df = loader.load_users(users_path)
        courses_df = loader.load_courses(courses_path)

        pipeline = RecommendationPipeline(
            scorer=TfidfScorer(),
            filter_=ExclusionFilter(),
            adjuster=LevelWeightAdjuster(settings.PENALTY_WEIGHTS),
        )
        result_df = pipeline.run(users_df, courses_df, top_k=top_k)

    recommendations = result_df.to_dict(orient="records")

    return JSONResponse(content={
        "total_users": int(result_df["user_id"].nunique()),
        "total_recommendations": len(recommendations),
        "top_k": top_k,
        "recommendations": recommendations,
    })

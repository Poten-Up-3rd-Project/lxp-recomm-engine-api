import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.config import settings
from app.core.adjuster import LevelWeightAdjuster
from app.core.filter import ExclusionFilter
from app.core.pipeline import RecommendationPipeline
from app.core.scorer import TfidfScorer
from app.infra.callback import CallbackClient
from app.infra.loader import DatasetLoader
from app.infra.storage import StorageClient
from app.schemas.request import ProcessRequest
from app.schemas.response import CallbackFailurePayload, CallbackSuccessPayload

logger = logging.getLogger(__name__)


async def run_recommendation_process(request: ProcessRequest) -> None:
    """추천 프로세스 전체를 실행한다: download → pipeline → upload → callback.

    Args:
        request: 추천 연산 요청 정보
    """
    batch_id = request.batch_id
    logger.info("[batch_id=%s] Process started", batch_id)

    storage = StorageClient(settings)
    loader = DatasetLoader()
    callback = CallbackClient(settings)

    try:
        # 1. R2에서 파일 다운로드
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            users_path = storage.download_file(request.users_file_path, tmp_path / "users.parquet")
            courses_path = storage.download_file(request.courses_file_path, tmp_path / "courses.parquet")

            # 2. DataFrame 로드
            users_df = loader.load_users(users_path)
            courses_df = loader.load_courses(courses_path)

            # 3. 파이프라인 실행
            pipeline = RecommendationPipeline(
                scorer=TfidfScorer(),
                filter_=ExclusionFilter(),
                adjuster=LevelWeightAdjuster(settings.PENALTY_WEIGHTS),
            )
            result_df = pipeline.run(users_df, courses_df, top_k=request.top_k)

            # 4. 결과 Parquet 저장 & 업로드
            result_path = tmp_path / "recommendations.parquet"
            result_df.to_parquet(result_path, index=False)

            today = datetime.utcnow().strftime("%Y/%m/%d")
            result_key = f"results/{today}/{batch_id}/recommendations.parquet"
            storage.upload_file(result_path, result_key)

        # 5. 성공 콜백
        if request.callback_url:
            payload = CallbackSuccessPayload(
                batch_id=batch_id,
                result_file_path=result_key,
                user_count=result_df["user_id"].nunique(),
            )
            await callback.send_success(request.callback_url, payload)

        logger.info("[batch_id=%s] Process completed successfully", batch_id)

    except Exception as e:
        logger.exception("[batch_id=%s] Process failed: %s", batch_id, e)
        if request.callback_url:
            error_code = type(e).__name__.upper()
            payload = CallbackFailurePayload(
                batch_id=batch_id,
                error_code=error_code,
                error_message=str(e),
            )
            try:
                await callback.send_failure(request.callback_url, payload)
            except Exception as cb_err:
                logger.error("[batch_id=%s] Callback also failed: %s", batch_id, cb_err)

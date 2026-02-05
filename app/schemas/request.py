from pydantic import BaseModel, Field

from app.config import settings


class ProcessRequest(BaseModel):
    """추천 연산 트리거 요청 모델."""

    batch_id: str = Field(..., description="배치 식별자 (멱등성 키)")
    users_file_path: str = Field(..., description="R2 내 사용자 데이터 경로")
    courses_file_path: str = Field(..., description="R2 내 강의 데이터 경로")
    top_k: int = Field(default=settings.DEFAULT_TOP_K, description="사용자당 추천 개수")
    callback_url: str | None = Field(default=None, description="완료 통보 URL")

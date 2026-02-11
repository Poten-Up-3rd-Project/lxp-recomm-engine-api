from datetime import datetime

from pydantic import BaseModel, Field


class ProcessResponse(BaseModel):
    """추천 연산 트리거 응답 모델 (202 Accepted)."""

    batch_id: str
    status: str = "ACCEPTED"
    message: str = "Processing started"


class HealthResponse(BaseModel):
    """헬스체크 응답 모델."""

    status: str = "UP"
    version: str = "0.1.0"
    uptime_seconds: float | None = None


class InfoResponse(BaseModel):
    """애플리케이션 정보 응답 모델 (Spring Actuator /info 대응)."""

    app: dict
    python: str
    start_time: str | None = None


class CallbackSuccessPayload(BaseModel):
    """연산 성공 시 Spring 콜백 페이로드."""

    batch_id: str
    status: str = "COMPLETED"
    result_file_path: str
    user_count: int
    processed_at: datetime = Field(default_factory=datetime.utcnow)


class CallbackFailurePayload(BaseModel):
    """연산 실패 시 Spring 콜백 페이로드."""

    batch_id: str
    status: str = "FAILED"
    error_code: str
    error_message: str
    failed_at: datetime = Field(default_factory=datetime.utcnow)

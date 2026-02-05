from fastapi import APIRouter

from app.schemas.response import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """헬스체크 엔드포인트."""
    return HealthResponse()

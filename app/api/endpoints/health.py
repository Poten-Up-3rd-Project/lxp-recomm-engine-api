import platform
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from app.schemas.response import HealthResponse, InfoResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """헬스체크 엔드포인트 (k8s liveness/readiness probe 대응)."""
    start_time: datetime | None = getattr(request.app.state, "start_time", None)
    uptime = None
    if start_time:
        uptime = (datetime.now(timezone.utc) - start_time).total_seconds()
    return HealthResponse(uptime_seconds=uptime)


@router.get("/info", response_model=InfoResponse)
async def app_info(request: Request) -> InfoResponse:
    """애플리케이션 메타 정보 엔드포인트 (Spring Actuator /info 대응)."""
    start_time: datetime | None = getattr(request.app.state, "start_time", None)
    return InfoResponse(
        app={
            "name": "lxp-recomm-engine",
            "version": "0.1.0",
            "description": "FastAPI Recommendation Engine for LXP",
        },
        python=platform.python_version(),
        start_time=start_time.isoformat() if start_time else None,
    )

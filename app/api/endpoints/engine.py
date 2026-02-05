import logging

from fastapi import APIRouter, BackgroundTasks, status

from app.schemas.request import ProcessRequest
from app.schemas.response import ProcessResponse
from app.services.process_service import run_recommendation_process

router = APIRouter(prefix="/engine", tags=["engine"])
logger = logging.getLogger(__name__)


@router.post(
    "/process",
    response_model=ProcessResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def process(
    request: ProcessRequest,
    background_tasks: BackgroundTasks,
) -> ProcessResponse:
    """추천 연산을 트리거한다. 즉시 202를 반환하고 백그라운드에서 처리한다."""
    logger.info("Received process request: batch_id=%s", request.batch_id)
    background_tasks.add_task(run_recommendation_process, request)
    return ProcessResponse(batch_id=request.batch_id)

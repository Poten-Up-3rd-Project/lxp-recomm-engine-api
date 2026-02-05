import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """R2 스토리지 관련 에러."""

    def __init__(self, message: str, batch_id: str = "") -> None:
        self.batch_id = batch_id
        super().__init__(message)


class ParsingError(Exception):
    """데이터 파싱 에러."""

    def __init__(self, message: str, batch_id: str = "") -> None:
        self.batch_id = batch_id
        super().__init__(message)


class ScoringError(Exception):
    """스코어링 연산 에러."""

    def __init__(self, message: str, batch_id: str = "") -> None:
        self.batch_id = batch_id
        super().__init__(message)


def register_exception_handlers(app: FastAPI) -> None:
    """FastAPI 앱에 커스텀 예외 핸들러를 등록한다."""

    @app.exception_handler(StorageError)
    async def storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
        logger.error("StorageError [batch_id=%s]: %s", exc.batch_id, exc)
        return JSONResponse(status_code=502, content={"error": "STORAGE_ERROR", "detail": str(exc)})

    @app.exception_handler(ParsingError)
    async def parsing_error_handler(request: Request, exc: ParsingError) -> JSONResponse:
        logger.error("ParsingError [batch_id=%s]: %s", exc.batch_id, exc)
        return JSONResponse(status_code=422, content={"error": "PARSING_ERROR", "detail": str(exc)})

    @app.exception_handler(ScoringError)
    async def scoring_error_handler(request: Request, exc: ScoringError) -> JSONResponse:
        logger.error("ScoringError [batch_id=%s]: %s", exc.batch_id, exc)
        return JSONResponse(status_code=500, content={"error": "SCORING_ERROR", "detail": str(exc)})

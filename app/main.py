from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import logging
import sys

from fastapi import FastAPI

from app.config import settings
from app.api.router import api_router
from app.exceptions.handlers import register_exception_handlers


class JsonFormatter(logging.Formatter):
    """구조화 JSON 로그 포매터."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "batch_id"):
            log_data["batch_id"] = record.batch_id
        if record.exc_info and record.exc_info[1]:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data, ensure_ascii=False)


def setup_logging() -> None:
    """구조화 JSON 로깅을 설정한다."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.LOG_LEVEL)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행되는 lifespan 이벤트."""
    setup_logging()
    logger = logging.getLogger(__name__)
    app.state.start_time = datetime.now(timezone.utc)
    logger.info("LXP-RecFlow engine starting up")
    yield
    logger.info("LXP-RecFlow engine shutting down")


app = FastAPI(
    title="LXP-RecFlow",
    version="0.1.0",
    description="FastAPI Recommendation Engine for LXP",
    lifespan=lifespan,
)

app.include_router(api_router)
register_exception_handlers(app)

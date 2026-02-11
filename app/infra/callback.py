import asyncio
import logging

import httpx

from app.config import Settings
from app.schemas.response import CallbackSuccessPayload, CallbackFailurePayload

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_SEC = 2


class CallbackClient:
    """Spring 콜백 호출 클라이언트."""

    def __init__(self, settings: Settings) -> None:
        self._timeout = settings.CALLBACK_TIMEOUT_SEC

    async def send_success(self, callback_url: str, payload: CallbackSuccessPayload) -> None:
        """성공 콜백을 전송한다."""
        await self._post(callback_url, payload.model_dump(mode="json"))

    async def send_failure(self, callback_url: str, payload: CallbackFailurePayload) -> None:
        """실패 콜백을 전송한다."""
        await self._post(callback_url, payload.model_dump(mode="json"))

    async def _post(self, url: str, data: dict) -> None:
        """HTTP POST 요청을 최대 3회 재시도하며 전송한다."""
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(url, json=data)
                    response.raise_for_status()
                    logger.info("Callback sent to %s, status=%d", url, response.status_code)
                    return
            except Exception as e:
                last_err = e
                logger.warning("Callback attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY_SEC)
        logger.error("Callback failed after %d retries: %s", MAX_RETRIES, last_err)
        raise last_err  # type: ignore[misc]

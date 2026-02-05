import logging
import time
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from app.config import Settings
from app.exceptions.handlers import StorageError

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_SEC = 2


class StorageClient:
    """Cloudflare R2(S3 호환) 업로드·다운로드 클라이언트."""

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.R2_BUCKET_NAME
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            config=BotoConfig(signature_version="s3v4"),
        )

    def download_file(self, key: str, local_path: Path) -> Path:
        """R2에서 파일을 다운로드한다.

        Args:
            key: R2 오브젝트 키
            local_path: 저장할 로컬 경로

        Returns:
            다운로드된 로컬 파일 경로
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading s3://%s/%s -> %s", self._bucket, key, local_path)
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._client.download_file(self._bucket, key, str(local_path))
                return local_path
            except Exception as e:
                last_err = e
                logger.warning("Download attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SEC)
        raise StorageError(f"Download failed after {MAX_RETRIES} retries: {last_err}")

    def upload_file(self, local_path: Path, key: str) -> str:
        """로컬 파일을 R2에 업로드한다.

        Args:
            local_path: 업로드할 로컬 파일 경로
            key: R2 오브젝트 키

        Returns:
            업로드된 오브젝트 키
        """
        logger.info("Uploading %s -> s3://%s/%s", local_path, self._bucket, key)
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._client.upload_file(str(local_path), self._bucket, key)
                return key
            except Exception as e:
                last_err = e
                logger.warning("Upload attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SEC)
        raise StorageError(f"Upload failed after {MAX_RETRIES} retries: {last_err}")

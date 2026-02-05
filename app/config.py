import json
from pydantic_settings import BaseSettings
from pydantic import field_validator


class Settings(BaseSettings):
    """애플리케이션 환경 설정."""

    # R2 / S3 설정
    R2_ENDPOINT_URL: str = ""
    R2_ACCESS_KEY_ID: str = ""
    R2_SECRET_ACCESS_KEY: str = ""
    R2_BUCKET_NAME: str = "lxp-recflow"

    # 추천 엔진 설정
    PENALTY_WEIGHTS: list[float] = [0.00, 0.15, 0.50, 0.85]
    DEFAULT_TOP_K: int = 10

    # 운영 설정
    LOG_LEVEL: str = "INFO"
    CALLBACK_TIMEOUT_SEC: int = 30

    @field_validator("PENALTY_WEIGHTS", mode="before")
    @classmethod
    def parse_penalty_weights(cls, v: str | list[float]) -> list[float]:
        """JSON 문자열로 전달된 penalty weights를 파싱한다."""
        if isinstance(v, str):
            return json.loads(v)
        return v

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()

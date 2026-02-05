import ast
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

USERS_REQUIRED_COLUMNS = {"id", "interest_tags", "level", "purchased_course_ids", "created_course_ids"}
COURSES_REQUIRED_COLUMNS = {"id", "tags", "level"}


class DatasetLoader:
    """Parquet/CSV 파일을 DataFrame으로 로드한다."""

    def load(self, file_path: Path) -> pd.DataFrame:
        """파일을 DataFrame으로 로드한다. Parquet 우선, 실패 시 CSV 폴백.

        Args:
            file_path: 로컬 파일 경로

        Returns:
            로드된 DataFrame

        Raises:
            FileNotFoundError: 파일이 존재하지 않을 때
            ValueError: 파일 파싱에 실패했을 때
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            df = pd.read_parquet(file_path)
            logger.info("Loaded parquet: %s (%d rows)", file_path, len(df))
            return df
        except Exception:
            logger.warning("Parquet load failed for %s, trying CSV fallback", file_path)

        try:
            df = pd.read_csv(file_path)
            logger.info("Loaded CSV: %s (%d rows)", file_path, len(df))
            return df
        except Exception as e:
            raise ValueError(f"Failed to load file {file_path}: {e}") from e

    def load_users(self, file_path: Path) -> pd.DataFrame:
        """사용자 데이터를 로드하고 컬럼을 검증한다."""
        df = self.load(file_path)
        missing = USERS_REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Users file missing columns: {missing}")
        list_cols = ["interest_tags", "purchased_course_ids", "created_course_ids"]
        df = self._parse_list_columns(df, list_cols)
        return df

    def load_courses(self, file_path: Path) -> pd.DataFrame:
        """강의 데이터를 로드하고 컬럼을 검증한다."""
        df = self.load(file_path)
        missing = COURSES_REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Courses file missing columns: {missing}")
        df = self._parse_list_columns(df, ["tags"])
        return df

    @staticmethod
    def _parse_list_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """CSV에서 문자열로 로드된 리스트 컬럼을 실제 list로 파싱한다."""
        for col in columns:
            if col not in df.columns:
                continue
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, str):
                df[col] = df[col].apply(
                    lambda v: ast.literal_eval(v) if isinstance(v, str) else (v if isinstance(v, list) else [])
                )
        return df

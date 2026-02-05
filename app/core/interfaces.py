from abc import ABC, abstractmethod

import pandas as pd


class BaseScorer(ABC):
    """사용자-강의 간 유사도 점수를 계산하는 인터페이스."""

    @abstractmethod
    def score(self, users: pd.DataFrame, courses: pd.DataFrame) -> pd.DataFrame:
        """유사도 점수를 계산한다.

        Args:
            users: 사용자 DataFrame (id, interest_tags, level, ...)
            courses: 강의 DataFrame (id, tags, level)

        Returns:
            DataFrame[user_id, course_id, score]
        """
        ...


class BaseFilter(ABC):
    """추천 후보에서 제거해야 할 항목을 필터링하는 인터페이스."""

    @abstractmethod
    def apply(self, scores: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
        """하드 필터를 적용한다.

        Args:
            scores: DataFrame[user_id, course_id, score]
            users: 사용자 DataFrame

        Returns:
            필터링된 DataFrame[user_id, course_id, score]
        """
        ...


class BaseAdjuster(ABC):
    """비즈니스 룰에 따라 점수를 보정하는 인터페이스."""

    @abstractmethod
    def adjust(
        self,
        scores: pd.DataFrame,
        users: pd.DataFrame,
        courses: pd.DataFrame,
    ) -> pd.DataFrame:
        """점수를 보정한다.

        Args:
            scores: DataFrame[user_id, course_id, score]
            users: 사용자 DataFrame
            courses: 강의 DataFrame

        Returns:
            보정된 DataFrame[user_id, course_id, score]
        """
        ...

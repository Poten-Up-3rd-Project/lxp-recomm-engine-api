import logging

import pandas as pd

from app.core.interfaces import BaseAdjuster

logger = logging.getLogger(__name__)

DEFAULT_PENALTY_WEIGHTS = [0.00, 0.15, 0.50, 0.85]


class LevelWeightAdjuster(BaseAdjuster):
    """레벨 차이 기반 점수 감점 보정기.

    사용자 레벨과 강의 난이도의 차이에 따라 점수를 감점한다.
    diff 0 → 0.00, diff 1 → 0.15, diff 2 → 0.50, diff 3 → 0.85
    adjusted_score = raw_score * (1.0 - penalty)
    """

    def __init__(self, penalty_weights: list[float] | None = None) -> None:
        self._penalty_weights = penalty_weights or DEFAULT_PENALTY_WEIGHTS

    def adjust(
        self,
        scores: pd.DataFrame,
        users: pd.DataFrame,
        courses: pd.DataFrame,
    ) -> pd.DataFrame:
        """레벨 차이에 따라 점수를 보정한다.

        Args:
            scores: DataFrame[user_id, course_id, score]
            users: DataFrame (id, level, ...)
            courses: DataFrame (id, level, ...)

        Returns:
            보정된 DataFrame[user_id, course_id, score]
        """
        user_levels = users[["id", "level"]].rename(columns={"id": "user_id", "level": "user_level"})
        course_levels = courses[["id", "level"]].rename(columns={"id": "course_id", "level": "course_level"})

        merged = scores.merge(user_levels, on="user_id").merge(course_levels, on="course_id")
        merged["level_diff"] = (merged["user_level"] - merged["course_level"]).abs()

        max_diff = len(self._penalty_weights) - 1
        merged["level_diff"] = merged["level_diff"].clip(upper=max_diff)
        merged["penalty"] = merged["level_diff"].map(
            {i: w for i, w in enumerate(self._penalty_weights)}
        )
        merged["score"] = merged["score"] * (1.0 - merged["penalty"])

        adjusted = merged[["user_id", "course_id", "score"]]
        logger.info("LevelWeightAdjuster applied: %d pairs adjusted", len(adjusted))
        return adjusted.reset_index(drop=True)

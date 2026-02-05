import logging

import pandas as pd

from app.core.interfaces import BaseFilter

logger = logging.getLogger(__name__)


class ExclusionFilter(BaseFilter):
    """이미 구매했거나 본인이 만든 강의를 추천 후보에서 제거한다."""

    def apply(self, scores: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
        """purchased_course_ids + created_course_ids에 해당하는 항목을 제거한다.

        Args:
            scores: DataFrame[user_id, course_id, score]
            users: DataFrame (id, purchased_course_ids, created_course_ids)

        Returns:
            필터링된 DataFrame[user_id, course_id, score]
        """
        exclusion_rows = []
        for _, user in users.iterrows():
            user_id = user["id"]
            purchased = user.get("purchased_course_ids")
            if purchased is None or (isinstance(purchased, float) and pd.isna(purchased)):
                purchased = []
            purchased = list(purchased)

            created = user.get("created_course_ids")
            if created is None or (isinstance(created, float) and pd.isna(created)):
                created = []
            created = list(created)
            excluded = set(purchased) | set(created)
            for course_id in excluded:
                exclusion_rows.append({"user_id": user_id, "course_id": course_id})

        if not exclusion_rows:
            return scores

        exclusion_df = pd.DataFrame(exclusion_rows)
        merged = scores.merge(exclusion_df, on=["user_id", "course_id"], how="left", indicator=True)
        filtered = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        removed_count = len(scores) - len(filtered)
        logger.info("ExclusionFilter removed %d pairs", removed_count)
        return filtered.reset_index(drop=True)

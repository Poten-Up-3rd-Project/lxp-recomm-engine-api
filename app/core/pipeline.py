import gc
import logging

import pandas as pd

from app.core.interfaces import BaseScorer, BaseFilter, BaseAdjuster

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000


class RecommendationPipeline:
    """추천 파이프라인 오케스트레이터.

    Scorer → Filter → Adjuster → Rank & Top-K 순서로 실행한다.
    """

    def __init__(
        self,
        scorer: BaseScorer,
        filter_: BaseFilter,
        adjuster: BaseAdjuster | None = None,
    ) -> None:
        self._scorer = scorer
        self._filter = filter_
        self._adjuster = adjuster

    def run(
        self,
        users: pd.DataFrame,
        courses: pd.DataFrame,
        top_k: int = 10,
    ) -> pd.DataFrame:
        """추천 파이프라인을 실행한다.

        Args:
            users: 사용자 DataFrame
            courses: 강의 DataFrame
            top_k: 사용자당 추천 개수

        Returns:
            DataFrame[user_id, course_id, score, rank]
        """
        logger.info("Pipeline started: %d users, %d courses, top_k=%d", len(users), len(courses), top_k)

        if len(users) > CHUNK_SIZE:
            result = self._run_chunked(users, courses, top_k)
        else:
            result = self._run_single(users, courses, top_k)

        # Fallback: top_k 미만인 사용자에게 인기 강의로 채움
        result = self._apply_fallback(result, users, courses, top_k)

        logger.info("Pipeline complete: %d recommendations for %d users",
                     len(result), result["user_id"].nunique())
        return result[["user_id", "course_id", "score", "rank"]]

    def _run_single(
        self,
        users: pd.DataFrame,
        courses: pd.DataFrame,
        top_k: int,
    ) -> pd.DataFrame:
        """단일 배치로 파이프라인을 실행한다."""
        scores = self._scorer.score(users, courses)
        logger.info("Scoring complete: %d pairs", len(scores))

        scores = self._filter.apply(scores, users)
        logger.info("Filtering complete: %d pairs remaining", len(scores))

        if self._adjuster is not None:
            scores = self._adjuster.adjust(scores, users, courses)
            logger.info("Adjustment complete")

        scores = scores.sort_values(["user_id", "score"], ascending=[True, False])
        scores["rank"] = scores.groupby("user_id").cumcount() + 1
        return scores[scores["rank"] <= top_k].reset_index(drop=True)

    def _run_chunked(
        self,
        users: pd.DataFrame,
        courses: pd.DataFrame,
        top_k: int,
    ) -> pd.DataFrame:
        """사용자를 청크 단위로 분할하여 파이프라인을 실행한다."""
        num_chunks = (len(users) + CHUNK_SIZE - 1) // CHUNK_SIZE
        logger.info("Chunked processing: %d users split into %d chunks", len(users), num_chunks)

        chunks = []
        for i in range(0, len(users), CHUNK_SIZE):
            user_chunk = users.iloc[i:i + CHUNK_SIZE]
            chunk_result = self._run_single(user_chunk, courses, top_k)
            chunks.append(chunk_result)

            del chunk_result
            gc.collect()
            logger.info("Chunk %d/%d processed", (i // CHUNK_SIZE) + 1, num_chunks)

        return pd.concat(chunks, ignore_index=True)

    def _apply_fallback(
        self,
        result: pd.DataFrame,
        users: pd.DataFrame,
        courses: pd.DataFrame,
        top_k: int,
    ) -> pd.DataFrame:
        """추천이 top_k 미만인 사용자에게 인기 강의(구매 빈도 기반)로 채운다."""
        all_user_ids = set(users["id"])
        users_with_recs = set(result["user_id"]) if not result.empty else set()
        user_rec_counts = result.groupby("user_id").size().to_dict() if not result.empty else {}

        users_needing_fallback = {
            uid for uid in all_user_ids
            if user_rec_counts.get(uid, 0) < top_k
        }

        if not users_needing_fallback:
            return result

        # 인기 강의 목록: 전체 사용자의 purchased_course_ids 빈도순
        all_purchased = users["purchased_course_ids"].dropna().explode()
        if all_purchased.empty:
            popular_courses = courses["id"].tolist()
        else:
            popular_courses = all_purchased.value_counts().index.tolist()
            remaining = [c for c in courses["id"] if c not in set(popular_courses)]
            popular_courses.extend(remaining)

        fallback_rows = []
        for uid in users_needing_fallback:
            current_count = user_rec_counts.get(uid, 0)
            need = top_k - current_count

            existing_courses = set(
                result.loc[result["user_id"] == uid, "course_id"]
            ) if current_count > 0 else set()

            user_row = users.loc[users["id"] == uid].iloc[0]
            purchased = user_row.get("purchased_course_ids")
            purchased = list(purchased) if purchased is not None and not (isinstance(purchased, float) and pd.isna(purchased)) else []
            created = user_row.get("created_course_ids")
            created = list(created) if created is not None and not (isinstance(created, float) and pd.isna(created)) else []
            excluded = set(purchased) | set(created)

            added = 0
            for course_id in popular_courses:
                if added >= need:
                    break
                if course_id in existing_courses or course_id in excluded:
                    continue
                fallback_rows.append({
                    "user_id": uid,
                    "course_id": course_id,
                    "score": 0.0,
                    "rank": current_count + added + 1,
                })
                added += 1

        if fallback_rows:
            fallback_df = pd.DataFrame(fallback_rows)
            result = pd.concat([result, fallback_df], ignore_index=True)
            logger.info("Fallback applied: %d rows added for %d users",
                         len(fallback_rows), len(users_needing_fallback))

        return result

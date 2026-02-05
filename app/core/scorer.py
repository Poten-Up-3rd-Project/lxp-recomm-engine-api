import logging

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from app.core.interfaces import BaseScorer

logger = logging.getLogger(__name__)


class TfidfScorer(BaseScorer):
    """TF-IDF 코사인 유사도 기반 스코어러.

    사용자의 interest_tags와 강의의 tags를 TF-IDF 벡터로 변환한 후
    코사인 유사도를 계산하여 추천 점수를 산출한다.
    """

    def score(self, users: pd.DataFrame, courses: pd.DataFrame) -> pd.DataFrame:
        """사용자-강의 간 TF-IDF 코사인 유사도 점수를 계산한다.

        Args:
            users: DataFrame (id, interest_tags, ...)
            courses: DataFrame (id, tags, ...)

        Returns:
            DataFrame[user_id, course_id, score]
        """
        user_docs = users["interest_tags"].apply(self._tags_to_text)
        course_docs = courses["tags"].apply(self._tags_to_text)

        all_docs = pd.concat([user_docs, course_docs], ignore_index=True)
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(all_docs)

        user_vectors = tfidf_matrix[: len(users)]
        course_vectors = tfidf_matrix[len(users) :]

        sim_matrix = cosine_similarity(user_vectors, course_vectors)

        user_ids = users["id"].values
        course_ids = courses["id"].values

        user_idx, course_idx = np.where(sim_matrix > 0)
        scores = sim_matrix[user_idx, course_idx]

        result = pd.DataFrame({
            "user_id": user_ids[user_idx],
            "course_id": course_ids[course_idx],
            "score": scores,
        })

        logger.info("TF-IDF scoring complete: %d user-course pairs", len(result))
        return result

    @staticmethod
    def _tags_to_text(tags: list[int]) -> str:
        """태그 ID 리스트를 공백 구분 문자열로 변환한다."""
        if tags is None or (isinstance(tags, float) and np.isnan(tags)):
            return ""
        return " ".join(f"tag_{t}" for t in tags)

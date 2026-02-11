import pytest
import pandas as pd


@pytest.fixture
def sample_users() -> pd.DataFrame:
    """테스트용 사용자 데이터."""
    return pd.DataFrame([
        {
            "id": "user_001",
            "interest_tags": [1, 2, 3],
            "level": 1,
            "purchased_course_ids": ["course_001"],
            "created_course_ids": [],
        },
        {
            "id": "user_002",
            "interest_tags": [3, 4, 5],
            "level": 2,
            "purchased_course_ids": [],
            "created_course_ids": ["course_003"],
        },
        {
            "id": "user_003",
            "interest_tags": [1, 5],
            "level": 0,
            "purchased_course_ids": [],
            "created_course_ids": [],
        },
    ])


@pytest.fixture
def sample_courses() -> pd.DataFrame:
    """테스트용 강의 데이터."""
    return pd.DataFrame([
        {"id": "course_001", "tags": [1, 2], "level": 1},
        {"id": "course_002", "tags": [2, 3], "level": 2},
        {"id": "course_003", "tags": [4, 5], "level": 0},
        {"id": "course_004", "tags": [1, 3, 5], "level": 1},
        {"id": "course_005", "tags": [6, 7], "level": 3},
    ])

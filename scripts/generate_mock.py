"""테스트용 Mock Parquet 데이터를 생성하는 스크립트."""

import random
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

NUM_USERS = 50
NUM_COURSES = 100
NUM_TAGS = 20


def generate_users(n: int) -> pd.DataFrame:
    """Mock 사용자 데이터를 생성한다."""
    rows = []
    for i in range(n):
        num_tags = random.randint(1, 5)
        num_purchased = random.randint(0, 3)
        num_created = random.randint(0, 1)

        rows.append({
            "id": f"user_{i:04d}",
            "interest_tags": random.sample(range(1, NUM_TAGS + 1), num_tags),
            "level": random.randint(0, 3),
            "purchased_course_ids": [f"course_{random.randint(0, NUM_COURSES - 1):04d}" for _ in range(num_purchased)],
            "created_course_ids": [f"course_{random.randint(0, NUM_COURSES - 1):04d}" for _ in range(num_created)],
        })
    return pd.DataFrame(rows)


def generate_courses(n: int) -> pd.DataFrame:
    """Mock 강의 데이터를 생성한다."""
    rows = []
    for i in range(n):
        num_tags = random.randint(1, 5)
        rows.append({
            "id": f"course_{i:04d}",
            "tags": random.sample(range(1, NUM_TAGS + 1), num_tags),
            "level": random.randint(0, 3),
        })
    return pd.DataFrame(rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    random.seed(42)

    users_df = generate_users(NUM_USERS)
    courses_df = generate_courses(NUM_COURSES)

    users_path = OUTPUT_DIR / "users.parquet"
    courses_path = OUTPUT_DIR / "courses.parquet"

    users_df.to_parquet(users_path, index=False)
    courses_df.to_parquet(courses_path, index=False)

    print(f"Generated {len(users_df)} users -> {users_path}")
    print(f"Generated {len(courses_df)} courses -> {courses_path}")


if __name__ == "__main__":
    main()

"""대규모 테스트용 Mock Parquet 데이터를 생성하는 스크립트.

사용법:
    python scripts/generate_large_mock.py --users 1000 --courses 500
    python scripts/generate_large_mock.py --users 5000 --courses 2000
"""

import argparse
import random
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).parent.parent / "test_data"
NUM_TAGS = 50


def generate_users(n: int, num_courses: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        num_tags = random.randint(1, 8)
        num_purchased = random.randint(0, 5)
        num_created = random.randint(0, 2)

        rows.append({
            "id": f"user_{i:06d}",
            "interest_tags": random.sample(range(1, NUM_TAGS + 1), num_tags),
            "level": random.randint(0, 3),
            "purchased_course_ids": [
                f"course_{random.randint(0, num_courses - 1):06d}" for _ in range(num_purchased)
            ],
            "created_course_ids": [
                f"course_{random.randint(0, num_courses - 1):06d}" for _ in range(num_created)
            ],
        })
    return pd.DataFrame(rows)


def generate_courses(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        num_tags = random.randint(1, 8)
        rows.append({
            "id": f"course_{i:06d}",
            "tags": random.sample(range(1, NUM_TAGS + 1), num_tags),
            "level": random.randint(0, 3),
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--courses", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    users_df = generate_users(args.users, args.courses)
    courses_df = generate_courses(args.courses)

    users_path = OUTPUT_DIR / "users.parquet"
    courses_path = OUTPUT_DIR / "courses.parquet"

    users_df.to_parquet(users_path, index=False)
    courses_df.to_parquet(courses_path, index=False)

    print(f"Generated {len(users_df)} users -> {users_path}")
    print(f"Generated {len(courses_df)} courses -> {courses_path}")


if __name__ == "__main__":
    main()

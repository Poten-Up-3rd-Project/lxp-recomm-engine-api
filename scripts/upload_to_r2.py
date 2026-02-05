"""로컬 CSV/Parquet 파일을 R2에 업로드하는 스크립트.

사용법:
    python scripts/upload_to_r2.py
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from app.config import settings
from app.infra.loader import DatasetLoader
from app.infra.storage import StorageClient


def main() -> None:
    project_root = Path(__file__).parent.parent

    # 1. CSV 로드 → Parquet 변환
    loader = DatasetLoader()
    users_df = loader.load_users(project_root / "users_small.csv")
    courses_df = loader.load_courses(project_root / "courses_small.csv")

    # 임시 Parquet 저장
    tmp_dir = project_root / "test_data"
    tmp_dir.mkdir(exist_ok=True)

    users_parquet = tmp_dir / "users.parquet"
    courses_parquet = tmp_dir / "courses.parquet"
    users_df.to_parquet(users_parquet, index=False)
    courses_df.to_parquet(courses_parquet, index=False)

    print(f"Parquet 변환 완료: {len(users_df)} users, {len(courses_df)} courses")

    # 2. R2 업로드
    storage = StorageClient(settings)
    today = datetime.utcnow().strftime("%Y/%m/%d")

    users_key = f"exports/{today}/users.parquet"
    courses_key = f"exports/{today}/courses.parquet"

    storage.upload_file(users_parquet, users_key)
    storage.upload_file(courses_parquet, courses_key)

    print(f"\nR2 업로드 완료:")
    print(f"  bucket: {settings.R2_BUCKET_NAME}")
    print(f"  users:   {users_key}")
    print(f"  courses: {courses_key}")
    print(f"\nAPI 호출 예시:")
    print(f'  curl -X POST http://localhost:8000/engine/process \\')
    print(f'    -H "Content-Type: application/json" \\')
    print(f'    -d \'{{"batch_id": "test_001", "users_file_path": "{users_key}", "courses_file_path": "{courses_key}", "top_k": 5}}\'')


if __name__ == "__main__":
    main()

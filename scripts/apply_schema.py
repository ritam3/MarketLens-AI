"""Apply the project schema.sql to the configured PostgreSQL database."""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from app.data.db.session import DATABASE_URL  # noqa: E402

SCHEMA_PATH = PROJECT_ROOT / "app" / "data" / "db" / "schema.sql"


def main() -> None:
    schema_sql = SCHEMA_PATH.read_text()
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

    with engine.begin() as connection:
        connection.execute(text(schema_sql))

    print(f"Applied schema from {SCHEMA_PATH}")


if __name__ == "__main__":
    main()

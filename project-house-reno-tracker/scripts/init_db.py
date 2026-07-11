"""
Idempotent schema bootstrap. Run once against the shared de_portfolio database
before starting the API for the first time.

Usage:
    cd project-house-reno-tracker
    python scripts/init_db.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.ext.asyncio import create_async_engine

from src.config import load_config
from src.models.orm import Base


async def main() -> None:
    cfg = load_config()
    engine = create_async_engine(cfg.postgres.async_url, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("Schema bootstrap complete.")


if __name__ == "__main__":
    asyncio.run(main())

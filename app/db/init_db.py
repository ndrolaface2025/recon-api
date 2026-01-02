"""Database initialization helpers.

This module provides lightweight async stubs used at app startup.
They are intentionally minimal: they do not run migrations automatically.
Running migrations should be done via Alembic CLI (`alembic upgrade head`).
"""
from typing import Any
import logging
from app.services.seed_service import SeedService


async def init_db(session: Any) -> None:
    """Placeholder for DB initialization logic.

    Called on app startup. Keep this lightweight — migrations should
    be applied separately with Alembic. You can implement schema
    checks or simple sanity queries here.
    """
    # Example: you could run a simple SELECT to ensure connectivity.
    try:
        await session.execute("SELECT 1")
    except Exception:
        # swallow exceptions here; caller may want to handle startup failures
        pass


async def seed_initial_data(session: Any) -> None:
    """Run idempotent seed routines using async repository APIs.

    This will create basic source rows if they do not exist.
    Keep this fast and idempotent so repeated app restarts are safe.
    """
    logger = logging.getLogger("app.init_db")
    try:
        seed_service = SeedService(session)
        result = await seed_service.seed_sources()
        logger.info("Seeded sources: inserted=%s skipped=%s", result["inserted"], result["skipped"])
    except Exception as exc:
        logger.exception("Seeding initial data failed: %s", exc)

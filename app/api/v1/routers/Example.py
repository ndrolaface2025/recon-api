from fastapi import APIRouter, HTTPException, Depends
import time
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import asyncio

from app.config import settings
from app.services.seed_service import SeedService
from app.services.services import get_service
from app.flexcube_db.engine import flexcube_engine

try:
    import asyncpg
except Exception:
    asyncpg = None


router = APIRouter(prefix="/database", tags=["Database"])


@router.get("/")
async def checkDbConnection():
    """Perform a quick DB connectivity check using asyncpg and return timing/result."""
    if asyncpg is None:
        raise HTTPException(status_code=500, detail="asyncpg not installed")

    dsn = settings.DATABASE_URL
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    if dsn.startswith("postgresql+asyncpg://"):
        dsn_for_connect = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    else:
        dsn_for_connect = dsn

    from urllib.parse import urlparse

    parsed = urlparse(dsn_for_connect)
    user = parsed.username
    password = parsed.password
    # host = parsed.hostname or 'localhost'
    # port = parsed.port or 5432
    host = "localhost"
    port = 5432
    database = parsed.path.lstrip("/")

    start = time.time()
    try:
        conn = await asyncio.wait_for(
            asyncpg.connect(
                user=user, password=password, database=database, host=host, port=port
            ),
            timeout=10,
        )
        await asyncio.wait_for(conn.execute("SELECT 1"), timeout=5)
        await conn.close()
        elapsed = time.time() - start
        return {"ok": True, "time_s": round(elapsed, 3)}
    except Exception as e:
        elapsed = time.time() - start
        raise HTTPException(
            status_code=503,
            detail={"ok": False, "error": str(e), "time_s": round(elapsed, 3)},
        )


# -------------------------------------------------------------------
# Flexcube DB health check (Oracle via SQLAlchemy ORM)
# -------------------------------------------------------------------
@router.get("/flexcube")
def check_flexcube_db_connection():
    """
    Flexcube DB connectivity check.
    """
    start = time.time()

    try:
        with flexcube_engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM DUAL"))

        return {
            "ok": True,
            "db": "flexcube",
            "time_s": round(time.time() - start, 3),
        }

    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "ok": False,
                "db": "flexcube",
                "error": str(exc.__cause__ or exc),
                "time_s": round(time.time() - start, 3),
            },
        )


@router.get("/seed")
async def seed_database(service: SeedService = Depends(get_service(SeedService))):
    # SeedService.seed_sources is async; await it and return the result
    return await service.seed_sources()
    # return {"message": "Source configuration seeded successfully","count":count}

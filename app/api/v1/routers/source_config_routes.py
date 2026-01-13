from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.source_config_service import SourceConfigService

router = APIRouter(prefix="/api/v1", tags=["Source Config"])


@router.post("/source-config/temp")
async def seed_source_config(
    db: AsyncSession = Depends(get_db),
):
    return await SourceConfigService.create_default_sources(db)

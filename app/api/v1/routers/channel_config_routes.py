from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.channel_config_service import ChannelConfigService

router = APIRouter(prefix="/api/v1")

@router.post("/channel-config")
async def create_channel_config(
    db: AsyncSession = Depends(get_db),
):
    return await ChannelConfigService.create_channel_config(db)

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.user_config_service import UserConfigService

router = APIRouter(prefix="/api/v1", tags=["User Config"])


@router.post("/user-config")
async def create_user_config(
    db: AsyncSession = Depends(get_db),
):
    return await UserConfigService.create_user_config(db)

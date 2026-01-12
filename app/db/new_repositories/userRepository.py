from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.user_config import UserConfig

class UserRepository:

    @staticmethod
    async def getUserDetails(db: AsyncSession):
        stmt = select(UserConfig.id).limit(1)
        result = await db.execute(stmt)
        record = result.scalars().first()
        if not record:
            return None
        return record
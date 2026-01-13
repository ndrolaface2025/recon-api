from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert
from app.db.models.user_config import UserConfig


class UserConfigService:

    @staticmethod
    async def create_user_config(db: AsyncSession) -> UserConfig:
        stmt = insert(UserConfig).values(
            f_name="Ackim",
            m_name=None,
            l_name="Chissa",
            gender=True,
            phone="",
            birth_date=None,
            email="Ackim@example.com",
            username="ackim",
            role=None,              
            status=True,
            created_by=None,        
            updated_by=None,
            version_number=1
        ).returning(UserConfig)

        result = await db.execute(stmt)
        await db.commit()

        return result.scalar_one()

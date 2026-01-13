from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert
from app.db.models.source_config import SourceConfig

class SourceConfigService:

    @staticmethod
    async def create_default_sources(db: AsyncSession):
        stmt = insert(SourceConfig).values([
            {
                "source_name": "ATM",
                "source_type": 1,
                "status": 1,
                "version_number": 1
            },
            {
                "source_name": "SWITCH",
                "source_type": 2,
                "status": 1,
                "version_number": 1
            },
            {
                "source_name": "CBS",
                "source_type": 3,
                "status": 1,
                "version_number": 1
            }
        ]).returning(SourceConfig)

        result = await db.execute(stmt)
        await db.commit()

        return result.scalars().all()
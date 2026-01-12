from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.source_config import SourceConfig


class SeedRepository:

    @staticmethod
    async def get_by_source_name(db: AsyncSession, source_type: int) -> Optional[SourceConfig]:
        stmt = select(SourceConfig).where(SourceConfig.source_type == source_type)
        result = await db.execute(stmt)
        return result.scalars().first()

    @staticmethod
    async def create(db: AsyncSession, data: dict) -> SourceConfig:
        source = SourceConfig(**data)
        db.add(source)
        await db.commit()
        await db.refresh(source)
        return source


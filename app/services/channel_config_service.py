from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert
from app.db.models.channel_config import ChannelConfig
from app.db.models.user_config import UserConfig
from app.db.models.source_config import SourceConfig
from sqlalchemy import select

class ChannelConfigService:

    @staticmethod
    async def create_channel_config(db: AsyncSession) -> ChannelConfig:
        result = await db.execute(
            select(UserConfig.id).where(UserConfig.username == "ackim")
        )
        user_id = result.scalar_one()

        result = await db.execute(
            select(SourceConfig.source_type, SourceConfig.id)
            .where(SourceConfig.status == 1)
        )

        sources = result.all()
        source_map = {source_type: id for source_type, id in sources}

        stmt = insert(ChannelConfig).values(
            channel_name="ATM",
            channel_description="Temp",
            channel_source_id = source_map.get(1),
            switch_source_id  = source_map.get(2),
            cbs_source_id     = source_map.get(3),
            network_source_id = source_map.get(4),
            status=True,
            created_by=user_id,
            updated_by=user_id,
            version_number=1
        ).returning(ChannelConfig)


        result = await db.execute(stmt)
        await db.commit()

        return result.scalar_one()

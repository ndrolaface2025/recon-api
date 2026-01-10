from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from app.db.models.transactions import Transaction

class ChannelSourceRepository:

    @staticmethod
    async def getChannelList(db: AsyncSession):
        try:
            # ✅ Total count
            total_stmt = select(func.count()).select_from(ChannelConfig)
            total_result = await db.execute(total_stmt)
            total = total_result.scalar() or 0

            # ✅ Channel list query
            stmt = (
                select(ChannelConfig)
                .order_by(ChannelConfig.created_at.desc())
            )

            result = await db.execute(stmt)
            channels = result.scalars().all()

            # ✅ Response data
            data = [
                {
                    "id": channel.id,
                    "channel_name": channel.channel_name,
                    "channel_description": channel.channel_description,
                    "channel_source_id": channel.channel_source_id,
                    "network_source_id": channel.network_source_id,
                    "cbs_source_id": channel.cbs_source_id,
                    "switch_source_id": channel.switch_source_id,
                    "status": channel.status,
                    "created_at": channel.created_at,
                    "version_number": channel.version_number,
                }
                for channel in channels
            ]

            return {
                "status": "success",
                "total": total,
                "count": len(data),
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("ChannelRepository.getChannelList error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch channel list",
                "error": str(e),
            }
        
    
    @staticmethod
    async def getSourceListByChannelId(db: AsyncSession, channel_id: int):
        try:

            column_names = [
                column.name
                for column in Transaction.__table__.columns
                if column.name in {
                    "reference_number",
                    "amount",
                    "date",
                    "account_number",
                    "ccy",
                }
            ]
            # 1️⃣ Fetch channel
            stmt = select(ChannelConfig).where(ChannelConfig.id == channel_id)
            result = await db.execute(stmt)
            channel = result.scalar_one_or_none()

            if not channel:
                return {
                    "status": "error",
                    "message": "Channel not found",
                }

            # 2️⃣ Collect source IDs (ignore None)
            source_ids = list(
                filter(
                    None,
                    [
                        channel.channel_source_id,
                        channel.network_source_id,
                        channel.cbs_source_id,
                        channel.switch_source_id,
                    ],
                )
            )

            if not source_ids:
                return {
                    "status": "success",
                    "data": [],
                }

            # 3️⃣ Fetch sources from SourceConfig
            source_stmt = select(SourceConfig).where(
                SourceConfig.id.in_(source_ids)
            )
            source_result = await db.execute(source_stmt)
            sources = source_result.scalars().all()

            # 4️⃣ Build response
            data = [
                {
                    "id": source.id,
                    "source_name": source.source_name,
                    "source_type": source.source_type,
                    "source_json": column_names,
                    "status": source.status,
                    "created_at": source.created_at,
                    "version_number": source.version_number,
                }
                for source in sources
            ]

            return {
                "status": "success",
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("ChannelRepository.getSourceListByChannelId error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch source list",
                "error": str(e),
            }

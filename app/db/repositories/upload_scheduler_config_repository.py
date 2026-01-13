from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.upload_schedulars_config import UploadSchedulerConfig


class UploadSchedulerConfigRepository:

    @staticmethod
    async def getSchedulerList(db: AsyncSession):
        try:
            total_stmt = select(func.count()).select_from(UploadSchedulerConfig)
            total_result = await db.execute(total_stmt)
            total = total_result.scalar() or 0

            stmt = (
                select(UploadSchedulerConfig)
                .order_by(UploadSchedulerConfig.created_at.desc())
            )

            result = await db.execute(stmt)
            schedulers = result.scalars().all()

            data = [
                {
                    "id": s.id,
                    "channel_id": s.channel_id,
                    "schedular_name": s.schedular_name,
                    "schedular_time": s.schedular_time,
                    "created_at": s.created_at,
                    "version_number": s.version_number,
                }
                for s in schedulers
            ]

            return {
                "status": "success",
                "total": total,
                "count": len(data),
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("UploadSchedulerConfigRepository.getSchedulerList error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch scheduler list",
                "error": str(e),
            }

    @staticmethod
    async def getSchedulerById(db: AsyncSession, scheduler_id: int):
        try:
            stmt = select(UploadSchedulerConfig).where(
                UploadSchedulerConfig.id == scheduler_id
            )
            result = await db.execute(stmt)
            scheduler = result.scalar_one_or_none()

            if not scheduler:
                return {
                    "status": "error",
                    "message": "Scheduler not found",
                }

            data = {
                "id": scheduler.id,
                "channel_id": scheduler.channel_id,
                "schedular_name": scheduler.schedular_name,
                "schedular_time": scheduler.schedular_time,
                "created_at": scheduler.created_at,
                "version_number": scheduler.version_number,
            }

            return {
                "status": "success",
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("UploadSchedulerConfigRepository.getSchedulerById error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch scheduler",
                "error": str(e),
            }

    @staticmethod
    async def getSchedulerByChannelId(db: AsyncSession, channel_id: int):
        try:
            stmt = (
                select(UploadSchedulerConfig)
                .where(UploadSchedulerConfig.channel_id == channel_id)
                .order_by(UploadSchedulerConfig.created_at.desc())
            )

            result = await db.execute(stmt)
            schedulers = result.scalars().all()

            data = [
                {
                    "id": s.id,
                    "schedular_name": s.schedular_name,
                    "schedular_time": s.schedular_time,
                    "created_at": s.created_at,
                    "version_number": s.version_number,
                }
                for s in schedulers
            ]

            return {
                "status": "success",
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print(
                "UploadSchedulerConfigRepository.getSchedulerByChannelId error:",
                str(e),
            )
            return {
                "status": "error",
                "message": "Failed to fetch scheduler by channel",
                "error": str(e),
            }

    @staticmethod
    async def createScheduler(db: AsyncSession, payload: dict):
        try:
            scheduler = UploadSchedulerConfig(**payload)
            db.add(scheduler)
            await db.commit()
            await db.refresh(scheduler)

            return {
                "status": "success",
                "data": {
                    "id": scheduler.id,
                    "channel_id": scheduler.channel_id,
                    "schedular_name": scheduler.schedular_name,
                    "schedular_time": scheduler.schedular_time,
                    "created_at": scheduler.created_at,
                    "version_number": scheduler.version_number,
                },
            }

        except Exception as e:
            await db.rollback()
            print("UploadSchedulerConfigRepository.createScheduler error:", str(e))
            return {
                "status": "error",
                "message": "Failed to create scheduler",
                "error": str(e),
            }

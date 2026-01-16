from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.upload_scheduler_config import UploadSchedulerConfig


class UploadSchedulerConfigRepository:

    @staticmethod
    async def create(db: AsyncSession, payload: dict):
        try:
            scheduler = UploadSchedulerConfig(**payload)
            db.add(scheduler)
            await db.commit()
            await db.refresh(scheduler)

            return {
                "status": "success",
                "data": UploadSchedulerConfigRepository._serialize(scheduler),
            }

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": "Failed to create scheduler",
                "error": str(e),
            }

    @staticmethod
    async def get_all(db: AsyncSession, filters: dict):
        try:
            stmt = select(UploadSchedulerConfig)

            if filters.get("upload_api_id") is not None:
                stmt = stmt.where(
                    UploadSchedulerConfig.upload_api_id == filters["upload_api_id"]
                )

            if filters.get("is_active") is not None:
                stmt = stmt.where(
                    UploadSchedulerConfig.is_active == filters["is_active"]
                )

            if filters.get("scheduler_name"):
                stmt = stmt.where(
                    UploadSchedulerConfig.scheduler_name.ilike(
                        f"%{filters['scheduler_name']}%"
                    )
                )

            count_stmt = select(func.count()).select_from(stmt.subquery())
            total = (await db.execute(count_stmt)).scalar() or 0

            stmt = stmt.order_by(UploadSchedulerConfig.created_at.desc())
            rows = (await db.execute(stmt)).scalars().all()

            return {
                "status": "success",
                "total": total,
                "count": len(rows),
                "data": [UploadSchedulerConfigRepository._serialize(r) for r in rows],
            }

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": "Failed to fetch scheduler list",
                "error": str(e),
            }

    @staticmethod
    async def get_by_id(db: AsyncSession, id: int):
        try:
            scheduler = (
                await db.execute(
                    select(UploadSchedulerConfig).where(UploadSchedulerConfig.id == id)
                )
            ).scalar_one_or_none()

            if not scheduler:
                return {
                    "status": "error",
                    "message": "Scheduler not found",
                }

            return {
                "status": "success",
                "data": UploadSchedulerConfigRepository._serialize(scheduler),
            }

        except Exception as e:
            return {
                "status": "error",
                "message": "Failed to fetch scheduler",
                "error": str(e),
            }

    @staticmethod
    async def get_by_upload_api_id(db: AsyncSession, upload_api_id: int):
        try:
            rows = (
                (
                    await db.execute(
                        select(UploadSchedulerConfig)
                        .where(UploadSchedulerConfig.upload_api_id == upload_api_id)
                        .order_by(UploadSchedulerConfig.created_at.desc())
                    )
                )
                .scalars()
                .all()
            )

            return {
                "status": "success",
                "data": [UploadSchedulerConfigRepository._serialize(r) for r in rows],
            }

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": "Failed to fetch schedulers by upload API",
                "error": str(e),
            }

    @staticmethod
    async def update(db: AsyncSession, id: int, payload: dict):
        try:
            stmt = (
                update(UploadSchedulerConfig)
                .where(UploadSchedulerConfig.id == id)
                .values(**payload)
                .execution_options(synchronize_session="fetch")
            )
            await db.execute(stmt)
            await db.commit()

            return await UploadSchedulerConfigRepository.get_by_id(db, id)

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": "Failed to update scheduler",
                "error": str(e),
            }

    @staticmethod
    async def enable(db: AsyncSession, id: int):
        return await UploadSchedulerConfigRepository.update(db, id, {"is_active": 1})

    @staticmethod
    async def disable(db: AsyncSession, id: int):
        return await UploadSchedulerConfigRepository.update(db, id, {"is_active": 0})

    @staticmethod
    def _serialize(s: UploadSchedulerConfig) -> dict:
        return {
            "id": s.id,
            "upload_api_id": s.upload_api_id,
            "scheduler_name": s.scheduler_name,
            "cron_expression": s.cron_expression,
            "timezone": s.timezone,
            "is_active": s.is_active,
            "created_at": s.created_at,
            "updated_at": s.updated_at,
            "version_number": s.version_number,
        }

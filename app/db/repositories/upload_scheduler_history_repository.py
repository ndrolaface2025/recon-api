from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.db.models.upload_scheduler_history import UploadSchedulerHistory


class UploadSchedulerHistoryRepository:

    @staticmethod
    async def create(db: AsyncSession, payload: dict):
        try:
            history = UploadSchedulerHistory(**payload)
            db.add(history)
            await db.commit()
            await db.refresh(history)

            return {
                "status": "success",
                "data": history,
            }

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": str(e),
            }

    @staticmethod
    async def get_all(db: AsyncSession, filters: dict):
        try:
            conditions = []

            if filters.get("scheduler_id"):
                conditions.append(
                    UploadSchedulerHistory.scheduler_id == filters["scheduler_id"]
                )

            if filters.get("status"):
                conditions.append(UploadSchedulerHistory.status == filters["status"])

            if filters.get("date_from"):
                conditions.append(
                    UploadSchedulerHistory.started_at >= filters["date_from"]
                )

            if filters.get("date_to"):
                conditions.append(
                    UploadSchedulerHistory.started_at <= filters["date_to"]
                )

            base_query = select(UploadSchedulerHistory)

            if conditions:
                base_query = base_query.where(and_(*conditions))

            # total count
            count_query = select(func.count()).select_from(base_query.subquery())
            total = (await db.execute(count_query)).scalar() or 0

            page = filters.get("page", 1)
            page_size = filters.get("page_size", 20)
            offset = (page - 1) * page_size

            data_query = (
                base_query.order_by(UploadSchedulerHistory.started_at.desc())
                .offset(offset)
                .limit(page_size)
            )

            result = await db.execute(data_query)
            rows = result.scalars().all()

            return {
                "status": "success",
                "data": rows,
                "total": total,
                "count": len(rows),
            }

        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
            }

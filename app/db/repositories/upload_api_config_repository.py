from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.upload_api_config import UploadAPIConfig


class UploadAPIConfigRepository:

    @staticmethod
    async def create(db: AsyncSession, payload: dict):
        try:
            cfg = UploadAPIConfig(**payload)
            db.add(cfg)
            await db.commit()
            await db.refresh(cfg)

            return {
                "status": "success",
                "data": UploadAPIConfigRepository._serialize(cfg),
            }

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": str(e),
            }

    @staticmethod
    async def get_all(db: AsyncSession, filters: dict):
        """
        Repository-level list fetch.
        - Applies filters
        - Applies pagination
        - Returns raw data + total count
        (NO API response shaping here)
        """
        try:
            stmt = select(UploadAPIConfig)

            if filters.get("channel_id") is not None:
                stmt = stmt.where(UploadAPIConfig.channel_id == filters["channel_id"])

            if filters.get("api_name"):
                stmt = stmt.where(
                    UploadAPIConfig.api_name.ilike(f"%{filters['api_name']}%")
                )

            if filters.get("method"):
                stmt = stmt.where(UploadAPIConfig.method == filters["method"])

            if filters.get("auth_type"):
                stmt = stmt.where(UploadAPIConfig.auth_type == filters["auth_type"])

            if filters.get("is_active") is not None:
                stmt = stmt.where(UploadAPIConfig.is_active == filters["is_active"])

            count_stmt = select(func.count()).select_from(stmt.subquery())
            total = (await db.execute(count_stmt)).scalar() or 0

            page = int(filters.get("page", 1))
            page_size = int(filters.get("page_size", 20))
            offset = (page - 1) * page_size

            stmt = (
                stmt.order_by(UploadAPIConfig.created_at.desc())
                .offset(offset)
                .limit(page_size)
            )

            rows = (await db.execute(stmt)).scalars().all()

            return {
                "status": "success",
                "data": [UploadAPIConfigRepository._serialize(r) for r in rows],
                "total": total,
            }

        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
            }

    @staticmethod
    async def get_by_id(db: AsyncSession, config_id: int):
        cfg = (
            await db.execute(
                select(UploadAPIConfig).where(UploadAPIConfig.id == config_id)
            )
        ).scalar_one_or_none()

        if not cfg:
            return {
                "status": "error",
                "message": "Upload API config not found",
            }

        return {
            "status": "success",
            "data": UploadAPIConfigRepository._serialize(cfg),
        }

    @staticmethod
    async def get_by_channel_id(db: AsyncSession, channel_id: int):
        rows = (
            (
                await db.execute(
                    select(UploadAPIConfig)
                    .where(
                        UploadAPIConfig.channel_id == channel_id,
                        UploadAPIConfig.is_active == 1,
                    )
                    .order_by(UploadAPIConfig.created_at.desc())
                )
            )
            .scalars()
            .all()
        )

        return {
            "status": "success",
            "data": [UploadAPIConfigRepository._serialize(r) for r in rows],
        }

    @staticmethod
    async def update(db: AsyncSession, id: int, payload: dict):
        try:
            stmt = (
                update(UploadAPIConfig)
                .where(UploadAPIConfig.id == id)
                .values(**payload)
                .execution_options(synchronize_session="fetch")
            )
            await db.execute(stmt)
            await db.commit()

            return await UploadAPIConfigRepository.get_by_id(db, id)

        except Exception as e:
            await db.rollback()
            return {
                "status": "error",
                "message": str(e),
            }

    @staticmethod
    async def disable(db: AsyncSession, id: int):
        return await UploadAPIConfigRepository.update(db, id, {"is_active": 0})

    @staticmethod
    async def enable(db: AsyncSession, id: int):
        return await UploadAPIConfigRepository.update(db, id, {"is_active": 1})

    @staticmethod
    def _serialize(cfg: UploadAPIConfig) -> dict:
        return {
            "id": cfg.id,
            "channel_id": cfg.channel_id,
            "api_name": cfg.api_name,
            "method": cfg.method,
            "base_url": cfg.base_url,
            "response_format": cfg.response_format,
            "auth_type": cfg.auth_type,
            "auth_token": cfg.auth_token,
            "api_time_out": cfg.api_time_out,
            "max_try": cfg.max_try,
            "is_active": cfg.is_active,
            "created_at": cfg.created_at,
            "updated_at": cfg.updated_at,
            "version_number": cfg.version_number,
        }

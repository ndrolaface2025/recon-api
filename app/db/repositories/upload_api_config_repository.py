from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.upload_api_config import UploadAPIConfig


class UploadAPIConfigRepository:

    @staticmethod
    async def createUploadAPIConfig(
        db: AsyncSession,
        payload: dict
    ):
        try:
            cfg = UploadAPIConfig(**payload)
            db.add(cfg)
            await db.commit()
            await db.refresh(cfg)

            return {
                "status": "success",
                "data": {
                    "id": cfg.id,
                    "channel_id": cfg.channel_id,
                    "api_name": cfg.api_name,
                    "method": cfg.method,
                    "base_url": cfg.base_url,
                    "responce_formate": cfg.responce_formate,
                    "auth_type": cfg.auth_type,
                    "api_time_out": cfg.api_time_out,
                    "max_try": cfg.max_try,
                    "created_at": cfg.created_at,
                    "version_number": cfg.version_number,
                }
            }

        except Exception as e:
            await db.rollback()
            print("UploadAPIConfigRepository.createUploadAPIConfig error:", str(e))
            return {
                "status": "error",
                "message": "Failed to create upload API config",
                "error": str(e),
            }

    @staticmethod
    async def getUploadAPIConfigList(db: AsyncSession):
        try:
            # ✅ Total count
            total_stmt = select(func.count()).select_from(UploadAPIConfig)
            total_result = await db.execute(total_stmt)
            total = total_result.scalar() or 0

            # ✅ Fetch list
            stmt = (
                select(UploadAPIConfig)
                .order_by(UploadAPIConfig.created_at.desc())
            )

            result = await db.execute(stmt)
            configs = result.scalars().all()

            data = [
                {
                    "id": cfg.id,
                    "channel_id": cfg.channel_id,
                    "api_name": cfg.api_name,
                    "method": cfg.method,
                    "base_url": cfg.base_url,
                    "responce_formate": cfg.responce_formate,
                    "auth_type": cfg.auth_type,
                    "api_time_out": cfg.api_time_out,
                    "max_try": cfg.max_try,
                    "created_at": cfg.created_at,
                    "version_number": cfg.version_number,
                }
                for cfg in configs
            ]

            return {
                "status": "success",
                "total": total,
                "count": len(data),
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("UploadAPIConfigRepository.getUploadAPIConfigList error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch upload API config list",
                "error": str(e),
            }

    @staticmethod
    async def getUploadAPIConfigById(db: AsyncSession, config_id: int):
        try:
            stmt = select(UploadAPIConfig).where(
                UploadAPIConfig.id == config_id
            )
            result = await db.execute(stmt)
            cfg = result.scalar_one_or_none()

            if not cfg:
                return {
                    "status": "error",
                    "message": "Upload API config not found",
                }

            data = {
                "id": cfg.id,
                "channel_id": cfg.channel_id,
                "api_name": cfg.api_name,
                "method": cfg.method,
                "base_url": cfg.base_url,
                "responce_formate": cfg.responce_formate,
                "auth_type": cfg.auth_type,
                "auth_token": cfg.auth_token,
                "api_time_out": cfg.api_time_out,
                "max_try": cfg.max_try,
                "created_at": cfg.created_at,
                "version_number": cfg.version_number,
            }

            return {
                "status": "success",
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print("UploadAPIConfigRepository.getUploadAPIConfigById error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch upload API config",
                "error": str(e),
            }

    @staticmethod
    async def getUploadAPIConfigByChannelId(
        db: AsyncSession, channel_id: int
    ):
        try:
            stmt = (
                select(UploadAPIConfig)
                .where(UploadAPIConfig.channel_id == channel_id)
                .order_by(UploadAPIConfig.created_at.desc())
            )

            result = await db.execute(stmt)
            configs = result.scalars().all()

            data = [
                {
                    "id": cfg.id,
                    "channel_id": cfg.channel_id,
                    "api_name": cfg.api_name,
                    "method": cfg.method,
                    "base_url": cfg.base_url,
                    "responce_formate": cfg.responce_formate,
                    "auth_type": cfg.auth_type,
                    "auth_token": cfg.auth_token,
                    "api_time_out": cfg.api_time_out,
                    "max_try": cfg.max_try,
                    "created_at": cfg.created_at,
                    "version_number": cfg.version_number,
                }
                for cfg in configs
            ]

            return {
                "status": "success",
                "data": data,
            }

        except Exception as e:
            await db.rollback()
            print(
                "UploadAPIConfigRepository.getUploadAPIConfigByChannelId error:",
                str(e),
            )
            return {
                "status": "error",
                "message": "Failed to fetch upload API configs by channel",
                "error": str(e),
            }

from math import ceil
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.upload_api_config_repository import (
    UploadAPIConfigRepository,
)


class UploadAPIConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, payload: dict):
        result = await UploadAPIConfigRepository.create(self.db, payload)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to create upload API config"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API config created successfully",
            "data": result.get("data"),
            "meta": None,
        }

    async def get_all(
        self,
        *,
        channel_id: int | None = None,
        api_name: str | None = None,
        method: str | None = None,
        auth_type: str | None = None,
        is_active: int | None = None,
        page: int = 1,
        page_size: int = 20,
    ):
        filters = {
            "channel_id": channel_id,
            "api_name": api_name,
            "method": method,
            "auth_type": auth_type,
            "is_active": is_active,
            "page": page,
            "page_size": page_size,
        }

        result = await UploadAPIConfigRepository.get_all(self.db, filters)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to fetch upload API configs"),
                "data": [],
                "meta": None,
            }

        records = result.get("data", [])
        total_records = result.get("total", 0)
        total_pages = ceil(total_records / page_size) if page_size else 1

        return {
            "success": True,
            "message": "Upload API configs fetched successfully",
            "data": records,
            "meta": {
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_records": total_records,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1,
                }
            },
        }

    async def get_by_id(self, config_id: int):
        result = await UploadAPIConfigRepository.get_by_id(self.db, config_id)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Upload API config not found"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API config fetched successfully",
            "data": result.get("data"),
            "meta": None,
        }

    async def get_by_channel_id(self, channel_id: int):
        result = await UploadAPIConfigRepository.get_by_channel_id(self.db, channel_id)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to fetch upload API configs"),
                "data": [],
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API configs fetched successfully",
            "data": result.get("data", []),
            "meta": None,
        }

    async def update(self, config_id: int, payload: dict):
        result = await UploadAPIConfigRepository.update(self.db, config_id, payload)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to update upload API config"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API config updated successfully",
            "data": result.get("data"),
            "meta": None,
        }

    async def enable(self, id: int):
        result = await UploadAPIConfigRepository.enable(self.db, id)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": "Failed to enable upload API config",
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API config enabled successfully",
            "data": result.get("data"),
            "meta": None,
        }

    async def disable(self, id: int):
        result = await UploadAPIConfigRepository.disable(self.db, id)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": "Failed to disable upload API config",
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Upload API config disabled successfully",
            "data": result.get("data"),
            "meta": None,
        }

from math import ceil
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.upload_scheduler_config_repository import (
    UploadSchedulerConfigRepository,
)


class UploadSchedulerConfigService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, payload: dict):
        result = await UploadSchedulerConfigRepository.create(self.db, payload)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to create scheduler"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Scheduler created successfully",
            "data": result["data"],
            "meta": None,
        }

    async def get_all(
        self,
        scheduler_name: str | None = None,
        upload_api_id: int | None = None,
        scheduler_id: int | None = None,
        is_active: int | None = None,
        page: int = 1,
        page_size: int = 20,
    ):
        filters = {
            "scheduler_name": scheduler_name,
            "scheduler_id": scheduler_id,
            "upload_api_id": upload_api_id,
            "is_active": is_active,
            "page": page,
            "page_size": page_size,
        }

        result = await UploadSchedulerConfigRepository.get_all(self.db, filters)

        if result.get("status") != "success":
            return self._error_response(
                result.get("message", "Failed to fetch schedulers"),
                page,
                page_size,
            )

        records = result.get("data", [])
        total_records = result.get("total", 0)
        total_pages = ceil(total_records / page_size) if page_size else 1

        return {
            "success": True,
            "message": "Scheduler list fetched successfully",
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

    async def get_by_id(self, id: int):
        result = await UploadSchedulerConfigRepository.get_by_id(self.db, id)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Scheduler not found"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Scheduler fetched successfully",
            "data": result["data"],
            "meta": None,
        }

    async def update(self, id: int, payload: dict):
        result = await UploadSchedulerConfigRepository.update(self.db, id, payload)

        if result.get("status") != "success":
            return {
                "success": False,
                "message": result.get("message", "Failed to update scheduler"),
                "data": None,
                "meta": None,
            }

        return {
            "success": True,
            "message": "Scheduler updated successfully",
            "data": result.get("data"),
            "meta": None,
        }

    async def enable(self, id: int):
        return await self.update(id, {"is_active": 1})

    async def disable(self, id: int):
        return await self.update(id, {"is_active": 0})

    @staticmethod
    def _error_response(message: str, page: int, page_size: int):
        return {
            "success": False,
            "message": message,
            "data": [],
            "meta": {
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_records": 0,
                    "total_pages": 0,
                    "has_next": False,
                    "has_previous": False,
                }
            },
        }

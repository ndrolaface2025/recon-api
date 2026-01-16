from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.upload_scheduler_config_repository import (
    UploadSchedulerConfigRepository,
)


class UploadSchedulerConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, payload: dict):
        result = await UploadSchedulerConfigRepository.create(self.db, payload)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler created successfully",
                "result": {"data": result.get("data")},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": None},
        }

    async def get_all(self, filters: dict):
        result = await UploadSchedulerConfigRepository.get_all(self.db, filters)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler list fetched successfully",
                "result": {
                    "data": result.get("data", []),
                    "total": result.get("total", 0),
                    "count": result.get("count", 0),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": []},
        }

    async def get_by_id(self, id: int):
        result = await UploadSchedulerConfigRepository.get_by_id(self.db, id)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler fetched successfully",
                "result": {"data": result.get("data")},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": None},
        }

    async def get_by_upload_api_id(self, upload_api_id: int):
        result = await UploadSchedulerConfigRepository.get_by_upload_api_id(
            self.db, upload_api_id
        )

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Schedulers fetched successfully",
                "result": {"data": result.get("data", [])},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": []},
        }

    async def update(self, id: int, payload: dict):
        result = await UploadSchedulerConfigRepository.update(self.db, id, payload)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler updated successfully",
                "result": {"data": result.get("data")},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": None},
        }

    async def enable(self, id: int):
        result = await UploadSchedulerConfigRepository.enable(self.db, id)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler enabled successfully",
                "result": {"data": result.get("data")},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": None},
        }

    async def disable(self, id: int):
        result = await UploadSchedulerConfigRepository.disable(self.db, id)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler disabled successfully",
                "result": {"data": result.get("data")},
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message"),
            "result": {"data": None},
        }

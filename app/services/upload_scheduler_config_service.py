from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.upload_scheduler_config_repository import (
    UploadSchedulerConfigRepository,
)


class UploadSchedulerConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_scheduler_list(self):
        getResult = await UploadSchedulerConfigRepository.getSchedulerList(
            self.db
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler list fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                    "total": getResult.get("total", 0),
                    "count": getResult.get("count", 0),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to fetch scheduler list"
            ),
            "result": {
                "data": [],
            },
        }

    async def get_scheduler_by_id(self, scheduler_id: int):
        getResult = await UploadSchedulerConfigRepository.getSchedulerById(
            self.db, scheduler_id
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler fetched successfully",
                "result": {
                    "data": getResult.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to fetch scheduler"
            ),
            "result": {
                "data": None,
            },
        }

    async def get_scheduler_by_channel_id(self, channel_id: int):
        getResult = (
            await UploadSchedulerConfigRepository.getSchedulerByChannelId(
                self.db, channel_id
            )
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to fetch scheduler by channel"
            ),
            "result": {
                "data": [],
            },
        }

    async def create_scheduler(self, payload: dict):
        getResult = await UploadSchedulerConfigRepository.createScheduler(
            self.db, payload
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Scheduler created successfully",
                "result": {
                    "data": getResult.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to create scheduler"
            ),
            "result": {
                "data": None,
            },
        }

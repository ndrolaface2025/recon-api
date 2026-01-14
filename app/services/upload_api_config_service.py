from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.upload_api_config_repository import (
    UploadAPIConfigRepository,
)


class UploadAPIConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, payload: dict):
        result = await UploadAPIConfigRepository.create(self.db, payload)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config created successfully",
                "result": {
                    "data": result.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message", "Failed to create upload API config"),
            "result": {
                "data": None,
            },
        }

    async def get_all(self, filters: dict):
        result = await UploadAPIConfigRepository.get_all(self.db, filters)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config list fetched successfully",
                "result": {
                    "data": result.get("data", []),
                    "total": result.get("total", 0),
                    "count": result.get("count", 0),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message", "Failed to fetch upload API config list"),
            "result": {
                "data": [],
                "total": 0,
                "count": 0,
            },
        }

    async def get_by_id(self, config_id: int):
        result = await UploadAPIConfigRepository.get_by_id(self.db, config_id)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config fetched successfully",
                "result": {
                    "data": result.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message", "Upload API config not found"),
            "result": {
                "data": None,
            },
        }

    async def get_by_channel_id(self, channel_id: int):
        result = await UploadAPIConfigRepository.get_by_channel_id(self.db, channel_id)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API configs fetched successfully",
                "result": {
                    "data": result.get("data", []),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get(
                "message", "Failed to fetch upload API configs by channel"
            ),
            "result": {
                "data": [],
            },
        }

    async def update(self, config_id: int, payload: dict):
        result = await UploadAPIConfigRepository.update(self.db, config_id, payload)

        if result.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config updated successfully",
                "result": {
                    "data": result.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": result.get("message", "Failed to update upload API config"),
            "result": {
                "data": None,
            },
        }

    async def enable(self, id: int):
        result = await UploadAPIConfigRepository.enable(self.db, id)

    async def disable(self, id: int):
        result = await UploadAPIConfigRepository.disable(self.db, id)

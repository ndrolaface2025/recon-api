from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.upload_api_config_repository import (
    UploadAPIConfigRepository,
)


class UploadAPIConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_upload_api_config(self, payload: dict):
        getResult = await UploadAPIConfigRepository.createUploadAPIConfig(
            self.db,
            payload
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config created successfully",
                "result": {
                    "data": getResult.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to create upload API config"
            ),
            "result": {
                "data": None,
            },
        }


    async def get_upload_api_config_list(self):
        getResult = await UploadAPIConfigRepository.getUploadAPIConfigList(
            self.db
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config list fetched successfully",
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
                "message", "Failed to fetch upload API config list"
            ),
            "result": {
                "data": [],
            },
        }

    async def get_upload_api_config_by_id(self, config_id: int):
        getResult = await UploadAPIConfigRepository.getUploadAPIConfigById(
            self.db, config_id
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API config fetched successfully",
                "result": {
                    "data": getResult.get("data"),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message", "Failed to fetch upload API config"
            ),
            "result": {
                "data": None,
            },
        }

    async def get_upload_api_config_by_channel_id(self, channel_id: int):
        getResult = (
            await UploadAPIConfigRepository.getUploadAPIConfigByChannelId(
                self.db, channel_id
            )
        )

        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "Upload API configs fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get(
                "message",
                "Failed to fetch upload API configs by channel",
            ),
            "result": {
                "data": [],
            },
        }

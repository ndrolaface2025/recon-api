from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.channel_source_repository import ChannelSourceRepository

class ChannelSourceService:

    def __init__(self, db: AsyncSession):
        self.db = db
        # self.batch_service = batch_service

    async def get_channel_list(self):
        getResult =  await ChannelSourceRepository.getChannelList(self.db)
        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "File list fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                }
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get("message", "Failed to fetch file list"),
            "result": {
                "data": [],
            }
        }
    
    async def get_source_list_By_channel_id(self, channel_id:int):
        getResult =  await ChannelSourceRepository.getSourceListByChannelId(self.db, channel_id)
        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "File list fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                }
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get("message", "Failed to fetch file list"),
            "result": {
                "data": [],
            }
        }
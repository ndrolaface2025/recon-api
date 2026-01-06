from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.batchConfigRepository import BatchConfigRepository

class BatchConfigService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def getBatchConfiguration(self, system_id: int):
        fileSaveDetail = await BatchConfigRepository.getBatchConfiguration(self.db, system_id)
        if fileSaveDetail is None:
            return {
                    "status": "success",
                    "errors": False,
                    "message": "No configuration found for the given system ID",
                    "data": []
                }
        else:
            return {
                "status": "success",
                "errors": False,
                "message": "Configuration found for the given system ID",
                "data": fileSaveDetail
            }
    
    async def saveBatchConfiguration(self, payload):
        record, action = await BatchConfigRepository.upsert_batch_configuration(
            self.db,
            payload
        )
        return {
                "status": "success",
                "errors": False,
                "message": " System configuration " + action + " successfully",
                "data": record
            }
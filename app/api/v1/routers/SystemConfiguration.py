from typing import Any
from fastapi import APIRouter, Query, Body
from fastapi.params import Depends

from app.services.batch_config_service import BatchConfigService
from app.services.services import get_service

router = APIRouter(prefix="/api/v1/config", tags=["System Configuration"])

@router.get("/batch")
async def GetBatchConfigurationDetails(
    system_id: int = Query(..., description="System ID"),
    service: BatchConfigService = Depends(get_service(BatchConfigService))
):
    return await service.getBatchConfiguration(system_id)

@router.post("/batch")
async def SaveBatchConfiguration(
    payload: Any = Body(...),
    service: BatchConfigService = Depends(get_service(BatchConfigService))
):
    return await service.saveBatchConfiguration(payload)

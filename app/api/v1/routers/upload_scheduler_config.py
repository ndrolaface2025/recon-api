from fastapi import APIRouter, Depends, Body
from app.services.upload_scheduler_config_service import (
    UploadSchedulerConfigService,
)
from app.services.services import get_service

router = APIRouter(
    prefix="/api/v1/upload-scheduler-config",
    tags=["Upload Scheduler Config"]
)


@router.get("/")
async def get_scheduler_list(
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    )
):
    return await service.get_scheduler_list()


@router.get("/{scheduler_id}")
async def get_scheduler_by_id(
    scheduler_id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    )
):
    return await service.get_scheduler_by_id(scheduler_id)


@router.get("/channel/{channel_id}")
async def get_scheduler_by_channel_id(
    channel_id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    )
):
    return await service.get_scheduler_by_channel_id(channel_id)


@router.post("/")
async def create_scheduler(
    payload: dict = Body(...),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    )
):
    return await service.create_scheduler(payload)

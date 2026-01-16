from fastapi import APIRouter, Depends, Body, Query
from typing import Optional

from app.services.upload_scheduler_config_service import (
    UploadSchedulerConfigService,
)
from app.services.services import get_service

router = APIRouter(
    prefix="/api/v1/upload-scheduler-config",
    tags=["Upload Scheduler Config"],
)


# ------------------------------------------------------------------
# GET ALL (WITH FILTERS)
# ------------------------------------------------------------------
@router.get("/")
async def get_scheduler_list(
    upload_api_id: Optional[int] = Query(None),
    scheduler_name: Optional[str] = Query(None),
    is_active: Optional[int] = Query(None),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    filters = {
        "upload_api_id": upload_api_id,
        "scheduler_name": scheduler_name,
        "is_active": is_active,
    }
    return await service.get_all(filters)


# ------------------------------------------------------------------
# GET BY ID
# ------------------------------------------------------------------
@router.get("/{id}")
async def get_scheduler_by_id(
    id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.get_by_id(id)


# ------------------------------------------------------------------
# GET BY UPLOAD API
# ------------------------------------------------------------------
@router.get("/upload-api/{upload_api_id}")
async def get_scheduler_by_upload_api_id(
    upload_api_id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.get_by_upload_api_id(upload_api_id)


# ------------------------------------------------------------------
# CREATE
# ------------------------------------------------------------------
@router.post("/")
async def create_scheduler(
    payload: dict = Body(...),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.create(payload)


# ------------------------------------------------------------------
# UPDATE
# ------------------------------------------------------------------
@router.put("/{id}")
async def update_scheduler(
    id: int,
    payload: dict = Body(...),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.update(id, payload)


# ------------------------------------------------------------------
# ENABLE
# ------------------------------------------------------------------
@router.patch("/{id}/enable")
async def enable_scheduler(
    id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.enable(id)


# ------------------------------------------------------------------
# DISABLE
# ------------------------------------------------------------------
@router.patch("/{id}/disable")
async def disable_scheduler(
    id: int,
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.disable(id)

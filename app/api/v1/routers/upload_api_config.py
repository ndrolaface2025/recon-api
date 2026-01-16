from fastapi import APIRouter, Depends, Body, Query
from typing import Optional

from app.services.upload_api_config_service import UploadAPIConfigService
from app.services.services import get_service

router = APIRouter(
    prefix="/api/v1/upload-api-config",
    tags=["Upload API Config"],
)


# ------------------------------------------------------------------
# GET ALL (WITH FILTERS)
# ------------------------------------------------------------------
@router.get("/")
async def get_all_upload_api_configs(
    channel_id: Optional[int] = Query(None),
    api_name: Optional[str] = Query(None),
    method: Optional[str] = Query(None),
    auth_type: Optional[str] = Query(None),
    is_active: Optional[int] = Query(None),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    filters = {
        "channel_id": channel_id,
        "api_name": api_name,
        "method": method,
        "auth_type": auth_type,
        "is_active": is_active,
    }
    return await service.get_all(filters)


# ------------------------------------------------------------------
# GET BY ID
# ------------------------------------------------------------------
@router.get("/{id}")
async def get_upload_api_config_by_id(
    id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_id(id)


# ------------------------------------------------------------------
# GET BY CHANNEL ID
# ------------------------------------------------------------------
@router.get("/channel/{channel_id}")
async def get_upload_api_config_by_channel_id(
    channel_id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_channel_id(channel_id)


# ------------------------------------------------------------------
# CREATE
# ------------------------------------------------------------------
@router.post("/")
async def create_upload_api_config(
    payload: dict = Body(...),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.create(payload)


# ------------------------------------------------------------------
# UPDATE (PARTIAL / FULL)
# ------------------------------------------------------------------
@router.put("/{id}")
async def update_upload_api_config(
    id: int,
    payload: dict = Body(...),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.update(id, payload)


# ------------------------------------------------------------------
# ENABLE
# ------------------------------------------------------------------
@router.patch("/{id}/enable")
async def enable_upload_api_config(
    id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.enable(id)


# ------------------------------------------------------------------
# DISABLE (SOFT DELETE)
# ------------------------------------------------------------------
@router.patch("/{id}/disable")
async def disable_upload_api_config(
    id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.disable(id)

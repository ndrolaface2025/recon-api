from fastapi import APIRouter, Depends, Body
from app.services.upload_api_config_service import UploadAPIConfigService
from app.services.services import get_service

router = APIRouter(prefix="/api/v1/upload-api-config", tags=["Upload API Config"])


@router.get("/")
async def get_upload_api_config_list(
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_upload_api_config_list()


@router.get("/{config_id}")
async def get_upload_api_config_by_id(
    config_id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_upload_api_config_by_id(config_id)


@router.get("/channel/{channel_id}")
async def get_upload_api_config_by_channel_id(
    channel_id: int,
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_upload_api_config_by_channel_id(channel_id)


@router.post("/")
async def create_upload_api_config(
    payload: dict = Body(...),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.create_upload_api_config(payload)

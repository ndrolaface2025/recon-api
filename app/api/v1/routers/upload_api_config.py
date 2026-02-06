from fastapi import APIRouter, Depends, Body, Query, Path
from typing import Optional, Dict, Any

from app.services.upload_api_config_service import UploadAPIConfigService
from app.services.services import get_service

router = APIRouter(
    prefix="/api/v1/upload-api-config",
    tags=["Upload API Config"],
)


@router.get(
    "/",
    summary="Get all Upload API configurations",
    description=(
        "Fetch upload API configurations with optional filters and pagination. "
        "All filters are combined using AND logic."
    ),
)
async def get_all_upload_api_configs(
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
    api_id: Optional[int] = Query(None, description="Filter by API ID"),
    api_name: Optional[str] = Query(None, description="Filter by API name"),
    method: Optional[str] = Query(
        None, description="Filter by method (LOCAL, FTP, HTTP, SFTP)"
    ),
    auth_type: Optional[str] = Query(None, description="Filter by auth type"),
    is_active: Optional[int] = Query(
        None, description="Filter by active status (1=active, 0=inactive)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Records per page"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_all(
        channel_id=channel_id,
        api_id=api_id,
        api_name=api_name,
        method=method,
        auth_type=auth_type,
        is_active=is_active,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/{id}",
    summary="Get Upload API configuration by ID",
    description="Retrieve a single upload API configuration by its ID.",
)
async def get_upload_api_config_by_id(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_id(id)


@router.get(
    "/channel/{channel_id}",
    summary="Get Upload API configurations by channel ID",
    description="Fetch all active upload API configurations for a channel.",
)
async def get_upload_api_config_by_channel_id(
    channel_id: int = Path(..., description="Channel ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_channel_id(channel_id)


@router.post(
    "/",
    summary="Create a new Upload API configuration",
    description="Create a new upload API configuration.",
)
async def create_upload_api_config(
    payload: Dict[str, Any] = Body(
        ...,
        description="Upload API configuration payload",
    ),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.create(payload)


@router.put(
    "/{id}",
    summary="Update an Upload API configuration",
    description="Update an existing upload API configuration.",
)
async def update_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    payload: Dict[str, Any] = Body(
        ...,
        description="Fields to update",
    ),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.update(id, payload)


@router.patch(
    "/{id}/enable",
    summary="Enable an Upload API configuration",
)
async def enable_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.enable(id)


@router.patch(
    "/{id}/disable",
    summary="Disable an Upload API configuration",
)
async def disable_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.disable(id)

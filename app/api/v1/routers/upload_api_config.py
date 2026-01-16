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
        "Fetch all upload API configurations with optional filters. "
        "All query parameters are optional and combined using AND logic."
    ),
    response_description="List of upload API configurations",
)
async def get_all_upload_api_configs(
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
    api_name: Optional[str] = Query(None, description="Filter by API name"),
    method: Optional[str] = Query(
        None, description="Filter by HTTP method (LOCAL, FTP, HTTP, etc.)"
    ),
    auth_type: Optional[str] = Query(None, description="Filter by authentication type"),
    is_active: Optional[int] = Query(
        None, description="Filter by active status (1 = active, 0 = inactive)"
    ),
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


@router.get(
    "/{id}",
    summary="Get Upload API configuration by ID",
    description="Retrieve a single upload API configuration using its unique ID.",
    response_description="Upload API configuration details",
)
async def get_upload_api_config_by_id(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_id(id)


@router.get(
    "/channel/{channel_id}",
    summary="Get Upload API configurations by channel ID",
    description="Fetch all upload API configurations associated with a specific channel.",
    response_description="List of upload API configurations for the channel",
)
async def get_upload_api_config_by_channel_id(
    channel_id: int = Path(..., description="Channel ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.get_by_channel_id(channel_id)


@router.post(
    "/",
    summary="Create a new Upload API configuration",
    description="Create a new upload API configuration using the provided payload.",
    response_description="Created upload API configuration",
)
async def create_upload_api_config(
    payload: Dict[str, Any] = Body(
        ...,
        description="Upload API configuration payload",
        example={
            "channel_id": 1,
            "api_name": "Transaction Upload",
            "method": "FTP",
            "base_url": "https://example.com/api/upload",
            "auth_type": "BASIC",
            "auth_token": "username:password",
            "is_active": 1,
        },
    ),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.create(payload)


@router.put(
    "/{id}",
    summary="Update an Upload API configuration",
    description="Update an existing upload API configuration. Supports full or partial updates.",
    response_description="Updated upload API configuration",
)
async def update_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    payload: Dict[str, Any] = Body(
        ...,
        description="Fields to update in the upload API configuration",
        example={
            "api_name": "Updated Transaction Upload",
            "is_active": 0,
        },
    ),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.update(id, payload)


@router.patch(
    "/{id}/enable",
    summary="Enable an Upload API configuration",
    description="Mark an upload API configuration as active.",
    response_description="Enabled upload API configuration",
)
async def enable_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.enable(id)


@router.patch(
    "/{id}/disable",
    summary="Disable an Upload API configuration",
    description="Soft delete / disable an upload API configuration.",
    response_description="Disabled upload API configuration",
)
async def disable_upload_api_config(
    id: int = Path(..., description="Upload API configuration ID"),
    service: UploadAPIConfigService = Depends(get_service(UploadAPIConfigService)),
):
    return await service.disable(id)

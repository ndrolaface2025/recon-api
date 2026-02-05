from fastapi import APIRouter, Depends, Body, Query, Path
from typing import Optional, Dict, Any

from app.db.session import get_db
from app.services.upload_scheduler_config_service import UploadSchedulerConfigService
from app.services.services import get_service
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(
    prefix="/api/v1/upload-scheduler-config",
    tags=["Upload Scheduler Config"],
)


@router.get(
    "/",
    summary="Get all upload scheduler configurations",
    description="Fetch all upload scheduler configurations with optional filters.",
    response_description="List of upload scheduler configurations",
)
async def list_schedulers(
    scheduler_name: str | None = Query(None),
    upload_api_id: int | None = Query(None),
    scheduler_id: int | None = Query(None),
    is_active: int | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    service = UploadSchedulerConfigService(db)

    return await service.get_all(
        scheduler_name=scheduler_name,
        scheduler_id=scheduler_id,
        upload_api_id=upload_api_id,
        is_active=is_active,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/{id}",
    summary="Get upload scheduler configuration by ID",
    description="Retrieve a single upload scheduler configuration using its ID.",
    response_description="Upload scheduler configuration details",
)
async def get_scheduler_by_id(
    id: int = Path(..., description="Upload scheduler configuration ID"),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.get_by_id(id)


@router.get(
    "/upload-api/{upload_api_id}",
    summary="Get upload scheduler configurations by upload API ID",
    description="Fetch all scheduler configurations associated with a specific upload API.",
    response_description="List of upload scheduler configurations",
)
async def get_scheduler_by_upload_api_id(
    upload_api_id: int = Path(..., description="Upload API ID"),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.get_by_upload_api_id(upload_api_id)


@router.post(
    "/",
    summary="Create a new upload scheduler configuration",
    description="Create a new upload scheduler configuration.",
    response_description="Created upload scheduler configuration",
)
async def create_scheduler(
    payload: Dict[str, Any] = Body(
        ...,
        description="Upload scheduler configuration payload",
    ),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.create(payload)


@router.put(
    "/{id}",
    summary="Update an upload scheduler configuration",
    description="Update an existing upload scheduler configuration.",
    response_description="Updated upload scheduler configuration",
)
async def update_scheduler(
    id: int = Path(..., description="Upload scheduler configuration ID"),
    payload: Dict[str, Any] = Body(
        ...,
        description="Fields to update in the upload scheduler configuration",
    ),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.update(id, payload)


@router.patch(
    "/{id}/enable",
    summary="Enable an upload scheduler configuration",
    description="Enable an upload scheduler configuration.",
    response_description="Enabled upload scheduler configuration",
)
async def enable_scheduler(
    id: int = Path(..., description="Upload scheduler configuration ID"),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.enable(id)


@router.patch(
    "/{id}/disable",
    summary="Disable an upload scheduler configuration",
    description="Disable an upload scheduler configuration.",
    response_description="Disabled upload scheduler configuration",
)
async def disable_scheduler(
    id: int = Path(..., description="Upload scheduler configuration ID"),
    service: UploadSchedulerConfigService = Depends(
        get_service(UploadSchedulerConfigService)
    ),
):
    return await service.disable(id)

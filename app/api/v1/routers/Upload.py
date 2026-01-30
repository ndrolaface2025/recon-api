import io
import json
import os
import time
import pandas as pd
from fastapi import APIRouter, Body, Depends, Form, UploadFile, File, HTTPException
from app.services.services import get_service
from app.services.upload_service import UploadService
from app.db.repositories.upload import UploadRepository
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/api/v1/upload", tags=["Upload"])


@router.post("/", name="upload_with_slash")
@router.post(
    "", name="upload_without_slash"
)  # Handle both with and without trailing slash
async def upload(
    file: UploadFile = File(...),
    channel_id: int = Form(...),
    source_id: int = Form(...),
    mappings: str = Form(...),
    service: UploadService = Depends(get_service(UploadService)),
):
    mappings_data = json.loads(mappings)
    print(f"[UPLOAD API DEBUG] *** Received upload request ***")
    print(f"[UPLOAD API DEBUG] channel_id={channel_id}")
    print(f"[UPLOAD API DEBUG] source_id={source_id}")
    print(f"[UPLOAD API DEBUG] file={file.filename}")
    print(f"[UPLOAD API DEBUG] mappings={mappings_data}")
    return await service.fileUpload(file, channel_id, source_id, mappings_data)


@router.post("/with-celery")
async def upload_with_celery(
    file: UploadFile = File(...),
    service: UploadService = Depends(get_service(UploadService)),
):
    return await service.uploadWithCelery(file)


@router.get("/progress/{file_id}")
async def get_upload_progress(file_id: int, db: AsyncSession = Depends(get_db)):
    """
    Get real-time progress of a file upload.

    Returns:
    - file_id: Upload file ID
    - file_name: Original filename
    - status: 0=pending, 1=processing, 2=completed, 3=failed
    - total_records: Total records in file
    - processed_records: Records processed so far
    - success_records: Successfully inserted records
    - failed_records: Failed records
    - duplicate_records: Skipped duplicate records
    - progress_percentage: Progress 0-100%
    - upload_started_at: When processing started
    - upload_completed_at: When processing completed
    - processing_time_seconds: Total processing time
    - error_message: Error message if failed
    - error_details: Detailed error information

    Example:
    ```
    GET /api/v1/upload/progress/12345
    ```
    """
    result = await UploadRepository.getUploadProgress(db, file_id)

    if result.get("error"):
        raise HTTPException(status_code=404, detail=result.get("message"))

    return {"status": "success", "data": result}


@router.get("/file-list")
async def getUploadFileList(
    offset: int = 0,
    limit: int = 10,
    service: UploadService = Depends(get_service(UploadService)),
):
    return await service.get_file_list(offset, limit)


# FIXME: TEMPORARY HARD DELETE — REMOVE BEFORE PRODUCTION
# WARNING: Deletions will NOT be allowed in the future (audit/compliance).
# TODO: Replace with soft-delete or archival logic.
@router.put("/delete")
async def delete_file(
    delete_id: int = Body(..., embed=True),
    service: UploadService = Depends(get_service(UploadService)),
):
    return await service.deleteFileAndTransactions(delete_id)

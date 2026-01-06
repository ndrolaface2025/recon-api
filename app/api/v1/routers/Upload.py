import io
import json
import os
import time
import pandas as pd
from fastapi import APIRouter, Depends, Form, UploadFile, File
from app.services.services import get_service
from app.services.upload_service import UploadService

router = APIRouter(prefix="/api/v1/upload", tags=["Upload"])


@router.post("/")
async def upload(file: UploadFile = File(...),channel_id: int = Form(...),source_id: int = Form(...),mappings: str = Form(...), service: UploadService = Depends(get_service(UploadService))):
    mappings_data = json.loads(mappings)
    # print("channel_id",channel_id)
    # print("source_id",source_id)
    # print("mappings",mappings_data)
    return await service.fileUpload(file,channel_id,source_id,mappings_data)

@router.post("/with-celery")
async def upload_with_celery(file: UploadFile = File(...), service: UploadService = Depends(get_service(UploadService))):
    return await service.uploadWithCelery(file)
    

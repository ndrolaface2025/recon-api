import io
import os
import time
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from app.services.services import get_service
from app.services.upload_service import UploadService

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/")
async def upload(file: UploadFile = File(...), service: UploadService = Depends(get_service(UploadService))):
    return await service.fileUpload(file)

@router.post("/with-celery")
async def upload_with_celery(file: UploadFile = File(...), service: UploadService = Depends(get_service(UploadService))):
    return await service.uploadWithCelery(file)
    

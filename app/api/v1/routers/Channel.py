import io
import json
import os
import time
import pandas as pd
from fastapi import APIRouter, Body, Depends, Form, UploadFile, File, HTTPException
from app.services.channel_source_service import ChannelSourceService
from app.services.services import get_service
from app.db.repositories.upload import UploadRepository
from app.db.session import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/api/v1/channel", tags=["Channel & source"])

@router.get("/")
async def getChannelList(service: ChannelSourceService = Depends(get_service(ChannelSourceService))):
    return await service.get_channel_list()

@router.get("/source/{channel_id}")
async def getChannelSourceList(channel_id: int,service: ChannelSourceService = Depends(get_service(ChannelSourceService))):
    return await service.get_source_list_By_channel_id(channel_id)

@router.get("/network")
async def getNetworkList(service: ChannelSourceService = Depends(get_service(ChannelSourceService))):
    return await service.get_channel_network_list()
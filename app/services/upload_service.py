import io
import os
from random import random
import secrets
import string
import time
from fastapi import HTTPException
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.upload import UploadRepository
from app.services.batch_config_service import BatchConfigService
from app.workers.tasks import start_recon_job
# from app.workers.tasks import process_batch
# from app.tasks.test_tasks import process_batch

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}

class UploadService:

    def __init__(self, db: AsyncSession):
        self.db = db
        # self.batch_service = batch_service
    
    def is_allowed_file(self, filename: str) -> bool:
        ext = os.path.splitext(filename)[1].lower()
        return ext in ALLOWED_EXTENSIONS
    
    async def fileUpload(self,  file, channel_id: int, source_id: int, mappings: dict):
        try:
            if not file.filename:
                raise HTTPException(status_code=400, detail="No file selected")

            if not self.is_allowed_file(file.filename):
                raise HTTPException(
                    status_code=400,
                    detail="Only CSV, XLS, and XLSX files are allowed"
                )
            
            contents = await file.read()
            ext = os.path.splitext(file.filename)[1].lower()

            if ext == ".csv":
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
            else:
                df = pd.read_excel(io.BytesIO(contents))

            # Replace NaN with None (JSON safe)
            df = df.where(pd.notnull(df), '')

            # columns = df.columns.tolist()
            total_records = len(df)
            # fileData = df.head(5).to_dict(orient="records")
            df.columns = [str(col).strip().lower() for col in df.columns]
            fileData = df.to_dict(orient="records")
            fileDetails = {"file_name": file.filename, "file_details": {"file_type": 1, "file_size": "{:.2f} KB".format(len(contents)/1024)},
                        "channel_id": channel_id, "status": 0,"record_details": {"total_records": total_records, "success": 0, "failed": 0},
                        "version_number":1,"created_by":1
                        }
            # recon_ref = f"RECON{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))}"
            fileSaveDetail = await UploadRepository.saveUploadedFileDetails(self.db, fileDetails)
            if fileSaveDetail.get("error"):
                return {
                    "status": "error",
                    "errors": True,
                    "message": fileSaveDetail.get("message"),
                    "data": []
                }
            # print(recon_ref)
            fileJson = {
                # "recon_reference_number": recon_ref,
                "channel_id" : channel_id,
                "source_id": source_id,
                "file_transactions_id": fileSaveDetail.get("insertedId"),
                "created_by": 1,
                "updated_by": 1,
                "version_number": 1
            }
            required_mappings = [
                m for m in mappings
                if m.get("required") is True
            ]

            getDateTimeColumnName = self.get_mapped_to_by_channel_column(required_mappings, "date")
            getAmountColumnName = self.get_mapped_to_by_channel_column(required_mappings, "amount")
            getAcountNumberColumnName = self.get_mapped_to_by_channel_column(required_mappings, "account_number")
            getCurrencyColumnName = self.get_mapped_to_by_channel_column(required_mappings, "currency")
            if total_records <= 20000:
                savefiledata = await UploadRepository.saveFileDetails(
                    self.db, 
                    fileData, 
                    fileJson,
                    getDateTimeColumnName,
                    getAmountColumnName,
                    getAcountNumberColumnName,
                    getCurrencyColumnName
                )
                return {
                    "status": "success",
                    "errors": False,
                    "message": "File uploaded successfully",
                    "data": savefiledata
                }
            else:
                await self.uploadWithCelery(fileData, total_records,getDateTimeColumnName,getAmountColumnName,getAcountNumberColumnName,getCurrencyColumnName)
                return {
                    "status": "success",
                    "errors": False,
                    "message": "The file is too large to process immediately and may take a significant amount of time.",
                    "data": []
                }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_mapped_to_by_channel_column(self, mappings: list[dict], channel_column: str) -> str | None:
        channel_column = str(channel_column).strip().lower()
        for m in mappings:
            if m.get("channel_column") and str(m["channel_column"]).strip().lower() == channel_column:
                return str(m.get("mapped_to")).strip().lower()
        return None

    async def uploadWithCelery(self, fileData, total_records, getDateTimeColumnName, getAmountColumnName, getAcountNumberColumnName, getCurrencyColumnName):
        # This is a placeholder for the actual Celery task invocation
        try:
            # system_id = 101  # Example system ID
            # batch_config = await self.batch_service.getBatchConfiguration(system_id)
            # print("Batch Configuration:", batch_config)
            batch_size = 10  # You can adjust this according to your system capacity
            jobs = [fileData[i:i + batch_size] for i in range(0, total_records, batch_size)]
            print(f"Total batches to process: {len(jobs)}")

            task_ids = []

            for idx, batch in enumerate(jobs, start=1):
                task = start_recon_job.delay(
                    idx,
                    batch,
                    fileData,
                    total_records,
                    getDateTimeColumnName,
                    getAmountColumnName,
                    getAcountNumberColumnName,
                    getCurrencyColumnName
                )

                task_ids.append({
                    "task_id": task.id,
                    "batch_number": idx,
                    "batch_size": len(batch),
                    "status": "QUEUED"
                })

            return {
                "message": "File accepted and processing has started",
                "total_batches": len(jobs),
                "tasks": task_ids
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
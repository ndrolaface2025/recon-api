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
# from app.workers.tasks import process_batch
# from app.tasks.test_tasks import process_batch

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}

class UploadService:

    def __init__(self, db: AsyncSession):
        self.db = db
    
    def is_allowed_file(self, filename: str) -> bool:
        ext = os.path.splitext(filename)[1].lower()
        return ext in ALLOWED_EXTENSIONS
    
    async def fileUpload(self,  file):
        start_time = time.time()

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

            columns = df.columns.tolist()
            total_records = len(df)
            # fileData = df.head(5).to_dict(orient="records")
            df.columns = [str(col).strip().lower() for col in df.columns]
            fileData = df.to_dict(orient="records")
            fileDetails = {"file_name": file.filename, "file_details": {"file_type": 1, "file_size": "{:.2f} KB".format(len(contents)/1024)},
                        "channel_id": 1, "status": 0,"record_details": {"total_records": len(df), "success": 0, "failed": 0},
                        "version_number":1,"created_by":1
                        }
            recon_ref = f"RECON{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))}"
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
                "recon_reference_number": recon_ref,
                "channel_id" : 1,
                "source_id": 17,
                "file_transactions_id": fileSaveDetail.get("insertedId"),
                "created_by": 1,
                "updated_by": 1,
                "version_number": 1
            }
            savefiledata = await UploadRepository.saveFileDetails(self.db, fileData, fileJson)
            return {
                "status": "success",
                "errors": False,
                "message": "File uploaded successfully",
                # "data": {
                #     "ok": True,
                #     "filename": file.filename,
                #     "extension": ext,
                #     "total_records": total_records,
                #     "columns": columns,
                #     "preview": fileData,
                #     "time_taken_sec": round(time.time() - start_time, 3)
                # }
                "data": savefiledata
            }


        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    async def uploadWithCelery(self, file):
        # This is a placeholder for the actual Celery task invocation
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

            columns = df.columns.tolist()
            total_records = len(df)
            df.columns = [str(col).strip().lower() for col in df.columns]
            fileData = df.to_dict(orient="records")

            batch_size = 10  # You can adjust this according to your system capacity
            jobs = [fileData[i:i + batch_size] for i in range(0, total_records, batch_size)]

            task_ids = []
            # for idx, batch in enumerate(jobs, start=1):
            #     try:
            #         task = process_batch.delay(idx,'testing', batch)
            #         print(f"✓ Task for batch {idx} queued with id: {task.id}")
            #         task_ids.append({
            #             'task_id': task.id,
            #             'batch_number': idx,
            #             'batch_size': len(batch),
            #             'status': 'QUEUED'
            #         })
            #     except Exception as e:
            #         print(f"✗ Failed to queue batch {idx}: {e}")
            
            #     print(f"\nTotal tasks queued: {len([t for t in task_ids if t['task_id']])}")

            # print("All tasks queued:", task_ids)

            return {
                "status": "success",
                "errors": False,
                "message": "File upload task has been queued",
                "data" : []
                # "task_id": task.id
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
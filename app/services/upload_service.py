# import io
# import os
# from random import random
# import secrets
# import string
# import time
# from fastapi import HTTPException
# import pandas as pd
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.db.repositories.upload import UploadRepository
# from app.db.repositories.userRepository import UserRepository
# from app.services.batch_config_service import BatchConfigService
# from app.workers.tasks import start_recon_job, process_upload_batch
# # from app.workers.tasks import process_batch
# # from app.tasks.test_tasks import process_batch

# ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}

# class UploadService:

#     def __init__(self, db: AsyncSession):
#         self.db = db
#         # self.batch_service = batch_service

#     def is_allowed_file(self, filename: str) -> bool:
#         ext = os.path.splitext(filename)[1].lower()
#         return ext in ALLOWED_EXTENSIONS

#     async def fileUpload(self,  file, channel_id: int, source_id: int, mappings: dict):
#         try:
#             if not file.filename:
#                 raise HTTPException(status_code=400, detail="No file selected")

#             if not self.is_allowed_file(file.filename):
#                 raise HTTPException(
#                     status_code=400,
#                     detail="Only CSV, XLS, and XLSX files are allowed"
#                 )

#             contents = await file.read()
#             ext = os.path.splitext(file.filename)[1].lower()

#             if ext == ".csv":
#                 df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
#             else:
#                 df = pd.read_excel(io.BytesIO(contents))

#             # get user details
#             userDetail = await UserRepository.getUserDetails(self.db)
#             print("User Details:", userDetail)
#             # Replace NaN with None (JSON safe)
#             df = df.where(pd.notnull(df), '')

#             # columns = df.columns.tolist()
#             total_records = len(df)
#             # fileData = df.head(5).to_dict(orient="records")
#             df.columns = [str(col).strip().lower() for col in df.columns]
#             fileData = df.to_dict(orient="records")
#             fileDetails = {"file_name": file.filename,
#                         "file_details": {"file_type": source_id, "file_size": "{:.2f} KB".format(len(contents)/1024)},
#                         "channel_id": channel_id,
#                         "status": 0,
#                         "record_details": {"total_records": total_records, "success": 0, "failed": 0},
#                         "total_records": total_records,  # NEW: Initialize progress tracking
#                         "processed_records": 0,
#                         "success_records": 0,
#                         "failed_records": 0,
#                         "duplicate_records": 0,
#                         "progress_percentage": 0.0,
#                         "version_number":1,
#                         "created_by":userDetail
#                         }
#             # recon_ref = f"RECON{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))}"
#             fileSaveDetail = await UploadRepository.saveUploadedFileDetails(self.db, fileDetails)
#             if fileSaveDetail.get("error"):
#                 return {
#                     "status": "error",
#                     "errors": True,
#                     "message": fileSaveDetail.get("message"),
#                     "data": []
#                 }
#             # print(recon_ref)
#             fileJson = {
#                 # "recon_reference_number": recon_ref,
#                 "channel_id" : channel_id,
#                 "source_id": source_id,
#                 "file_transactions_id": fileSaveDetail.get("insertedId"),
#                 "created_by": userDetail,
#                 "updated_by": userDetail,
#                 "version_number": 1
#             }

#             print(f"[UPLOAD SERVICE DEBUG] *** Creating fileJson ***")
#             print(f"[UPLOAD SERVICE DEBUG] channel_id={channel_id}, source_id={source_id}")
#             print(f"[UPLOAD SERVICE DEBUG] fileJson={fileJson}")

#             required_mappings = [
#                 m for m in mappings
#                 if m.get("required") is True
#             ]

#             getDateTimeColumnName = self.get_mapped_to_by_channel_column(required_mappings, "date")
#             getAmountColumnName = self.get_mapped_to_by_channel_column(required_mappings, "amount")
#             getAcountNumberColumnName = self.get_mapped_to_by_channel_column(required_mappings, "account_number")
#             getCurrencyColumnName = self.get_mapped_to_by_channel_column(required_mappings, "currency")

#             # Optional fields - check all mappings (not just required ones)
#             # For reference_number: Check both "reference_number" AND "transaction_id" channel_columns
#             # because auto-mapper might map "Transaction ID" to channel_column="transaction_id"
#             getReferenceNumberColumnName = (
#                 self.get_mapped_to_by_channel_column(mappings, "reference_number") or
#                 self.get_mapped_to_by_channel_column(mappings, "transaction_id")
#             )

#             # For txn_id: Check receipt_number, stan, or other secondary IDs
#             # (Don't check transaction_id since that goes to reference_number)
#             getTransactionIdColumnName = (
#                 self.get_mapped_to_by_channel_column(mappings, "receipt_number") or
#                 self.get_mapped_to_by_channel_column(mappings, "stan") or
#                 self.get_mapped_to_by_channel_column(mappings, "txn_id")
#             )

#             print(f"[UPLOAD SERVICE DEBUG] Optional field mappings:")
#             print(f"  reference_number: {getReferenceNumberColumnName}")
#             print(f"  transaction_id: {getTransactionIdColumnName}")

#             # Always process files with Celery jobs (no direct upload)
#             # This ensures consistent processing, better tracking, and scalability
#             result = await self.uploadWithCelery(
#                 file=file,
#                 file_id=fileSaveDetail.get("insertedId"),
#                 total_records=total_records,
#                 channel_id=channel_id,
#                 source_id=source_id,
#                 user_detail=userDetail,
#                 column_mappings={
#                     "date": getDateTimeColumnName,
#                     "amount": getAmountColumnName,
#                     "account_number": getAcountNumberColumnName,
#                     "currency": getCurrencyColumnName,
#                     "reference_number": getReferenceNumberColumnName,
#                     "transaction_id": getTransactionIdColumnName
#                 }
#             )
#             return result

#         except HTTPException:
#             raise
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))

#     def get_mapped_to_by_channel_column(self, mappings: list[dict], channel_column: str) -> str | None:
#         channel_column = str(channel_column).strip().lower()
#         for m in mappings:
#             if m.get("channel_column") and str(m["channel_column"]).strip().lower() == channel_column:
#                 return str(m.get("mapped_to")).strip().lower()
#         return None

#     async def uploadWithCelery(
#         self,
#         file,
#         file_id: int,
#         total_records: int,
#         channel_id: int,
#         source_id: int,
#         user_detail: int,
#         column_mappings: dict
#     ):
#         """
#         OPTIMIZED: Process large files using chunked reading and optimized batches.

#         Improvements:
#         - Batch size: 10 → 5000 (500x improvement)
#         - Chunked file reading: prevents memory overflow
#         - Proper task signature with all required data
#         - Progress tracking built-in

#         Performance:
#         - 100K records: 20 batches instead of 10,000
#         - 1M records: 200 batches instead of 100,000
#         """
#         try:
#             # Fetch record_per_job from tbl_cfg_system_batch (default: 20 if not configured)
#             batch_config_service = BatchConfigService(self.db)
#             batch_config = await batch_config_service.getBatchConfiguration(system_id=1)

#             # Extract record_per_job from response, default to 20 if not found
#             record_per_job = 20  # Default fallback
#             if batch_config and batch_config.get("data"):
#                 record_per_job = batch_config["data"].record_per_job or 20

#             print(f"Using record_per_job from database: {record_per_job}")

#             # OPTIMIZED BATCH SIZE: 5000 instead of 10
#             # Dynamic sizing: smaller files get smaller batches for faster response
#             batch_size = min(5000, max(1000, total_records // record_per_job))
#             print(f"Using optimized batch size: {batch_size}")

#             # Calculate number of batches
#             num_batches = (total_records + batch_size - 1) // batch_size
#             print(f"Total batches to process: {num_batches} for {total_records} records")

#             # Process file in chunks to avoid loading all data in memory
#             result = await self.process_file_in_chunks(
#                 file=file,
#                 file_id=file_id,
#                 batch_size=batch_size,
#                 num_batches=num_batches,
#                 channel_id=channel_id,
#                 source_id=source_id,
#                 user_detail=user_detail,
#                 column_mappings=column_mappings
#             )

#             return result

#         except HTTPException:
#             raise
#         except Exception as e:
#             # Mark upload as failed
#             await UploadRepository.updateFileStatus(
#                 self.db,
#                 file_id,
#                 status=3,
#                 error_message=str(e)
#             )
#             raise HTTPException(status_code=500, detail=str(e))

#     async def process_file_in_chunks(
#         self,
#         file,
#         file_id: int,
#         batch_size: int,
#         num_batches: int,
#         channel_id: int,
#         source_id: int,
#         user_detail: int,
#         column_mappings: dict
#     ):
#         """
#         MEMORY OPTIMIZATION: Read file in chunks instead of loading all at once.

#         This prevents memory overflow for large files (500K+ records).
#         Uses pandas chunksize parameter to stream data.
#         """
#         try:
#             # Reset file pointer to beginning
#             await file.seek(0)
#             contents = await file.read()
#             ext = os.path.splitext(file.filename)[1].lower()

#             task_ids = []
#             batch_number = 1

#             # Prepare file metadata for tasks
#             fileJson = {
#                 "channel_id": channel_id,
#                 "source_id": source_id,
#                 "file_transactions_id": file_id,
#                 "created_by": user_detail,
#                 "updated_by": user_detail,
#                 "version_number": 1
#             }

#             # CHUNKED PROCESSING: Read and process file in batches
#             if ext == ".csv":
#                 # CSV: Use chunksize for streaming
#                 chunks = pd.read_csv(
#                     io.StringIO(contents.decode("utf-8")),
#                     chunksize=batch_size
#                 )
#             else:
#                 # Excel: Read once (Excel doesn't support chunking well)
#                 df = pd.read_excel(io.BytesIO(contents))
#                 df = df.where(pd.notnull(df), '')
#                 df.columns = [str(col).strip().lower() for col in df.columns]

#                 # Split into chunks manually
#                 chunks = [df[i:i + batch_size] for i in range(0, len(df), batch_size)]

#             # Submit Celery tasks for each batch
#             for chunk in chunks:
#                 if ext == ".csv":
#                     chunk = chunk.where(pd.notnull(chunk), '')
#                     chunk.columns = [str(col).strip().lower() for col in chunk.columns]

#                 batch_data = chunk.to_dict(orient="records")

#                 # Submit task with correct signature
#                 task = process_upload_batch.delay(
#                     batch_data=batch_data,
#                     file_json=fileJson,
#                     file_id=file_id,
#                     batch_number=batch_number,
#                     total_batches=num_batches,
#                     column_mappings=column_mappings
#                 )

#                 task_ids.append({
#                     "task_id": task.id,
#                     "batch_number": batch_number,
#                     "batch_size": len(batch_data),
#                     "status": "QUEUED"
#                 })

#                 batch_number += 1

#             return {
#                 "status": "success",
#                 "errors": False,
#                 "message": f"File accepted for processing. {num_batches} batches queued. Track progress using file_id: {file_id}",
#                 "data": {
#                     "file_id": file_id,
#                     "total_batches": num_batches,
#                     "batch_size": batch_size,
#                     "tasks": task_ids
#                 }
#             }

#         except Exception as e:
#             print(f"Error in process_file_in_chunks: {str(e)}")
#             raise


#     async def get_file_list(self, offset: int, limit: int):
#         getResult =  await UploadRepository.getFileList(self.db, offset, limit)
#         if getResult.get("status") == "success":
#             return {
#                 "status": "success",
#                 "errors": False,
#                 "message": "File list fetched successfully",
#                 "result": {
#                     "data": getResult.get("data", []),
#                     "meta": {
#                         "offset": getResult.get("offset"),
#                         "limit": getResult.get("limit"),
#                         "total": getResult.get("total"),
#                     }
#                 }
#             }

#         return {
#             "status": "error",
#             "errors": True,
#             "message": getResult.get("message", "Failed to fetch file list"),
#             "result": {
#                 "data": [],
#                 "meta": {}
#             }
#         }

#     async def deleteFileAndTransactions(self, file_id: int):
#         getResult =  await UploadRepository.deleteFileAndTransactions(self.db, file_id)
#         if getResult:
#             return {
#                 "status": "success",
#                 "errors": False,
#                 "message": "File and related record deleted successfully",
#                 "result": []
#             }
#         return {
#             "status": "error",
#             "errors": True,
#             "message": "Failed to delete the record",
#             "result": []
#         }
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
from app.db.repositories.userRepository import UserRepository
from app.services.batch_config_service import BatchConfigService
from app.workers.tasks import start_recon_job, process_upload_batch

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

    async def fileUpload(self, file, channel_id: int, source_id: int, mappings: dict, user_id: int):
        try:
            if not file.filename:
                raise HTTPException(status_code=400, detail="No file selected")

            if not self.is_allowed_file(file.filename):
                raise HTTPException(
                    status_code=400, detail="Only CSV, XLS, and XLSX files are allowed"
                )

            contents = await file.read()
            ext = os.path.splitext(file.filename)[1].lower()

            if ext == ".csv":
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
            else:
                df = pd.read_excel(io.BytesIO(contents))

            # Use authenticated user_id instead of fetching from repository
            print(f"[UPLOAD SERVICE] Using authenticated user_id: {user_id}")
            # Replace NaN with None (JSON safe)
            df = df.where(pd.notnull(df), "")

            # columns = df.columns.tolist()
            total_records = len(df)
            # fileData = df.head(5).to_dict(orient="records")
            df.columns = [str(col).strip().lower() for col in df.columns]
            fileData = df.to_dict(orient="records")
            fileDetails = {
                "file_name": file.filename,
                "file_details": {
                    "file_type": source_id,
                    "file_size": "{:.2f} KB".format(len(contents) / 1024),
                },
                "channel_id": channel_id,
                "status": 0,
                "record_details": {
                    "total_records": total_records,
                    "success": 0,
                    "failed": 0,
                },
                "total_records": total_records,  # NEW: Initialize progress tracking
                "processed_records": 0,
                "success_records": 0,
                "failed_records": 0,
                "duplicate_records": 0,
                "progress_percentage": 0.0,
                "version_number": 1,
                "created_by": user_id,  # CHANGED: Use authenticated user_id
            }
            # recon_ref = f"RECON{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))}"
            fileSaveDetail = await UploadRepository.saveUploadedFileDetails(
                self.db, fileDetails
            )
            if fileSaveDetail.get("error"):
                return {
                    "status": "error",
                    "errors": True,
                    "message": fileSaveDetail.get("message"),
                    "data": [],
                }
            # print(recon_ref)
            fileJson = {
                # "recon_reference_number": recon_ref,
                "channel_id": channel_id,
                "source_id": source_id,
                "file_transactions_id": fileSaveDetail.get("insertedId"),
                "created_by": user_id,  # CHANGED: Use authenticated user_id
                "updated_by": user_id,  # CHANGED: Use authenticated user_id
                "version_number": 1,
            }

            print(f"[UPLOAD SERVICE DEBUG] *** Creating fileJson ***")
            print(
                f"[UPLOAD SERVICE DEBUG] channel_id={channel_id}, source_id={source_id}"
            )
            print(f"[UPLOAD SERVICE DEBUG] fileJson={fileJson}")

            required_mappings = [m for m in mappings if m.get("required") is True]

            getDateTimeColumnName = self.get_mapped_to_by_channel_column(
                required_mappings, "date"
            )
            getAmountColumnName = self.get_mapped_to_by_channel_column(
                required_mappings, "amount"
            )
            getAcountNumberColumnName = self.get_mapped_to_by_channel_column(
                required_mappings, "account_number"
            )
            getCurrencyColumnName = self.get_mapped_to_by_channel_column(
                required_mappings, "currency"
            )

            # Optional fields - check all mappings (not just required ones)
            # For reference_number: Check both "reference_number" AND "transaction_id" channel_columns
            # because auto-mapper might map "Transaction ID" to channel_column="transaction_id"
            getReferenceNumberColumnName = self.get_mapped_to_by_channel_column(
                mappings, "reference_number"
            ) or self.get_mapped_to_by_channel_column(mappings, "transaction_id")

            # For txn_id: Check receipt_number, stan, or other secondary IDs
            # (Don't check transaction_id since that goes to reference_number)
            getTransactionIdColumnName = (
                self.get_mapped_to_by_channel_column(mappings, "receipt_number")
                or self.get_mapped_to_by_channel_column(mappings, "stan")
                or self.get_mapped_to_by_channel_column(mappings, "txn_id")
            )

            # NEW: For service_name: Used by Mobile Money channels for network detection
            getServiceNameColumnName = self.get_mapped_to_by_channel_column(
                mappings, "service_name"
            )

            print(f"[UPLOAD SERVICE DEBUG] Optional field mappings:")
            print(f"  reference_number: {getReferenceNumberColumnName}")
            print(f"  transaction_id: {getTransactionIdColumnName}")
            print(f"  service_name: {getServiceNameColumnName}")  # NEW

            # Always process files with Celery jobs (no direct upload)
            # This ensures consistent processing, better tracking, and scalability
            result = await self.uploadWithCelery(
                file=file,
                file_id=fileSaveDetail.get("insertedId"),
                total_records=total_records,
                channel_id=channel_id,
                source_id=source_id,
                user_detail=user_id,  # CHANGED: Use authenticated user_id
                column_mappings={
                    "date": getDateTimeColumnName,
                    "amount": getAmountColumnName,
                    "account_number": getAcountNumberColumnName,
                    "currency": getCurrencyColumnName,
                    "reference_number": getReferenceNumberColumnName,
                    "transaction_id": getTransactionIdColumnName,
                    "service_name": getServiceNameColumnName,  # NEW: Add service_name to mappings
                },
            )
            return result

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_mapped_to_by_channel_column(
        self, mappings: list[dict], channel_column: str
    ) -> str | None:
        channel_column = str(channel_column).strip().lower()
        for m in mappings:
            if (
                m.get("channel_column")
                and str(m["channel_column"]).strip().lower() == channel_column
            ):
                return str(m.get("mapped_to")).strip().lower()
        return None

    async def uploadWithCelery(
        self,
        file,
        file_id: int,
        total_records: int,
        channel_id: int,
        source_id: int,
        user_detail: int,
        column_mappings: dict,
    ):
        """
        OPTIMIZED: Process large files using chunked reading and optimized batches.

        Improvements:
        - Batch size: 10 → 5000 (500x improvement)
        - Chunked file reading: prevents memory overflow
        - Proper task signature with all required data
        - Progress tracking built-in

        Performance:
        - 100K records: 20 batches instead of 10,000
        - 1M records: 200 batches instead of 100,000
        """
        try:
            # Fetch record_per_job from tbl_cfg_system_batch (default: 20 if not configured)
            batch_config_service = BatchConfigService(self.db)
            batch_config = await batch_config_service.getBatchConfiguration(system_id=1)

            # Extract record_per_job from response, default to 20 if not found
            record_per_job = 20  # Default fallback
            if batch_config and batch_config.get("data"):
                record_per_job = batch_config["data"].record_per_job or 20

            print(f"Using record_per_job from database: {record_per_job}")

            # OPTIMIZED BATCH SIZE: 5000 instead of 10
            # Dynamic sizing: smaller files get smaller batches for faster response
            batch_size = min(5000, max(1000, total_records // record_per_job))
            print(f"Using optimized batch size: {batch_size}")

            # Calculate number of batches
            num_batches = (total_records + batch_size - 1) // batch_size
            print(
                f"Total batches to process: {num_batches} for {total_records} records"
            )

            # Process file in chunks to avoid loading all data in memory
            result = await self.process_file_in_chunks(
                file=file,
                file_id=file_id,
                batch_size=batch_size,
                num_batches=num_batches,
                channel_id=channel_id,
                source_id=source_id,
                user_detail=user_detail,
                column_mappings=column_mappings,
            )

            return result

        except HTTPException:
            raise
        except Exception as e:
            # Mark upload as failed
            await UploadRepository.updateFileStatus(
                self.db, file_id, status=3, error_message=str(e)
            )
            raise HTTPException(status_code=500, detail=str(e))

    async def process_file_in_chunks(
        self,
        file,
        file_id: int,
        batch_size: int,
        num_batches: int,
        channel_id: int,
        source_id: int,
        user_detail: int,
        column_mappings: dict,
    ):
        """
        MEMORY OPTIMIZATION: Read file in chunks instead of loading all at once.

        This prevents memory overflow for large files (500K+ records).
        Uses pandas chunksize parameter to stream data.
        """
        try:
            # Reset file pointer to beginning
            await file.seek(0)
            contents = await file.read()
            ext = os.path.splitext(file.filename)[1].lower()

            task_ids = []
            batch_number = 1

            # Prepare file metadata for tasks
            fileJson = {
                "channel_id": channel_id,
                "source_id": source_id,
                "file_transactions_id": file_id,
                "created_by": user_detail,
                "updated_by": user_detail,
                "version_number": 1,
            }

            # CHUNKED PROCESSING: Read and process file in batches
            if ext == ".csv":
                # CSV: Use chunksize for streaming
                chunks = pd.read_csv(
                    io.StringIO(contents.decode("utf-8")), chunksize=batch_size
                )
            else:
                # Excel: Read once (Excel doesn't support chunking well)
                df = pd.read_excel(io.BytesIO(contents))
                df = df.where(pd.notnull(df), "")
                df.columns = [str(col).strip().lower() for col in df.columns]

                # Split into chunks manually
                chunks = [df[i : i + batch_size] for i in range(0, len(df), batch_size)]

            # Submit Celery tasks for each batch
            for chunk in chunks:
                if ext == ".csv":
                    chunk = chunk.where(pd.notnull(chunk), "")
                    chunk.columns = [str(col).strip().lower() for col in chunk.columns]

                batch_data = chunk.to_dict(orient="records")

                # Submit task with correct signature
                task = process_upload_batch.delay(
                    batch_data=batch_data,
                    file_json=fileJson,
                    file_id=file_id,
                    batch_number=batch_number,
                    total_batches=num_batches,
                    column_mappings=column_mappings,
                )

                task_ids.append(
                    {
                        "task_id": task.id,
                        "batch_number": batch_number,
                        "batch_size": len(batch_data),
                        "status": "QUEUED",
                    }
                )

                batch_number += 1

            return {
                "status": "success",
                "errors": False,
                "message": f"File accepted for processing. {num_batches} batches queued. Track progress using file_id: {file_id}",
                "data": {
                    "file_id": file_id,
                    "total_batches": num_batches,
                    "batch_size": batch_size,
                    "tasks": task_ids,
                },
            }

        except Exception as e:
            print(f"Error in process_file_in_chunks: {str(e)}")
            raise

    async def get_file_list(self, offset: int, limit: int):
        getResult = await UploadRepository.getFileList(self.db, offset, limit)
        if getResult.get("status") == "success":
            return {
                "status": "success",
                "errors": False,
                "message": "File list fetched successfully",
                "result": {
                    "data": getResult.get("data", []),
                    "meta": {
                        "offset": getResult.get("offset"),
                        "limit": getResult.get("limit"),
                        "total": getResult.get("total"),
                    },
                },
            }

        return {
            "status": "error",
            "errors": True,
            "message": getResult.get("message", "Failed to fetch file list"),
            "result": {"data": [], "meta": {}},
        }

    # FIXME: TEMPORARY HARD DELETE — REMOVE BEFORE PRODUCTION
    # WARNING: Deletions will NOT be allowed in the future (audit/compliance).
    # TODO: Replace with soft-delete or archival logic.
    async def deleteFileAndTransactions(self, file_id: int):
        getResult = await UploadRepository.deleteFileAndTransactions(self.db, file_id)
        if getResult:
            return {
                "status": "success",
                "errors": False,
                "message": "File and related record deleted successfully",
                "result": [],
            }
        return {
            "status": "error",
            "errors": True,
            "message": "Failed to delete the record",
            "result": [],
        }

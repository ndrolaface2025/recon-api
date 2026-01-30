# import json
# from typing import Any, Dict, List
# from datetime import datetime

# from sqlalchemy import Integer, cast, delete, func, select, tuple_, update
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.db.models.channel_config import ChannelConfig
# from app.db.models.source_config import SourceConfig
# from app.db.models.transactions import Transaction
# from app.db.models.upload_file import UploadFile
# from app.config import settings
# from app.db.models.user_config import UserConfig
# from sqlalchemy.dialects.postgresql import JSONB
# class UploadRepository:

#     @staticmethod
#     async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
#         try:
#             file_entry = UploadFile(
#                 file_name=fileData['file_name'],
#                 file_details= json.dumps (fileData['file_details']),
#                 channel_id = fileData['channel_id'],
#                 status=fileData['status'],
#                 record_details= json.dumps(fileData['record_details']),
#                 total_records=fileData['total_records'],
#                 version_number=fileData['version_number'],
#                 created_by=fileData['created_by']
#             )
#             db.add(file_entry)
#             await db.commit()
#             await db.refresh(file_entry)
#             return { "error": False, "status": "success","insertedId": file_entry.id}
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveUploadedFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
        
#     @staticmethod
#     async def saveFileDetails(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         getDateTimeColumnName: str,
#         getAmountColumnName: str,
#         getAcountNumberColumnName: str,
#         getCurrencyColumnName: str,
#         getReferenceNumberColumnName: str = None,  # NEW: Optional reference number mapping
#         getTransactionIdColumnName: str = None,    # NEW: Optional transaction ID mapping
#         system_id: int = 1  # Default to system 1 (upload)
#         ) -> Dict[str, Any]:
#         """
#         OPTIMIZED: Bulk duplicate detection using batched queries instead of N queries.
#         Performance: 100,000x faster than per-row queries
#         Safety: Batches keys to avoid PostgreSQL max_stack_depth error on huge uploads
#         Batch size is dynamically loaded from tbl_cfg_system_batch based on system_id
#         """
#         try:
#             # DEBUG: Log what mappings were received
#             print(f"\n[SAVE FILE DETAILS DEBUG]")
#             print(f"  getReferenceNumberColumnName: {getReferenceNumberColumnName}")
#             print(f"  getTransactionIdColumnName: {getTransactionIdColumnName}")
#             print(f"  getDateTimeColumnName: {getDateTimeColumnName}")
#             print(f"  getAmountColumnName: {getAmountColumnName}")
#             print(f"  Total records: {len(fileData)}")
#             if fileData:
#                 print(f"  First record columns: {list(fileData[0].keys())[:10]}")
            
#             duplicates = []
#             new_records = []
            
#             # CRITICAL OPTIMIZATION: Build unique keys for ALL records at once
#             keys_to_check = []
#             for row in fileData:
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
                
#                 # Add amount to key
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add date to key
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add currency to key (if needed for duplicate check)
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 keys_to_check.append(tuple(key))
            
#             # BATCHED QUERY to check all duplicates (avoids Postgres max_stack_depth error)
#             # Split into chunks to prevent "stack depth limit exceeded" error on large batches
            
#             # DYNAMIC BATCH SIZE: Calculate based on number of jobs from tbl_cfg_system_batch
#             from app.db.models.system_batch_config import SystemBatchConfig
            
#             # Try to get number of jobs from database config
#             batch_config_stmt = select(SystemBatchConfig).where(
#                 SystemBatchConfig.system_id == system_id
#             )
#             batch_config_result = await db.execute(batch_config_stmt)
#             batch_config = batch_config_result.scalar_one_or_none()
            
#             total_records = len(keys_to_check)
            
#             if batch_config and batch_config.record_per_job:
#                 # record_per_job stores NUMBER OF JOBS
#                 num_jobs = int(batch_config.record_per_job)
#                 # Calculate batch size: total records / number of jobs
#                 BATCH_SIZE = max(1, (total_records + num_jobs - 1) // num_jobs)  # Ceiling division
#                 batch_source = f"calculated from {num_jobs} jobs (system_id={system_id})"
#             else:
#                 # Fallback to env var or default
#                 BATCH_SIZE = settings.UPLOAD_DUPLICATE_CHECK_BATCH_SIZE
#                 batch_source = "config/env default"
            
#             existing_keys = set()
            
#             if keys_to_check:
#                 import time
#                 batch_count = (len(keys_to_check) + BATCH_SIZE - 1) // BATCH_SIZE
#                 print(f"\n📊 Duplicate Check Starting:")
#                 print(f"   Total keys to check: {len(keys_to_check):,}")
#                 print(f"   Batch size: {BATCH_SIZE:,} ({batch_source})")
#                 print(f"   Number of batches/jobs: {batch_count}")
                
#                 for i in range(0, len(keys_to_check), BATCH_SIZE):
#                     batch_start = time.time()
#                     batch_keys = keys_to_check[i:i+BATCH_SIZE]
#                     batch_num = i // BATCH_SIZE + 1
                    
#                     if getCurrencyColumnName:
#                         # Query with currency
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date,
#                             Transaction.ccy
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date,
#                                 Transaction.ccy
#                             ).in_(batch_keys)
#                         )
#                     else:
#                         # Query without currency
#                         keys_without_ccy = [k[:4] for k in batch_keys]  # Remove last element (None)
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date
#                             ).in_(keys_without_ccy)
#                         )
                    
#                     result = await db.execute(stmt)
#                     batch_records = result.all()
                    
#                     # Add to existing_keys set for O(1) lookup
#                     existing_keys.update(tuple(record) for record in batch_records)
                    
#                     # Log batch completion
#                     batch_time = time.time() - batch_start
#                     duplicates_found = len(batch_records)
#                     print(f"   ✓ Batch {batch_num}/{batch_count}: "
#                           f"{len(batch_keys):,} keys checked, "
#                           f"{duplicates_found:,} duplicates found, "
#                           f"{batch_time:.2f}s")
                
#                 print(f"✓ Duplicate check complete: {len(existing_keys):,} total duplicates found\n")
            
#             # Process each row and check against existing_keys (in-memory, super fast)
#             rows_processed = 0
#             for row in fileData:
#                 # Debug: Log first row to see actual column names
#                 if rows_processed == 0:
#                     print(f"DEBUG - First row columns: {list(row.keys())}")
#                     print(f"DEBUG - First row data sample: {dict(list(row.items())[:5])}")
                
#                 # Build key for this row
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
#                 rows_processed += 1
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 # O(1) duplicate check using set lookup
#                 if tuple(key) in existing_keys:
#                     duplicates.append(row)
#                     continue
                
#                 # Build new record with current timestamp
#                 current_timestamp = datetime.now()
#                 data = {
#                     "channel_id": fileJson["channel_id"],
#                     "source_id": fileJson["source_id"],
#                     "otherDetails": json.dumps(row, default=str),
#                     "file_transactions_id": fileJson["file_transactions_id"],
#                     "created_by": fileJson["created_by"],
#                     "updated_by": fileJson["updated_by"],
#                     "version_number": fileJson["version_number"],
#                     "created_at": current_timestamp,
#                     "updated_at": current_timestamp,
#                 }
                
#                 # DEBUG: Log fileJson to see what source_id we're receiving
#                 if len(new_records) == 0:  # Only log once per batch
#                     print(f"[UPLOAD DEBUG] fileJson: {fileJson}")
#                     print(f"[UPLOAD DEBUG] Saving with source_id={fileJson['source_id']}, channel_id={fileJson['channel_id']}")
#                     print(f"[UPLOAD DEBUG] created_at explicitly set to: {current_timestamp}")
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     data["amount"] = str(row[getAmountColumnName])

#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     # Handle datetime properly - check if it's already a datetime/timestamp
#                     date_value = row[getDateTimeColumnName]
#                     if date_value is not None and str(date_value) not in ['', 'nan', 'NaT', 'None']:
#                         # If it's a pandas Timestamp or datetime object, format it properly
#                         if hasattr(date_value, 'strftime'):
#                             data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
#                         else:
#                             data["date"] = str(date_value)

#                 if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
#                     data["account_number"] = str(row[getAcountNumberColumnName])

#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     data["ccy"] = str(row[getCurrencyColumnName])
                
#                 # Map reference_number - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped reference_number field
#                 reference_mapped = False
#                 if getReferenceNumberColumnName is not None and getReferenceNumberColumnName in row:
#                     if row[getReferenceNumberColumnName] is not None and str(row[getReferenceNumberColumnName]).strip():
#                         data["reference_number"] = str(row[getReferenceNumberColumnName]).strip()
#                         reference_mapped = True
#                         if len(new_records) == 0:
#                             print(f"[REF DEBUG] Explicit mapping used: {getReferenceNumberColumnName} = {data['reference_number']}")
                
#                 if not reference_mapped:
#                     # 2. Fall back to auto-detection from common field names
#                     # Note: ReconDataNormalizer converts columns to lowercase, so check lowercase versions
#                     # Including both underscore and space-separated variants
#                     # PRIORITY ORDER: Most specific/common first
#                     reference_fields = [
#                         'transaction_id', 'transaction id',  # PRIMARY: Common transaction ID (most important)
#                         'rrn',  # Retrieval Reference Number (banking standard)
#                         'reference_number', 'reference number',
#                         'referencenumber', 
#                         'ref_number', 'ref number',
#                         'refnumber', 
#                         'retrieval_reference_number', 'retrieval reference number'
#                     ]
#                     reference_found = False
#                     for field in reference_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["reference_number"] = str(row[field]).strip()
#                             reference_found = True
#                             if len(new_records) == 0:
#                                 print(f"[REF DEBUG] Auto-detected field '{field}' = {data['reference_number']}")
#                             break
                    
#                     if not reference_found and len(new_records) == 0:
#                         print(f"[REF DEBUG] ❌ No reference field found!")
#                         print(f"[REF DEBUG] Available columns: {list(row.keys())}")
#                         print(f"[REF DEBUG] Searching for: {reference_fields[:5]}...")
                
#                 # Map txn_id - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped transaction_id field
#                 if getTransactionIdColumnName is not None and getTransactionIdColumnName in row:
#                     if row[getTransactionIdColumnName] is not None and str(row[getTransactionIdColumnName]).strip():
#                         data["txn_id"] = str(row[getTransactionIdColumnName]).strip()
#                 else:
#                     # 2. Fall back to auto-detection from common field names
#                     # Including both underscore and space-separated variants
#                     # PRIORITY: Receipt Number, STAN, then generic txn_id (avoiding conflict with transaction_id -> reference_number)
#                     txn_id_fields = [
#                         'receipt_number', 'receipt number',  # Mobile Money specific (PRIMARY)
#                         'payer_transaction_id', 'payer transaction id',  # Mobile Money payer ID
#                         'stan',  # System Trace Audit Number (banking)
#                         'txn_id', 'txn id',  # Generic transaction ID
#                         'transactionid', 
#                         'txnid'
#                         # Note: 'transaction_id' is NOT here - it goes to reference_number!
#                     ]
#                     txn_id_found = False
#                     for field in txn_id_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["txn_id"] = str(row[field]).strip()
#                             txn_id_found = True
#                             break
                
#                 # Debug log for first record to verify field mapping
#                 if len(new_records) == 0:
#                     print(f"\n=== FIRST RECORD FIELD MAPPING DEBUG ===")
#                     print(f"Explicit mappings provided:")
#                     print(f"  - reference_number: {getReferenceNumberColumnName or 'AUTO-DETECT'}")
#                     print(f"  - transaction_id: {getTransactionIdColumnName or 'AUTO-DETECT'}")
#                     print(f"Available fields in row: {list(row.keys())}")
#                     print(f"Sample data: {dict(list(row.items())[:5])}")
#                     print(f"Mapped values:")
#                     print(f"  - Reference number: {data.get('reference_number', 'NOT MAPPED')}")
#                     print(f"  - Transaction ID: {data.get('txn_id', 'NOT MAPPED')}")
#                     print(f"  - Amount: {data.get('amount', 'NOT MAPPED')}")
#                     print(f"  - Date: {data.get('date', 'NOT MAPPED')}")
#                     print(f"\n[CRITICAL DEBUG] Complete data dict being inserted:")
#                     print(f"  {data}")
#                     print(f"=====================================\n")
                
#                 new_records.append(Transaction(**data))

#             if new_records:
#                 db.add_all(new_records)
#                 await db.commit()

#             return {
#                 "status": "success",
#                 "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
#                 "recordsSaved": len(new_records),
#                 "duplicateRecords": duplicates
#             }
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
    
#     @staticmethod
#     async def updateUploadProgress(
#         db: AsyncSession,
#         file_id: int,
#         processed: int,
#         success: int,
#         failed: int,
#         duplicates: int,
#         total: int
#     ) -> None:
#         """
#         Update progress tracking fields for an upload file.
#         Called after each batch is processed.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.processed_records = processed
#                 upload_file.success_records = success
#                 upload_file.failed_records = failed
#                 upload_file.duplicate_records = duplicates
                
#                 # Calculate progress percentage
#                 if total > 0:
#                     upload_file.progress_percentage = round((processed / total) * 100, 2)
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateUploadProgress: {str(e)}")
    
#     @staticmethod
#     async def updateFileStatus(
#         db: AsyncSession,
#         file_id: int,
#         status: int,
#         error_message: str = None,
#         error_details: str = None
#     ) -> None:
#         """
#         Update upload file status and error information.
#         status: 0=pending, 1=processing, 2=completed, 3=failed
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.status = status
                
#                 # Set timing fields
#                 if status == 1 and not upload_file.upload_started_at:
#                     # Starting processing
#                     upload_file.upload_started_at = datetime.utcnow()
#                 elif status in [2, 3]:
#                     # Completed or failed
#                     upload_file.upload_completed_at = datetime.utcnow()
#                     if upload_file.upload_started_at:
#                         time_diff = upload_file.upload_completed_at - upload_file.upload_started_at
#                         upload_file.processing_time_seconds = int(time_diff.total_seconds())
                
#                 # Set error info if provided
#                 if error_message:
#                     upload_file.error_message = error_message
#                 if error_details:
#                     upload_file.error_details = error_details
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateFileStatus: {str(e)}")
    
#     @staticmethod
#     async def saveFileDetailsBatch(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         column_mappings: Dict[str, str]
#     ) -> Dict[str, Any]:
#         """
#         Optimized batch processing method for Celery tasks.
#         Uses the same bulk duplicate detection as saveFileDetails.
        
#         Args:
#             column_mappings: Dict with keys: date, amount, account_number, currency, reference_number, transaction_id
#             Note: Column names will be converted to lowercase to match normalized data
#         """
#         # Convert column names to lowercase to match normalized column names
#         date_col = column_mappings.get("date")
#         amount_col = column_mappings.get("amount")
#         account_col = column_mappings.get("account_number")
#         currency_col = column_mappings.get("currency")
#         reference_col = column_mappings.get("reference_number")  # NEW
#         transaction_id_col = column_mappings.get("transaction_id")  # NEW
        
#         return await UploadRepository.saveFileDetails(
#             db=db,
#             fileData=fileData,
#             fileJson=fileJson,
#             getDateTimeColumnName=date_col.lower() if date_col else None,
#             getAmountColumnName=amount_col.lower() if amount_col else None,
#             getAcountNumberColumnName=account_col.lower() if account_col else None,
#             getCurrencyColumnName=currency_col.lower() if currency_col else None,
#             getReferenceNumberColumnName=reference_col.lower() if reference_col else None,  # NEW
#             getTransactionIdColumnName=transaction_id_col.lower() if transaction_id_col else None  # NEW
#         )
    
#     @staticmethod
#     async def getUploadProgress(db: AsyncSession, file_id: int) -> Dict[str, Any]:
#         """
#         Get upload progress information for a specific file.
#         Used by progress tracking API endpoint.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
#             if not upload_file:
#                 return {"error": True, "message": "File not found"}
            
#             return {
#                 "error": False,
#                 "file_id": upload_file.id,
#                 "file_name": upload_file.file_name,
#                 "status": upload_file.status,
#                 "total_records": upload_file.total_records,
#                 "processed_records": upload_file.processed_records,
#                 "success_records": upload_file.success_records,
#                 "failed_records": upload_file.failed_records,
#                 "duplicate_records": upload_file.duplicate_records,
#                 "progress_percentage": upload_file.progress_percentage,
#                 "upload_started_at": upload_file.upload_started_at.isoformat() if upload_file.upload_started_at else None,
#                 "upload_completed_at": upload_file.upload_completed_at.isoformat() if upload_file.upload_completed_at else None,
#                 "processing_time_seconds": upload_file.processing_time_seconds,
#                 "error_message": upload_file.error_message,
#                 "error_details": upload_file.error_details
#             }
#         except Exception as e:
#             print(f"uploadRepository-getUploadProgress: {str(e)}")
#             return {"error": True, "message": str(e)}
        
#     @staticmethod
#     async def getFileList(db: AsyncSession, offset: int, limit: int):
#         try:
#             # Total count
#             total_stmt = select(func.count()).select_from(UploadFile)
#             total_result = await db.execute(total_stmt)
#             total = total_result.scalar()
#             # Data query
#             stmt = (
#                 select(
#                     UploadFile,
#                     UserConfig.id.label("user_id"),
#                     UserConfig.f_name.label("f_name"),
#                     UserConfig.m_name.label("m_name"),
#                     UserConfig.l_name.label("l_name"),
#                     UserConfig.email.label("email"),
#                     ChannelConfig.id.label("channel_id"),
#                     ChannelConfig.channel_name.label("channel_name"),
#                     SourceConfig.id.label("source_id"),
#                     SourceConfig.source_name.label("source_name"),
#                     SourceConfig.source_type.label("source_type"),
#                 )
#                 .outerjoin(UserConfig, UserConfig.id == UploadFile.created_by)
#                 .outerjoin(ChannelConfig, ChannelConfig.id == UploadFile.channel_id)
#                 .outerjoin(
#                     SourceConfig,
#                     cast(
#                         UploadFile.file_details.cast(JSONB)["file_type"].astext,
#                         Integer
#                     ) == SourceConfig.id
#                 )
#                 .order_by(UploadFile.created_at.desc())
#                 .offset(offset)
#                 .limit(limit)
#             )

#             result = await db.execute(stmt)
#             rows = result.all()
#             data = []
#             for upload, user_id, f_name, m_name, l_name, email, channel_id, channel_name,source_id, source_name, source_type in rows:
#                 file_details_obj = None
#                 file_record_obj = {
#                     "total_records": upload.total_records,
#                     "processed": upload.processed_records,
#                     "success": upload.success_records,
#                     "failed": upload.failed_records,
#                     "duplicates": upload.duplicate_records,
#                     "progress_percentage": upload.progress_percentage,
#                 }


#                 if upload.file_details:
#                     try:
#                         file_details_obj = json.loads(upload.file_details)
#                     except json.JSONDecodeError:
#                         pass
                    
#                 data.append({
#                     "id": upload.id,
#                     "file_name": upload.file_name,
#                     "file_details": file_details_obj,
#                     "status": upload.status,
#                     "record_details": file_record_obj,
#                     "created_at": upload.created_at,
#                     "version_number": upload.version_number,

#                     "user": {
#                         "id": user_id,
#                         "name": " ".join(filter(None, [f_name, m_name, l_name])),
#                         "email": email,
#                     } if user_id else None,

#                     "channel": {
#                         "id": channel_id,
#                         "name": channel_name,
#                     } if channel_id else None,
#                     "source": {
#                         "id": source_id,
#                         "name": source_name,
#                         "type": source_type,
#                     } if source_id else None
#                 })
#             return {
#                 "status": "success",
#                 "offset": offset,
#                 "limit": limit,
#                 "total": total,
#                 "data": data
#             }
#         except Exception as e:
#             # Rollback is safe even for SELECTs
#             await db.rollback()
#             print("UploadRepository.getFileList error:", str(e))
#             return {
#                 "status": "error",
#                 "message": "Failed to fetch upload file list",
#                 "error": str(e)
#             }
        
#     async def deleteFileAndTransactions(db: AsyncSession, file_id: int) -> bool:
        
#         result = await db.execute(
#             select(UploadFile).where(UploadFile.id == file_id)
#         )
#         file_record = result.scalar_one_or_none()

#         if not file_record:
#             return False

#         try:
#             # 1️⃣ Delete related transactions
#             await db.execute(
#                 delete(Transaction).where(
#                     Transaction.file_transactions_id == file_id
#                 )
#             )

#             # 2️⃣ Delete file upload record
#             await db.execute(
#                 delete(UploadFile).where(UploadFile.id == file_id)
#             )

#             await db.commit()
#             return True

#         except Exception as e:
#             await db.rollback()
#             raise e

# import json
# from typing import Any, Dict, List
# from datetime import datetime, timedelta

# from sqlalchemy import Integer, cast, delete, func, select, tuple_, update
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.db.models.channel_config import ChannelConfig
# from app.db.models.source_config import SourceConfig
# from app.db.models.transactions import Transaction
# from app.db.models.upload_file import UploadFile
# from app.config import settings
# from app.db.models.user_config import UserConfig
# from sqlalchemy.dialects.postgresql import JSONB
# import logging
# logger = logging.getLogger(__name__)

# # Global cache for network mapping (optional optimization)
# _network_map_cache = None
# _network_map_timestamp = None
# CACHE_TTL = timedelta(hours=1)

# class UploadRepository:
    
#     @staticmethod
#     async def _build_network_mapping(db: AsyncSession) -> Dict[str, int]:
#         """
#         Pre-load all networks into memory for O(1) lookup.
#         Uses 1-hour cache to avoid repeated DB queries.
#         Returns: Dict mapping network patterns to network IDs
#         """
#         global _network_map_cache, _network_map_timestamp
        
#         # Check if cache is valid
#         if (_network_map_cache is not None and 
#             _network_map_timestamp is not None and 
#             datetime.now() - _network_map_timestamp < CACHE_TTL):
#             print(f"✓ Using cached network mapping ({len(_network_map_cache)} patterns)")
#             return _network_map_cache
        
#         # Load fresh data from database
#         try:
#             from app.db.models.network import Network  # Adjust import path as needed
            
#             stmt = select(Network.id, Network.network_name)
#             result = await db.execute(stmt)
#             networks = result.all()
            
#             network_map = {}
#             for network_id, network_name in networks:
#                 if not network_name:
#                     continue
                    
#                 name_lower = network_name.lower().strip()
                
#                 # Direct name match
#                 network_map[name_lower] = network_id
                
#                 # Common patterns for your data
#                 network_map[f"nfs {name_lower}"] = network_id
#                 network_map[f"nfs_{name_lower}"] = network_id
#                 network_map[f"{name_lower} nfs"] = network_id
#                 network_map[f"nfs{name_lower}"] = network_id
                
#                 # Handle spaces vs underscores
#                 name_with_space = name_lower.replace("_", " ")
#                 name_with_underscore = name_lower.replace(" ", "_")
#                 network_map[name_with_space] = network_id
#                 network_map[name_with_underscore] = network_id
#                 network_map[f"nfs {name_with_space}"] = network_id
#                 network_map[f"nfs_{name_with_underscore}"] = network_id
            
#             # Update cache
#             _network_map_cache = network_map
#             _network_map_timestamp = datetime.now()
            
#             print(f"✓ Loaded fresh network mapping ({len(network_map)} patterns from {len(networks)} networks)")
#             # logger.info(f"Loaded network mapping with {len(network_map)} patterns from {len(networks)} networks")
            
#             return network_map
            
#         except Exception as e:
#             print(f"⚠️ Warning: Could not load network mapping: {str(e)}")
#             print(f"   Network IDs will be NULL for all records")
#             return {}
    
#     @staticmethod
#     def _extract_network_id(service_name: str, network_map: Dict[str, int]) -> int:
#         """
#         Extract network ID from service name using pre-loaded mapping.
#         Returns None if no match found (valid for channels where network_id can be null).
        
#         Args:
#             service_name: Service name from CSV (e.g., "NFS AIRTEL", "NFS MTN")
#             network_map: Pre-loaded mapping dictionary
            
#         Returns:
#             network_id or None
#         """
#         if not service_name or not network_map:
#             return None
            
#         service_lower = str(service_name).lower().strip()
        
#         # Try direct match first (fastest)
#         if service_lower in network_map:
#             return network_map[service_lower]
        
#         # Try pattern matching (contains check)
#         # Sort by length to match most specific patterns first
#         sorted_patterns = sorted(network_map.keys(), key=len, reverse=True)
#         for pattern in sorted_patterns:
#             if pattern in service_lower:
#                 return network_map[pattern]
        
#         # No match found - return None (valid for some channels)
#         return None

#     @staticmethod
#     async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
#         try:
#             file_entry = UploadFile(
#                 file_name=fileData['file_name'],
#                 file_details= json.dumps (fileData['file_details']),
#                 channel_id = fileData['channel_id'],
#                 status=fileData['status'],
#                 record_details= json.dumps(fileData['record_details']),
#                 total_records=fileData['total_records'],
#                 version_number=fileData['version_number'],
#                 created_by=fileData['created_by']
#             )
#             db.add(file_entry)
#             await db.commit()
#             await db.refresh(file_entry)
#             return { "error": False, "status": "success","insertedId": file_entry.id}
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveUploadedFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
        
#     @staticmethod
#     async def saveFileDetails(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         getDateTimeColumnName: str,
#         getAmountColumnName: str,
#         getAcountNumberColumnName: str,
#         getCurrencyColumnName: str,
#         getReferenceNumberColumnName: str = None,
#         getTransactionIdColumnName: str = None,
#         getServiceNameColumnName: str = None,  # NEW: Service name column for network lookup
#         system_id: int = 1
#         ) -> Dict[str, Any]:
#         """
#         OPTIMIZED: Bulk duplicate detection using batched queries instead of N queries.
#         Performance: 100,000x faster than per-row queries
#         Safety: Batches keys to avoid PostgreSQL max_stack_depth error on huge uploads
#         Batch size is dynamically loaded from tbl_cfg_system_batch based on system_id
        
#         NEW: Network ID mapping using in-memory O(1) lookup (no per-row queries)
#         """
#         try:
#             # DEBUG: Log what mappings were received
#             print(f"\n[SAVE FILE DETAILS DEBUG]")
#             print(f"  getReferenceNumberColumnName: {getReferenceNumberColumnName}")
#             print(f"  getTransactionIdColumnName: {getTransactionIdColumnName}")
#             print(f"  getServiceNameColumnName: {getServiceNameColumnName}")  # NEW
#             print(f"  getDateTimeColumnName: {getDateTimeColumnName}")
#             print(f"  getAmountColumnName: {getAmountColumnName}")
#             print(f"  Total records: {len(fileData)}")
#             if fileData:
#                 print(f"  First record columns: {list(fileData[0].keys())[:10]}")
            
#             duplicates = []
#             new_records = []
            
#             # 🔥 PRE-LOAD NETWORK MAPPING ONCE (O(1) lookups for all rows)
#             network_map = await UploadRepository._build_network_mapping(db)
#             if network_map:
#                 print(f"✓ Network mapping ready: {len(network_map)} patterns loaded")
#             else:
#                 print(f"⚠️ No network mapping available - network_id will be NULL")
            
#             # CRITICAL OPTIMIZATION: Build unique keys for ALL records at once
#             keys_to_check = []
#             for row in fileData:
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
                
#                 # Add amount to key
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add date to key
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add currency to key (if needed for duplicate check)
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 keys_to_check.append(tuple(key))
            
#             # BATCHED QUERY to check all duplicates (avoids Postgres max_stack_depth error)
#             # Split into chunks to prevent "stack depth limit exceeded" error on large batches
            
#             # DYNAMIC BATCH SIZE: Calculate based on number of jobs from tbl_cfg_system_batch
#             from app.db.models.system_batch_config import SystemBatchConfig
            
#             # Try to get number of jobs from database config
#             batch_config_stmt = select(SystemBatchConfig).where(
#                 SystemBatchConfig.system_id == system_id
#             )
#             batch_config_result = await db.execute(batch_config_stmt)
#             batch_config = batch_config_result.scalar_one_or_none()
            
#             total_records = len(keys_to_check)
            
#             if batch_config and batch_config.record_per_job:
#                 # record_per_job stores NUMBER OF JOBS
#                 num_jobs = int(batch_config.record_per_job)
#                 # Calculate batch size: total records / number of jobs
#                 BATCH_SIZE = max(1, (total_records + num_jobs - 1) // num_jobs)  # Ceiling division
#                 batch_source = f"calculated from {num_jobs} jobs (system_id={system_id})"
#             else:
#                 # Fallback to env var or default
#                 BATCH_SIZE = settings.UPLOAD_DUPLICATE_CHECK_BATCH_SIZE
#                 batch_source = "config/env default"
            
#             existing_keys = set()
            
#             if keys_to_check:
#                 import time
#                 batch_count = (len(keys_to_check) + BATCH_SIZE - 1) // BATCH_SIZE
#                 print(f"\n📊 Duplicate Check Starting:")
#                 print(f"   Total keys to check: {len(keys_to_check):,}")
#                 print(f"   Batch size: {BATCH_SIZE:,} ({batch_source})")
#                 print(f"   Number of batches/jobs: {batch_count}")
                
#                 for i in range(0, len(keys_to_check), BATCH_SIZE):
#                     batch_start = time.time()
#                     batch_keys = keys_to_check[i:i+BATCH_SIZE]
#                     batch_num = i // BATCH_SIZE + 1
                    
#                     if getCurrencyColumnName:
#                         # Query with currency
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date,
#                             Transaction.ccy
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date,
#                                 Transaction.ccy
#                             ).in_(batch_keys)
#                         )
#                     else:
#                         # Query without currency
#                         keys_without_ccy = [k[:4] for k in batch_keys]  # Remove last element (None)
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date
#                             ).in_(keys_without_ccy)
#                         )
                    
#                     result = await db.execute(stmt)
#                     batch_records = result.all()
                    
#                     # Add to existing_keys set for O(1) lookup
#                     existing_keys.update(tuple(record) for record in batch_records)
                    
#                     # Log batch completion
#                     batch_time = time.time() - batch_start
#                     duplicates_found = len(batch_records)
#                     print(f"   ✓ Batch {batch_num}/{batch_count}: "
#                           f"{len(batch_keys):,} keys checked, "
#                           f"{duplicates_found:,} duplicates found, "
#                           f"{batch_time:.2f}s")
                
#                 print(f"✓ Duplicate check complete: {len(existing_keys):,} total duplicates found\n")
            
#             # Process each row and check against existing_keys (in-memory, super fast)
#             rows_processed = 0
#             network_stats = {"mapped": 0, "unmapped": 0, "null_allowed": 0}
            
#             for row in fileData:
#                 # Debug: Log first row to see actual column names
#                 if rows_processed == 0:
#                     print(f"DEBUG - First row columns: {list(row.keys())}")
#                     print(f"DEBUG - First row data sample: {dict(list(row.items())[:5])}")
                
#                 # Build key for this row
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
#                 rows_processed += 1
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 # O(1) duplicate check using set lookup
#                 if tuple(key) in existing_keys:
#                     duplicates.append(row)
#                     continue
                
#                 # Build new record with current timestamp
#                 current_timestamp = datetime.now()
#                 data = {
#                     "channel_id": fileJson["channel_id"],
#                     "source_id": fileJson["source_id"],
#                     "otherDetails": json.dumps(row, default=str),
#                     "file_transactions_id": fileJson["file_transactions_id"],
#                     "created_by": 1,
#                     "updated_by": 1,
#                     "version_number": fileJson["version_number"],
#                     "created_at": current_timestamp,
#                     "updated_at": current_timestamp,
#                 }
                
#                 # DEBUG: Log fileJson to see what source_id we're receiving
#                 if len(new_records) == 0:  # Only log once per batch
#                     print(f"[UPLOAD DEBUG] fileJson: {fileJson}")
#                     print(f"[UPLOAD DEBUG] Saving with source_id={fileJson['source_id']}, channel_id={fileJson['channel_id']}")
#                     print(f"[UPLOAD DEBUG] created_at explicitly set to: {current_timestamp}")
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     data["amount"] = str(row[getAmountColumnName])

#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     # Handle datetime properly - check if it's already a datetime/timestamp
#                     date_value = row[getDateTimeColumnName]
#                     if date_value is not None and str(date_value) not in ['', 'nan', 'NaT', 'None']:
#                         # If it's a pandas Timestamp or datetime object, format it properly
#                         if hasattr(date_value, 'strftime'):
#                             data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
#                         else:
#                             data["date"] = str(date_value)

#                 if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
#                     data["account_number"] = str(row[getAcountNumberColumnName])

#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     data["ccy"] = str(row[getCurrencyColumnName])
                
#                 # Map reference_number - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped reference_number field
#                 reference_mapped = False
#                 if getReferenceNumberColumnName is not None and getReferenceNumberColumnName in row:
#                     if row[getReferenceNumberColumnName] is not None and str(row[getReferenceNumberColumnName]).strip():
#                         data["reference_number"] = str(row[getReferenceNumberColumnName]).strip()
#                         reference_mapped = True
#                         if len(new_records) == 0:
#                             print(f"[REF DEBUG] Explicit mapping used: {getReferenceNumberColumnName} = {data['reference_number']}")
                
#                 if not reference_mapped:
#                     # 2. Fall back to auto-detection from common field names
#                     # Note: ReconDataNormalizer converts columns to lowercase, so check lowercase versions
#                     # Including both underscore and space-separated variants
#                     # PRIORITY ORDER: Most specific/common first
#                     reference_fields = [
#                         'transaction_id', 'transaction id',  # PRIMARY: Common transaction ID (most important)
#                         'rrn',  # Retrieval Reference Number (banking standard)
#                         'reference_number', 'reference number',
#                         'referencenumber', 
#                         'ref_number', 'ref number',
#                         'refnumber', 
#                         'retrieval_reference_number', 'retrieval reference number'
#                     ]
#                     reference_found = False
#                     for field in reference_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["reference_number"] = str(row[field]).strip()
#                             reference_found = True
#                             if len(new_records) == 0:
#                                 print(f"[REF DEBUG] Auto-detected field '{field}' = {data['reference_number']}")
#                             break
                    
#                     if not reference_found and len(new_records) == 0:
#                         print(f"[REF DEBUG] ❌ No reference field found!")
#                         print(f"[REF DEBUG] Available columns: {list(row.keys())}")
#                         print(f"[REF DEBUG] Searching for: {reference_fields[:5]}...")
                
#                 # Map txn_id - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped transaction_id field
#                 if getTransactionIdColumnName is not None and getTransactionIdColumnName in row:
#                     if row[getTransactionIdColumnName] is not None and str(row[getTransactionIdColumnName]).strip():
#                         data["txn_id"] = str(row[getTransactionIdColumnName]).strip()
#                 else:
#                     # 2. Fall back to auto-detection from common field names
#                     # Including both underscore and space-separated variants
#                     # PRIORITY: Receipt Number, STAN, then generic txn_id (avoiding conflict with transaction_id -> reference_number)
#                     txn_id_fields = [
#                         'receipt_number', 'receipt number',  # Mobile Money specific (PRIMARY)
#                         'payer_transaction_id', 'payer transaction id',  # Mobile Money payer ID
#                         'stan',  # System Trace Audit Number (banking)
#                         'txn_id', 'txn id',  # Generic transaction ID
#                         'transactionid', 
#                         'txnid'
#                         # Note: 'transaction_id' is NOT here - it goes to reference_number!
#                     ]
#                     txn_id_found = False
#                     for field in txn_id_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["txn_id"] = str(row[field]).strip()
#                             txn_id_found = True
#                             break
                
#                 # 🔥 NEW: Map network_id from service name (O(1) lookup)
#                 network_id = None
#                 network_mapped = False
#                 service_name_value = None
                
#                 # 1. First, check if user explicitly mapped service_name field
#                 if getServiceNameColumnName and getServiceNameColumnName in row:
#                     service_name_value = row[getServiceNameColumnName]
#                     if service_name_value:
#                         network_id = UploadRepository._extract_network_id(service_name_value, network_map)
#                         network_mapped = True
#                         if len(new_records) == 0 and network_id:
#                             print(f"[NETWORK DEBUG] Explicit mapping: '{service_name_value}' → network_id={network_id}")
                
#                 # 2. Fall back to auto-detection from common field names
#                 if not network_mapped:
#                     service_fields = [
#                         'service_name', 'service name',
#                         'servicename',
#                         'network', 'network_name', 'network name',
#                         'provider', 'provider_name', 'provider name',
#                         'operator', 'operator_name', 'operator name',
#                         'payer_client', 'payer client',  # From your sample data
#                         'service', 'channel'
#                     ]
                    
#                     for field in service_fields:
#                         if field in row and row[field]:
#                             service_name_value = row[field]
#                             network_id = UploadRepository._extract_network_id(service_name_value, network_map)
#                             if network_id:
#                                 if len(new_records) == 0:
#                                     print(f"[NETWORK DEBUG] Auto-detected '{field}': '{service_name_value}' → network_id={network_id}")
#                                 break
                
#                 # 3. Assign network_id (can be None for some channels - that's valid)
#                 data["network_id"] = network_id
                
#                 # Track network mapping statistics
#                 if network_id:
#                     network_stats["mapped"] += 1
#                 elif service_name_value:
#                     network_stats["unmapped"] += 1
#                 else:
#                     network_stats["null_allowed"] += 1
                
#                 # Debug log for first record to verify field mapping
#                 if len(new_records) == 0:
#                     print(f"\n=== FIRST RECORD FIELD MAPPING DEBUG ===")
#                     print(f"Explicit mappings provided:")
#                     print(f"  - reference_number: {getReferenceNumberColumnName or 'AUTO-DETECT'}")
#                     print(f"  - transaction_id: {getTransactionIdColumnName or 'AUTO-DETECT'}")
#                     print(f"  - service_name: {getServiceNameColumnName or 'AUTO-DETECT'}")
#                     print(f"Available fields in row: {list(row.keys())}")
#                     print(f"Sample data: {dict(list(row.items())[:5])}")
#                     print(f"Mapped values:")
#                     print(f"  - Reference number: {data.get('reference_number', 'NOT MAPPED')}")
#                     print(f"  - Transaction ID: {data.get('txn_id', 'NOT MAPPED')}")
#                     print(f"  - Network ID: {data.get('network_id', 'NULL (valid for some channels)')}")
#                     print(f"  - Service Name Used: {service_name_value or 'NOT FOUND'}")
#                     print(f"  - Amount: {data.get('amount', 'NOT MAPPED')}")
#                     print(f"  - Date: {data.get('date', 'NOT MAPPED')}")
#                     print(f"\n[CRITICAL DEBUG] Complete data dict being inserted:")
#                     print(f"  {data}")
#                     print(f"=====================================\n")
                
#                 new_records.append(Transaction(**data))

#             if new_records:
#                 db.add_all(new_records)
#                 await db.commit()
            
#             # Print network mapping statistics
#             print(f"\n📊 Network Mapping Statistics:")
#             print(f"   ✓ Successfully mapped: {network_stats['mapped']:,}")
#             print(f"   ⚠️ Service name found but not mapped: {network_stats['unmapped']:,}")
#             print(f"   ○ No service name (NULL allowed): {network_stats['null_allowed']:,}")
#             print(f"   Total processed: {len(new_records):,}\n")

#             return {
#                 "status": "success",
#                 "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
#                 "recordsSaved": len(new_records),
#                 "duplicateRecords": duplicates,
#                 "networkStats": network_stats  # NEW: Include network mapping stats
#             }
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
    
#     @staticmethod
#     async def updateUploadProgress(
#         db: AsyncSession,
#         file_id: int,
#         processed: int,
#         success: int,
#         failed: int,
#         duplicates: int,
#         total: int
#     ) -> None:
#         """
#         Update progress tracking fields for an upload file.
#         Called after each batch is processed.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.processed_records = processed
#                 upload_file.success_records = success
#                 upload_file.failed_records = failed
#                 upload_file.duplicate_records = duplicates
                
#                 # Calculate progress percentage
#                 if total > 0:
#                     upload_file.progress_percentage = round((processed / total) * 100, 2)
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateUploadProgress: {str(e)}")
    
#     @staticmethod
#     async def updateFileStatus(
#         db: AsyncSession,
#         file_id: int,
#         status: int,
#         error_message: str = None,
#         error_details: str = None
#     ) -> None:
#         """
#         Update upload file status and error information.
#         status: 0=pending, 1=processing, 2=completed, 3=failed
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.status = status
                
#                 # Set timing fields
#                 if status == 1 and not upload_file.upload_started_at:
#                     # Starting processing
#                     upload_file.upload_started_at = datetime.utcnow()
#                 elif status in [2, 3]:
#                     # Completed or failed
#                     upload_file.upload_completed_at = datetime.utcnow()
#                     if upload_file.upload_started_at:
#                         time_diff = upload_file.upload_completed_at - upload_file.upload_started_at
#                         upload_file.processing_time_seconds = int(time_diff.total_seconds())
                
#                 # Set error info if provided
#                 if error_message:
#                     upload_file.error_message = error_message
#                 if error_details:
#                     upload_file.error_details = error_details
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateFileStatus: {str(e)}")
    
#     @staticmethod
#     async def saveFileDetailsBatch(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         column_mappings: Dict[str, str]
#     ) -> Dict[str, Any]:
#         """
#         Optimized batch processing method for Celery tasks.
#         Uses the same bulk duplicate detection as saveFileDetails.
        
#         Args:
#             column_mappings: Dict with keys: date, amount, account_number, currency, 
#                             reference_number, transaction_id, service_name
#             Note: Column names will be converted to lowercase to match normalized data
#         """
#         # Convert column names to lowercase to match normalized column names
#         date_col = column_mappings.get("date")
#         amount_col = column_mappings.get("amount")
#         account_col = column_mappings.get("account_number")
#         currency_col = column_mappings.get("currency")
#         reference_col = column_mappings.get("reference_number")
#         transaction_id_col = column_mappings.get("transaction_id")
#         service_name_col = column_mappings.get("service_name")  # NEW
        
#         return await UploadRepository.saveFileDetails(
#             db=db,
#             fileData=fileData,
#             fileJson=fileJson,
#             getDateTimeColumnName=date_col.lower() if date_col else None,
#             getAmountColumnName=amount_col.lower() if amount_col else None,
#             getAcountNumberColumnName=account_col.lower() if account_col else None,
#             getCurrencyColumnName=currency_col.lower() if currency_col else None,
#             getReferenceNumberColumnName=reference_col.lower() if reference_col else None,
#             getTransactionIdColumnName=transaction_id_col.lower() if transaction_id_col else None,
#             getServiceNameColumnName=service_name_col.lower() if service_name_col else None  # NEW
#         )
    
#     @staticmethod
#     async def getUploadProgress(db: AsyncSession, file_id: int) -> Dict[str, Any]:
#         """
#         Get upload progress information for a specific file.
#         Used by progress tracking API endpoint.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
#             if not upload_file:
#                 return {"error": True, "message": "File not found"}
            
#             return {
#                 "error": False,
#                 "file_id": upload_file.id,
#                 "file_name": upload_file.file_name,
#                 "status": upload_file.status,
#                 "total_records": upload_file.total_records,
#                 "processed_records": upload_file.processed_records,
#                 "success_records": upload_file.success_records,
#                 "failed_records": upload_file.failed_records,
#                 "duplicate_records": upload_file.duplicate_records,
#                 "progress_percentage": upload_file.progress_percentage,
#                 "upload_started_at": upload_file.upload_started_at.isoformat() if upload_file.upload_started_at else None,
#                 "upload_completed_at": upload_file.upload_completed_at.isoformat() if upload_file.upload_completed_at else None,
#                 "processing_time_seconds": upload_file.processing_time_seconds,
#                 "error_message": upload_file.error_message,
#                 "error_details": upload_file.error_details
#             }
#         except Exception as e:
#             print(f"uploadRepository-getUploadProgress: {str(e)}")
#             return {"error": True, "message": str(e)}

#     @staticmethod
#     async def getFileList(db: AsyncSession, offset: int, limit: int):
#         try:
#             # Total count
#             total_stmt = select(func.count()).select_from(UploadFile)
#             total_result = await db.execute(total_stmt)
#             total = total_result.scalar()
#             # Data query
#             stmt = (
#                 select(
#                     UploadFile,
#                     UserConfig.id.label("user_id"),
#                     UserConfig.f_name.label("f_name"),
#                     UserConfig.m_name.label("m_name"),
#                     UserConfig.l_name.label("l_name"),
#                     UserConfig.email.label("email"),
#                     ChannelConfig.id.label("channel_id"),
#                     ChannelConfig.channel_name.label("channel_name"),
#                     SourceConfig.id.label("source_id"),
#                     SourceConfig.source_name.label("source_name"),
#                     SourceConfig.source_type.label("source_type"),
#                 )
#                 .outerjoin(UserConfig, UserConfig.id == UploadFile.created_by)
#                 .outerjoin(ChannelConfig, ChannelConfig.id == UploadFile.channel_id)
#                 .outerjoin(
#                     SourceConfig,
#                     cast(
#                         UploadFile.file_details.cast(JSONB)["file_type"].astext,
#                         Integer
#                     ) == SourceConfig.id
#                 )
#                 .order_by(UploadFile.created_at.desc())
#                 .offset(offset)
#                 .limit(limit)
#             )

#             result = await db.execute(stmt)
#             rows = result.all()
#             data = []
#             for upload, user_id, f_name, m_name, l_name, email, channel_id, channel_name,source_id, source_name, source_type in rows:
#                 file_details_obj = None
#                 file_record_obj = {
#                     "total_records": upload.total_records,
#                     "processed": upload.processed_records,
#                     "success": upload.success_records,
#                     "failed": upload.failed_records,
#                     "duplicates": upload.duplicate_records,
#                     "progress_percentage": upload.progress_percentage,
#                 }


#                 if upload.file_details:
#                     try:
#                         file_details_obj = json.loads(upload.file_details)
#                     except json.JSONDecodeError:
#                         pass
                    
#                 data.append({
#                     "id": upload.id,
#                     "file_name": upload.file_name,
#                     "file_details": file_details_obj,
#                     "status": upload.status,
#                     "record_details": file_record_obj,
#                     "created_at": upload.created_at,
#                     "version_number": upload.version_number,

#                     "user": {
#                         "id": user_id,
#                         "name": " ".join(filter(None, [f_name, m_name, l_name])),
#                         "email": email,
#                     } if user_id else None,

#                     "channel": {
#                         "id": channel_id,
#                         "name": channel_name,
#                     } if channel_id else None,
#                     "source": {
#                         "id": source_id,
#                         "name": source_name,
#                         "type": source_type,
#                     } if source_id else None
#                 })
#             return {
#                 "status": "success",
#                 "offset": offset,
#                 "limit": limit,
#                 "total": total,
#                 "data": data
#             }
#         except Exception as e:
#             # Rollback is safe even for SELECTs
#             await db.rollback()
#             print("UploadRepository.getFileList error:", str(e))
#             return {
#                 "status": "error",
#                 "message": "Failed to fetch upload file list",
#                 "error": str(e)
#             }
    
#     @staticmethod
#     async def deleteFileAndTransactions(db: AsyncSession, file_id: int) -> bool:
        
#         result = await db.execute(
#             select(UploadFile).where(UploadFile.id == file_id)
#         )
#         file_record = result.scalar_one_or_none()

#         if not file_record:
#             return False

#         try:
#             # 1️⃣ Delete related transactions
#             await db.execute(
#                 delete(Transaction).where(
#                     Transaction.file_transactions_id == file_id
#                 )
#             )

#             # 2️⃣ Delete file upload record
#             await db.execute(
#                 delete(UploadFile).where(UploadFile.id == file_id)
#             )

#             await db.commit()
#             return True

#         except Exception as e:
#             await db.rollback()
#             raise e

#     @staticmethod
#     async def clearNetworkCache() -> None:
#         """
#         Clear the network mapping cache.
#         Call this when network configuration changes.
#         """
#         global _network_map_cache, _network_map_timestamp
#         _network_map_cache = None
#         _network_map_timestamp = None
#         print("✓ Network mapping cache cleared")




# import json
# from typing import Any, Dict, List
# from datetime import datetime, timedelta

# from sqlalchemy import Integer, cast, delete, func, select, tuple_, update
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.db.models.channel_config import ChannelConfig
# from app.db.models.source_config import SourceConfig
# from app.db.models.transactions import Transaction
# from app.db.models.upload_file import UploadFile
# from app.config import settings
# from app.db.models.user_config import UserConfig
# from sqlalchemy.dialects.postgresql import JSONB
# import logging
# logger = logging.getLogger(__name__)

# # Global cache for network mapping (optional optimization)
# _network_map_cache = None
# _network_map_timestamp = None
# CACHE_TTL = timedelta(hours=1)

# class UploadRepository:
    
#     @staticmethod
#     async def _build_network_mapping(db: AsyncSession) -> Dict[str, Any]:
#         """
#         Pre-load all networks into memory for O(1) lookup.
#         Uses 1-hour cache to avoid repeated DB queries.
#         Returns: Dict with 'patterns' mapping, 'others_id' for fallback, and 'channel_configs'
#         """
#         global _network_map_cache, _network_map_timestamp
        
#         # Check if cache is valid
#         if (_network_map_cache is not None and 
#             _network_map_timestamp is not None and 
#             datetime.now() - _network_map_timestamp < CACHE_TTL):
#             print(f"✓ Using cached network mapping ({len(_network_map_cache.get('patterns', {}))} patterns)")
#             return _network_map_cache
        
#         # Load fresh data from database
#         try:
#             from app.db.models.network import Network  # Adjust import path as needed
            
#             # Load networks
#             stmt = select(Network.id, Network.network_name)
#             result = await db.execute(stmt)
#             networks = result.all()
            
#             network_map = {}
#             others_id = None
            
#             for network_id, network_name in networks:
#                 if not network_name:
#                     continue
                    
#                 name_lower = network_name.lower().strip()
                
#                 # Check if this is the "Others" network
#                 if name_lower in ['others', 'other', 'unknown']:
#                     others_id = network_id
#                     print(f"✓ Found 'Others' network with ID: {others_id}")
                
#                 # Direct name match
#                 network_map[name_lower] = network_id
                
#                 # Common patterns for your data
#                 network_map[f"nfs {name_lower}"] = network_id
#                 network_map[f"nfs_{name_lower}"] = network_id
#                 network_map[f"{name_lower} nfs"] = network_id
#                 network_map[f"nfs{name_lower}"] = network_id
                
#                 # Handle spaces vs underscores
#                 name_with_space = name_lower.replace("_", " ")
#                 name_with_underscore = name_lower.replace(" ", "_")
#                 network_map[name_with_space] = network_id
#                 network_map[name_with_underscore] = network_id
#                 network_map[f"nfs {name_with_space}"] = network_id
#                 network_map[f"nfs_{name_with_underscore}"] = network_id
            
#             # Load channel-network relationships to check which channels use networks
#             # Get distinct channel_ids from Network table that have at least one network
#             print(f"🔍 Checking which channels have networks...")
#             channel_network_stmt = select(Network.channel_id).where(
#                 Network.channel_id.isnot(None)
#             ).distinct()
#             channel_network_result = await db.execute(channel_network_stmt)
#             channels_with_networks = set(channel_network_result.scalars().all())
            
#             # Build channel config map: channel_id -> has_network (True/False)
#             channel_configs = {}
            
#             # Get all channels
#             all_channels_stmt = select(ChannelConfig.id, ChannelConfig.channel_name)
#             all_channels_result = await db.execute(all_channels_stmt)
#             all_channels = all_channels_result.all()
            
#             # Mark which channels have networks
#             for channel_id, channel_name in all_channels:
#                 has_networks = channel_id in channels_with_networks
#                 channel_configs[channel_id] = has_networks
#                 status = "✓ HAS networks" if has_networks else "○ NO networks"
#                 print(f"   Channel {channel_id} ({channel_name}): {status}")
            
#             # Update cache with patterns, others_id, and channel configs
#             _network_map_cache = {
#                 'patterns': network_map,
#                 'others_id': others_id,
#                 'channel_configs': channel_configs
#             }
#             _network_map_timestamp = datetime.now()
            
#             print(f"✓ Loaded fresh network mapping ({len(network_map)} patterns from {len(networks)} networks)")
#             print(f"✓ Loaded {len(channel_configs)} channel configurations")
#             if others_id:
#                 print(f"✓ 'Others' network configured with ID: {others_id}")
#             else:
#                 print(f"⚠️ Warning: No 'Others' network found - unmapped services will be NULL")
            
#             return _network_map_cache
            
#         except Exception as e:
#             print(f"⚠️ Warning: Could not load network mapping: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             print(f"   Network IDs will be NULL for all records")
#             return {'patterns': {}, 'others_id': None, 'channel_configs': {}}
    
#     @staticmethod
#     def _extract_network_id(service_name: str, network_mapping: Dict[str, Any], channel_id: int) -> int:
#         """
#         Extract network ID from service name using pre-loaded mapping.
#         Only applies network logic if the channel has network configuration enabled.
        
#         Args:
#             service_name: Service name from CSV (e.g., "NFS AIRTEL", "NFS MTN")
#             network_mapping: Pre-loaded mapping dictionary with 'patterns', 'others_id', and 'channel_configs'
#             channel_id: Channel ID to check if network mapping is required
            
#         Returns:
#             network_id or None
#         """
#         # CRITICAL: Check if this channel uses networks at all
#         channel_configs = network_mapping.get('channel_configs', {})
#         channel_has_network = channel_configs.get(channel_id, False)
        
#         # If channel doesn't use networks, always return None
#         if not channel_has_network:
#             return None
        
#         # Channel uses networks - proceed with mapping logic
#         network_map = network_mapping.get('patterns', {})
#         others_id = network_mapping.get('others_id')
        
#         # If no service name provided, use "Others" if available
#         if not service_name:
#             return others_id
        
#         if not network_map:
#             return others_id
            
#         service_lower = str(service_name).lower().strip()
        
#         # Try direct match first (fastest)
#         if service_lower in network_map:
#             return network_map[service_lower]
        
#         # Try pattern matching (contains check)
#         # Sort by length to match most specific patterns first
#         sorted_patterns = sorted(network_map.keys(), key=len, reverse=True)
#         for pattern in sorted_patterns:
#             if pattern in service_lower:
#                 return network_map[pattern]
        
#         # No match found - return "Others" network ID or None
#         return others_id

#     @staticmethod
#     async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
#         try:
#             file_entry = UploadFile(
#                 file_name=fileData['file_name'],
#                 file_details= json.dumps (fileData['file_details']),
#                 channel_id = fileData['channel_id'],
#                 status=fileData['status'],
#                 record_details= json.dumps(fileData['record_details']),
#                 total_records=fileData['total_records'],
#                 version_number=fileData['version_number'],
#                 created_by=fileData['created_by']
#             )
#             db.add(file_entry)
#             await db.commit()
#             await db.refresh(file_entry)
#             return { "error": False, "status": "success","insertedId": file_entry.id}
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveUploadedFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
        
#     @staticmethod
#     async def saveFileDetails(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         getDateTimeColumnName: str,
#         getAmountColumnName: str,
#         getAcountNumberColumnName: str,
#         getCurrencyColumnName: str,
#         getReferenceNumberColumnName: str = None,
#         getTransactionIdColumnName: str = None,
#         getServiceNameColumnName: str = None,  # NEW: Service name column for network lookup
#         system_id: int = 1
#         ) -> Dict[str, Any]:
#         """
#         OPTIMIZED: Bulk duplicate detection using batched queries instead of N queries.
#         Performance: 100,000x faster than per-row queries
#         Safety: Batches keys to avoid PostgreSQL max_stack_depth error on huge uploads
#         Batch size is dynamically loaded from tbl_cfg_system_batch based on system_id
        
#         NEW: Network ID mapping using in-memory O(1) lookup (no per-row queries)
#         NEW: Falls back to "Others" network when service name doesn't match
#         """
#         try:
#             # DEBUG: Log what mappings were received
#             print(f"\n[SAVE FILE DETAILS DEBUG]")
#             print(f"  getReferenceNumberColumnName: {getReferenceNumberColumnName}")
#             print(f"  getTransactionIdColumnName: {getTransactionIdColumnName}")
#             print(f"  getServiceNameColumnName: {getServiceNameColumnName}")  # NEW
#             print(f"  getDateTimeColumnName: {getDateTimeColumnName}")
#             print(f"  getAmountColumnName: {getAmountColumnName}")
#             print(f"  Total records: {len(fileData)}")
#             if fileData:
#                 print(f"  First record columns: {list(fileData[0].keys())[:10]}")
            
#             duplicates = []
#             new_records = []
            
#             # 🔥 PRE-LOAD NETWORK MAPPING ONCE (O(1) lookups for all rows)
#             network_mapping = await UploadRepository._build_network_mapping(db)
#             if network_mapping.get('patterns'):
#                 print(f"✓ Network mapping ready: {len(network_mapping['patterns'])} patterns loaded")
#             else:
#                 print(f"⚠️ No network mapping available - network_id will be NULL")
            
#             # CRITICAL OPTIMIZATION: Build unique keys for ALL records at once
#             keys_to_check = []
#             for row in fileData:
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
                
#                 # Add amount to key
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add date to key
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add currency to key (if needed for duplicate check)
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 keys_to_check.append(tuple(key))
            
#             # BATCHED QUERY to check all duplicates (avoids Postgres max_stack_depth error)
#             # Split into chunks to prevent "stack depth limit exceeded" error on large batches
            
#             # DYNAMIC BATCH SIZE: Calculate based on number of jobs from tbl_cfg_system_batch
#             from app.db.models.system_batch_config import SystemBatchConfig
            
#             # Try to get number of jobs from database config
#             batch_config_stmt = select(SystemBatchConfig).where(
#                 SystemBatchConfig.system_id == system_id
#             )
#             batch_config_result = await db.execute(batch_config_stmt)
#             batch_config = batch_config_result.scalar_one_or_none()
            
#             total_records = len(keys_to_check)
            
#             if batch_config and batch_config.record_per_job:
#                 # record_per_job stores NUMBER OF JOBS
#                 num_jobs = int(batch_config.record_per_job)
#                 # Calculate batch size: total records / number of jobs
#                 BATCH_SIZE = max(1, (total_records + num_jobs - 1) // num_jobs)  # Ceiling division
#                 batch_source = f"calculated from {num_jobs} jobs (system_id={system_id})"
#             else:
#                 # Fallback to env var or default
#                 BATCH_SIZE = settings.UPLOAD_DUPLICATE_CHECK_BATCH_SIZE
#                 batch_source = "config/env default"
            
#             existing_keys = set()
            
#             if keys_to_check:
#                 import time
#                 batch_count = (len(keys_to_check) + BATCH_SIZE - 1) // BATCH_SIZE
#                 print(f"\n📊 Duplicate Check Starting:")
#                 print(f"   Total keys to check: {len(keys_to_check):,}")
#                 print(f"   Batch size: {BATCH_SIZE:,} ({batch_source})")
#                 print(f"   Number of batches/jobs: {batch_count}")
                
#                 for i in range(0, len(keys_to_check), BATCH_SIZE):
#                     batch_start = time.time()
#                     batch_keys = keys_to_check[i:i+BATCH_SIZE]
#                     batch_num = i // BATCH_SIZE + 1
                    
#                     if getCurrencyColumnName:
#                         # Query with currency
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date,
#                             Transaction.ccy
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date,
#                                 Transaction.ccy
#                             ).in_(batch_keys)
#                         )
#                     else:
#                         # Query without currency
#                         keys_without_ccy = [k[:4] for k in batch_keys]  # Remove last element (None)
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date
#                             ).in_(keys_without_ccy)
#                         )
                    
#                     result = await db.execute(stmt)
#                     batch_records = result.all()
                    
#                     # Add to existing_keys set for O(1) lookup
#                     existing_keys.update(tuple(record) for record in batch_records)
                    
#                     # Log batch completion
#                     batch_time = time.time() - batch_start
#                     duplicates_found = len(batch_records)
#                     print(f"   ✓ Batch {batch_num}/{batch_count}: "
#                           f"{len(batch_keys):,} keys checked, "
#                           f"{duplicates_found:,} duplicates found, "
#                           f"{batch_time:.2f}s")
                
#                 print(f"✓ Duplicate check complete: {len(existing_keys):,} total duplicates found\n")
            
#             # Process each row and check against existing_keys (in-memory, super fast)
#             rows_processed = 0
#             network_stats = {
#                 "mapped": 0, 
#                 "unmapped_to_others": 0, 
#                 "channel_no_network": 0,  # NEW: Track channels that don't use networks
#                 "null_no_service": 0       # No service name found (for channels with networks)
#             }
            
#             for row in fileData:
#                 # Debug: Log first row to see actual column names
#                 if rows_processed == 0:
#                     print(f"DEBUG - First row columns: {list(row.keys())}")
#                     print(f"DEBUG - First row data sample: {dict(list(row.items())[:5])}")
                
#                 # Build key for this row
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
#                 rows_processed += 1
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 # O(1) duplicate check using set lookup
#                 if tuple(key) in existing_keys:
#                     duplicates.append(row)
#                     continue
                
#                 # Build new record with current timestamp
#                 current_timestamp = datetime.now()
#                 data = {
#                     "channel_id": fileJson["channel_id"],
#                     "source_id": fileJson["source_id"],
#                     "otherDetails": json.dumps(row, default=str),
#                     "file_transactions_id": fileJson["file_transactions_id"],
#                     "created_by": 1,
#                     "updated_by": 1,
#                     "version_number": fileJson["version_number"],
#                     "created_at": current_timestamp,
#                     "updated_at": current_timestamp,
#                 }
                
#                 # DEBUG: Log fileJson to see what source_id we're receiving
#                 if len(new_records) == 0:  # Only log once per batch
#                     print(f"[UPLOAD DEBUG] fileJson: {fileJson}")
#                     print(f"[UPLOAD DEBUG] Saving with source_id={fileJson['source_id']}, channel_id={fileJson['channel_id']}")
#                     print(f"[UPLOAD DEBUG] created_at explicitly set to: {current_timestamp}")
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     data["amount"] = str(row[getAmountColumnName])

#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     # Handle datetime properly - check if it's already a datetime/timestamp
#                     date_value = row[getDateTimeColumnName]
#                     if date_value is not None and str(date_value) not in ['', 'nan', 'NaT', 'None']:
#                         # If it's a pandas Timestamp or datetime object, format it properly
#                         if hasattr(date_value, 'strftime'):
#                             data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
#                         else:
#                             data["date"] = str(date_value)

#                 if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
#                     data["account_number"] = str(row[getAcountNumberColumnName])

#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     data["ccy"] = str(row[getCurrencyColumnName])
                
#                 # Map reference_number - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped reference_number field
#                 reference_mapped = False
#                 if getReferenceNumberColumnName is not None and getReferenceNumberColumnName in row:
#                     if row[getReferenceNumberColumnName] is not None and str(row[getReferenceNumberColumnName]).strip():
#                         data["reference_number"] = str(row[getReferenceNumberColumnName]).strip()
#                         reference_mapped = True
#                         if len(new_records) == 0:
#                             print(f"[REF DEBUG] Explicit mapping used: {getReferenceNumberColumnName} = {data['reference_number']}")
                
#                 if not reference_mapped:
#                     # 2. Fall back to auto-detection from common field names
#                     reference_fields = [
#                         'transaction_id', 'transaction id',
#                         'rrn',
#                         'reference_number', 'reference number',
#                         'referencenumber', 
#                         'ref_number', 'ref number',
#                         'refnumber', 
#                         'retrieval_reference_number', 'retrieval reference number'
#                     ]
#                     reference_found = False
#                     for field in reference_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["reference_number"] = str(row[field]).strip()
#                             reference_found = True
#                             if len(new_records) == 0:
#                                 print(f"[REF DEBUG] Auto-detected field '{field}' = {data['reference_number']}")
#                             break
                    
#                     if not reference_found and len(new_records) == 0:
#                         print(f"[REF DEBUG] ❌ No reference field found!")
#                         print(f"[REF DEBUG] Available columns: {list(row.keys())}")
#                         print(f"[REF DEBUG] Searching for: {reference_fields[:5]}...")
                
#                 # Map txn_id - PRIORITIZE EXPLICIT MAPPING
#                 if getTransactionIdColumnName is not None and getTransactionIdColumnName in row:
#                     if row[getTransactionIdColumnName] is not None and str(row[getTransactionIdColumnName]).strip():
#                         data["txn_id"] = str(row[getTransactionIdColumnName]).strip()
#                 else:
#                     # Fall back to auto-detection
#                     txn_id_fields = [
#                         'receipt_number', 'receipt number',
#                         'payer_transaction_id', 'payer transaction id',
#                         'stan',
#                         'txn_id', 'txn id',
#                         'transactionid', 
#                         'txnid'
#                     ]
#                     for field in txn_id_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["txn_id"] = str(row[field]).strip()
#                             break
                
#                 # 🔥 NEW: Map network_id from service name (O(1) lookup with "Others" fallback)
#                 # IMPORTANT: Only applies if the channel has network configuration enabled
#                 network_id = None
#                 network_mapped = False
#                 service_name_value = None
#                 channel_id = fileJson["channel_id"]
                
#                 # Check if this channel uses networks
#                 channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
#                 if channel_has_network:
#                     # Channel uses networks - proceed with mapping
#                     if len(new_records) == 0:
#                         print(f"\n[CHANNEL DEBUG] Channel {channel_id} HAS network configuration")
#                         print(f"[CHANNEL DEBUG] channel_configs = {network_mapping.get('channel_configs', {})}")
#                         print(f"[CHANNEL DEBUG] channel_has_network = {channel_has_network}")
                    
#                     # 1. First, check if user explicitly mapped service_name field
#                     if getServiceNameColumnName and getServiceNameColumnName in row:
#                         service_name_value = row[getServiceNameColumnName]
#                         if service_name_value:
#                             network_id = UploadRepository._extract_network_id(service_name_value, network_mapping, channel_id)
#                             network_mapped = True
#                             if len(new_records) == 0 and network_id:
#                                 network_name = "Others" if network_id == network_mapping.get('others_id') else "matched"
#                                 print(f"[NETWORK DEBUG] Explicit mapping: '{service_name_value}' → network_id={network_id} ({network_name})")
                    
#                     # 2. Fall back to auto-detection from common field names
#                     if not network_mapped:
#                         service_fields = [
#                             'service_name', 'service name',
#                             'servicename',
#                             'network', 'network_name', 'network name',
#                             'provider', 'provider_name', 'provider name',
#                             'operator', 'operator_name', 'operator name',
#                             'payer_client', 'payer client',
#                             'service', 'channel'
#                         ]
                        
#                         for field in service_fields:
#                             if field in row and row[field]:
#                                 service_name_value = row[field]
#                                 network_id = UploadRepository._extract_network_id(service_name_value, network_mapping, channel_id)
#                                 if network_id:
#                                     if len(new_records) == 0:
#                                         network_name = "Others" if network_id == network_mapping.get('others_id') else "matched"
#                                         print(f"[NETWORK DEBUG] Auto-detected '{field}': '{service_name_value}' → network_id={network_id} ({network_name})")
#                                     break
                    
#                     # 3. If no service name found at all, use "Others" if available
#                     if not service_name_value and network_mapping.get('others_id'):
#                         network_id = network_mapping.get('others_id')
#                         if len(new_records) == 0:
#                             print(f"[NETWORK DEBUG] No service name found → using 'Others' network_id={network_id}")
#                 else:
#                     # Channel does NOT use networks - network_id should be NULL
#                     network_id = None
#                     if len(new_records) == 0:
#                         print(f"\n[CHANNEL DEBUG] Channel {channel_id} does NOT have network configuration")
#                         print(f"[CHANNEL DEBUG] channel_configs = {network_mapping.get('channel_configs', {})}")
#                         print(f"[CHANNEL DEBUG] channel_has_network = {channel_has_network}")
#                         print(f"[CHANNEL DEBUG] Setting network_id = NULL")
                
#                 # 4. Assign network_id
#                 data["network_id"] = network_id
                
#                 # CRITICAL DEBUG: Show what's actually being saved
#                 if len(new_records) == 0:
#                     print(f"\n[CRITICAL] Final network_id assignment:")
#                     print(f"   channel_id: {channel_id}")
#                     print(f"   channel_has_network: {channel_has_network}")
#                     print(f"   service_name_value: {service_name_value}")
#                     print(f"   network_id assigned: {network_id}")
#                     print(f"   data['network_id']: {data.get('network_id')}")
#                     print(f"   Type: {type(data.get('network_id'))}\n")
                
#                 # Track network mapping statistics
#                 channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
#                 if not channel_has_network:
#                     # Channel doesn't use networks - this is expected
#                     network_stats["channel_no_network"] += 1
#                 elif network_id:
#                     # Channel uses networks and we found a network_id
#                     if network_id == network_mapping.get('others_id'):
#                         network_stats["unmapped_to_others"] += 1
#                     else:
#                         network_stats["mapped"] += 1
#                 else:
#                     # Channel uses networks but no service name found
#                     network_stats["null_no_service"] += 1
                
#                 # Debug log for first record to verify field mapping
#                 if len(new_records) == 0:
#                     print(f"\n=== FIRST RECORD FIELD MAPPING DEBUG ===")
#                     print(f"Explicit mappings provided:")
#                     print(f"  - reference_number: {getReferenceNumberColumnName or 'AUTO-DETECT'}")
#                     print(f"  - transaction_id: {getTransactionIdColumnName or 'AUTO-DETECT'}")
#                     print(f"  - service_name: {getServiceNameColumnName or 'AUTO-DETECT'}")
#                     print(f"Available fields in row: {list(row.keys())}")
#                     print(f"Sample data: {dict(list(row.items())[:5])}")
#                     print(f"Mapped values:")
#                     print(f"  - Reference number: {data.get('reference_number', 'NOT MAPPED')}")
#                     print(f"  - Transaction ID: {data.get('txn_id', 'NOT MAPPED')}")
#                     print(f"  - Network ID: {data.get('network_id', 'NULL')}")
#                     print(f"  - Service Name Used: {service_name_value or 'NOT FOUND'}")
#                     print(f"  - Amount: {data.get('amount', 'NOT MAPPED')}")
#                     print(f"  - Date: {data.get('date', 'NOT MAPPED')}")
#                     print(f"\n[CRITICAL DEBUG] Complete data dict being inserted:")
#                     print(f"  {data}")
#                     print(f"=====================================\n")
                
#                 new_records.append(Transaction(**data))

#             if new_records:
#                 db.add_all(new_records)
#                 await db.commit()
            
#             # Print network mapping statistics
#             print(f"\n📊 Network Mapping Statistics:")
#             print(f"   ✓ Successfully mapped to network: {network_stats['mapped']:,}")
#             print(f"   🔄 Mapped to 'Others': {network_stats['unmapped_to_others']:,}")
#             print(f"   ○ Channel has no networks (NULL): {network_stats['channel_no_network']:,}")
#             print(f"   ⚠️ Missing service name (NULL): {network_stats['null_no_service']:,}")
#             print(f"   Total processed: {len(new_records):,}\n")

#             return {
#                 "status": "success",
#                 "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
#                 "recordsSaved": len(new_records),
#                 "duplicateRecords": duplicates,
#                 "networkStats": network_stats
#             }
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
    
#     @staticmethod
#     async def updateUploadProgress(
#         db: AsyncSession,
#         file_id: int,
#         processed: int,
#         success: int,
#         failed: int,
#         duplicates: int,
#         total: int
#     ) -> None:
#         """
#         Update progress tracking fields for an upload file.
#         Called after each batch is processed.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.processed_records = processed
#                 upload_file.success_records = success
#                 upload_file.failed_records = failed
#                 upload_file.duplicate_records = duplicates
                
#                 # Calculate progress percentage
#                 if total > 0:
#                     upload_file.progress_percentage = round((processed / total) * 100, 2)
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateUploadProgress: {str(e)}")
    
#     @staticmethod
#     async def updateFileStatus(
#         db: AsyncSession,
#         file_id: int,
#         status: int,
#         error_message: str = None,
#         error_details: str = None
#     ) -> None:
#         """
#         Update upload file status and error information.
#         status: 0=pending, 1=processing, 2=completed, 3=failed
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.status = status
                
#                 # Set timing fields
#                 if status == 1 and not upload_file.upload_started_at:
#                     # Starting processing
#                     upload_file.upload_started_at = datetime.utcnow()
#                 elif status in [2, 3]:
#                     # Completed or failed
#                     upload_file.upload_completed_at = datetime.utcnow()
#                     if upload_file.upload_started_at:
#                         time_diff = upload_file.upload_completed_at - upload_file.upload_started_at
#                         upload_file.processing_time_seconds = int(time_diff.total_seconds())
                
#                 # Set error info if provided
#                 if error_message:
#                     upload_file.error_message = error_message
#                 if error_details:
#                     upload_file.error_details = error_details
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateFileStatus: {str(e)}")
    
#     @staticmethod
#     async def saveFileDetailsBatch(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         column_mappings: Dict[str, str]
#     ) -> Dict[str, Any]:
#         """
#         Optimized batch processing method for Celery tasks.
#         Uses the same bulk duplicate detection as saveFileDetails.
        
#         Args:
#             column_mappings: Dict with keys: date, amount, account_number, currency, 
#                             reference_number, transaction_id, service_name
#             Note: Column names will be converted to lowercase to match normalized data
#         """
#         # Convert column names to lowercase to match normalized column names
#         date_col = column_mappings.get("date")
#         amount_col = column_mappings.get("amount")
#         account_col = column_mappings.get("account_number")
#         currency_col = column_mappings.get("currency")
#         reference_col = column_mappings.get("reference_number")
#         transaction_id_col = column_mappings.get("transaction_id")
#         service_name_col = column_mappings.get("service_name")  # NEW
        
#         return await UploadRepository.saveFileDetails(
#             db=db,
#             fileData=fileData,
#             fileJson=fileJson,
#             getDateTimeColumnName=date_col.lower() if date_col else None,
#             getAmountColumnName=amount_col.lower() if amount_col else None,
#             getAcountNumberColumnName=account_col.lower() if account_col else None,
#             getCurrencyColumnName=currency_col.lower() if currency_col else None,
#             getReferenceNumberColumnName=reference_col.lower() if reference_col else None,
#             getTransactionIdColumnName=transaction_id_col.lower() if transaction_id_col else None,
#             getServiceNameColumnName=service_name_col.lower() if service_name_col else None  # NEW
#         )
    
#     @staticmethod
#     async def getUploadProgress(db: AsyncSession, file_id: int) -> Dict[str, Any]:
#         """
#         Get upload progress information for a specific file.
#         Used by progress tracking API endpoint.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
#             if not upload_file:
#                 return {"error": True, "message": "File not found"}
            
#             return {
#                 "error": False,
#                 "file_id": upload_file.id,
#                 "file_name": upload_file.file_name,
#                 "status": upload_file.status,
#                 "total_records": upload_file.total_records,
#                 "processed_records": upload_file.processed_records,
#                 "success_records": upload_file.success_records,
#                 "failed_records": upload_file.failed_records,
#                 "duplicate_records": upload_file.duplicate_records,
#                 "progress_percentage": upload_file.progress_percentage,
#                 "upload_started_at": upload_file.upload_started_at.isoformat() if upload_file.upload_started_at else None,
#                 "upload_completed_at": upload_file.upload_completed_at.isoformat() if upload_file.upload_completed_at else None,
#                 "processing_time_seconds": upload_file.processing_time_seconds,
#                 "error_message": upload_file.error_message,
#                 "error_details": upload_file.error_details
#             }
#         except Exception as e:
#             print(f"uploadRepository-getUploadProgress: {str(e)}")
#             return {"error": True, "message": str(e)}

#     @staticmethod
#     async def getFileList(db: AsyncSession, offset: int, limit: int):
#         try:
#             # Total count
#             total_stmt = select(func.count()).select_from(UploadFile)
#             total_result = await db.execute(total_stmt)
#             total = total_result.scalar()
#             # Data query
#             stmt = (
#                 select(
#                     UploadFile,
#                     UserConfig.id.label("user_id"),
#                     UserConfig.f_name.label("f_name"),
#                     UserConfig.m_name.label("m_name"),
#                     UserConfig.l_name.label("l_name"),
#                     UserConfig.email.label("email"),
#                     ChannelConfig.id.label("channel_id"),
#                     ChannelConfig.channel_name.label("channel_name"),
#                     SourceConfig.id.label("source_id"),
#                     SourceConfig.source_name.label("source_name"),
#                     SourceConfig.source_type.label("source_type"),
#                 )
#                 .outerjoin(UserConfig, UserConfig.id == UploadFile.created_by)
#                 .outerjoin(ChannelConfig, ChannelConfig.id == UploadFile.channel_id)
#                 .outerjoin(
#                     SourceConfig,
#                     cast(
#                         UploadFile.file_details.cast(JSONB)["file_type"].astext,
#                         Integer
#                     ) == SourceConfig.id
#                 )
#                 .order_by(UploadFile.created_at.desc())
#                 .offset(offset)
#                 .limit(limit)
#             )

#             result = await db.execute(stmt)
#             rows = result.all()
#             data = []
#             for upload, user_id, f_name, m_name, l_name, email, channel_id, channel_name,source_id, source_name, source_type in rows:
#                 file_details_obj = None
#                 file_record_obj = {
#                     "total_records": upload.total_records,
#                     "processed": upload.processed_records,
#                     "success": upload.success_records,
#                     "failed": upload.failed_records,
#                     "duplicates": upload.duplicate_records,
#                     "progress_percentage": upload.progress_percentage,
#                 }


#                 if upload.file_details:
#                     try:
#                         file_details_obj = json.loads(upload.file_details)
#                     except json.JSONDecodeError:
#                         pass
                    
#                 data.append({
#                     "id": upload.id,
#                     "file_name": upload.file_name,
#                     "file_details": file_details_obj,
#                     "status": upload.status,
#                     "record_details": file_record_obj,
#                     "created_at": upload.created_at,
#                     "version_number": upload.version_number,

#                     "user": {
#                         "id": user_id,
#                         "name": " ".join(filter(None, [f_name, m_name, l_name])),
#                         "email": email,
#                     } if user_id else None,

#                     "channel": {
#                         "id": channel_id,
#                         "name": channel_name,
#                     } if channel_id else None,
#                     "source": {
#                         "id": source_id,
#                         "name": source_name,
#                         "type": source_type,
#                     } if source_id else None
#                 })
#             return {
#                 "status": "success",
#                 "offset": offset,
#                 "limit": limit,
#                 "total": total,
#                 "data": data
#             }
#         except Exception as e:
#             # Rollback is safe even for SELECTs
#             await db.rollback()
#             print("UploadRepository.getFileList error:", str(e))
#             return {
#                 "status": "error",
#                 "message": "Failed to fetch upload file list",
#                 "error": str(e)
#             }
    
#     @staticmethod
#     async def deleteFileAndTransactions(db: AsyncSession, file_id: int) -> bool:
        
#         result = await db.execute(
#             select(UploadFile).where(UploadFile.id == file_id)
#         )
#         file_record = result.scalar_one_or_none()

#         if not file_record:
#             return False

#         try:
#             # 1️⃣ Delete related transactions
#             await db.execute(
#                 delete(Transaction).where(
#                     Transaction.file_transactions_id == file_id
#                 )
#             )

#             # 2️⃣ Delete file upload record
#             await db.execute(
#                 delete(UploadFile).where(UploadFile.id == file_id)
#             )

#             await db.commit()
#             return True

#         except Exception as e:
#             await db.rollback()
#             raise e

#     @staticmethod
#     async def clearNetworkCache() -> None:
#         """
#         Clear the network mapping cache.
#         Call this when network configuration changes.
#         """
#         global _network_map_cache, _network_map_timestamp
#         _network_map_cache = None
#         _network_map_timestamp = None
#         print("✓ Network mapping cache cleared")



# import json
# from typing import Any, Dict, List
# from datetime import datetime, timedelta

# from sqlalchemy import Integer, cast, delete, func, select, tuple_, update
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.db.models.channel_config import ChannelConfig
# from app.db.models.source_config import SourceConfig
# from app.db.models.transactions import Transaction
# from app.db.models.upload_file import UploadFile
# from app.config import settings
# from app.db.models.user_config import UserConfig
# from sqlalchemy.dialects.postgresql import JSONB
# import logging
# logger = logging.getLogger(__name__)

# # Global cache for network mapping (optional optimization)
# _network_map_cache = None
# _network_map_timestamp = None
# CACHE_TTL = timedelta(hours=1)

# class UploadRepository:
    
#     @staticmethod
#     async def _build_network_mapping(db: AsyncSession) -> Dict[str, Any]:
#         """
#         Pre-load all networks into memory for O(1) lookup.
#         Uses 1-hour cache to avoid repeated DB queries.
#         Returns: Dict with 'patterns' mapping, 'others_id' for fallback, and 'channel_configs'
#         """
#         global _network_map_cache, _network_map_timestamp
        
#         # Check if cache is valid
#         if (_network_map_cache is not None and 
#             _network_map_timestamp is not None and 
#             datetime.now() - _network_map_timestamp < CACHE_TTL):
#             print(f"✓ Using cached network mapping ({len(_network_map_cache.get('patterns', {}))} patterns)")
#             return _network_map_cache
        
#         # Load fresh data from database
#         try:
#             from app.db.models.network import Network  # Adjust import path as needed
            
#             # Load networks
#             stmt = select(Network.id, Network.network_name)
#             result = await db.execute(stmt)
#             networks = result.all()
            
#             network_map = {}
#             others_id = None
            
#             for network_id, network_name in networks:
#                 if not network_name:
#                     continue
                    
#                 name_lower = network_name.lower().strip()
                
#                 # Check if this is the "Others" network
#                 if name_lower in ['others', 'other', 'unknown']:
#                     others_id = network_id
#                     print(f"✓ Found 'Others' network with ID: {others_id}")
                
#                 # Direct name match
#                 network_map[name_lower] = network_id
                
#                 # Common patterns for your data
#                 network_map[f"nfs {name_lower}"] = network_id
#                 network_map[f"nfs_{name_lower}"] = network_id
#                 network_map[f"{name_lower} nfs"] = network_id
#                 network_map[f"nfs{name_lower}"] = network_id
                
#                 # Handle spaces vs underscores
#                 name_with_space = name_lower.replace("_", " ")
#                 name_with_underscore = name_lower.replace(" ", "_")
#                 network_map[name_with_space] = network_id
#                 network_map[name_with_underscore] = network_id
#                 network_map[f"nfs {name_with_space}"] = network_id
#                 network_map[f"nfs_{name_with_underscore}"] = network_id
            
#             # Load channel-network relationships to check which channels use networks
#             # Get distinct channel_ids from Network table that have at least one network
#             print(f"🔍 Checking which channels have networks...")
#             channel_network_stmt = select(Network.channel_id).where(
#                 Network.channel_id.isnot(None)
#             ).distinct()
#             channel_network_result = await db.execute(channel_network_stmt)
#             channels_with_networks = set(channel_network_result.scalars().all())
            
#             # Build channel config map: channel_id -> has_network (True/False)
#             channel_configs = {}
            
#             # Get all channels
#             all_channels_stmt = select(ChannelConfig.id, ChannelConfig.channel_name)
#             all_channels_result = await db.execute(all_channels_stmt)
#             all_channels = all_channels_result.all()
            
#             # Mark which channels have networks
#             for channel_id, channel_name in all_channels:
#                 has_networks = channel_id in channels_with_networks
#                 channel_configs[channel_id] = has_networks
#                 status = "✓ HAS networks" if has_networks else "○ NO networks"
#                 print(f"   Channel {channel_id} ({channel_name}): {status}")
            
#             # Update cache with patterns, others_id, and channel configs
#             _network_map_cache = {
#                 'patterns': network_map,
#                 'others_id': others_id,
#                 'channel_configs': channel_configs
#             }
#             _network_map_timestamp = datetime.now()
            
#             print(f"✓ Loaded fresh network mapping ({len(network_map)} patterns from {len(networks)} networks)")
#             print(f"✓ Loaded {len(channel_configs)} channel configurations")
#             if others_id:
#                 print(f"✓ 'Others' network configured with ID: {others_id}")
#             else:
#                 print(f"⚠️ Warning: No 'Others' network found - unmapped services will be NULL")
            
#             return _network_map_cache
            
#         except Exception as e:
#             print(f"⚠️ Warning: Could not load network mapping: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             print(f"   Network IDs will be NULL for all records")
#             return {'patterns': {}, 'others_id': None, 'channel_configs': {}}
    
#     @staticmethod
#     def _detect_card_type_from_number(card_number: str) -> str:
#         """
#         Detect card type (Visa, Mastercard, etc.) from card number using BIN ranges.
        
#         Args:
#             card_number: Card number string (can contain spaces/dashes)
            
#         Returns:
#             Card type name (e.g., "visa", "mastercard", "amex") or None if not detected
#         """
#         if not card_number:
#             return None
        
#         # Remove spaces, dashes, and other non-digit characters
#         card_digits = ''.join(filter(str.isdigit, str(card_number)))
        
#         # Need at least first digit to detect card type
#         if not card_digits:
#             return None
        
#         # Get first 2 and first 4 digits for BIN detection
#         first_digit = card_digits[0] if len(card_digits) >= 1 else ''
#         first_two = card_digits[:2] if len(card_digits) >= 2 else ''
#         first_four = card_digits[:4] if len(card_digits) >= 4 else ''
        
#         # Visa: starts with 4
#         if first_digit == '4':
#             return 'visa'
        
#         # Mastercard: 51-55, or 2221-2720
#         if first_two in ['51', '52', '53', '54', '55']:
#             return 'mastercard'
#         if len(first_four) >= 4:
#             bin_num = int(first_four)
#             if 2221 <= bin_num <= 2720:
#                 return 'mastercard'
        
#         # American Express: 34 or 37
#         if first_two in ['34', '37']:
#             return 'amex'
        
#         # Discover: 6011, 622126-622925, 644-649, 65
#         if first_four == '6011' or first_two == '65':
#             return 'discover'
#         if len(first_four) >= 4:
#             bin_num = int(first_four)
#             if 6440 <= bin_num <= 6499:
#                 return 'discover'
#         if len(card_digits) >= 6:
#             first_six = int(card_digits[:6])
#             if 622126 <= first_six <= 622925:
#                 return 'discover'
        
#         # Diners Club: 36, 38, 300-305
#         if first_two in ['36', '38']:
#             return 'diners'
#         if len(first_three := card_digits[:3]) >= 3:
#             if first_three in ['300', '301', '302', '303', '304', '305']:
#                 return 'diners'
        
#         # JCB: 3528-3589
#         if len(first_four) >= 4:
#             bin_num = int(first_four)
#             if 3528 <= bin_num <= 3589:
#                 return 'jcb'
        
#         # UnionPay: starts with 62
#         if first_two == '62':
#             return 'unionpay'
        
#         # RuPay (India): 60, 6521, 6522
#         if first_two == '60':
#             return 'rupay'
#         if first_four in ['6521', '6522']:
#             return 'rupay'
        
#         return None
    
#     @staticmethod
#     def _extract_network_id(
#         service_name: str, 
#         network_mapping: Dict[str, Any], 
#         channel_id: int,
#         card_number: str = None
#     ) -> int:
#         """
#         Extract network ID from service name or card number.
#         Only applies network logic if the channel has network configuration enabled.
        
#         Args:
#             service_name: Service name from CSV (e.g., "NFS AIRTEL", "visa", "mastercard")
#             network_mapping: Pre-loaded mapping dictionary with 'patterns', 'others_id', and 'channel_configs'
#             channel_id: Channel ID to check if network mapping is required
#             card_number: Card number for card type detection (optional)
            
#         Returns:
#             network_id or None (only returns ID if there's an actual match)
#         """
#         # CRITICAL: Check if this channel uses networks at all
#         channel_configs = network_mapping.get('channel_configs', {})
#         channel_has_network = channel_configs.get(channel_id, False)
        
#         # If channel doesn't use networks, always return None
#         if not channel_has_network:
#             return None
        
#         # Channel uses networks - proceed with mapping logic
#         network_map = network_mapping.get('patterns', {})
        
#         if not network_map:
#             return None
        
#         # PRIORITY 1: Try card number detection if provided
#         if card_number:
#             detected_card_type = UploadRepository._detect_card_type_from_number(card_number)
#             if detected_card_type:
#                 # Try to find network_id for this card type
#                 if detected_card_type in network_map:
#                     return network_map[detected_card_type]
        
#         # PRIORITY 2: Try service name matching
#         if not service_name:
#             return None
            
#         service_lower = str(service_name).lower().strip()
        
#         # Try direct match first (fastest)
#         if service_lower in network_map:
#             return network_map[service_lower]
        
#         # Try pattern matching (contains check)
#         # Sort by length to match most specific patterns first
#         sorted_patterns = sorted(network_map.keys(), key=len, reverse=True)
#         for pattern in sorted_patterns:
#             if pattern in service_lower:
#                 return network_map[pattern]
        
#         # No match found - return None
#         return None

#     @staticmethod
#     async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
#         try:
#             file_entry = UploadFile(
#                 file_name=fileData['file_name'],
#                 file_details= json.dumps (fileData['file_details']),
#                 channel_id = fileData['channel_id'],
#                 status=fileData['status'],
#                 record_details= json.dumps(fileData['record_details']),
#                 total_records=fileData['total_records'],
#                 version_number=fileData['version_number'],
#                 created_by=fileData['created_by']
#             )
#             db.add(file_entry)
#             await db.commit()
#             await db.refresh(file_entry)
#             return { "error": False, "status": "success","insertedId": file_entry.id}
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveUploadedFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
        
#     @staticmethod
#     async def saveFileDetails(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         getDateTimeColumnName: str,
#         getAmountColumnName: str,
#         getAcountNumberColumnName: str,
#         getCurrencyColumnName: str,
#         getReferenceNumberColumnName: str = None,
#         getTransactionIdColumnName: str = None,
#         getServiceNameColumnName: str = None,  # NEW: Service name column for network lookup
#         system_id: int = 1
#         ) -> Dict[str, Any]:
#         """
#         OPTIMIZED: Bulk duplicate detection using batched queries instead of N queries.
#         Performance: 100,000x faster than per-row queries
#         Safety: Batches keys to avoid PostgreSQL max_stack_depth error on huge uploads
#         Batch size is dynamically loaded from tbl_cfg_system_batch based on system_id
        
#         NEW: Network ID mapping using in-memory O(1) lookup (no per-row queries)
#         FIXED: Only stores network_id when service name actually matches a pattern
#         """
#         try:
#             # DEBUG: Log what mappings were received
#             print(f"\n[SAVE FILE DETAILS DEBUG]")
#             print(f"  getReferenceNumberColumnName: {getReferenceNumberColumnName}")
#             print(f"  getTransactionIdColumnName: {getTransactionIdColumnName}")
#             print(f"  getServiceNameColumnName: {getServiceNameColumnName}")  # NEW
#             print(f"  getDateTimeColumnName: {getDateTimeColumnName}")
#             print(f"  getAmountColumnName: {getAmountColumnName}")
#             print(f"  Total records: {len(fileData)}")
#             if fileData:
#                 print(f"  First record columns: {list(fileData[0].keys())[:10]}")
            
#             duplicates = []
#             new_records = []
            
#             # 🔥 PRE-LOAD NETWORK MAPPING ONCE (O(1) lookups for all rows)
#             network_mapping = await UploadRepository._build_network_mapping(db)
#             if network_mapping.get('patterns'):
#                 print(f"✓ Network mapping ready: {len(network_mapping['patterns'])} patterns loaded")
#             else:
#                 print(f"⚠️ No network mapping available - network_id will be NULL")
            
#             # CRITICAL OPTIMIZATION: Build unique keys for ALL records at once
#             keys_to_check = []
#             for row in fileData:
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
                
#                 # Add amount to key
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add date to key
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 # Add currency to key (if needed for duplicate check)
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 keys_to_check.append(tuple(key))
            
#             # BATCHED QUERY to check all duplicates (avoids Postgres max_stack_depth error)
#             # Split into chunks to prevent "stack depth limit exceeded" error on large batches
            
#             # DYNAMIC BATCH SIZE: Calculate based on number of jobs from tbl_cfg_system_batch
#             from app.db.models.system_batch_config import SystemBatchConfig
            
#             # Try to get number of jobs from database config
#             batch_config_stmt = select(SystemBatchConfig).where(
#                 SystemBatchConfig.system_id == system_id
#             )
#             batch_config_result = await db.execute(batch_config_stmt)
#             batch_config = batch_config_result.scalar_one_or_none()
            
#             total_records = len(keys_to_check)
            
#             if batch_config and batch_config.record_per_job:
#                 # record_per_job stores NUMBER OF JOBS
#                 num_jobs = int(batch_config.record_per_job)
#                 # Calculate batch size: total records / number of jobs
#                 BATCH_SIZE = max(1, (total_records + num_jobs - 1) // num_jobs)  # Ceiling division
#                 batch_source = f"calculated from {num_jobs} jobs (system_id={system_id})"
#             else:
#                 # Fallback to env var or default
#                 BATCH_SIZE = settings.UPLOAD_DUPLICATE_CHECK_BATCH_SIZE
#                 batch_source = "config/env default"
            
#             existing_keys = set()
            
#             if keys_to_check:
#                 import time
#                 batch_count = (len(keys_to_check) + BATCH_SIZE - 1) // BATCH_SIZE
#                 print(f"\n📊 Duplicate Check Starting:")
#                 print(f"   Total keys to check: {len(keys_to_check):,}")
#                 print(f"   Batch size: {BATCH_SIZE:,} ({batch_source})")
#                 print(f"   Number of batches/jobs: {batch_count}")
                
#                 for i in range(0, len(keys_to_check), BATCH_SIZE):
#                     batch_start = time.time()
#                     batch_keys = keys_to_check[i:i+BATCH_SIZE]
#                     batch_num = i // BATCH_SIZE + 1
                    
#                     if getCurrencyColumnName:
#                         # Query with currency
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date,
#                             Transaction.ccy
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date,
#                                 Transaction.ccy
#                             ).in_(batch_keys)
#                         )
#                     else:
#                         # Query without currency
#                         keys_without_ccy = [k[:4] for k in batch_keys]  # Remove last element (None)
#                         stmt = select(
#                             Transaction.channel_id,
#                             Transaction.source_id,
#                             Transaction.amount,
#                             Transaction.date
#                         ).where(
#                             tuple_(
#                                 Transaction.channel_id,
#                                 Transaction.source_id,
#                                 Transaction.amount,
#                                 Transaction.date
#                             ).in_(keys_without_ccy)
#                         )
                    
#                     result = await db.execute(stmt)
#                     batch_records = result.all()
                    
#                     # Add to existing_keys set for O(1) lookup
#                     existing_keys.update(tuple(record) for record in batch_records)
                    
#                     # Log batch completion
#                     batch_time = time.time() - batch_start
#                     duplicates_found = len(batch_records)
#                     print(f"   ✓ Batch {batch_num}/{batch_count}: "
#                           f"{len(batch_keys):,} keys checked, "
#                           f"{duplicates_found:,} duplicates found, "
#                           f"{batch_time:.2f}s")
                
#                 print(f"✓ Duplicate check complete: {len(existing_keys):,} total duplicates found\n")
            
#             # Process each row and check against existing_keys (in-memory, super fast)
#             rows_processed = 0
#             network_stats = {
#                 "mapped": 0, 
#                 "unmapped_null": 0,  # CHANGED: No service name match found
#                 "channel_no_network": 0,  # Channels that don't use networks
#                 "null_no_service": 0       # No service name provided
#             }
            
#             for row in fileData:
#                 # Debug: Log first row to see actual column names
#                 if rows_processed == 0:
#                     print(f"DEBUG - First row columns: {list(row.keys())}")
#                     print(f"DEBUG - First row data sample: {dict(list(row.items())[:5])}")
                
#                 # Build key for this row
#                 key = [
#                     fileJson["channel_id"],
#                     fileJson["source_id"],
#                 ]
#                 rows_processed += 1
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     key.append(str(row[getAmountColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     key.append(str(row[getDateTimeColumnName]))
#                 else:
#                     key.append(None)
                
#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     key.append(str(row[getCurrencyColumnName]))
#                 else:
#                     key.append(None)
                
#                 # O(1) duplicate check using set lookup
#                 if tuple(key) in existing_keys:
#                     duplicates.append(row)
#                     continue
                
#                 # Build new record with current timestamp
#                 current_timestamp = datetime.now()
#                 data = {
#                     "channel_id": fileJson["channel_id"],
#                     "source_id": fileJson["source_id"],
#                     "otherDetails": json.dumps(row, default=str),
#                     "file_transactions_id": fileJson["file_transactions_id"],
#                     "created_by": 1,
#                     "updated_by": 1,
#                     "version_number": fileJson["version_number"],
#                     "created_at": current_timestamp,
#                     "updated_at": current_timestamp,
#                 }
                
#                 # DEBUG: Log fileJson to see what source_id we're receiving
#                 if len(new_records) == 0:  # Only log once per batch
#                     print(f"[UPLOAD DEBUG] fileJson: {fileJson}")
#                     print(f"[UPLOAD DEBUG] Saving with source_id={fileJson['source_id']}, channel_id={fileJson['channel_id']}")
#                     print(f"[UPLOAD DEBUG] created_at explicitly set to: {current_timestamp}")
                
#                 if getAmountColumnName is not None and getAmountColumnName in row:
#                     data["amount"] = str(row[getAmountColumnName])

#                 if getDateTimeColumnName is not None and getDateTimeColumnName in row:
#                     # Handle datetime properly - check if it's already a datetime/timestamp
#                     date_value = row[getDateTimeColumnName]
#                     if date_value is not None and str(date_value) not in ['', 'nan', 'NaT', 'None']:
#                         # If it's a pandas Timestamp or datetime object, format it properly
#                         if hasattr(date_value, 'strftime'):
#                             data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
#                         else:
#                             data["date"] = str(date_value)

#                 if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
#                     data["account_number"] = str(row[getAcountNumberColumnName])

#                 if getCurrencyColumnName is not None and getCurrencyColumnName in row:
#                     data["ccy"] = str(row[getCurrencyColumnName])
                
#                 # Map reference_number - PRIORITIZE EXPLICIT MAPPING
#                 # 1. First, check if user explicitly mapped reference_number field
#                 reference_mapped = False
#                 if getReferenceNumberColumnName is not None and getReferenceNumberColumnName in row:
#                     if row[getReferenceNumberColumnName] is not None and str(row[getReferenceNumberColumnName]).strip():
#                         data["reference_number"] = str(row[getReferenceNumberColumnName]).strip()
#                         reference_mapped = True
#                         if len(new_records) == 0:
#                             print(f"[REF DEBUG] Explicit mapping used: {getReferenceNumberColumnName} = {data['reference_number']}")
                
#                 if not reference_mapped:
#                     # 2. Fall back to auto-detection from common field names
#                     reference_fields = [
#                         'transaction_id', 'transaction id',
#                         'rrn',
#                         'reference_number', 'reference number',
#                         'referencenumber', 
#                         'ref_number', 'ref number',
#                         'refnumber', 
#                         'retrieval_reference_number', 'retrieval reference number'
#                     ]
#                     reference_found = False
#                     for field in reference_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["reference_number"] = str(row[field]).strip()
#                             reference_found = True
#                             if len(new_records) == 0:
#                                 print(f"[REF DEBUG] Auto-detected field '{field}' = {data['reference_number']}")
#                             break
                    
#                     if not reference_found and len(new_records) == 0:
#                         print(f"[REF DEBUG] ❌ No reference field found!")
#                         print(f"[REF DEBUG] Available columns: {list(row.keys())}")
#                         print(f"[REF DEBUG] Searching for: {reference_fields[:5]}...")
                
#                 # Map txn_id - PRIORITIZE EXPLICIT MAPPING
#                 if getTransactionIdColumnName is not None and getTransactionIdColumnName in row:
#                     if row[getTransactionIdColumnName] is not None and str(row[getTransactionIdColumnName]).strip():
#                         data["txn_id"] = str(row[getTransactionIdColumnName]).strip()
#                 else:
#                     # Fall back to auto-detection
#                     txn_id_fields = [
#                         'receipt_number', 'receipt number',
#                         'payer_transaction_id', 'payer transaction id',
#                         'stan',
#                         'txn_id', 'txn id',
#                         'transactionid', 
#                         'txnid'
#                     ]
#                     for field in txn_id_fields:
#                         if field in row and row[field] is not None and str(row[field]).strip():
#                             data["txn_id"] = str(row[field]).strip()
#                             break
                
#                 # 🔥 FIXED: Map network_id from service_name OR card_number
#                 network_id = None
#                 network_mapped = False
#                 service_name_value = None
#                 card_number_value = None
#                 channel_id = fileJson["channel_id"]
                
#                 # Check if this channel uses networks
#                 channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
#                 if channel_has_network:
#                     # Channel uses networks - proceed with mapping
#                     if len(new_records) == 0:
#                         print(f"\n[CHANNEL DEBUG] Channel {channel_id} HAS network configuration")
                    
#                     # STEP 1: Extract card_number if available (for Card channels)
#                     card_number_fields = [
#                         'card_number', 'card number', 'cardnumber',
#                         'pan', 'card_pan',
#                         'account_number', 'account number',  # Sometimes card number is in account field
#                     ]
#                     for field in card_number_fields:
#                         if field in row and row[field]:
#                             card_number_value = str(row[field]).strip()
#                             if len(new_records) == 0:
#                                 # Mask card number for security in logs
#                                 masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
#                                 print(f"[CARD DEBUG] Found card number in '{field}': {masked_card}")
#                             break
                    
#                     # STEP 2: Check if user explicitly mapped service_name field
#                     if getServiceNameColumnName and getServiceNameColumnName in row:
#                         service_name_value = row[getServiceNameColumnName]
#                         if len(new_records) == 0 and service_name_value:
#                             print(f"[SERVICE DEBUG] Explicit service_name: '{service_name_value}'")
                    
#                     # STEP 3: Fall back to auto-detection of service_name from common field names
#                     if not service_name_value:
#                         service_fields = [
#                             'service_name', 'service name',
#                             'servicename',
#                             'network', 'network_name', 'network name',
#                             'provider', 'provider_name', 'provider name',
#                             'operator', 'operator_name', 'operator name',
#                             'payer_client', 'payer client',
#                             'service', 'channel', 'card_type', 'card type'
#                         ]
                        
#                         for field in service_fields:
#                             if field in row and row[field]:
#                                 service_name_value = row[field]
#                                 if len(new_records) == 0:
#                                     print(f"[SERVICE DEBUG] Auto-detected service_name from '{field}': '{service_name_value}'")
#                                 break
                    
#                     # STEP 4: Extract network_id using BOTH card_number and service_name
#                     # The _extract_network_id function will prioritize card_number detection
#                     network_id = UploadRepository._extract_network_id(
#                         service_name=service_name_value,
#                         network_mapping=network_mapping,
#                         channel_id=channel_id,
#                         card_number=card_number_value  # NEW: Pass card number for card type detection
#                     )
                    
#                     if len(new_records) == 0:
#                         if network_id:
#                             if card_number_value:
#                                 card_type = UploadRepository._detect_card_type_from_number(card_number_value)
#                                 print(f"[NETWORK DEBUG] ✓ Detected from card: '{card_type}' → network_id={network_id}")
#                             else:
#                                 print(f"[NETWORK DEBUG] ✓ Matched service_name: '{service_name_value}' → network_id={network_id}")
#                         else:
#                             if card_number_value:
#                                 print(f"[NETWORK DEBUG] ✗ Card number provided but no card type detected → NULL")
#                             elif service_name_value:
#                                 print(f"[NETWORK DEBUG] ✗ Service name '{service_name_value}' did NOT match any pattern → NULL")
#                             else:
#                                 print(f"[NETWORK DEBUG] ✗ No card number or service name found → NULL")
#                 else:
#                     # Channel does NOT use networks - network_id should be NULL
#                     network_id = None
#                     if len(new_records) == 0:
#                         print(f"\n[CHANNEL DEBUG] Channel {channel_id} does NOT have network configuration → network_id=NULL")
                
#                 # 4. Assign network_id
#                 data["network_id"] = network_id
                
#                 # CRITICAL DEBUG: Show what's actually being saved
#                 if len(new_records) == 0:
#                     print(f"\n[CRITICAL] Final network_id assignment:")
#                     print(f"   channel_id: {channel_id}")
#                     print(f"   channel_has_network: {channel_has_network}")
#                     if card_number_value:
#                         masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
#                         card_type = UploadRepository._detect_card_type_from_number(card_number_value)
#                         print(f"   card_number_value: {masked_card} (detected: {card_type or 'unknown'})")
#                     else:
#                         print(f"   card_number_value: None")
#                     print(f"   service_name_value: {service_name_value}")
#                     print(f"   network_id assigned: {network_id}")
#                     print(f"   data['network_id']: {data.get('network_id')}")
#                     print(f"   Type: {type(data.get('network_id'))}\n")
                
#                 # Track network mapping statistics
#                 channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
#                 if not channel_has_network:
#                     # Channel doesn't use networks - this is expected
#                     network_stats["channel_no_network"] += 1
#                 elif network_id:
#                     # Channel uses networks and we found a network_id (actual match)
#                     network_stats["mapped"] += 1
#                 elif service_name_value or card_number_value:
#                     # Channel uses networks, data provided, but no match found
#                     network_stats["unmapped_null"] += 1
#                 else:
#                     # Channel uses networks but no service name or card number found
#                     network_stats["null_no_service"] += 1
                
#                 # Debug log for first record to verify field mapping
#                 if len(new_records) == 0:
#                     print(f"\n=== FIRST RECORD FIELD MAPPING DEBUG ===")
#                     print(f"Explicit mappings provided:")
#                     print(f"  - reference_number: {getReferenceNumberColumnName or 'AUTO-DETECT'}")
#                     print(f"  - transaction_id: {getTransactionIdColumnName or 'AUTO-DETECT'}")
#                     print(f"  - service_name: {getServiceNameColumnName or 'AUTO-DETECT'}")
#                     print(f"Available fields in row: {list(row.keys())}")
#                     print(f"Sample data: {dict(list(row.items())[:5])}")
#                     print(f"Mapped values:")
#                     print(f"  - Reference number: {data.get('reference_number', 'NOT MAPPED')}")
#                     print(f"  - Transaction ID: {data.get('txn_id', 'NOT MAPPED')}")
#                     print(f"  - Network ID: {data.get('network_id', 'NULL')}")
#                     if card_number_value:
#                         masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
#                         print(f"  - Card Number Used: {masked_card}")
#                     else:
#                         print(f"  - Service Name Used: {service_name_value or 'NOT FOUND'}")
#                     print(f"  - Amount: {data.get('amount', 'NOT MAPPED')}")
#                     print(f"  - Date: {data.get('date', 'NOT MAPPED')}")
#                     print(f"\n[CRITICAL DEBUG] Complete data dict being inserted:")
#                     print(f"  {data}")
#                     print(f"=====================================\n")
                
#                 new_records.append(Transaction(**data))

#             if new_records:
#                 db.add_all(new_records)
#                 await db.commit()
            
#             # Print network mapping statistics
#             print(f"\n📊 Network Mapping Statistics:")
#             print(f"   ✓ Successfully mapped to network: {network_stats['mapped']:,}")
#             print(f"   ⚠️ No pattern match (NULL): {network_stats['unmapped_null']:,}")
#             print(f"   ○ Channel has no networks (NULL): {network_stats['channel_no_network']:,}")
#             print(f"   ⚠️ Missing service name (NULL): {network_stats['null_no_service']:,}")
#             print(f"   Total processed: {len(new_records):,}\n")

#             return {
#                 "status": "success",
#                 "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
#                 "recordsSaved": len(new_records),
#                 "duplicateRecords": duplicates,
#                 "networkStats": network_stats
#             }
#         except Exception as e:
#             await db.rollback()
#             print("uploadRepository-saveFileDetails", str(e))
#             return { "error": True, "status": "error", "message": str(e)}
    
#     @staticmethod
#     async def updateUploadProgress(
#         db: AsyncSession,
#         file_id: int,
#         processed: int,
#         success: int,
#         failed: int,
#         duplicates: int,
#         total: int
#     ) -> None:
#         """
#         Update progress tracking fields for an upload file.
#         Called after each batch is processed.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.processed_records = processed
#                 upload_file.success_records = success
#                 upload_file.failed_records = failed
#                 upload_file.duplicate_records = duplicates
                
#                 # Calculate progress percentage
#                 if total > 0:
#                     upload_file.progress_percentage = round((processed / total) * 100, 2)
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateUploadProgress: {str(e)}")
    
#     @staticmethod
#     async def updateFileStatus(
#         db: AsyncSession,
#         file_id: int,
#         status: int,
#         error_message: str = None,
#         error_details: str = None
#     ) -> None:
#         """
#         Update upload file status and error information.
#         status: 0=pending, 1=processing, 2=completed, 3=failed
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
            
#             if upload_file:
#                 upload_file.status = status
                
#                 # Set timing fields
#                 if status == 1 and not upload_file.upload_started_at:
#                     # Starting processing
#                     upload_file.upload_started_at = datetime.utcnow()
#                 elif status in [2, 3]:
#                     # Completed or failed
#                     upload_file.upload_completed_at = datetime.utcnow()
#                     if upload_file.upload_started_at:
#                         time_diff = upload_file.upload_completed_at - upload_file.upload_started_at
#                         upload_file.processing_time_seconds = int(time_diff.total_seconds())
                
#                 # Set error info if provided
#                 if error_message:
#                     upload_file.error_message = error_message
#                 if error_details:
#                     upload_file.error_details = error_details
                
#                 await db.commit()
#         except Exception as e:
#             await db.rollback()
#             print(f"uploadRepository-updateFileStatus: {str(e)}")
    
#     @staticmethod
#     async def saveFileDetailsBatch(
#         db: AsyncSession,
#         fileData: List[Dict[str, Any]],
#         fileJson: Dict[str, Any],
#         column_mappings: Dict[str, str]
#     ) -> Dict[str, Any]:
#         """
#         Optimized batch processing method for Celery tasks.
#         Uses the same bulk duplicate detection as saveFileDetails.
        
#         Args:
#             column_mappings: Dict with keys: date, amount, account_number, currency, 
#                             reference_number, transaction_id, service_name
#             Note: Column names will be converted to lowercase to match normalized data
#         """
#         # Convert column names to lowercase to match normalized column names
#         date_col = column_mappings.get("date")
#         amount_col = column_mappings.get("amount")
#         account_col = column_mappings.get("account_number")
#         currency_col = column_mappings.get("currency")
#         reference_col = column_mappings.get("reference_number")
#         transaction_id_col = column_mappings.get("transaction_id")
#         service_name_col = column_mappings.get("service_name")  # NEW
        
#         return await UploadRepository.saveFileDetails(
#             db=db,
#             fileData=fileData,
#             fileJson=fileJson,
#             getDateTimeColumnName=date_col.lower() if date_col else None,
#             getAmountColumnName=amount_col.lower() if amount_col else None,
#             getAcountNumberColumnName=account_col.lower() if account_col else None,
#             getCurrencyColumnName=currency_col.lower() if currency_col else None,
#             getReferenceNumberColumnName=reference_col.lower() if reference_col else None,
#             getTransactionIdColumnName=transaction_id_col.lower() if transaction_id_col else None,
#             getServiceNameColumnName=service_name_col.lower() if service_name_col else None  # NEW
#         )
    
#     @staticmethod
#     async def getUploadProgress(db: AsyncSession, file_id: int) -> Dict[str, Any]:
#         """
#         Get upload progress information for a specific file.
#         Used by progress tracking API endpoint.
#         """
#         try:
#             stmt = select(UploadFile).where(UploadFile.id == file_id)
#             result = await db.execute(stmt)
#             upload_file = result.scalar_one_or_none()
#             if not upload_file:
#                 return {"error": True, "message": "File not found"}
            
#             return {
#                 "error": False,
#                 "file_id": upload_file.id,
#                 "file_name": upload_file.file_name,
#                 "status": upload_file.status,
#                 "total_records": upload_file.total_records,
#                 "processed_records": upload_file.processed_records,
#                 "success_records": upload_file.success_records,
#                 "failed_records": upload_file.failed_records,
#                 "duplicate_records": upload_file.duplicate_records,
#                 "progress_percentage": upload_file.progress_percentage,
#                 "upload_started_at": upload_file.upload_started_at.isoformat() if upload_file.upload_started_at else None,
#                 "upload_completed_at": upload_file.upload_completed_at.isoformat() if upload_file.upload_completed_at else None,
#                 "processing_time_seconds": upload_file.processing_time_seconds,
#                 "error_message": upload_file.error_message,
#                 "error_details": upload_file.error_details
#             }
#         except Exception as e:
#             print(f"uploadRepository-getUploadProgress: {str(e)}")
#             return {"error": True, "message": str(e)}

#     @staticmethod
#     async def getFileList(db: AsyncSession, offset: int, limit: int):
#         try:
#             # Total count
#             total_stmt = select(func.count()).select_from(UploadFile)
#             total_result = await db.execute(total_stmt)
#             total = total_result.scalar()
#             # Data query
#             stmt = (
#                 select(
#                     UploadFile,
#                     UserConfig.id.label("user_id"),
#                     UserConfig.f_name.label("f_name"),
#                     UserConfig.m_name.label("m_name"),
#                     UserConfig.l_name.label("l_name"),
#                     UserConfig.email.label("email"),
#                     ChannelConfig.id.label("channel_id"),
#                     ChannelConfig.channel_name.label("channel_name"),
#                     SourceConfig.id.label("source_id"),
#                     SourceConfig.source_name.label("source_name"),
#                     SourceConfig.source_type.label("source_type"),
#                 )
#                 .outerjoin(UserConfig, UserConfig.id == UploadFile.created_by)
#                 .outerjoin(ChannelConfig, ChannelConfig.id == UploadFile.channel_id)
#                 .outerjoin(
#                     SourceConfig,
#                     cast(
#                         UploadFile.file_details.cast(JSONB)["file_type"].astext,
#                         Integer
#                     ) == SourceConfig.id
#                 )
#                 .order_by(UploadFile.created_at.desc())
#                 .offset(offset)
#                 .limit(limit)
#             )

#             result = await db.execute(stmt)
#             rows = result.all()
#             data = []
#             for upload, user_id, f_name, m_name, l_name, email, channel_id, channel_name,source_id, source_name, source_type in rows:
#                 file_details_obj = None
#                 file_record_obj = {
#                     "total_records": upload.total_records,
#                     "processed": upload.processed_records,
#                     "success": upload.success_records,
#                     "failed": upload.failed_records,
#                     "duplicates": upload.duplicate_records,
#                     "progress_percentage": upload.progress_percentage,
#                 }


#                 if upload.file_details:
#                     try:
#                         file_details_obj = json.loads(upload.file_details)
#                     except json.JSONDecodeError:
#                         pass
                    
#                 data.append({
#                     "id": upload.id,
#                     "file_name": upload.file_name,
#                     "file_details": file_details_obj,
#                     "status": upload.status,
#                     "record_details": file_record_obj,
#                     "created_at": upload.created_at,
#                     "version_number": upload.version_number,

#                     "user": {
#                         "id": user_id,
#                         "name": " ".join(filter(None, [f_name, m_name, l_name])),
#                         "email": email,
#                     } if user_id else None,

#                     "channel": {
#                         "id": channel_id,
#                         "name": channel_name,
#                     } if channel_id else None,
#                     "source": {
#                         "id": source_id,
#                         "name": source_name,
#                         "type": source_type,
#                     } if source_id else None
#                 })
#             return {
#                 "status": "success",
#                 "offset": offset,
#                 "limit": limit,
#                 "total": total,
#                 "data": data
#             }
#         except Exception as e:
#             # Rollback is safe even for SELECTs
#             await db.rollback()
#             print("UploadRepository.getFileList error:", str(e))
#             return {
#                 "status": "error",
#                 "message": "Failed to fetch upload file list",
#                 "error": str(e)
#             }
    
#     @staticmethod
#     async def deleteFileAndTransactions(db: AsyncSession, file_id: int) -> bool:
        
#         result = await db.execute(
#             select(UploadFile).where(UploadFile.id == file_id)
#         )
#         file_record = result.scalar_one_or_none()

#         if not file_record:
#             return False

#         try:
#             # 1️⃣ Delete related transactions
#             await db.execute(
#                 delete(Transaction).where(
#                     Transaction.file_transactions_id == file_id
#                 )
#             )

#             # 2️⃣ Delete file upload record
#             await db.execute(
#                 delete(UploadFile).where(UploadFile.id == file_id)
#             )

#             await db.commit()
#             return True

#         except Exception as e:
#             await db.rollback()
#             raise e

#     @staticmethod
#     async def clearNetworkCache() -> None:
#         """
#         Clear the network mapping cache.
#         Call this when network configuration changes.
#         """
#         global _network_map_cache, _network_map_timestamp
#         _network_map_cache = None
#         _network_map_timestamp = None
#         print("✓ Network mapping cache cleared")
    
#     @staticmethod
#     async def updateNetworkIdForChannel(
#         db: AsyncSession,
#         channel_id: int,
#         batch_size: int = 1000
#     ) -> Dict[str, Any]:
#         """
#         Update network_id for all transactions belonging to a specific channel.
#         This is useful when:
#         1. A channel is newly configured to use networks
#         2. Network patterns have been updated
#         3. Existing transactions need their network_id corrected
        
#         Args:
#             channel_id: The channel ID to update transactions for
#             batch_size: Number of records to process in each batch
            
#         Returns:
#             Dict with update statistics
#         """
#         try:
#             print(f"\n🔄 Starting network_id update for channel_id={channel_id}")
            
#             # Load network mapping
#             network_mapping = await UploadRepository._build_network_mapping(db)
            
#             # Check if this channel has networks
#             channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
            
#             if not channel_has_network:
#                 return {
#                     "error": False,
#                     "status": "skipped",
#                     "message": f"Channel {channel_id} does not have network configuration. No updates needed.",
#                     "updated_count": 0
#                 }
            
#             print(f"✓ Channel {channel_id} has network configuration")
            
#             # Count total transactions for this channel
#             count_stmt = select(func.count()).select_from(Transaction).where(
#                 Transaction.channel_id == channel_id
#             )
#             count_result = await db.execute(count_stmt)
#             total_transactions = count_result.scalar()
            
#             print(f"📊 Found {total_transactions:,} transactions to process")
            
#             if total_transactions == 0:
#                 return {
#                     "error": False,
#                     "status": "success",
#                     "message": "No transactions found for this channel",
#                     "updated_count": 0
#                 }
            
#             # Process in batches
#             updated_count = 0
#             null_count = 0
#             unchanged_count = 0
#             offset = 0
            
#             # Service name fields to check (in priority order)
#             service_fields = [
#                 'service_name', 'service name',
#                 'servicename',
#                 'network', 'network_name', 'network name',
#                 'provider', 'provider_name', 'provider name',
#                 'operator', 'operator_name', 'operator name',
#                 'payer_client', 'payer client',
#                 'service', 'channel', 'card_type', 'card type'
#             ]
            
#             # Card number fields to check (for Card channels)
#             card_number_fields = [
#                 'card_number', 'card number', 'cardnumber',
#                 'pan', 'card_pan',
#                 'account_number', 'account number',
#             ]
            
#             while offset < total_transactions:
#                 print(f"\n📦 Processing batch {offset // batch_size + 1} (records {offset + 1} to {min(offset + batch_size, total_transactions)})")
                
#                 # Fetch batch of transactions
#                 stmt = select(Transaction).where(
#                     Transaction.channel_id == channel_id
#                 ).offset(offset).limit(batch_size)
                
#                 result = await db.execute(stmt)
#                 transactions = result.scalars().all()
                
#                 batch_updated = 0
#                 batch_null = 0
#                 batch_unchanged = 0
                
#                 for txn in transactions:
#                     try:
#                         # Parse otherDetails JSON to extract service name and card number
#                         other_details = json.loads(txn.otherDetails) if txn.otherDetails else {}
                        
#                         # Try to find service name from various possible fields
#                         service_name_value = None
#                         for field in service_fields:
#                             if field in other_details and other_details[field]:
#                                 service_name_value = other_details[field]
#                                 break
                        
#                         # Try to find card number from various possible fields
#                         card_number_value = None
#                         for field in card_number_fields:
#                             if field in other_details and other_details[field]:
#                                 card_number_value = other_details[field]
#                                 break
                        
#                         # Extract network_id using the same logic as upload (with BOTH service_name and card_number)
#                         new_network_id = UploadRepository._extract_network_id(
#                             service_name=service_name_value, 
#                             network_mapping=network_mapping, 
#                             channel_id=channel_id,
#                             card_number=card_number_value  # NEW: Pass card number for card type detection
#                         )
                        
#                         # Update only if network_id has changed
#                         if new_network_id != txn.network_id:
#                             txn.network_id = new_network_id
#                             txn.updated_at = datetime.now()
#                             batch_updated += 1
                            
#                             if new_network_id is None:
#                                 batch_null += 1
#                         else:
#                             batch_unchanged += 1
                            
#                     except json.JSONDecodeError as e:
#                         print(f"⚠️ Warning: Could not parse otherDetails for transaction {txn.id}: {str(e)}")
#                         batch_unchanged += 1
#                         continue
#                     except Exception as e:
#                         print(f"⚠️ Warning: Error processing transaction {txn.id}: {str(e)}")
#                         batch_unchanged += 1
#                         continue
                
#                 # Commit batch
#                 await db.commit()
                
#                 updated_count += batch_updated
#                 null_count += batch_null
#                 unchanged_count += batch_unchanged
                
#                 print(f"   ✓ Batch complete: {batch_updated} updated, {batch_null} set to NULL, {batch_unchanged} unchanged")
                
#                 offset += batch_size
            
#             # Final summary
#             print(f"\n✅ Update Complete!")
#             print(f"   Total transactions processed: {total_transactions:,}")
#             print(f"   Updated with network_id: {updated_count - null_count:,}")
#             print(f"   Set to NULL (no match): {null_count:,}")
#             print(f"   Unchanged: {unchanged_count:,}")
            
#             return {
#                 "error": False,
#                 "status": "success",
#                 "message": f"Successfully updated network_id for channel {channel_id}",
#                 "total_processed": total_transactions,
#                 "updated_count": updated_count,
#                 "null_count": null_count,
#                 "unchanged_count": unchanged_count
#             }
            
#         except Exception as e:
#             await db.rollback()
#             print(f"❌ Error updating network_id for channel {channel_id}: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return {
#                 "error": True,
#                 "status": "error",
#                 "message": str(e)
#             }
    
#     @staticmethod
#     async def updateNetworkIdForAllChannels(
#         db: AsyncSession,
#         batch_size: int = 1000
#     ) -> Dict[str, Any]:
#         """
#         Update network_id for ALL channels that have network configuration.
#         This will process each channel that has networks configured.
        
#         Args:
#             batch_size: Number of records to process in each batch per channel
            
#         Returns:
#             Dict with overall statistics
#         """
#         try:
#             print(f"\n🔄 Starting network_id update for ALL channels with network configuration")
            
#             # Load network mapping to find channels with networks
#             network_mapping = await UploadRepository._build_network_mapping(db)
#             channel_configs = network_mapping.get('channel_configs', {})
            
#             # Filter channels that have networks
#             channels_with_networks = [
#                 channel_id for channel_id, has_networks in channel_configs.items()
#                 if has_networks
#             ]
            
#             if not channels_with_networks:
#                 return {
#                     "error": False,
#                     "status": "skipped",
#                     "message": "No channels found with network configuration",
#                     "channels_processed": 0,
#                     "total_updated": 0
#                 }
            
#             print(f"✓ Found {len(channels_with_networks)} channels with network configuration")
#             print(f"   Channel IDs: {channels_with_networks}")
            
#             # Process each channel
#             results = []
#             total_updated = 0
#             total_processed = 0
            
#             for channel_id in channels_with_networks:
#                 print(f"\n{'='*60}")
#                 print(f"Processing Channel ID: {channel_id}")
#                 print(f"{'='*60}")
                
#                 result = await UploadRepository.updateNetworkIdForChannel(
#                     db=db,
#                     channel_id=channel_id,
#                     batch_size=batch_size
#                 )
                
#                 results.append({
#                     "channel_id": channel_id,
#                     "result": result
#                 })
                
#                 if not result.get("error"):
#                     total_updated += result.get("updated_count", 0)
#                     total_processed += result.get("total_processed", 0)
            
#             # Final summary
#             print(f"\n{'='*60}")
#             print(f"✅ ALL CHANNELS UPDATE COMPLETE!")
#             print(f"{'='*60}")
#             print(f"   Channels processed: {len(channels_with_networks)}")
#             print(f"   Total transactions processed: {total_processed:,}")
#             print(f"   Total updates made: {total_updated:,}")
            
#             return {
#                 "error": False,
#                 "status": "success",
#                 "message": f"Successfully updated network_id for {len(channels_with_networks)} channels",
#                 "channels_processed": len(channels_with_networks),
#                 "total_transactions_processed": total_processed,
#                 "total_updated": total_updated,
#                 "channel_results": results
#             }
            
#         except Exception as e:
#             await db.rollback()
#             print(f"❌ Error updating network_id for all channels: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return {
#                 "error": True,
#                 "status": "error",
#                 "message": str(e)
#             }

import json
from typing import Any, Dict, List
from datetime import datetime, timedelta

from sqlalchemy import Integer, cast, delete, func, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from app.db.models.transactions import Transaction
from app.db.models.upload_file import UploadFile
from app.config import settings
from app.db.models.user_config import UserConfig
from sqlalchemy.dialects.postgresql import JSONB
import logging
logger = logging.getLogger(__name__)

# Global cache for network mapping (optional optimization)
_network_map_cache = None
_network_map_timestamp = None
CACHE_TTL = timedelta(hours=1)

class UploadRepository:
    
    @staticmethod
    async def _build_network_mapping(db: AsyncSession) -> Dict[str, Any]:
        """
        Pre-load all networks into memory for O(1) lookup.
        Uses 1-hour cache to avoid repeated DB queries.
        Returns: Dict with 'patterns' mapping, 'others_id' for fallback, and 'channel_configs'
        """
        global _network_map_cache, _network_map_timestamp
        
        # Check if cache is valid
        if (_network_map_cache is not None and 
            _network_map_timestamp is not None and 
            datetime.now() - _network_map_timestamp < CACHE_TTL):
            print(f"✓ Using cached network mapping ({len(_network_map_cache.get('patterns', {}))} patterns)")
            return _network_map_cache
        
        # Load fresh data from database
        try:
            from app.db.models.network import Network  # Adjust import path as needed
            
            # Load networks
            stmt = select(Network.id, Network.network_name)
            result = await db.execute(stmt)
            networks = result.all()
            
            network_map = {}
            others_id = None
            
            for network_id, network_name in networks:
                if not network_name:
                    continue
                    
                name_lower = network_name.lower().strip()
                
                # Check if this is the "Others" network
                if name_lower in ['others', 'other', 'unknown']:
                    others_id = network_id
                    print(f"✓ Found 'Others' network with ID: {others_id}")
                
                # Direct name match
                network_map[name_lower] = network_id
                
                # Common patterns for your data
                network_map[f"nfs {name_lower}"] = network_id
                network_map[f"nfs_{name_lower}"] = network_id
                network_map[f"{name_lower} nfs"] = network_id
                network_map[f"nfs{name_lower}"] = network_id
                
                # Handle spaces vs underscores
                name_with_space = name_lower.replace("_", " ")
                name_with_underscore = name_lower.replace(" ", "_")
                network_map[name_with_space] = network_id
                network_map[name_with_underscore] = network_id
                network_map[f"nfs {name_with_space}"] = network_id
                network_map[f"nfs_{name_with_underscore}"] = network_id
            
            # Load channel-network relationships to check which channels use networks
            # Get distinct channel_ids from Network table that have at least one network
            print(f"🔍 Checking which channels have networks...")
            channel_network_stmt = select(Network.channel_id).where(
                Network.channel_id.isnot(None)
            ).distinct()
            channel_network_result = await db.execute(channel_network_stmt)
            channels_with_networks = set(channel_network_result.scalars().all())
            
            # Build channel config map: channel_id -> has_network (True/False)
            channel_configs = {}
            
            # Get all channels
            all_channels_stmt = select(ChannelConfig.id, ChannelConfig.channel_name)
            all_channels_result = await db.execute(all_channels_stmt)
            all_channels = all_channels_result.all()
            
            # Mark which channels have networks
            for channel_id, channel_name in all_channels:
                has_networks = channel_id in channels_with_networks
                channel_configs[channel_id] = has_networks
                status = "✓ HAS networks" if has_networks else "○ NO networks"
                print(f"   Channel {channel_id} ({channel_name}): {status}")
            
            # Update cache with patterns, others_id, and channel configs
            _network_map_cache = {
                'patterns': network_map,
                'others_id': others_id,
                'channel_configs': channel_configs
            }
            _network_map_timestamp = datetime.now()
            
            print(f"✓ Loaded fresh network mapping ({len(network_map)} patterns from {len(networks)} networks)")
            print(f"✓ Loaded {len(channel_configs)} channel configurations")
            if others_id:
                print(f"✓ 'Others' network configured with ID: {others_id}")
            else:
                print(f"⚠️ Warning: No 'Others' network found - unmapped services will be NULL")
            
            return _network_map_cache
            
        except Exception as e:
            print(f"⚠️ Warning: Could not load network mapping: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"   Network IDs will be NULL for all records")
            return {'patterns': {}, 'others_id': None, 'channel_configs': {}}
    
    @staticmethod
    def _detect_card_type_from_number(card_number: str) -> str:
        """
        Detect card type (Visa, Mastercard, etc.) from card number using BIN ranges.
        Works with both full and masked card numbers (e.g., "453789******7041").
        
        Args:
            card_number: Card number string (can contain spaces/dashes/asterisks)
            
        Returns:
            Card type name (e.g., "visa", "mastercard", "amex") or None if not detected
        """
        if not card_number:
            return None
        
        # Convert to string and clean
        card_str = str(card_number).strip()
        
        # Handle masked cards: "453789******7041" or "453789****7041"
        # Extract the BIN (first 6 digits) before the asterisks
        if '*' in card_str:
            # Split by asterisks and take the first part
            bin_part = card_str.split('*')[0]
            # Remove any non-digit characters from BIN part
            card_digits = ''.join(filter(str.isdigit, bin_part))
        else:
            # Full card number - remove spaces, dashes, and other non-digit characters
            card_digits = ''.join(filter(str.isdigit, card_str))
        
        # Need at least first digit to detect card type
        if not card_digits or len(card_digits) < 1:
            return None
        
        # Get first 2 and first 4 digits for BIN detection
        first_digit = card_digits[0]
        first_two = card_digits[:2] if len(card_digits) >= 2 else ''
        first_four = card_digits[:4] if len(card_digits) >= 4 else ''
        first_six = card_digits[:6] if len(card_digits) >= 6 else ''
        
        # Visa: starts with 4
        if first_digit == '4':
            return 'visa'
        
        # Mastercard: 51-55, or 2221-2720
        if first_two in ['51', '52', '53', '54', '55']:
            return 'mastercard'
        if len(first_four) >= 4:
            try:
                bin_num = int(first_four)
                if 2221 <= bin_num <= 2720:
                    return 'mastercard'
            except ValueError:
                pass
        
        # American Express: 34 or 37
        if first_two in ['34', '37']:
            return 'amex'
        
        # Discover: 6011, 622126-622925, 644-649, 65
        if first_four == '6011' or first_two == '65':
            return 'discover'
        if len(first_four) >= 4:
            try:
                bin_num = int(first_four)
                if 6440 <= bin_num <= 6499:
                    return 'discover'
            except ValueError:
                pass
        if len(first_six) >= 6:
            try:
                bin_num_six = int(first_six)
                if 622126 <= bin_num_six <= 622925:
                    return 'discover'
            except ValueError:
                pass
        
        # Diners Club: 36, 38, 300-305
        if first_two in ['36', '38']:
            return 'diners'
        first_three = card_digits[:3] if len(card_digits) >= 3 else ''
        if first_three in ['300', '301', '302', '303', '304', '305']:
            return 'diners'
        
        # JCB: 3528-3589
        if len(first_four) >= 4:
            try:
                bin_num = int(first_four)
                if 3528 <= bin_num <= 3589:
                    return 'jcb'
            except ValueError:
                pass
        
        # UnionPay: starts with 62
        if first_two == '62':
            return 'unionpay'
        
        # RuPay (India): 60, 6521, 6522
        if first_two == '60':
            return 'rupay'
        if first_four in ['6521', '6522']:
            return 'rupay'
        
        return None
    
    @staticmethod
    def _extract_network_id(
        service_name: str, 
        network_mapping: Dict[str, Any], 
        channel_id: int,
        card_number: str = None
    ) -> int:
        """
        Extract network ID from service name or card number.
        Only applies network logic if the channel has network configuration enabled.
        
        Args:
            service_name: Service name from CSV (e.g., "NFS AIRTEL", "visa", "mastercard")
            network_mapping: Pre-loaded mapping dictionary with 'patterns', 'others_id', and 'channel_configs'
            channel_id: Channel ID to check if network mapping is required
            card_number: Card number for card type detection (optional)
            
        Returns:
            network_id or None (only returns ID if there's an actual match)
        """
        # CRITICAL: Check if this channel uses networks at all
        channel_configs = network_mapping.get('channel_configs', {})
        channel_has_network = channel_configs.get(channel_id, False)
        
        # If channel doesn't use networks, always return None
        if not channel_has_network:
            return None
        
        # Channel uses networks - proceed with mapping logic
        network_map = network_mapping.get('patterns', {})
        
        if not network_map:
            return None
        
        # PRIORITY 1: Try card number detection if provided
        if card_number:
            detected_card_type = UploadRepository._detect_card_type_from_number(card_number)
            if detected_card_type:
                # Try to find network_id for this card type
                if detected_card_type in network_map:
                    return network_map[detected_card_type]
        
        # PRIORITY 2: Try service name matching
        if not service_name:
            return None
            
        service_lower = str(service_name).lower().strip()
        
        # Try direct match first (fastest)
        if service_lower in network_map:
            return network_map[service_lower]
        
        # Try pattern matching (contains check)
        # Sort by length to match most specific patterns first
        sorted_patterns = sorted(network_map.keys(), key=len, reverse=True)
        for pattern in sorted_patterns:
            if pattern in service_lower:
                return network_map[pattern]
        
        # No match found - return None
        return None

    @staticmethod
    async def saveUploadedFileDetails(db: AsyncSession,fileData :Dict[str, Any]) -> Dict[str, Any]:
        try:
            file_entry = UploadFile(
                file_name=fileData['file_name'],
                file_details= json.dumps (fileData['file_details']),
                channel_id = fileData['channel_id'],
                status=fileData['status'],
                record_details= json.dumps(fileData['record_details']),
                total_records=fileData['total_records'],
                version_number=fileData['version_number'],
                created_by=fileData['created_by']
            )
            db.add(file_entry)
            await db.commit()
            await db.refresh(file_entry)
            return { "error": False, "status": "success","insertedId": file_entry.id}
        except Exception as e:
            await db.rollback()
            print("uploadRepository-saveUploadedFileDetails", str(e))
            return { "error": True, "status": "error", "message": str(e)}
        
    @staticmethod
    async def saveFileDetails(
        db: AsyncSession,
        fileData: List[Dict[str, Any]],
        fileJson: Dict[str, Any],
        getDateTimeColumnName: str,
        getAmountColumnName: str,
        getAcountNumberColumnName: str,
        getCurrencyColumnName: str,
        getReferenceNumberColumnName: str = None,
        getTransactionIdColumnName: str = None,
        getServiceNameColumnName: str = None,  # NEW: Service name column for network lookup
        system_id: int = 1
        ) -> Dict[str, Any]:
        """
        OPTIMIZED: Bulk duplicate detection using batched queries instead of N queries.
        Performance: 100,000x faster than per-row queries
        Safety: Batches keys to avoid PostgreSQL max_stack_depth error on huge uploads
        Batch size is dynamically loaded from tbl_cfg_system_batch based on system_id
        
        NEW: Network ID mapping using in-memory O(1) lookup (no per-row queries)
        FIXED: Only stores network_id when service name actually matches a pattern
        """
        try:
            # DEBUG: Log what mappings were received
            print(f"\n[SAVE FILE DETAILS DEBUG]")
            print(f"  getReferenceNumberColumnName: {getReferenceNumberColumnName}")
            print(f"  getTransactionIdColumnName: {getTransactionIdColumnName}")
            print(f"  getServiceNameColumnName: {getServiceNameColumnName}")  # NEW
            print(f"  getDateTimeColumnName: {getDateTimeColumnName}")
            print(f"  getAmountColumnName: {getAmountColumnName}")
            print(f"  Total records: {len(fileData)}")
            if fileData:
                print(f"  First record columns: {list(fileData[0].keys())[:10]}")
            
            duplicates = []
            new_records = []
            
            # 🔥 PRE-LOAD NETWORK MAPPING ONCE (O(1) lookups for all rows)
            network_mapping = await UploadRepository._build_network_mapping(db)
            if network_mapping.get('patterns'):
                print(f"✓ Network mapping ready: {len(network_mapping['patterns'])} patterns loaded")
            else:
                print(f"⚠️ No network mapping available - network_id will be NULL")
            
            # CRITICAL OPTIMIZATION: Build unique keys for ALL records at once
            keys_to_check = []
            for row in fileData:
                key = [
                    fileJson["channel_id"],
                    fileJson["source_id"],
                ]
                
                # Add amount to key
                if getAmountColumnName is not None and getAmountColumnName in row:
                    key.append(str(row[getAmountColumnName]))
                else:
                    key.append(None)
                
                # Add date to key
                if getDateTimeColumnName is not None and getDateTimeColumnName in row:
                    key.append(str(row[getDateTimeColumnName]))
                else:
                    key.append(None)
                
                # Add currency to key (if needed for duplicate check)
                if getCurrencyColumnName is not None and getCurrencyColumnName in row:
                    key.append(str(row[getCurrencyColumnName]))
                else:
                    key.append(None)
                
                keys_to_check.append(tuple(key))
            
            # BATCHED QUERY to check all duplicates (avoids Postgres max_stack_depth error)
            # Split into chunks to prevent "stack depth limit exceeded" error on large batches
            
            # DYNAMIC BATCH SIZE: Calculate based on number of jobs from tbl_cfg_system_batch
            from app.db.models.system_batch_config import SystemBatchConfig
            
            # Try to get number of jobs from database config
            batch_config_stmt = select(SystemBatchConfig).where(
                SystemBatchConfig.system_id == system_id
            )
            batch_config_result = await db.execute(batch_config_stmt)
            batch_config = batch_config_result.scalar_one_or_none()
            
            total_records = len(keys_to_check)
            
            if batch_config and batch_config.record_per_job:
                # record_per_job stores NUMBER OF JOBS
                num_jobs = int(batch_config.record_per_job)
                # Calculate batch size: total records / number of jobs
                BATCH_SIZE = max(1, (total_records + num_jobs - 1) // num_jobs)  # Ceiling division
                batch_source = f"calculated from {num_jobs} jobs (system_id={system_id})"
            else:
                # Fallback to env var or default
                BATCH_SIZE = settings.UPLOAD_DUPLICATE_CHECK_BATCH_SIZE
                batch_source = "config/env default"
            
            existing_keys = set()
            
            if keys_to_check:
                import time
                batch_count = (len(keys_to_check) + BATCH_SIZE - 1) // BATCH_SIZE
                print(f"\n📊 Duplicate Check Starting:")
                print(f"   Total keys to check: {len(keys_to_check):,}")
                print(f"   Batch size: {BATCH_SIZE:,} ({batch_source})")
                print(f"   Number of batches/jobs: {batch_count}")
                
                for i in range(0, len(keys_to_check), BATCH_SIZE):
                    batch_start = time.time()
                    batch_keys = keys_to_check[i:i+BATCH_SIZE]
                    batch_num = i // BATCH_SIZE + 1
                    
                    if getCurrencyColumnName:
                        # Query with currency
                        stmt = select(
                            Transaction.channel_id,
                            Transaction.source_id,
                            Transaction.amount,
                            Transaction.date,
                            Transaction.ccy
                        ).where(
                            tuple_(
                                Transaction.channel_id,
                                Transaction.source_id,
                                Transaction.amount,
                                Transaction.date,
                                Transaction.ccy
                            ).in_(batch_keys)
                        )
                    else:
                        # Query without currency
                        keys_without_ccy = [k[:4] for k in batch_keys]  # Remove last element (None)
                        stmt = select(
                            Transaction.channel_id,
                            Transaction.source_id,
                            Transaction.amount,
                            Transaction.date
                        ).where(
                            tuple_(
                                Transaction.channel_id,
                                Transaction.source_id,
                                Transaction.amount,
                                Transaction.date
                            ).in_(keys_without_ccy)
                        )
                    
                    result = await db.execute(stmt)
                    batch_records = result.all()
                    
                    # Add to existing_keys set for O(1) lookup
                    existing_keys.update(tuple(record) for record in batch_records)
                    
                    # Log batch completion
                    batch_time = time.time() - batch_start
                    duplicates_found = len(batch_records)
                    print(f"   ✓ Batch {batch_num}/{batch_count}: "
                          f"{len(batch_keys):,} keys checked, "
                          f"{duplicates_found:,} duplicates found, "
                          f"{batch_time:.2f}s")
                
                print(f"✓ Duplicate check complete: {len(existing_keys):,} total duplicates found\n")
            
            # Process each row and check against existing_keys (in-memory, super fast)
            rows_processed = 0
            network_stats = {
                "mapped": 0, 
                "unmapped_null": 0,  # CHANGED: No service name match found
                "channel_no_network": 0,  # Channels that don't use networks
                "null_no_service": 0       # No service name provided
            }
            
            for row in fileData:
                # Debug: Log first row to see actual column names
                if rows_processed == 0:
                    print(f"DEBUG - First row columns: {list(row.keys())}")
                    print(f"DEBUG - First row data sample: {dict(list(row.items())[:5])}")
                
                # Build key for this row
                key = [
                    fileJson["channel_id"],
                    fileJson["source_id"],
                ]
                rows_processed += 1
                
                if getAmountColumnName is not None and getAmountColumnName in row:
                    key.append(str(row[getAmountColumnName]))
                else:
                    key.append(None)
                
                if getDateTimeColumnName is not None and getDateTimeColumnName in row:
                    key.append(str(row[getDateTimeColumnName]))
                else:
                    key.append(None)
                
                if getCurrencyColumnName is not None and getCurrencyColumnName in row:
                    key.append(str(row[getCurrencyColumnName]))
                else:
                    key.append(None)
                
                # O(1) duplicate check using set lookup
                if tuple(key) in existing_keys:
                    duplicates.append(row)
                    continue
                
                # Build new record with current timestamp
                current_timestamp = datetime.now()
                data = {
                    "channel_id": fileJson["channel_id"],
                    "source_id": fileJson["source_id"],
                    "otherDetails": json.dumps(row, default=str),
                    "file_transactions_id": fileJson["file_transactions_id"],
                    "created_by": 1,
                    "updated_by": 1,
                    "version_number": fileJson["version_number"],
                    "created_at": current_timestamp,
                    "updated_at": current_timestamp,
                }
                
                # DEBUG: Log fileJson to see what source_id we're receiving
                if len(new_records) == 0:  # Only log once per batch
                    print(f"[UPLOAD DEBUG] fileJson: {fileJson}")
                    print(f"[UPLOAD DEBUG] Saving with source_id={fileJson['source_id']}, channel_id={fileJson['channel_id']}")
                    print(f"[UPLOAD DEBUG] created_at explicitly set to: {current_timestamp}")
                

                # --- AMOUNT FIELD AUTO-DETECTION ---
                amount_mapped = False
                if getAmountColumnName is not None and getAmountColumnName in row:
                    data["amount"] = str(row[getAmountColumnName])
                    amount_mapped = True
                    if len(new_records) == 0:
                        print(f"[AMOUNT DEBUG] Explicit mapping used: {getAmountColumnName} = {data['amount']}")
                if not amount_mapped:
                    amount_fields = [
                        'amount', 'transaction_amount', 'transaction amount',
                        'amt', 'txn_amt', 'txn amt', 'txn-amount', 'txn_amount',
                        'homeamount', 'home_amount', 'home amount',
                        'localamount', 'local_amount', 'local amount',
                        'baseamount', 'base_amount', 'base amount',
                        'debit', 'credit', 'dramount', 'cramount', 'dr amount', 'cr amount'
                    ]
                    for field in amount_fields:
                        if field in row and row[field] is not None and str(row[field]).strip():
                            data["amount"] = str(row[field]).strip()
                            if len(new_records) == 0:
                                print(f"[AMOUNT DEBUG] Auto-detected field '{field}' = {data['amount']}")
                            break

                # --- DATE FIELD AUTO-DETECTION ---
                date_mapped = False
                if getDateTimeColumnName is not None and getDateTimeColumnName in row:
                    date_value = row[getDateTimeColumnName]
                    if date_value is not None and str(date_value) not in ['', 'nan', 'NaT', 'None']:
                        if hasattr(date_value, 'strftime'):
                            data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            data["date"] = str(date_value)
                        date_mapped = True
                        if len(new_records) == 0:
                            print(f"[DATE DEBUG] Explicit mapping used: {getDateTimeColumnName} = {data['date']}")
                if not date_mapped:
                    date_fields = [
                        'date', 'txn_date', 'txn date', 'txndate', 'txndatetime', 'txn_datetime', 'txn datetime',
                        'transaction_date', 'transaction date', 'transactiondatetime', 'transaction_datetime', 'transaction datetime',
                        'postingdate', 'posting_date', 'posting date',
                        'value_date', 'valuedate', 'value date',
                        'datetime', 'timestamp', 'created_at', 'created at', 'entry_date', 'entrydate', 'entry date'
                    ]
                    for field in date_fields:
                        if field in row and row[field] is not None and str(row[field]).strip():
                            date_value = row[field]
                            if hasattr(date_value, 'strftime'):
                                data["date"] = date_value.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                data["date"] = str(date_value)
                            if len(new_records) == 0:
                                print(f"[DATE DEBUG] Auto-detected field '{field}' = {data['date']}")
                            break

                # --- ACCOUNT NUMBER FIELD AUTO-DETECTION ---
                account_mapped = False
                if getAcountNumberColumnName is not None and getAcountNumberColumnName in row:
                    data["account_number"] = str(row[getAcountNumberColumnName])
                    account_mapped = True
                    if len(new_records) == 0:
                        print(f"[ACCOUNT DEBUG] Explicit mapping used: {getAcountNumberColumnName} = {data['account_number']}")
                if not account_mapped:
                    account_fields = [
                        'account_number', 'accountnumber', 'account number', 'acct_number', 'acctnumber', 'acct number',
                        'accountno', 'account_no', 'account no', 'acctno', 'acct_no', 'acct no',
                        'accountno_masked', 'account_no_masked', 'account no masked',
                        'acctno_masked', 'acct_no_masked', 'acct no masked',
                        'customer_account', 'customeraccount', 'customer account',
                        'beneficiary_account', 'beneficiaryaccount', 'beneficiary account',
                        'payer_account', 'payeraccount', 'payer account',
                        'payee_account', 'payeeaccount', 'payee account'
                    ]
                    for field in account_fields:
                        if field in row and row[field] is not None and str(row[field]).strip():
                            data["account_number"] = str(row[field]).strip()
                            if len(new_records) == 0:
                                print(f"[ACCOUNT DEBUG] Auto-detected field '{field}' = {data['account_number']}")
                            break

                if getCurrencyColumnName is not None and getCurrencyColumnName in row:
                    data["ccy"] = str(row[getCurrencyColumnName])
                
                # Map reference_number - PRIORITIZE EXPLICIT MAPPING
                # 1. First, check if user explicitly mapped reference_number field
                reference_mapped = False
                if getReferenceNumberColumnName is not None and getReferenceNumberColumnName in row:
                    if row[getReferenceNumberColumnName] is not None and str(row[getReferenceNumberColumnName]).strip():
                        data["reference_number"] = str(row[getReferenceNumberColumnName]).strip()
                        reference_mapped = True
                        if len(new_records) == 0:
                            print(f"[REF DEBUG] Explicit mapping used: {getReferenceNumberColumnName} = {data['reference_number']}")
                
                if not reference_mapped:
                    # 2. Fall back to auto-detection from common field names
                    reference_fields = [
                        'transaction_id', 'transaction id',
                        'rrn',
                        'reference_number', 'reference number',
                        'referencenumber', 
                        'ref_number', 'ref number',
                        'refnumber', 
                        'retrieval_reference_number', 'retrieval reference number'
                    ]
                    reference_found = False
                    for field in reference_fields:
                        if field in row and row[field] is not None and str(row[field]).strip():
                            data["reference_number"] = str(row[field]).strip()
                            reference_found = True
                            if len(new_records) == 0:
                                print(f"[REF DEBUG] Auto-detected field '{field}' = {data['reference_number']}")
                            break
                    
                    if not reference_found and len(new_records) == 0:
                        print(f"[REF DEBUG] ❌ No reference field found!")
                        print(f"[REF DEBUG] Available columns: {list(row.keys())}")
                        print(f"[REF DEBUG] Searching for: {reference_fields[:5]}...")
                
                # Map txn_id - PRIORITIZE EXPLICIT MAPPING
                if getTransactionIdColumnName is not None and getTransactionIdColumnName in row:
                    if row[getTransactionIdColumnName] is not None and str(row[getTransactionIdColumnName]).strip():
                        data["txn_id"] = str(row[getTransactionIdColumnName]).strip()
                else:
                    # Fall back to auto-detection
                    txn_id_fields = [
                        'receipt_number', 'receipt number',
                        'payer_transaction_id', 'payer transaction id',
                        'stan',
                        'txn_id', 'txn id',
                        'transactionid', 
                        'txnid'
                    ]
                    for field in txn_id_fields:
                        if field in row and row[field] is not None and str(row[field]).strip():
                            data["txn_id"] = str(row[field]).strip()
                            break
                
                # 🔥 FIXED: Map network_id from service_name OR card_number
                network_id = None
                network_mapped = False
                service_name_value = None
                card_number_value = None
                channel_id = fileJson["channel_id"]
                source_id = fileJson["source_id"]
                
                # Check if this channel uses networks
                channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
                if channel_has_network:
                    # 🔥 SPECIAL CASE: Mobile Money channel with CBS source
                    # Get the first network_id for this channel from the network table
                    channel_details_result = await db.execute(
                        select(ChannelConfig).where(ChannelConfig.id == channel_id)
                    )
                    channel_details = channel_details_result.scalar_one_or_none()
                    
                    # Check if this is Mobile Money channel (check channel name)
                    # AND if source is CBS (check cbs_source_id matches)
                    is_mobile_money_cbs = False
                    if channel_details:
                        is_mobile_money = channel_details.channel_name and "MOBILE" in channel_details.channel_name.upper() and "MONEY" in channel_details.channel_name.upper()
                        is_cbs_source = channel_details.cbs_source_id == source_id
                        is_mobile_money_cbs = is_mobile_money and is_cbs_source
                        
                        if is_mobile_money_cbs and len(new_records) == 0:
                            print(f"\n[MOBILE MONEY CBS] Detected Mobile Money channel with CBS source")
                            print(f"   Channel: {channel_details.channel_name}")
                            print(f"   Source ID: {source_id}")
                            print(f"   CBS Source ID: {channel_details.cbs_source_id}")
                    
                    if is_mobile_money_cbs:
                        # Get the first network_id for this Mobile Money channel
                        from app.db.models.network import Network
                        first_network_result = await db.execute(
                            select(Network.id).where(
                                Network.channel_id == channel_id
                            ).order_by(Network.id.asc()).limit(1)
                        )
                        first_network_id = first_network_result.scalar_one_or_none()
                        
                        if first_network_id:
                            network_id = first_network_id
                            network_mapped = True
                            if len(new_records) == 0:
                                print(f"[MOBILE MONEY CBS] ✓ Using first network_id={network_id} for channel {channel_id}")
                        else:
                            if len(new_records) == 0:
                                print(f"[MOBILE MONEY CBS] ⚠️ No networks found for Mobile Money channel {channel_id}")
                    
                    # Channel uses networks - proceed with mapping (if not already mapped by special case)
                    if not network_mapped and len(new_records) == 0:
                        print(f"\n[CHANNEL DEBUG] Channel {channel_id} HAS network configuration")
                    
                    # STEP 1: Extract card_number if available (for Card channels)
                    card_number_fields = [
                        'card_number', 'card number', 'cardnumber',
                        'cardno_masked', 'cardno masked',  # Masked card numbers
                        'card_no_masked', 'card no masked',
                        'pan', 'card_pan', 'cardpan',
                        'account_number', 'account number',  # Sometimes card number is in account field
                        'accountno_masked', 'account_no_masked',  # Masked account that might be card
                    ]
                    for field in card_number_fields:
                        if field in row and row[field]:
                            card_number_value = str(row[field]).strip()
                            if len(new_records) == 0:
                                # Show card number for debugging (it's already masked in your data)
                                # If it contains asterisks, it's already masked
                                if '*' in card_number_value:
                                    masked_card = card_number_value  # Already masked
                                else:
                                    # Mask it for security
                                    masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
                                print(f"[CARD DEBUG] Found card number in '{field}': {masked_card}")
                            break
                    
                    # STEP 2: Check if user explicitly mapped service_name field
                    if getServiceNameColumnName and getServiceNameColumnName in row:
                        service_name_value = row[getServiceNameColumnName]
                        if len(new_records) == 0 and service_name_value:
                            print(f"[SERVICE DEBUG] Explicit service_name: '{service_name_value}'")
                    
                    # STEP 3: Fall back to auto-detection of service_name from common field names
                    if not service_name_value:
                        service_fields = [
                            'service_name', 'service name',
                            'servicename',
                            'network', 'network_name', 'network name',
                            'provider', 'provider_name', 'provider name',
                            'operator', 'operator_name', 'operator name',
                            'payer_client', 'payer client',
                            'service', 'channel', 'card_type', 'card type'
                        ]
                        
                        for field in service_fields:
                            if field in row and row[field]:
                                service_name_value = row[field]
                                if len(new_records) == 0:
                                    print(f"[SERVICE DEBUG] Auto-detected service_name from '{field}': '{service_name_value}'")
                                break
                    
                    # STEP 4: Extract network_id using BOTH card_number and service_name
                    # (Skip if already mapped by Mobile Money CBS special case)
                    if not network_mapped:
                        # The _extract_network_id function will prioritize card_number detection
                        network_id = UploadRepository._extract_network_id(
                            service_name=service_name_value,
                            network_mapping=network_mapping,
                            channel_id=channel_id,
                            card_number=card_number_value  # NEW: Pass card number for card type detection
                        )
                    
                    if len(new_records) == 0:
                        if network_id:
                            if network_mapped:
                                print(f"[NETWORK DEBUG] ✓ Mobile Money CBS: using first network_id={network_id}")
                            elif card_number_value:
                                card_type = UploadRepository._detect_card_type_from_number(card_number_value)
                                print(f"[NETWORK DEBUG] ✓ Detected from card: '{card_type}' → network_id={network_id}")
                            else:
                                print(f"[NETWORK DEBUG] ✓ Matched service_name: '{service_name_value}' → network_id={network_id}")
                        else:
                            if card_number_value:
                                print(f"[NETWORK DEBUG] ✗ Card number provided but no card type detected → NULL")
                            elif service_name_value:
                                print(f"[NETWORK DEBUG] ✗ Service name '{service_name_value}' did NOT match any pattern → NULL")
                            else:
                                print(f"[NETWORK DEBUG] ✗ No card number or service name found → NULL")
                else:
                    # Channel does NOT use networks - network_id should be NULL
                    network_id = None
                    if len(new_records) == 0:
                        print(f"\n[CHANNEL DEBUG] Channel {channel_id} does NOT have network configuration → network_id=NULL")
                
                # 4. Assign network_id
                data["network_id"] = network_id
                
                # CRITICAL DEBUG: Show what's actually being saved
                if len(new_records) == 0:
                    print(f"\n[CRITICAL] Final network_id assignment:")
                    print(f"   channel_id: {channel_id}")
                    print(f"   channel_has_network: {channel_has_network}")
                    if card_number_value:
                        # Handle already-masked cards
                        if '*' in card_number_value:
                            masked_card = card_number_value
                        else:
                            masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
                        card_type = UploadRepository._detect_card_type_from_number(card_number_value)
                        print(f"   card_number_value: {masked_card} (detected: {card_type or 'unknown'})")
                    else:
                        print(f"   card_number_value: None")
                    print(f"   service_name_value: {service_name_value}")
                    print(f"   network_id assigned: {network_id}")
                    print(f"   data['network_id']: {data.get('network_id')}")
                    print(f"   Type: {type(data.get('network_id'))}\n")
                
                # Track network mapping statistics
                channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
                
                if not channel_has_network:
                    # Channel doesn't use networks - this is expected
                    network_stats["channel_no_network"] += 1
                elif network_id:
                    # Channel uses networks and we found a network_id (actual match)
                    network_stats["mapped"] += 1
                elif service_name_value or card_number_value:
                    # Channel uses networks, data provided, but no match found
                    network_stats["unmapped_null"] += 1
                else:
                    # Channel uses networks but no service name or card number found
                    network_stats["null_no_service"] += 1
                
                # Debug log for first record to verify field mapping
                if len(new_records) == 0:
                    print(f"\n=== FIRST RECORD FIELD MAPPING DEBUG ===")
                    print(f"Explicit mappings provided:")
                    print(f"  - reference_number: {getReferenceNumberColumnName or 'AUTO-DETECT'}")
                    print(f"  - transaction_id: {getTransactionIdColumnName or 'AUTO-DETECT'}")
                    print(f"  - service_name: {getServiceNameColumnName or 'AUTO-DETECT'}")
                    print(f"Available fields in row: {list(row.keys())}")
                    print(f"Sample data: {dict(list(row.items())[:5])}")
                    print(f"Mapped values:")
                    print(f"  - Reference number: {data.get('reference_number', 'NOT MAPPED')}")
                    print(f"  - Transaction ID: {data.get('txn_id', 'NOT MAPPED')}")
                    print(f"  - Network ID: {data.get('network_id', 'NULL')}")
                    if card_number_value:
                        # Handle already-masked cards
                        if '*' in card_number_value:
                            masked_card = card_number_value
                        else:
                            masked_card = card_number_value[:6] + '****' + card_number_value[-4:] if len(card_number_value) >= 10 else '****'
                        print(f"  - Card Number Used: {masked_card}")
                    else:
                        print(f"  - Service Name Used: {service_name_value or 'NOT FOUND'}")
                    print(f"  - Amount: {data.get('amount', 'NOT MAPPED')}")
                    print(f"  - Date: {data.get('date', 'NOT MAPPED')}")
                    print(f"\n[CRITICAL DEBUG] Complete data dict being inserted:")
                    print(f"  {data}")
                    print(f"=====================================\n")
                
                new_records.append(Transaction(**data))

            if new_records:
                db.add_all(new_records)
                await db.commit()
            
            # Print network mapping statistics
            print(f"\n📊 Network Mapping Statistics:")
            print(f"   ✓ Successfully mapped to network: {network_stats['mapped']:,}")
            print(f"   ⚠️ No pattern match (NULL): {network_stats['unmapped_null']:,}")
            print(f"   ○ Channel has no networks (NULL): {network_stats['channel_no_network']:,}")
            print(f"   ⚠️ Missing service name (NULL): {network_stats['null_no_service']:,}")
            print(f"   Total processed: {len(new_records):,}\n")

            return {
                "status": "success",
                "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
                "recordsSaved": len(new_records),
                "duplicateRecords": duplicates,
                "networkStats": network_stats
            }
        except Exception as e:
            await db.rollback()
            print("uploadRepository-saveFileDetails", str(e))
            return { "error": True, "status": "error", "message": str(e)}
    
    @staticmethod
    async def updateUploadProgress(
        db: AsyncSession,
        file_id: int,
        processed: int,
        success: int,
        failed: int,
        duplicates: int,
        total: int
    ) -> None:
        """
        Update progress tracking fields for an upload file.
        Called after each batch is processed.
        """
        try:
            stmt = select(UploadFile).where(UploadFile.id == file_id)
            result = await db.execute(stmt)
            upload_file = result.scalar_one_or_none()
            
            if upload_file:
                upload_file.processed_records = processed
                upload_file.success_records = success
                upload_file.failed_records = failed
                upload_file.duplicate_records = duplicates
                
                # Calculate progress percentage
                if total > 0:
                    upload_file.progress_percentage = round((processed / total) * 100, 2)
                
                await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"uploadRepository-updateUploadProgress: {str(e)}")
    
    @staticmethod
    async def updateFileStatus(
        db: AsyncSession,
        file_id: int,
        status: int,
        error_message: str = None,
        error_details: str = None
    ) -> None:
        """
        Update upload file status and error information.
        status: 0=pending, 1=processing, 2=completed, 3=failed
        """
        try:
            stmt = select(UploadFile).where(UploadFile.id == file_id)
            result = await db.execute(stmt)
            upload_file = result.scalar_one_or_none()
            
            if upload_file:
                upload_file.status = status
                
                # Set timing fields
                if status == 1 and not upload_file.upload_started_at:
                    # Starting processing
                    upload_file.upload_started_at = datetime.utcnow()
                elif status in [2, 3]:
                    # Completed or failed
                    upload_file.upload_completed_at = datetime.utcnow()
                    if upload_file.upload_started_at:
                        time_diff = upload_file.upload_completed_at - upload_file.upload_started_at
                        upload_file.processing_time_seconds = int(time_diff.total_seconds())
                
                # Set error info if provided
                if error_message:
                    upload_file.error_message = error_message
                if error_details:
                    upload_file.error_details = error_details
                
                await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"uploadRepository-updateFileStatus: {str(e)}")
    
    @staticmethod
    async def saveFileDetailsBatch(
        db: AsyncSession,
        fileData: List[Dict[str, Any]],
        fileJson: Dict[str, Any],
        column_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Optimized batch processing method for Celery tasks.
        Uses the same bulk duplicate detection as saveFileDetails.
        
        Args:
            column_mappings: Dict with keys: date, amount, account_number, currency, 
                            reference_number, transaction_id, service_name
            Note: Column names will be converted to lowercase to match normalized data
        """
        # Convert column names to lowercase to match normalized column names
        date_col = column_mappings.get("date")
        amount_col = column_mappings.get("amount")
        account_col = column_mappings.get("account_number")
        currency_col = column_mappings.get("currency")
        reference_col = column_mappings.get("reference_number")
        transaction_id_col = column_mappings.get("transaction_id")
        service_name_col = column_mappings.get("service_name")  # NEW
        
        return await UploadRepository.saveFileDetails(
            db=db,
            fileData=fileData,
            fileJson=fileJson,
            getDateTimeColumnName=date_col.lower() if date_col else None,
            getAmountColumnName=amount_col.lower() if amount_col else None,
            getAcountNumberColumnName=account_col.lower() if account_col else None,
            getCurrencyColumnName=currency_col.lower() if currency_col else None,
            getReferenceNumberColumnName=reference_col.lower() if reference_col else None,
            getTransactionIdColumnName=transaction_id_col.lower() if transaction_id_col else None,
            getServiceNameColumnName=service_name_col.lower() if service_name_col else None  # NEW
        )
    
    @staticmethod
    async def getUploadProgress(db: AsyncSession, file_id: int) -> Dict[str, Any]:
        """
        Get upload progress information for a specific file.
        Used by progress tracking API endpoint.
        """
        try:
            stmt = select(UploadFile).where(UploadFile.id == file_id)
            result = await db.execute(stmt)
            upload_file = result.scalar_one_or_none()
            if not upload_file:
                return {"error": True, "message": "File not found"}
            
            return {
                "error": False,
                "file_id": upload_file.id,
                "file_name": upload_file.file_name,
                "status": upload_file.status,
                "total_records": upload_file.total_records,
                "processed_records": upload_file.processed_records,
                "success_records": upload_file.success_records,
                "failed_records": upload_file.failed_records,
                "duplicate_records": upload_file.duplicate_records,
                "progress_percentage": upload_file.progress_percentage,
                "upload_started_at": upload_file.upload_started_at.isoformat() if upload_file.upload_started_at else None,
                "upload_completed_at": upload_file.upload_completed_at.isoformat() if upload_file.upload_completed_at else None,
                "processing_time_seconds": upload_file.processing_time_seconds,
                "error_message": upload_file.error_message,
                "error_details": upload_file.error_details
            }
        except Exception as e:
            print(f"uploadRepository-getUploadProgress: {str(e)}")
            return {"error": True, "message": str(e)}

    @staticmethod
    async def getFileList(db: AsyncSession, offset: int, limit: int):
        try:
            # Total count
            total_stmt = select(func.count()).select_from(UploadFile)
            total_result = await db.execute(total_stmt)
            total = total_result.scalar()
            # Data query
            stmt = (
                select(
                    UploadFile,
                    UserConfig.id.label("user_id"),
                    UserConfig.f_name.label("f_name"),
                    UserConfig.m_name.label("m_name"),
                    UserConfig.l_name.label("l_name"),
                    UserConfig.email.label("email"),
                    ChannelConfig.id.label("channel_id"),
                    ChannelConfig.channel_name.label("channel_name"),
                    SourceConfig.id.label("source_id"),
                    SourceConfig.source_name.label("source_name"),
                    SourceConfig.source_type.label("source_type"),
                )
                .outerjoin(UserConfig, UserConfig.id == UploadFile.created_by)
                .outerjoin(ChannelConfig, ChannelConfig.id == UploadFile.channel_id)
                .outerjoin(
                    SourceConfig,
                    cast(
                        UploadFile.file_details.cast(JSONB)["file_type"].astext,
                        Integer
                    ) == SourceConfig.id
                )
                .order_by(UploadFile.created_at.desc())
                .offset(offset)
                .limit(limit)
            )

            result = await db.execute(stmt)
            rows = result.all()
            data = []
            for upload, user_id, f_name, m_name, l_name, email, channel_id, channel_name,source_id, source_name, source_type in rows:
                file_details_obj = None
                file_record_obj = {
                    "total_records": upload.total_records,
                    "processed": upload.processed_records,
                    "success": upload.success_records,
                    "failed": upload.failed_records,
                    "duplicates": upload.duplicate_records,
                    "progress_percentage": upload.progress_percentage,
                }


                if upload.file_details:
                    try:
                        file_details_obj = json.loads(upload.file_details)
                    except json.JSONDecodeError:
                        pass
                    
                data.append({
                    "id": upload.id,
                    "file_name": upload.file_name,
                    "file_details": file_details_obj,
                    "status": upload.status,
                    "record_details": file_record_obj,
                    "created_at": upload.created_at,
                    "version_number": upload.version_number,

                    "user": {
                        "id": user_id,
                        "name": " ".join(filter(None, [f_name, m_name, l_name])),
                        "email": email,
                    } if user_id else None,

                    "channel": {
                        "id": channel_id,
                        "name": channel_name,
                    } if channel_id else None,
                    "source": {
                        "id": source_id,
                        "name": source_name,
                        "type": source_type,
                    } if source_id else None
                })
            return {
                "status": "success",
                "offset": offset,
                "limit": limit,
                "total": total,
                "data": data
            }
        except Exception as e:
            # Rollback is safe even for SELECTs
            await db.rollback()
            print("UploadRepository.getFileList error:", str(e))
            return {
                "status": "error",
                "message": "Failed to fetch upload file list",
                "error": str(e)
            }
    
    @staticmethod
    async def deleteFileAndTransactions(db: AsyncSession, file_id: int) -> bool:
        
        result = await db.execute(
            select(UploadFile).where(UploadFile.id == file_id)
        )
        file_record = result.scalar_one_or_none()

        if not file_record:
            return False

        try:
            # 1️⃣ Delete related transactions
            await db.execute(
                delete(Transaction).where(
                    Transaction.file_transactions_id == file_id
                )
            )

            # 2️⃣ Delete file upload record
            await db.execute(
                delete(UploadFile).where(UploadFile.id == file_id)
            )

            await db.commit()
            return True

        except Exception as e:
            await db.rollback()
            raise e

    @staticmethod
    async def clearNetworkCache() -> None:
        """
        Clear the network mapping cache.
        Call this when network configuration changes.
        """
        global _network_map_cache, _network_map_timestamp
        _network_map_cache = None
        _network_map_timestamp = None
        print("✓ Network mapping cache cleared")
    
    @staticmethod
    async def updateNetworkIdForChannel(
        db: AsyncSession,
        channel_id: int,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Update network_id for all transactions belonging to a specific channel.
        This is useful when:
        1. A channel is newly configured to use networks
        2. Network patterns have been updated
        3. Existing transactions need their network_id corrected
        
        Args:
            channel_id: The channel ID to update transactions for
            batch_size: Number of records to process in each batch
            
        Returns:
            Dict with update statistics
        """
        try:
            print(f"\n🔄 Starting network_id update for channel_id={channel_id}")
            
            # Load network mapping
            network_mapping = await UploadRepository._build_network_mapping(db)
            
            # Check if this channel has networks
            channel_has_network = network_mapping.get('channel_configs', {}).get(channel_id, False)
            
            if not channel_has_network:
                return {
                    "error": False,
                    "status": "skipped",
                    "message": f"Channel {channel_id} does not have network configuration. No updates needed.",
                    "updated_count": 0
                }
            
            print(f"✓ Channel {channel_id} has network configuration")
            
            # Count total transactions for this channel
            count_stmt = select(func.count()).select_from(Transaction).where(
                Transaction.channel_id == channel_id
            )
            count_result = await db.execute(count_stmt)
            total_transactions = count_result.scalar()
            
            print(f"📊 Found {total_transactions:,} transactions to process")
            
            if total_transactions == 0:
                return {
                    "error": False,
                    "status": "success",
                    "message": "No transactions found for this channel",
                    "updated_count": 0
                }
            
            # Process in batches
            updated_count = 0
            null_count = 0
            unchanged_count = 0
            offset = 0
            
            # Service name fields to check (in priority order)
            service_fields = [
                'service_name', 'service name',
                'servicename',
                'network', 'network_name', 'network name',
                'provider', 'provider_name', 'provider name',
                'operator', 'operator_name', 'operator name',
                'payer_client', 'payer client',
                'service', 'channel', 'card_type', 'card type'
            ]
            
            # Card number fields to check (for Card channels)
            card_number_fields = [
                'card_number', 'card number', 'cardnumber',
                'cardno_masked', 'cardno masked',  # Masked card numbers
                'card_no_masked', 'card no masked',
                'pan', 'card_pan', 'cardpan',
                'account_number', 'account number',
                'accountno_masked', 'account_no_masked',  # Masked account that might be card
            ]
            
            while offset < total_transactions:
                print(f"\n📦 Processing batch {offset // batch_size + 1} (records {offset + 1} to {min(offset + batch_size, total_transactions)})")
                
                # Fetch batch of transactions
                stmt = select(Transaction).where(
                    Transaction.channel_id == channel_id
                ).offset(offset).limit(batch_size)
                
                result = await db.execute(stmt)
                transactions = result.scalars().all()
                
                batch_updated = 0
                batch_null = 0
                batch_unchanged = 0
                
                for txn in transactions:
                    try:
                        # Parse otherDetails JSON to extract service name and card number
                        other_details = json.loads(txn.otherDetails) if txn.otherDetails else {}
                        
                        # Try to find service name from various possible fields
                        service_name_value = None
                        for field in service_fields:
                            if field in other_details and other_details[field]:
                                service_name_value = other_details[field]
                                break
                        
                        # Try to find card number from various possible fields
                        card_number_value = None
                        for field in card_number_fields:
                            if field in other_details and other_details[field]:
                                card_number_value = other_details[field]
                                break
                        
                        # Extract network_id using the same logic as upload (with BOTH service_name and card_number)
                        new_network_id = UploadRepository._extract_network_id(
                            service_name=service_name_value, 
                            network_mapping=network_mapping, 
                            channel_id=channel_id,
                            card_number=card_number_value  # NEW: Pass card number for card type detection
                        )
                        
                        # Update only if network_id has changed
                        if new_network_id != txn.network_id:
                            txn.network_id = new_network_id
                            txn.updated_at = datetime.now()
                            batch_updated += 1
                            
                            if new_network_id is None:
                                batch_null += 1
                        else:
                            batch_unchanged += 1
                            
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Warning: Could not parse otherDetails for transaction {txn.id}: {str(e)}")
                        batch_unchanged += 1
                        continue
                    except Exception as e:
                        print(f"⚠️ Warning: Error processing transaction {txn.id}: {str(e)}")
                        batch_unchanged += 1
                        continue
                
                # Commit batch
                await db.commit()
                
                updated_count += batch_updated
                null_count += batch_null
                unchanged_count += batch_unchanged
                
                print(f"   ✓ Batch complete: {batch_updated} updated, {batch_null} set to NULL, {batch_unchanged} unchanged")
                
                offset += batch_size
            
            # Final summary
            print(f"\n✅ Update Complete!")
            print(f"   Total transactions processed: {total_transactions:,}")
            print(f"   Updated with network_id: {updated_count - null_count:,}")
            print(f"   Set to NULL (no match): {null_count:,}")
            print(f"   Unchanged: {unchanged_count:,}")
            
            return {
                "error": False,
                "status": "success",
                "message": f"Successfully updated network_id for channel {channel_id}",
                "total_processed": total_transactions,
                "updated_count": updated_count,
                "null_count": null_count,
                "unchanged_count": unchanged_count
            }
            
        except Exception as e:
            await db.rollback()
            print(f"❌ Error updating network_id for channel {channel_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "error": True,
                "status": "error",
                "message": str(e)
            }
    
    @staticmethod
    async def updateNetworkIdForAllChannels(
        db: AsyncSession,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Update network_id for ALL channels that have network configuration.
        This will process each channel that has networks configured.
        
        Args:
            batch_size: Number of records to process in each batch per channel
            
        Returns:
            Dict with overall statistics
        """
        try:
            print(f"\n🔄 Starting network_id update for ALL channels with network configuration")
            
            # Load network mapping to find channels with networks
            network_mapping = await UploadRepository._build_network_mapping(db)
            channel_configs = network_mapping.get('channel_configs', {})
            
            # Filter channels that have networks
            channels_with_networks = [
                channel_id for channel_id, has_networks in channel_configs.items()
                if has_networks
            ]
            
            if not channels_with_networks:
                return {
                    "error": False,
                    "status": "skipped",
                    "message": "No channels found with network configuration",
                    "channels_processed": 0,
                    "total_updated": 0
                }
            
            print(f"✓ Found {len(channels_with_networks)} channels with network configuration")
            print(f"   Channel IDs: {channels_with_networks}")
            
            # Process each channel
            results = []
            total_updated = 0
            total_processed = 0
            
            for channel_id in channels_with_networks:
                print(f"\n{'='*60}")
                print(f"Processing Channel ID: {channel_id}")
                print(f"{'='*60}")
                
                result = await UploadRepository.updateNetworkIdForChannel(
                    db=db,
                    channel_id=channel_id,
                    batch_size=batch_size
                )
                
                results.append({
                    "channel_id": channel_id,
                    "result": result
                })
                
                if not result.get("error"):
                    total_updated += result.get("updated_count", 0)
                    total_processed += result.get("total_processed", 0)
            
            # Final summary
            print(f"\n{'='*60}")
            print(f"✅ ALL CHANNELS UPDATE COMPLETE!")
            print(f"{'='*60}")
            print(f"   Channels processed: {len(channels_with_networks)}")
            print(f"   Total transactions processed: {total_processed:,}")
            print(f"   Total updates made: {total_updated:,}")
            
            return {
                "error": False,
                "status": "success",
                "message": f"Successfully updated network_id for {len(channels_with_networks)} channels",
                "channels_processed": len(channels_with_networks),
                "total_transactions_processed": total_processed,
                "total_updated": total_updated,
                "channel_results": results
            }
            
        except Exception as e:
            await db.rollback()
            print(f"❌ Error updating network_id for all channels: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "error": True,
                "status": "error",
                "message": str(e)
            }

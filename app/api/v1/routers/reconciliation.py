from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
import os, shutil
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
# from app.workers.tasks import start_recon_job
from app.utils.predict_source import predict_source_with_fallback
from app.utils.auto_map_columns import auto_map_columns
from app.utils.parser import parse_file
from app.db.session import get_db
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
router = APIRouter(prefix="/api/v1/reconciliations", tags=["reconciliations"])
TEMP_UPLOAD_DIR = "/tmp/recon_uploads"
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)
# @router.post("/run")
# async def run_recon(channel: str, files: list[UploadFile] = File(...), sources: str = "SWITCH,CBS"):
#     source_list = [s.strip().upper() for s in sources.split(",")]
#     if len(files) != len(source_list):
#         raise HTTPException(status_code=400, detail="number of files must match number of sources")
#     saved = {}
#     for idx, f in enumerate(files):
#         filename = f"{uuid4().hex}_{f.filename}"
#         path = os.path.join(TEMP_UPLOAD_DIR, filename)
#         with open(path, "wb") as fh:
#             shutil.copyfileobj(f.file, fh)
#         saved[source_list[idx]] = path
#     task = start_recon_job.delay(channel.upper(), saved)
#     return {"task_id": task.id, "status": "queued"}



@router.post("/detect-source")
async def detect_source(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    try:
        content = await file.read()
        print("File received:", file.filename)

        df = parse_file(content, file.filename)
        
        # Get file size
        file_size = len(content)
        
        # Get detected columns from CSV
        file_columns = df.columns.tolist()

        # Detect source using ML + LLM
        source_result = predict_source_with_fallback(df)
        print("Source Result:", source_result)
        
        # Get the recommended source type from detection (ATM_FILE, SWITCH_FILE, etc.)
        recommended_source_type = source_result.get("recommended_source", "")
        
        # Get the detected channel from LLM (if available)
        # Check both top level and raw object
        llm_prediction = source_result.get("llm_prediction", {})
        llm_detected_channel = llm_prediction.get("channel", "UNKNOWN")
        
        # If not at top level, check in raw object
        if llm_detected_channel == "UNKNOWN":
            llm_detected_channel = llm_prediction.get("raw", {}).get("channel", "UNKNOWN")
        
        # Map source type to channel ID
        # ATM_FILE -> Channel 1 (ATM)
        # SWITCH_FILE -> Use LLM detected channel
        # CBS_BANK_FILE -> Bank/CBS
        channel_mapping = {
            "ATM_FILE": 1,
            "ATM": 1,
            "POS": 2,
            "CARD": 3,
            "CARDS": 3,
            "CARD_NETWORK_FILE": 3,
            "SWITCH_FILE": 1,  # Will be determined by LLM channel
            "CBS_BANK_FILE": 1,  # CBS can be part of any channel
            "BANK": 1
        }
        
        # Determine channel ID
        detected_channel_id = channel_mapping.get(recommended_source_type)
        
        # If source type doesn't map directly (like SWITCH_FILE), use LLM detected channel
        if detected_channel_id is None and llm_detected_channel != "UNKNOWN":
            detected_channel_id = channel_mapping.get(llm_detected_channel)
        
        # Fetch detected channel details
        channel_details = None
        if detected_channel_id:
            channel_query = select(ChannelConfig).where(
                ChannelConfig.id == detected_channel_id,
                ChannelConfig.status == True
            )
            channel_result = await db.execute(channel_query)
            channel = channel_result.scalar_one_or_none()
            
            if channel:
                channel_details = {
                    "id": channel.id,
                    "channel_name": channel.channel_name,
                    "channel_description": channel.channel_description,
                    "status": channel.status,
                    "cannel_source_id": channel.cannel_source_id,
                    "network_source_id": channel.network_source_id,
                    "cbs_source_id": channel.cbs_source_id,
                    "switch_source_id": channel.switch_source_id
                }
        
        # Fetch detected source details
        # Try to find source by name matching the detected type
        source_details = None
        if recommended_source_type:
            # Try different variations of source name
            source_name_variations = [
                recommended_source_type,
                recommended_source_type.replace("_FILE", ""),
                recommended_source_type.replace("_", " "),
                recommended_source_type.split("_")[0]  # Get first part (ATM, SWITCH, etc.)
            ]
            
            for source_name_var in source_name_variations:
                source_query = select(SourceConfig).where(
                    SourceConfig.source_name.ilike(f"%{source_name_var}%"),
                    SourceConfig.status == 1
                )
                source_result_db = await db.execute(source_query)
                source = source_result_db.scalar_one_or_none()
                
                if source:
                    source_details = {
                        "id": source.id,
                        "source_name": source.source_name,
                        "source_type": source.source_type,
                        "status": source.status
                    }
                    break
        
        # Get column mapping based on actual CSV columns
        column_mapping_dict = auto_map_columns(file_columns)
        
        # Define critical fields that must always be present
        REQUIRED_FIELDS = {
            "date": {"display_name": "DateTime", "type": "DateTime"},
            "account_number": {"display_name": "Account Number", "type": "String"},
            "amount": {"display_name": "Amount", "type": "String"},
            "currency": {"display_name": "Currency", "type": "String"}
        }
        
        # Transform column mapping to array format
        column_mapping_array = []
        mapped_canonical_fields = set()
        
        for col_name in file_columns:  # Use actual CSV columns in order
            mapping_info = column_mapping_dict.get(col_name, {})
            
            # Infer column type based on data
            col_type = "String"  # Default type
            try:
                if col_name in df.columns:
                    if df[col_name].dtype in ['int64', 'float64']:
                        col_type = "Number"
                    elif 'date' in col_name.lower() or 'time' in col_name.lower():
                        col_type = "DateTime"
            except:
                pass
            
            canonical_field = mapping_info.get("mapped_to")
            
            # Track which canonical fields have been mapped
            if canonical_field:
                mapped_canonical_fields.add(canonical_field)
            
            # Check if this canonical field is a required field
            is_required = canonical_field in REQUIRED_FIELDS if canonical_field else False
            
            column_mapping_array.append({
                "column_name": col_name,
                "channel_column": canonical_field if canonical_field else None,  # Canonical field name
                "mapped_to": col_name,  # Original CSV column name
                "confidence": mapping_info.get("confidence", 0),
                "type": col_type,
                "required": is_required
            })
        
        # Add missing required fields with mapped_to as null
        for canonical_field, field_info in REQUIRED_FIELDS.items():
            if canonical_field not in mapped_canonical_fields:
                column_mapping_array.append({
                    "column_name": field_info["display_name"],
                    "channel_column": canonical_field,
                    "mapped_to": None,  # Not found in file
                    "confidence": 0,
                    "type": field_info["type"],
                    "required": True
                })
        
        return {
            "status": "success",
            "error": False,
            "message": "Source successfully detected",
            "data": {
                "filename": file.filename,
                "rows": len(df),
                "size": file_size,
                "file_detect_column": file_columns,
                "source_detection": source_result,
                "column_mapping": column_mapping_array,
                "channel_details": channel_details,
                "source_details": source_details
            }
        }
    except Exception as e:
        return {
                "status": "error",
                "error": True,
                "message": str(e),
                "data": []
            }

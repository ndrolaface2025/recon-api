from fastapi import APIRouter, UploadFile, File, HTTPException
import os, shutil
from uuid import uuid4
from app.workers.tasks import start_recon_job
from app.utils.predict_source import predict_source_with_fallback
from app.utils.auto_map_columns import auto_map_columns
from app.utils.parser import parse_file
router = APIRouter(prefix="/api/v1/reconciliations", tags=["reconciliations"])
TEMP_UPLOAD_DIR = "/tmp/recon_uploads"
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)
@router.post("/run")
async def run_recon(channel: str, files: list[UploadFile] = File(...), sources: str = "SWITCH,CBS"):
    source_list = [s.strip().upper() for s in sources.split(",")]
    if len(files) != len(source_list):
        raise HTTPException(status_code=400, detail="number of files must match number of sources")
    saved = {}
    for idx, f in enumerate(files):
        filename = f"{uuid4().hex}_{f.filename}"
        path = os.path.join(TEMP_UPLOAD_DIR, filename)
        with open(path, "wb") as fh:
            shutil.copyfileobj(f.file, fh)
        saved[source_list[idx]] = path
    task = start_recon_job.delay(channel.upper(), saved)
    return {"task_id": task.id, "status": "queued"}



@router.post("/detect-source")
async def detect_source(file: UploadFile = File(...)):
    try:
        content = await file.read()
        print("File received:", file.filename)

        df = parse_file(content, file.filename)
        
        # Get file size
        file_size = len(content)
        
        # Get detected columns
        file_columns = df.columns.tolist()

        source_result = predict_source_with_fallback(df)
        print("Source Result:", source_result)
        
        # Get column mapping
        column_mapping_dict = auto_map_columns(df.columns)
        
        # Transform column mapping to array format with additional fields
        column_mapping_array = []
        for col_name, mapping_info in column_mapping_dict.items():
            # Infer column type based on data
            col_type = "String"  # Default type
            try:
                if df[col_name].dtype in ['int64', 'float64']:
                    col_type = "Number"
                elif 'date' in col_name.lower() or 'time' in col_name.lower():
                    col_type = "DateTime"
            except:
                pass
            
            column_mapping_array.append({
                "column_name": col_name,
                "channel_column": mapping_info.get("mapped_to", "").lower().replace("_", "_") if mapping_info.get("mapped_to") else None,
                "mapped_to": mapping_info.get("mapped_to"),
                "confidence": mapping_info.get("confidence", 0),
                "type": col_type
            })

        return {
            "filename": file.filename,
            "rows": len(df),
            "size": file_size,
            "file_detect_column": file_columns,
            "source_detection": source_result,
            "column_mapping": column_mapping_array
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

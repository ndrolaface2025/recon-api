from fastapi import APIRouter, UploadFile, File, HTTPException
import os, shutil
from uuid import uuid4
from app.workers.tasks import start_recon_job
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

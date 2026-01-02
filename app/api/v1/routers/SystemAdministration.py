from fastapi import APIRouter

router = APIRouter(prefix="/system", tags=["System Administration"])

@router.get("/")
async def health_check():
    return {"status": "ok"}

from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
from app.flexcube_db.engine import flexcube_engine


FlexcubeSessionLocal = None

if flexcube_engine:
    FlexcubeSessionLocal = sessionmaker(
        bind=flexcube_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def get_flexcube_db():
    """
    Flexcube DB session provider.
    READ-ONLY usage only.
    """
    if not FlexcubeSessionLocal:
        raise HTTPException(
            status_code=503,
            detail="Flexcube DB is not configured",
        )

    db = FlexcubeSessionLocal()
    try:
        yield db
    finally:
        db.close()

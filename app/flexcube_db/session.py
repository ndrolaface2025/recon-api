from sqlalchemy.orm import sessionmaker
from app.flexcube_db.engine import flexcube_engine


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
    db = FlexcubeSessionLocal()
    try:
        yield db
    finally:
        db.close()

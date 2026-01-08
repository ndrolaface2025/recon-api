from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.config import settings

# Configure engine with connection pooling for Celery workers
engine = create_async_engine(
    settings.DATABASE_URL,
    future=True,
    echo=False,
    pool_size=10,  # Number of connections to keep open
    max_overflow=20,  # Additional connections to create if needed
    pool_pre_ping=True,  # Test connections before using
    pool_recycle=3600,  # Recycle connections after 1 hour
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,  # Prevent automatic flushes for better control
    autocommit=False
)
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

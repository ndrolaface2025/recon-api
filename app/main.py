from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.config import settings
from app.logging_config import configure_logging
from app.api.v1.routers import all_routers
from app.db.session import AsyncSessionLocal
from app.db.init_db import init_db, seed_initial_data
from app.api.v1.routers.reconciliation import router as reconciliation_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
# Configure CORS
allowed_origins = [
    "http://localhost:5173",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for FastAPI app."""
    # Startup: Initialize database
    async with AsyncSessionLocal() as session:
        await init_db(session)
        await seed_initial_data(session)
    print("✓ App startup complete - Database initialized")
    yield
    # Shutdown
    print("✓ App shutting down")


def create_app():
    configure_logging()
    app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
    for r in all_routers:
        app.include_router(r)
    return app

# app = create_app()
app.include_router(reconciliation_router)

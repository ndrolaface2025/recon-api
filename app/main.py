from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.logging_config import configure_logging
from app.api.v1.routers import all_routers
from app.api.v2.routers import all_routers_2
from app.db.session import AsyncSessionLocal
from app.db.init_db import init_db, seed_initial_data
from app.api.v1.routers.reconciliation import router as reconciliation_router


# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for FastAPI app."""
    async with AsyncSessionLocal() as session:
        await init_db(session)
        await seed_initial_data(session)
    print("✓ App startup complete - Database initialized")
    yield
    print("✓ App shutting down")


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title=settings.APP_NAME,
        lifespan=lifespan,
    )

    # ✅ CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://localhost:3000",
            "https://ai-recon.vercel.app",  # Production frontend
            "https://rolatax.rolaface.com",  # Production backend (for same-origin requests)
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ✅ Include routers
    for router in all_routers:
        app.include_router(router)
    for router in all_routers_2:
        app.include_router(router)

    app.include_router(reconciliation_router)

    return app


# ✅ Single app instance
app = create_app()

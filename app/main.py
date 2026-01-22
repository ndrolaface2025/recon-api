from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings
from app.logging_config import configure_logging
from app.api.v1.routers import all_routers
from app.api.v2.routers import all_routers_2
from app.db.session import AsyncSessionLocal
from app.db.init_db import init_db, seed_initial_data
from app.api.v1.routers.reconciliation import router as reconciliation_router


# Custom middleware to ensure CORS headers are always present
class EnsureCORSMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Handle preflight OPTIONS requests
        if request.method == "OPTIONS":
            return JSONResponse(
                content={},
                headers={
                    "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
                    "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Allow-Credentials": "true",
                },
            )

        response = await call_next(request)

        # Ensure CORS headers are present on all responses
        origin = request.headers.get("origin", "")
        allowed_origins = [
            "http://localhost:5173",
            "http://localhost:3000",
            "https://ai-recon.vercel.app",
            "https://api.recon.rolaface.com",  # Production backend (for same-origin requests)
            "https://recon.rolaface.com",  # Production frontned
            "https://staging.recon.rolaface.com",  # Staging frontned
        ]

        if origin in allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"] = (
                "GET, POST, PUT, PATCH, DELETE, OPTIONS"
            )
            response.headers["Access-Control-Allow-Headers"] = "*"

        return response


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
        redirect_slashes=False,  # Disable automatic trailing slash redirects
    )

    # ✅ Custom CORS middleware (runs first to ensure CORS headers are always present)
    app.add_middleware(EnsureCORSMiddleware)

    # ✅ Standard CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://localhost:3000",
            "https://ai-recon.vercel.app",
            "https://api.recon.rolaface.com",  # Production backend (for same-origin requests)
            "https://recon.rolaface.com",  # Production frontned
            "https://staging.recon.rolaface.com",  # Staging frontned
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

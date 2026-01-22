# app/routes/auth_routes.py
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.user_config import UserConfig
from app.schemas.auth_schemas import LoginRequest, LoginResponse, UserResponse
from app.services.auth_service import AuthService
from app.dependencies.auth_dependencies import get_current_user_id, get_current_user
from app.db.session import get_db  # Your existing DB session


router = APIRouter(prefix="/auth", tags=["Authentication"])

# Dependency to get auth service
def get_auth_service(db: AsyncSession = Depends(get_db)) -> AuthService:
    return AuthService(db)

@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: LoginRequest,
    service: AuthService = Depends(get_auth_service)
):
    """
    Login endpoint
    - Takes username and password
    - Returns JWT token and user details
    """
    return await service.login(login_data)

@router.post("/logout")
async def logout(user_id: int = Depends(get_current_user_id)):
    """
    Logout endpoint (optional)
    - Token invalidation happens on frontend
    - This endpoint just confirms the token is valid
    - You can add token blacklisting here if needed
    """
    return {
        "message": "Logged out successfully",
        "user_id": user_id
    }

@router.get("/me", response_model=UserResponse)
async def get_current_user_details(
    current_user: UserConfig = Depends(get_current_user)
):
    """
    Get current logged-in user details
    - Requires valid JWT token in Authorization header
    - Returns complete user information
    """
    return UserResponse(
        id=current_user.id,
        f_name=current_user.f_name,
        m_name=current_user.m_name,
        l_name=current_user.l_name,
        gender=current_user.gender,
        phone=current_user.phone,
        birth_date=current_user.birth_date,
        email=current_user.email,
        username=current_user.username,
        role=current_user.role,
        status=current_user.status,
        created_at=current_user.created_at
    )

# Example: Protected route
@router.get("/protected")
async def protected_route(
    user_id: int = Depends(get_current_user_id)
):
    """
    Example of a protected route
    - Requires valid JWT token
    - Only returns user_id
    """
    return {
        "message": "This is a protected route",
        "user_id": user_id
    }
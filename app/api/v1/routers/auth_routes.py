# app/routes/auth_routes.py
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.user_config import UserConfig
from app.schemas.auth_schemas import ForgotPasswordRequest, ForgotPasswordResponse, LoginRequest, LoginResponse, ResetPasswordRequest, ResetPasswordResponse, UserResponse, VerifyResetTokenRequest, VerifyResetTokenResponse
from app.services.auth_service import AuthService
from app.dependencies.auth_dependencies import get_current_user_id, get_current_user
from app.db.session import get_db
from app.services.password_service import PasswordResetService  # Your existing DB session


router = APIRouter(prefix="/auth", tags=["Authentication"])

# Dependency to get auth service
def get_auth_service(db: AsyncSession = Depends(get_db)) -> AuthService:
    return AuthService(db)

def get_password_service(db: AsyncSession = Depends(get_db)) -> PasswordResetService:
    return PasswordResetService(db)

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

@router.post("/forgot-password", response_model=ForgotPasswordResponse)
async def forgot_password(
    request: ForgotPasswordRequest,
    service: PasswordResetService = Depends(get_password_service)
):
    """
    Forgot Password - Step 1
    - User provides username or email
    - System sends reset link to registered email
    - Returns masked email for confirmation
    """
    return await service.forgot_password(request)

@router.post("/verify-reset-token", response_model=VerifyResetTokenResponse)
async def verify_reset_token(
    request: VerifyResetTokenRequest,
    service: PasswordResetService = Depends(get_password_service)
):
    """
    Verify Reset Token
    - Checks if token is valid and not expired
    - Used by frontend to validate token before showing reset form
    """
    return await service.verify_reset_token(request.token)

@router.post("/reset-password", response_model=ResetPasswordResponse)
async def reset_password(
    request: ResetPasswordRequest,
    service: PasswordResetService = Depends(get_password_service)
):
    """
    Reset Password - Step 2
    - User provides token and new password
    - System validates token and updates password
    - User can login with new password
    """
    return await service.reset_password(request)
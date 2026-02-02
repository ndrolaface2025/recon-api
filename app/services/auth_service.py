# app/services/auth_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status
from app.db.repositories.auth_repository import AuthRepository
from app.utils.jwt_utils import verify_password, create_access_token
from app.schemas.auth_schemas import LoginRequest, LoginResponse, UserResponse
from typing import Optional

class AuthService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def login(self, login_data: LoginRequest) -> LoginResponse:
        """
        Authenticate user and return token with user details
        """
        # Get user by username
        user = await AuthRepository.get_user_by_username(self.db, login_data.username)
        
        # Check if user exists
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password"
            )
        
        # Check if user is active
        if not user.status:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is inactive"
            )
        
        # Verify password
        if not verify_password(login_data.password, user.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password"
            )
        
        # Create access token with user data
        access_token = create_access_token(
            data={
                "sub": str(user.id),
                "username": user.username,
                "role": user.role
            }
        )
        
        # Prepare user response (without password)
        user_response = UserResponse(
            id=user.id,
            f_name=user.f_name,
            m_name=user.m_name,
            l_name=user.l_name,
            gender=user.gender,
            phone=user.phone,
            birth_date=user.birth_date,
            email=user.email,
            username=user.username,
            role=user.role,
            status=user.status,
            created_at=user.created_at
        )
        
        return LoginResponse(
            access_token=access_token,
            token_type="bearer",
            user=user_response
        )
    
    async def get_current_user(self, user_id: int) -> UserResponse:
        """
        Get current user details by ID
        """
        user = await AuthRepository.get_user_by_id(self.db, user_id)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        return UserResponse(
            id=user.id,
            f_name=user.f_name,
            m_name=user.m_name,
            l_name=user.l_name,
            gender=user.gender,
            phone=user.phone,
            birth_date=user.birth_date,
            email=user.email,
            username=user.username,
            role=user.role,
            status=user.status,
            created_at=user.created_at
        )
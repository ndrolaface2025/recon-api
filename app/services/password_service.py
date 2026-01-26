# app/services/password_service.py

from datetime import timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status
from app.db.repositories.auth_repository import AuthRepository
from app.services.mail_service import EmailService
from app.utils.jwt_utils import create_access_token, decode_access_token, get_password_hash
from app.schemas.auth_schemas import (
    ForgotPasswordRequest, 
    ForgotPasswordResponse,
    ResetPasswordRequest,
    ResetPasswordResponse,
    VerifyResetTokenResponse
)
import os
from dotenv import load_dotenv

load_dotenv()

class PasswordResetService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.email_service = EmailService()
        self.frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
        self.reset_token_expire_minutes = int(os.getenv("RESET_TOKEN_EXPIRE_MINUTES", 30))
    
    def _mask_email(self, email: str) -> str:
        """Mask email for privacy (e.g., j***@example.com)"""
        if not email or "@" not in email:
            return "***@***.com"
        
        local, domain = email.split("@")
        if len(local) <= 2:
            masked_local = local[0] + "***"
        else:
            masked_local = local[0] + "***" + local[-1]
        
        return f"{masked_local}@{domain}"
    
    async def forgot_password(
        self, 
        request: ForgotPasswordRequest
    ) -> ForgotPasswordResponse:
        """
        Handle forgot password request
        - Check if user exists by username or email
        - Generate reset token
        - Send email with reset link
        """
        # Find user by username or email
        user = await AuthRepository.get_user_by_username_or_email(
            self.db, 
            request.identifier
        )
        
        # Security: Don't reveal if user exists or not
        if not user:
            # Still return success but with generic message
            return ForgotPasswordResponse(
                message="If an account exists with this information, a password reset link has been sent.",
                email="***@***.com"
            )
        
        # Check if user has email
        if not user.email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No email associated with this account. Please contact administrator."
            )
        
        # Check if user is active
        if not user.status:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is inactive. Please contact administrator."
            )
        
        # Generate reset token (JWT with user_id and purpose)
        reset_token = create_access_token(
            data={
                "sub": str(user.id),
                "purpose": "password_reset",
                "email": user.email
            },
            expires_delta=timedelta(minutes=self.reset_token_expire_minutes)
        )
        
        # Create reset link
        reset_link = f"{self.frontend_url}/reset-password?token={reset_token}"
        
        # Send email
        try:
            self.email_service.send_password_reset_email(
                to_email=user.email,
                reset_link=reset_link,
                username=user.username or user.f_name or "User"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send email: {str(e)}"
            )
        
        return ForgotPasswordResponse(
            message="Password reset link has been sent to your email.",
            email=self._mask_email(user.email)
        )
    
    async def verify_reset_token(self, token: str) -> VerifyResetTokenResponse:
        """
        Verify if reset token is valid
        """
        # Decode token
        payload = decode_access_token(token)
        
        if not payload:
            return VerifyResetTokenResponse(
                valid=False,
                message="Invalid or expired reset token"
            )
        
        # Check if token purpose is password_reset
        if payload.get("purpose") != "password_reset":
            return VerifyResetTokenResponse(
                valid=False,
                message="Invalid token purpose"
            )
        
        # Get user_id from token
        user_id = payload.get("sub")
        if not user_id:
            return VerifyResetTokenResponse(
                valid=False,
                message="Invalid token data"
            )
        
        # Check if user exists
        user = await AuthRepository.get_user_by_id(self.db, int(user_id))
        if not user:
            return VerifyResetTokenResponse(
                valid=False,
                message="User not found"
            )
        
        return VerifyResetTokenResponse(
            valid=True,
            message="Token is valid"
        )
    
    async def reset_password(
        self, 
        request: ResetPasswordRequest
    ) -> ResetPasswordResponse:
        """
        Reset user password with token
        """
        # Decode and verify token
        payload = decode_access_token(request.token)
        
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired reset token"
            )
        
        # Verify token purpose
        if payload.get("purpose") != "password_reset":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid token purpose"
            )
        
        # Get user_id from token
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid token data"
            )
        
        # Get user
        user = await AuthRepository.get_user_by_id(self.db, int(user_id))
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Hash new password
        new_password_hash = get_password_hash(request.new_password)
        
        # Update password
        success = await AuthRepository.update_user_password(
            self.db,
            user_id=int(user_id),
            new_password_hash=new_password_hash
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update password"
            )
        
        return ResetPasswordResponse(
            message="Password has been reset successfully. You can now login with your new password."
        )
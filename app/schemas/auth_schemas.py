# app/schemas/auth_schemas.py
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, EmailStr, validator
from typing import Optional

# Login Request
class LoginRequest(BaseModel):
    username: str
    password: str

# Token Response
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

# User Response (without password)
class UserResponse(BaseModel):
    id: int
    f_name: Optional[str] = None
    m_name: Optional[str] = None
    l_name: Optional[str] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    birth_date: Optional[datetime] = None
    email: Optional[str] = None
    username: Optional[str] = None
    role: Optional[str] = None
    status: Optional[bool] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# Login Response (Token + User Details)
class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse

# # these new schemas for password reset:
class ForgotPasswordRequest(BaseModel):
    """Request schema for forgot password"""
    identifier: str  # Can be either username or email
    
    class Config:
        json_schema_extra = {
            "example": {
                "identifier": "john_doe or john@example.com"
            }
        }

class ForgotPasswordResponse(BaseModel):
    """Response schema for forgot password"""
    message: str
    email: str  # Partially masked email

class ResetPasswordRequest(BaseModel):
    """Request schema for reset password"""
    token: str
    new_password: str
    confirm_password: str
    
    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('Passwords do not match')
        return v
    
    @validator('new_password')
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "new_password": "NewSecurePassword123",
                "confirm_password": "NewSecurePassword123"
            }
        }

class ResetPasswordResponse(BaseModel):
    """Response schema for reset password"""
    message: str

class VerifyResetTokenRequest(BaseModel):
    """Request schema to verify reset token"""
    token: str

class VerifyResetTokenResponse(BaseModel):
    """Response schema for token verification"""
    valid: bool
    message: str
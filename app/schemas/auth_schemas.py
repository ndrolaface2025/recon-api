# app/schemas/auth_schemas.py
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

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
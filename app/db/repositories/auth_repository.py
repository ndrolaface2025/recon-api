# app/repositories/auth_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
from app.db.models.user_config import UserConfig

class AuthRepository:
    
    @staticmethod
    async def get_user_by_username(db: AsyncSession, username: str) -> Optional[UserConfig]:
        """Get user by username"""
        query = select(UserConfig).where(UserConfig.username == username)
        result = await db.execute(query)
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[UserConfig]:
        """Get user by ID"""
        query = select(UserConfig).where(UserConfig.id == user_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_user_by_email(db: AsyncSession, email: str) -> Optional[UserConfig]:
        """Get user by email"""
        query = select(UserConfig).where(UserConfig.email == email)
        result = await db.execute(query)
        return result.scalar_one_or_none()
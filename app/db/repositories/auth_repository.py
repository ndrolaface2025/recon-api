# app/repositories/auth_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
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
    
    # NEW METHODS FOR PASSWORD RESET
    
    @staticmethod
    async def get_user_by_username_or_email(
        db: AsyncSession, 
        identifier: str
    ) -> Optional[UserConfig]:
        """Get user by username OR email"""
        query = select(UserConfig).where(
            or_(
                UserConfig.username == identifier,
                UserConfig.email == identifier
            )
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()
    
    @staticmethod
    async def update_user_password(
        db: AsyncSession, 
        user_id: int, 
        new_password_hash: str
    ) -> bool:
        """Update user password"""
        try:
            user = await AuthRepository.get_user_by_id(db, user_id)
            if user:
                user.password = new_password_hash
                await db.commit()
                return True
            return False
        except Exception as e:
            await db.rollback()
            raise e
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Type, TypeVar
from app.db.session import get_db

T = TypeVar("T")


def get_service(service_class: Type[T]):
    async def _get_service(
        db: AsyncSession = Depends(get_db)
    ) -> T:
        return service_class(db)

    return _get_service

from datetime import datetime, timezone
from sqlalchemy import any_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from app.db.models.module_config import ModuleConfig
from app.db.models.roles_config import RoleConfig
from app.db.models.user_config import UserConfig

from sqlalchemy import select, func, cast, BigInteger
from sqlalchemy.dialects.postgresql import ARRAY
from app.utils.jwt_utils import get_password_hash

class SystemAdministrationRepository:

    @staticmethod
    async def getModuleList(db: AsyncSession):
        stmt = select(ModuleConfig)

        result = await db.execute(stmt)
        record = result.scalars().all()
        if not record:
            return None
        return record
    
    @staticmethod
    async def save_module(db: AsyncSession, payload: dict):
        new_record = ModuleConfig(
            module_name=payload["module_name"],
            module_description=payload["module_description"],
            module_permission_json=payload.get("module_permission_json"),
            created_at = datetime.utcnow(),
        )
        db.add(new_record)
        await db.commit()
        await db.refresh(new_record)
        return new_record


    @staticmethod
    async def update_module( db: AsyncSession, update_id: int, payload: dict):
        stmt = select(ModuleConfig).where( ModuleConfig.id == update_id )

        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return None

        # ✅ Update fields safely
        if "module_name" in payload:
            record.module_name = payload["module_name"]

        if "module_description" in payload:
            record.module_description = payload["module_description"]

        if "module_permission_json" in payload:
            record.module_permission_json = payload["module_permission_json"]

        record.updated_at = datetime.utcnow()

        await db.commit()
        await db.refresh(record)
        return record
    
    @staticmethod
    async def delete_module(db: AsyncSession, module_id: int):
        stmt = select(ModuleConfig).where(
            ModuleConfig.id == module_id
        )

        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return False
        await db.delete(record)
        await db.commit()

        return True
    
    # Roles 

    @staticmethod
    async def save_role(db: AsyncSession, payload: dict):
        new_record = RoleConfig(
            name=payload["name"],
            description=payload["description"],
            permission_json=payload.get("permission_json"),
            created_at = datetime.utcnow(),
        )
        db.add(new_record)
        await db.commit()
        await db.refresh(new_record)
        return new_record
    
    @staticmethod
    async def get_role_List(db: AsyncSession):
        stmt = select(RoleConfig)

        result = await db.execute(stmt)
        record = result.scalars().all()
        if not record:
            return None
        return record
    

    @staticmethod
    async def update_role( db: AsyncSession, update_id: int, payload: dict):
        stmt = select(RoleConfig).where( RoleConfig.id == update_id )

        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return None

        # ✅ Update fields safely
        if "name" in payload:
            record.name = payload["name"]

        if "description" in payload:
            record.description = payload["description"]

        if "permission_json" in payload:
            record.permission_json = payload["permission_json"]

        record.updated_at = datetime.utcnow()

        await db.commit()
        await db.refresh(record)
        return record
    

    @staticmethod
    async def delete_role(db: AsyncSession, role_id: int):
        stmt = select(RoleConfig).where(
            RoleConfig.id == role_id
        )

        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return False
        await db.delete(record)
        await db.commit()

        return True
    

    @staticmethod
    async def save_user(db: AsyncSession, payload: dict):
        new_record = UserConfig(
            f_name=payload["f_name"],
            m_name=payload["m_name"],
            l_name=payload.get("l_name"),
            gender=payload["gender"],
            phone=payload["phone"],
            birth_date=datetime.strptime(
                        payload.get("birth_date"),
                        "%Y-%m-%d"
                    ).date(),
            email=payload["email"],
            role=payload["role"],
            status=payload.get("status"),
            version_number=payload.get("version_number"),
            username=payload.get("username"),
            created_at = datetime.utcnow(),
            password=get_password_hash("1234")
        )
        db.add(new_record)
        await db.commit()
        await db.refresh(new_record)
        return new_record
    
    @staticmethod
    async def get_user_List(db: AsyncSession):
        stmt = (
    select(UserConfig, RoleConfig)
    .outerjoin(
        RoleConfig,
        RoleConfig.id == func.any(
            cast(
                func.string_to_array(
                    func.replace(
                        func.replace(UserConfig.role, '[', ''),
                        ']', ''
                    ),
                    ','
                ),
                ARRAY(BigInteger)
            )
        )
    )
)

        result = await db.execute(stmt)
        return result.all()
    

    @staticmethod
    async def update_user( db: AsyncSession, update_id: int, payload: dict):
        stmt = select(UserConfig).where( UserConfig.id == update_id )

        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return None

        # ✅ Update fields safely
        if "f_name" in payload:
            record.f_name = payload["f_name"]

        if "m_name" in payload:
            record.m_name = payload["m_name"]

        if "l_name" in payload:
            record.permission_json = payload["l_name"]
        
        if "gender" in payload:
            record.gender = payload["gender"]
        
        if "phone" in payload:
            record.phone = payload["phone"]

        if "birth_date" in payload:
            record.birth_date = datetime.strptime(
                                    payload["birth_date"],
                                    "%Y-%m-%d"
                                ).date()

        if "email" in payload:
            record.email = payload["email"]

        if "role" in payload:
            record.role = payload["role"]
        
        if "status" in payload:
            record.status = payload["status"]
        
        if "username" in payload:
            record.username = payload["username"]
            record.version_number = record.version_number + 1

        record.updated_at = datetime.utcnow()

        await db.commit()
        await db.refresh(record)
        return record
    
    @staticmethod
    async def delete_user(db: AsyncSession, role_id: int):
        stmt = select(UserConfig).where(
            UserConfig.id == role_id
        )
        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return False
        await db.delete(record)
        await db.commit()

        return True
    
    @staticmethod
    async def email_exists(db: AsyncSession, email: str) -> bool:
        stmt = select(UserConfig.id).where(UserConfig.email == email)
        result = await db.execute(stmt)
        return result.scalar() is not None
    
    @staticmethod
    async def username_exists(db: AsyncSession, username: str) -> bool:
        stmt = select(UserConfig.id).where(UserConfig.username == username)
        result = await db.execute(stmt)
        return result.scalar() is not None
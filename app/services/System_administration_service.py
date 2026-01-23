from collections import defaultdict
import json
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.repositories.batchConfigRepository import BatchConfigRepository
from app.db.repositories.systemAdministrationRepository import SystemAdministrationRepository

class SystemAdministrationService:

    def __init__(self, db: AsyncSession):
        self.db = db

    async def getModuleList(self):
        list = await SystemAdministrationRepository.getModuleList(self.db)
        if list is None:
            return {
                    "status": "error",
                    "errors": False,
                    "message": "No module data available",
                    "data": []
                }
        else:
            return {
                "status": "success",
                "errors": False,
                "message": "Modules loaded successfully.",
                "data": list
            }
    
    async def save_module(self, payload):
        record = await SystemAdministrationRepository.save_module(
            self.db,
            payload
        )
        return {
                "status": "success",
                "errors": False,
                "message": "Module configuration successfully",
                "data": record
            }

    async def update_module(self, update_id: int, payload: dict):
        return await SystemAdministrationRepository.update_module(
            self.db, update_id, payload
        )
    
    async def delete_module(self, module_id: int):
        return await SystemAdministrationRepository.delete_module(
            self.db, module_id
        )
    
    # Roles Service

    async def save_role(self, payload):
        record = await SystemAdministrationRepository.save_role(
            self.db,
            payload
        )
        return {
                "status": "success",
                "errors": False,
                "message": "Role configuration successfully",
                "data": record
            }
    
    async def get_role_List(self):
        list = await SystemAdministrationRepository.get_role_List(self.db)
        if list is None:
            return {
                    "status": "error",
                    "errors": False,
                    "message": "No role data available",
                    "data": []
                }
        else:
            return {
                "status": "success",
                "errors": False,
                "message": "Role loaded successfully.",
                "data": list
            }
        
    async def update_role(self, update_id: int, payload: dict):
        return await SystemAdministrationRepository.update_role(
            self.db, update_id, payload
        )
    
    async def delete_role(self, role_id: int):
        return await SystemAdministrationRepository.delete_role(
            self.db, role_id
        )
    

    # User services

    async def save_user(self, payload):
        record = await SystemAdministrationRepository.save_user(
            self.db,
            payload
        )
        return {
                "status": "success",
                "errors": False,
                "message": "User Details Saved successfully",
                "data": record
            }
    

    async def get_user_List(self, offset: int = 0, limit: int = 10, search: str = None):
        rows, total = await SystemAdministrationRepository.get_user_List(
            self.db, 
            offset=offset, 
            limit=limit, 
            search=search
        )
        
        if not rows:
            return {
                "status": "error",
                "errors": False,
                "message": "No user data available",
                "data": [],
                "meta": {
                    "offset": offset,
                    "limit": limit,
                    "total": 0
                }
            }
        
        users_map = defaultdict(lambda: {
            "roles": []
        })

        for user, role in rows:
            # Create user once
            if user.id not in users_map:
                users_map[user.id].update({
                    "id": user.id,
                    "f_name": user.f_name,
                    "m_name": user.m_name,
                    "l_name": user.l_name,
                    "gender": user.gender,
                    "phone": user.phone,
                    "password": user.password,
                    "username": user.username,
                    "version_number": user.version_number,
                    "email": user.email,
                    "status": user.status,
                    "birth_date": user.birth_date.date() if user.birth_date else None,
                    "user_roles": user.role,
                })

            # Append roles (many-to-one)
            if role:
                users_map[user.id]["roles"].append({
                    "id": role.id,
                    "name": role.name
                })

        return {
            "status": "success",
            "errors": False,
            "message": "User loaded successfully.",
            "data": list(users_map.values()),
            "meta": {
                "offset": offset,
                "limit": limit,
                "total": total
            }
        }
        
    async def update_user(self, update_id: int, payload: dict):
        return await SystemAdministrationRepository.update_user(
            self.db, update_id, payload
        )
    
    async def delete_user(self, user_id: int):
        return await SystemAdministrationRepository.delete_user(
            self.db, user_id
        )
    
    async def check_email_exists(self, email: str):
        exists = await SystemAdministrationRepository.email_exists(self.db, email)

        return {
            "status": "success",
            "errors": False,
            "exists": exists,
            "message": "Email already exists" if exists else "Email is available"
        }
    
    async def check_username_exists(self, username: str):
        exists = await SystemAdministrationRepository.username_exists(self.db, username)

        return {
            "status": "success",
            "errors": False,
            "exists": exists,
            "message": "Username already exists" if exists else "Username is available"
        }
    
    async def get_user_By_id(self, id: int = 0):
        rows, total = await SystemAdministrationRepository.get_user_By_id(
            self.db, 
            id=id
        )
        
        if not rows:
            return {
                "status": "error",
                "errors": False,
                "message": "No user data available",
                "data": []
            }
        
        users_map = defaultdict(lambda: {
            "roles": []
        })

        for user, role in rows:
            # Create user once
            if user.id not in users_map:
                users_map[user.id].update({
                    "id": user.id,
                    "f_name": user.f_name,
                    "m_name": user.m_name,
                    "l_name": user.l_name,
                    "gender": user.gender,
                    "phone": user.phone,
                    "password": user.password,
                    "username": user.username,
                    "version_number": user.version_number,
                    "email": user.email,
                    "status": user.status,
                    "birth_date": user.birth_date.date() if user.birth_date else None,
                    "user_roles": user.role,
                })

            # Append roles (many-to-one)
            if role:
                users_map[user.id]["roles"].append({
                    "id": role.id,
                    "name": role.name
                })

        return {
            "status": "success",
            "errors": False,
            "message": "User loaded successfully.",
            "data": list(users_map.values()),
        }
    
    
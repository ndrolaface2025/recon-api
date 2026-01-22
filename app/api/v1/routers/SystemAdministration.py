from typing import Any
from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.params import Depends
from app.services.System_administration_service import SystemAdministrationService
from app.services.services import get_service
router = APIRouter(prefix="/api/v1/system", tags=["System Administration"])

# Module APIS
@router.get("/module")
async def Get_module_List(
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.getModuleList()

@router.post("/module")
async def Save_module(
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.save_module(payload)

@router.put("/module/{update_id}")
async def Update_module(
    update_id: int,
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    result =  await service.update_module(update_id, payload)

    if not result:
        raise HTTPException(status="error", errors= True, message="Module not found")

    return {
        "status": "success",
        "errors": False,
        "message": "Module updated successfully",
        "data": result,
    }

@router.delete("/module/{module_id}")
async def delete_module(
    module_id: int,
    service: SystemAdministrationService = Depends(
        get_service(SystemAdministrationService)
    ),
):
    result = await service.delete_module(module_id)
    if not result:
         raise HTTPException(status="error", errors= True, message="Module not found")
    return {
        "status": "success",
        "errors": False,
        "message": "Module deleted successfully",
        "data": None,
    }

# Role Routes
@router.post("/roles")
async def Save_role(
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.save_role(payload)

@router.get("/roles")
async def Get_role_List(
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.get_role_List()

@router.put("/roles/{update_id}")
async def Update_role(
    update_id : int,
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):  
    result =  await service.update_role(update_id, payload)

    if not result:
        raise HTTPException(status="error", errors= True, message="Module not found")

    return {
        "status": "success",
        "errors": False,
        "message": "Module updated successfully",
        "data": result,
    }

@router.delete("/roles/{role_id}")
async def delete_role(
    role_id: int,
    service: SystemAdministrationService = Depends(
        get_service(SystemAdministrationService)
    ),
):
    result = await service.delete_role(role_id)
    if not result:
         raise HTTPException(status="error", errors= True, message="Role not found")
    return {
        "status": "success",
        "errors": False,
        "message": "Role deleted successfully",
        "data": None,
    }


# Users Routes
@router.post("/user")
async def Save_role(
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.save_user(payload)

@router.get("/user")
async def Get_user_List(
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    return await service.get_user_List()


@router.put("/user/{update_id}")
async def Update_user(
    update_id: int,
    payload: Any = Body(...),
    service: SystemAdministrationService = Depends(get_service(SystemAdministrationService))
):
    result =  await service.update_user(update_id, payload)

    if not result:
        raise HTTPException(status="error", errors= True, message="User not found")

    return {
        "status": "success",
        "errors": False,
        "message": "User updated successfully",
        "data": result,
    }

@router.delete("/user/{user_id}")
async def delete_user(
    user_id: int,
    service: SystemAdministrationService = Depends(
        get_service(SystemAdministrationService)
    ),
):
    result = await service.delete_user(user_id)
    if not result:
         raise HTTPException(status="error", errors= True, message="User not found")
    return {
        "status": "success",
        "errors": False,
        "message": "User deleted successfully",
        "data": None,
    }

@router.get("/user/check-email")
async def check_email(
    email: str = Query(..., description="Email to check"),
    service: SystemAdministrationService = Depends(
        get_service(SystemAdministrationService)
    ),
):
    return await service.check_email_exists(email)

@router.get("/user/check-username")
async def check_username(
    username: str = Query(..., description="Username to check"),
    service: SystemAdministrationService = Depends(
        get_service(SystemAdministrationService)
    ),
):
    return await service.check_username_exists(username)
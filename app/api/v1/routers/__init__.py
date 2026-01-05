# from .reconciliation import router as reconciliation_router
from .SystemAdministration import router as system_administration_router
from .Example import router as example_router
from .Upload import router as upload_router

all_routers = [
    # reconciliation_router,
    upload_router,
    system_administration_router,
    example_router
]
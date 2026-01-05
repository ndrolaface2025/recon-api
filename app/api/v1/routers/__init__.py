# from .reconciliation import router as reconciliation_router
from .SystemAdministration import router as system_administration_router
from .Example import router as example_router
from .matching_rules import router as matching_rules_router

all_routers = [
    # reconciliation_router,
    system_administration_router,
    example_router,
    matching_rules_router
]
# from .reconciliation import router as reconciliation_router
from .SystemAdministration import router as system_administration_router
from .Example import router as example_router
from .Upload import router as upload_router
from .SystemConfiguration import router as system_configuration_router
from .matching_rules import router as matching_rules_router
from .matching_execution import router as matching_execution_router
from .transactions import router as transactions_router

all_routers = [
    # reconciliation_router,
    upload_router,
    system_administration_router,
    system_configuration_router,
    example_router,
    matching_rules_router,
    matching_execution_router,
    transactions_router,
]
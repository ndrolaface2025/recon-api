# from .reconciliation import router as reconciliation_router
from .SystemAdministration import router as system_administration_router
from .Example import router as example_router
from .Upload import router as upload_router
from .ManualTxnRoute import router as manual_router
from .SystemConfiguration import router as system_configuration_router
from .matching_rules import router as matching_rules_router
from .matching_execution import router as matching_execution_router
from .auto_matching import router as auto_matching_router
from .transactions import router as transactions_router
from .TxnJournalEntryRoute import router as txn_journal_router
from .GeneralLedgerRoute import router as general_ledger_router
from .Channel import router as channel_router
from .upload_api_config import router as upload_api_config
from .upload_scheduler_config import router as upload_scheduler_config
from .search import router as search_router
from .transaction_search import router as transaction_search_router
from .auth_routes import router as auth_routes
from .recon_run_routes import router as recon_run_routes
from .flexcube import router as flexcube_router

all_routers = [
    auth_routes,
    # reconciliation_router,
    general_ledger_router,
    txn_journal_router,
    manual_router,
    upload_router,
    system_administration_router,
    system_configuration_router,
    example_router,
    matching_rules_router,
    matching_execution_router,
    auto_matching_router,
    transactions_router,
    channel_router,
    upload_api_config,
    upload_scheduler_config,
    search_router,
    transaction_search_router,
    recon_run_routes,
    flexcube_router,
]

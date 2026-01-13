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
from .channel_config_routes import router as channel_config_router
from .user_config_routes import router as user_config_router
from .source_config_routes import router as source_config_router

all_routers = [
    # reconciliation_router,
    source_config_router,
    user_config_router,
    channel_config_router,
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
    channel_router
]
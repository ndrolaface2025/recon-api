from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Type, TypeVar
from app.db.session import get_db
from sqlalchemy import select, func, and_
from app.db.models.transactions import Transaction
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from app.db.models.matching_rule_config import MatchingRuleConfig
T = TypeVar("T")


def get_service(service_class: Type[T]):
    async def _get_service(
        db: AsyncSession = Depends(get_db)
    ) -> T:
        return service_class(db)

    return _get_service

def _get_match_status_label(status: int) -> str:
    """Convert match_status code to label"""
    status_map = {
        0: "Unmatched",
        1: "Matched",
        2: "Partial",
    }
    return status_map.get(status, "Unknown")


async def paginate_matched_groups(
    db: AsyncSession,
    conditions: List,
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
):
    """
    Paginate matched transactions by recon_reference_number groups.
    
    Strategy:
    1. Get distinct recon_reference_numbers (paginated)
    2. Fetch ALL transactions for those reference numbers
    3. Return grouped results
    """
    
    # STEP 1: Get paginated list of recon_reference_numbers
    # This query gets DISTINCT reference numbers with their sort values
    
    # Create a subquery to get one row per recon_reference_number with sort value
    subq = (
        select(
            Transaction.recon_reference_number,
            func.min(getattr(Transaction, sort_by)).label("sort_value"),
            func.count(Transaction.id).label("group_size"),
        )
        .where(and_(*conditions))
        .where(Transaction.recon_reference_number.isnot(None))  # Exclude NULL reference numbers
        .group_by(Transaction.recon_reference_number)
    ).subquery()
    
    # Count total groups for pagination
    count_query = select(func.count()).select_from(subq)
    total_result = await db.execute(count_query)
    total_group_count = total_result.scalar()

    # Get paginated reference numbers
    offset = (page - 1) * page_size

    ref_query = select(subq.c.recon_reference_number, subq.c.group_size)
    
    if sort_order.lower() == "desc":
        ref_query = ref_query.order_by(subq.c.sort_value.desc())
    else:
        ref_query = ref_query.order_by(subq.c.sort_value.asc())
    
    ref_query = ref_query.limit(page_size).offset(offset)
    
    ref_result = await db.execute(ref_query)
    ref_rows = ref_result.all()
    
    if not ref_rows:
        return [], total_group_count
    
    # Extract reference numbers and calculate total transactions
    reference_numbers = [row.recon_reference_number for row in ref_rows]
    
    # STEP 2: Fetch ALL transactions for these reference numbers
    main_query = (
        select(
            Transaction,
            ChannelConfig.channel_name,
            SourceConfig.source_name,
            MatchingRuleConfig.rule_name.label("match_rule_name"),
        )
        .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
        .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
        .outerjoin(
            MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id
        )
        .where(Transaction.recon_reference_number.in_(reference_numbers))
    )
    
    # Apply original conditions
    if conditions:
        main_query = main_query.where(and_(*conditions))
    
    # Sort within each group
    sort_column = getattr(Transaction, sort_by)
    if sort_order.lower() == "desc":
        main_query = main_query.order_by(
            Transaction.recon_reference_number.desc(),
            sort_column.desc(),
        )
    else:
        main_query = main_query.order_by(
            Transaction.recon_reference_number.asc(),
            sort_column.asc(),
        )
    
    result = await db.execute(main_query)
    rows = result.all()
    
   # STEP 3: Group transactions by recon_reference_number AND source
    grouped_data = {}
    
    for row in rows:
        ref_num = row.Transaction.recon_reference_number
        
        # Create source key like "atm_transactions", "switch_transactions", etc.
        source_key = f"{row.source_name.lower()}_transactions" if row.source_name else "unknown_transactions"
        
        # Initialize group if not exists
        if ref_num not in grouped_data:
            grouped_data[ref_num] = {}
        
        # Initialize source list if not exists
        if source_key not in grouped_data[ref_num]:
            grouped_data[ref_num][source_key] = []
        
        # Build transaction dict with all fields
        transaction_dict = {
            "id": row.Transaction.id,
            "recon_reference_number": row.Transaction.recon_reference_number,
            "channel_id": row.Transaction.channel_id,
            "channel_name": row.channel_name,
            "network_id": row.Transaction.network_id,
            "source_id": row.Transaction.source_id,
            "source_name": row.source_name,
            "reference_number": row.Transaction.reference_number,
            "source_reference_number": row.Transaction.source_reference_number,
            "amount": row.Transaction.amount,
            "date": row.Transaction.date,
            "account_number": row.Transaction.account_number,
            "currency": row.Transaction.ccy,
            "match_status": row.Transaction.match_status,
            "other_details": row.Transaction.otherDetails,
            "match_status_label": _get_match_status_label(row.Transaction.match_status),
            "match_rule_id": row.Transaction.match_rule_id,
            "match_rule_name": row.match_rule_name,
            "match_condition": row.Transaction.match_conditon,
            "reconciliation_status": row.Transaction.reconciliation_status,
            "reconciled_by": row.Transaction.reconciled_by,
            "comment": row.Transaction.comment,
            "created_at": row.Transaction.created_at.isoformat() if row.Transaction.created_at else None,
            "updated_at": row.Transaction.updated_at.isoformat() if row.Transaction.updated_at else None,
        }
        
        grouped_data[ref_num][source_key].append(transaction_dict)
    
    # STEP 4: Convert to list format maintaining reference_numbers order
    grouped_list = []
    for ref_num in reference_numbers:
        if ref_num in grouped_data:
            grouped_list.append(grouped_data[ref_num])

    return grouped_list, total_group_count

async def paginate_individual_transactions(
    db: AsyncSession,
    conditions: List,
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
):
    """
    Normal pagination for unmatched transactions.
    Returns flat list of individual transactions.
    """
    
    query = (
        select(
            Transaction,
            ChannelConfig.channel_name,
            SourceConfig.source_name,
            MatchingRuleConfig.rule_name.label("match_rule_name"),
            func.count().over().label("total_count"),
        )
        .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
        .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
        .outerjoin(
            MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id
        )
    )
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # Apply sorting
    sort_column = getattr(Transaction, sort_by)
    if sort_order.lower() == "desc":
        query = query.order_by(sort_column.desc())
    else:
        query = query.order_by(sort_column.asc())
    
    # Apply pagination
    offset = (page - 1) * page_size
    query = query.limit(page_size).offset(offset)
    print(
        query.compile(
            dialect=db.bind.dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    result = await db.execute(query)
    rows = result.all()
    
    total_records = rows[0].total_count if rows else 0
    
    # Build response (flat list for unmatched)
    transactions = []
    for row in rows:
        transaction_dict = {
            "id": row.Transaction.id,
            "recon_reference_number": row.Transaction.recon_reference_number,
            "channel_id": row.Transaction.channel_id,
            "channel_name": row.channel_name,
            "network_id": row.Transaction.network_id,
            "source_id": row.Transaction.source_id,
            "source_name": row.source_name,
            "reference_number": row.Transaction.reference_number,
            "source_reference_number": row.Transaction.source_reference_number,
            "amount": row.Transaction.amount,
            "date": row.Transaction.date,
            "account_number": row.Transaction.account_number,
            "currency": row.Transaction.ccy,
            "match_status": row.Transaction.match_status,
            "other_details": row.Transaction.otherDetails,
            "match_status_label": _get_match_status_label(row.Transaction.match_status),
            "match_rule_id": row.Transaction.match_rule_id,
            "match_rule_name": row.match_rule_name,
            "match_condition": row.Transaction.match_conditon,
            "reconciliation_status": row.Transaction.reconciliation_status,
            "reconciled_by": row.Transaction.reconciled_by,
            "comment": row.Transaction.comment,
            "created_at": row.Transaction.created_at.isoformat() if row.Transaction.created_at else None,
            "updated_at": row.Transaction.updated_at.isoformat() if row.Transaction.updated_at else None,
        }
        transactions.append(transaction_dict)
    
    return transactions, total_records
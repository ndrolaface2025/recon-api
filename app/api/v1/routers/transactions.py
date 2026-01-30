from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, and_, or_, Numeric
from typing import Optional, List
from datetime import datetime

from app.db.session import get_db
from app.db.models.transactions import Transaction
from app.services.transactionService import TransactionService
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from app.db.models.matching_rule_config import MatchingRuleConfig

router = APIRouter(prefix="/api/v1/reconciliations", tags=["reconciliations"])


@router.get("/transactions")
async def get_transactions(
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
    match_status: Optional[str] = Query(None, description="Filter by status: matched, partial, unmatched"),
    source_id: Optional[int] = Query(None, description="Filter by source ID"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(30, ge=1, le=500, description="Records per page"),
    sort_by: str = Query("created_at", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch transactions with filtering by channel, match status, and date range.
    
    Match Status values:
    - matched: Full match (match_status = 1)
    - partial: Partial match (match_status = 2)
    - unmatched: No match (match_status = 0 or NULL)
    """
    try:
        # Build the base query
        query = select(
            Transaction,
            ChannelConfig.channel_name,
            SourceConfig.source_name,
            MatchingRuleConfig.rule_name.label('match_rule_name')
        ).outerjoin(
            ChannelConfig, Transaction.channel_id == ChannelConfig.id
        ).outerjoin(
            SourceConfig, Transaction.source_id == SourceConfig.id
        ).outerjoin(
            MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id
        )
        
        # Build WHERE conditions
        conditions = []
        
        # Filter by channel
        if channel_id is not None:
            conditions.append(Transaction.channel_id == channel_id)
        
        # Filter by source
        if source_id is not None:
            conditions.append(Transaction.source_id == source_id)
        
        # Filter by match status
        if match_status:
            status_lower = match_status.lower()
            if status_lower == "matched":
                conditions.append(Transaction.match_status == 1)
            elif status_lower == "partial":
                conditions.append(Transaction.match_status == 2)
            elif status_lower == "unmatched":
                conditions.append(
                    or_(
                        Transaction.match_status == 0,
                        Transaction.match_status.is_(None)
                    )
                )
        
        # Filter by date range
        if date_from:
            conditions.append(Transaction.date >= date_from)
        if date_to:
            conditions.append(Transaction.date <= date_to)
        
        # Apply conditions
        if conditions:
            query = query.where(and_(*conditions))
        
        # Apply sorting
        if sort_order.lower() == "desc":
            query = query.order_by(getattr(Transaction, sort_by).desc())
        else:
            query = query.order_by(getattr(Transaction, sort_by).asc())
        
        # For matched/partial status: Fetch ALL matching records (no pagination yet)
        # We'll group them first, then paginate the groups to avoid splitting matched sets
        if match_status and match_status.lower() in ["matched", "partial"]:
            # Fetch all matching transactions without pagination
            result = await db.execute(query)
            rows = result.all()
        else:
            # For unmatched: Use normal pagination on individual transactions
            # Get total count for pagination
            count_query = select(func.count()).select_from(Transaction)
            if conditions:
                count_query = count_query.where(and_(*conditions))
            
            total_result = await db.execute(count_query)
            total_records = total_result.scalar()
            
            # Apply pagination
            offset = (page - 1) * page_size
            query = query.limit(page_size).offset(offset)
            
            # Execute query
            result = await db.execute(query)
            rows = result.all()
        
        # Helper function to create transaction dict
        def create_transaction_dict(txn, channel_name, source_name, match_rule_name):
            if txn.match_status == 1:
                match_status_label = "Matched"
            elif txn.match_status == 2:
                match_status_label = "Partially Matched"
            else:
                match_status_label = "Unmatched"
            
            return {
                "id": txn.id,
                "recon_reference_number": txn.recon_reference_number,
                "channel_id": txn.channel_id,
                "channel_name": channel_name,
                "source_id": txn.source_id,
                "source_name": source_name,
                "reference_number": txn.reference_number,
                "source_reference_number": txn.source_reference_number,
                "amount": txn.amount,
                "date": txn.date,
                "account_number": txn.account_number,
                "currency": txn.ccy,
                "match_status": txn.match_status,
                "other_details": txn.otherDetails,
                "match_status_label": match_status_label,
                "match_rule_id": txn.match_rule_id,
                "match_rule_name": match_rule_name,
                "match_condition": txn.match_conditon,
                "reconciled_status": txn.reconciled_status,
                "reconciled_by": txn.reconciled_by,
                "comment": txn.comment,
                "created_at": txn.created_at.isoformat() if txn.created_at else None,
                "updated_at": txn.updated_at.isoformat() if txn.updated_at else None
            }
        
        # Transform results based on match status
        transactions = []
        
        # Group transactions by recon_reference_number for matched/partial
        if match_status and match_status.lower() in ["matched", "partial"]:
            # Group by recon_reference_number (this groups all matched sources together)
            grouped = {}
            for row in rows:
                txn = row[0]
                channel_name = row[1]
                source_name = row[2]
                match_rule_name = row[3]
                
                # Grouping logic:
                # - For FULL matches (status=1): Group by recon_reference_number
                # - For PARTIAL matches (status=2): Group by reference_number (RRN)
                # This ensures all sources with the same RRN are grouped together
                if txn.match_status == 1 and txn.recon_reference_number:
                    # Full match: Use recon_reference_number
                    group_key = txn.recon_reference_number
                elif txn.match_status == 2 and txn.reference_number:
                    # Partial match: Use reference_number (RRN) with a prefix to avoid collisions
                    group_key = f"partial_{txn.reference_number}"
                else:
                    # Fallback: Individual transaction
                    group_key = f"txn_{txn.id}"
                
                if group_key not in grouped:
                    grouped[group_key] = {
                        "atm_transactions": [],
                        "switch_transactions": [],
                        "cbs_transactions": [],
                        "network_transactions": [],
                        "card_transactions": [],
                        "settlement_transactions": [],
                        "ej_transactions": [],
                        "platform_transactions": []
                    }
                
                txn_dict = create_transaction_dict(txn, channel_name, source_name, match_rule_name)
                
                # Group by source name (case-insensitive)
                source_key = source_name.lower() if source_name else "unknown"
                if "atm" in source_key:
                    grouped[group_key]["atm_transactions"].append(txn_dict)
                elif "switch" in source_key:
                    grouped[group_key]["switch_transactions"].append(txn_dict)
                elif "cbs" in source_key:
                    grouped[group_key]["cbs_transactions"].append(txn_dict)
                elif "network" in source_key:
                    grouped[group_key]["network_transactions"].append(txn_dict)
                elif "card" in source_key:
                    grouped[group_key]["card_transactions"].append(txn_dict)
                elif "settlement" in source_key:
                    grouped[group_key]["settlement_transactions"].append(txn_dict)
                elif "ej" in source_key or "journal" in source_key:
                    grouped[group_key]["ej_transactions"].append(txn_dict)
                elif "platform" in source_key:
                    grouped[group_key]["platform_transactions"].append(txn_dict)
            
            # Convert grouped data to list (all groups)
            all_groups = []
            for group_key, group_data in grouped.items():
                # Only include source arrays that have data
                transaction_group = {}
                
                if group_data["atm_transactions"]:
                    transaction_group["atm_transactions"] = group_data["atm_transactions"]
                if group_data["switch_transactions"]:
                    transaction_group["switch_transactions"] = group_data["switch_transactions"]
                if group_data["cbs_transactions"]:
                    transaction_group["cbs_transactions"] = group_data["cbs_transactions"]
                if group_data["network_transactions"]:
                    transaction_group["network_transactions"] = group_data["network_transactions"]
                if group_data["card_transactions"]:
                    transaction_group["card_transactions"] = group_data["card_transactions"]
                if group_data["settlement_transactions"]:
                    transaction_group["settlement_transactions"] = group_data["settlement_transactions"]
                if group_data["ej_transactions"]:
                    transaction_group["ej_transactions"] = group_data["ej_transactions"]
                if group_data["platform_transactions"]:
                    transaction_group["platform_transactions"] = group_data["platform_transactions"]
                
                all_groups.append(transaction_group)
            
            # NOW paginate the groups (not individual transactions)
            total_records = len(all_groups)  # Total number of matched groups
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            transactions = all_groups[start_idx:end_idx]  # Slice the groups
        
        else:
            # For unmatched transactions, return all sources separately
            for row in rows:
                txn = row[0]
                channel_name = row[1]
                source_name = row[2]
                match_rule_name = row[3]
                
                transactions.append(create_transaction_dict(txn, channel_name, source_name, match_rule_name))
        
        # Calculate pagination metadata
        total_pages = (total_records + page_size - 1) // page_size
        has_next = page < total_pages
        has_previous = page > 1
        
        # Get summary counts
        summary_query = select(
            func.count(case((Transaction.match_status == 1, 1))).label('total_matched'),
            func.count(case((Transaction.match_status == 2, 1))).label('total_partial'),
            func.count(case((or_(Transaction.match_status == 0, Transaction.match_status.is_(None)), 1))).label('total_unmatched'),
        )
        if conditions:
            summary_query = summary_query.where(and_(*conditions))
        
        summary_result = await db.execute(summary_query)
        summary = summary_result.one()
        
        return {
            "status": "success",
            "error": False,
            "message": "Transactions retrieved successfully",
            "data": {
                "transactions": transactions,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_records": total_records,
                    "total_pages": total_pages,
                    "has_next": has_next,
                    "has_previous": has_previous
                },
                "summary": {
                    "total_matched": summary.total_matched or 0,
                    "total_partial": summary.total_partial or 0,
                    "total_unmatched": summary.total_unmatched or 0
                }
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": True,
            "message": str(e),
            "data": {}
        }


@router.get("/summary")
async def get_reconciliation_summary(
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get reconciliation summary grouped by channel with match status counts.
    """
    try:
        # Build date filter conditions
        conditions = []
        if date_from:
            conditions.append(Transaction.date >= date_from)
        if date_to:
            conditions.append(Transaction.date <= date_to)
        
        # Query for channel summary
        query = select(
            ChannelConfig.id.label('channel_id'),
            ChannelConfig.channel_name,
            func.count(case((Transaction.match_status == 1, 1))).label('matched_count'),
            func.count(case((Transaction.match_status == 2, 1))).label('partial_count'),
            func.count(case((or_(Transaction.match_status == 0, Transaction.match_status.is_(None)), 1))).label('unmatched_count'),
            func.sum(case((Transaction.match_status == 1, func.cast(Transaction.amount, Numeric)))).label('matched_amount'),
            func.sum(case((Transaction.match_status == 2, func.cast(Transaction.amount, Numeric)))).label('partial_amount'),
            func.sum(case((or_(Transaction.match_status == 0, Transaction.match_status.is_(None)), func.cast(Transaction.amount, Numeric)))).label('unmatched_amount'),
            func.count(Transaction.id).label('total_count'),
            func.sum(func.cast(Transaction.amount, Numeric)).label('total_amount')
        ).outerjoin(
            Transaction, ChannelConfig.id == Transaction.channel_id
        ).group_by(
            ChannelConfig.id, ChannelConfig.channel_name
        ).order_by(
            ChannelConfig.id
        )
        
        if conditions:
            query = query.where(and_(*conditions))
        
        result = await db.execute(query)
        rows = result.all()
        
        # Transform results
        channels = []
        overall_matched = 0
        overall_partial = 0
        overall_unmatched = 0
        overall_matched_amt = 0
        overall_partial_amt = 0
        overall_unmatched_amt = 0
        overall_total = 0
        overall_total_amt = 0
        
        for row in rows:
            matched_count = row.matched_count or 0
            partial_count = row.partial_count or 0
            unmatched_count = row.unmatched_count or 0
            total_count = row.total_count or 0
            
            matched_amount = float(row.matched_amount or 0)
            partial_amount = float(row.partial_amount or 0)
            unmatched_amount = float(row.unmatched_amount or 0)
            total_amount = float(row.total_amount or 0)
            
            matched_pct = (matched_count / total_count * 100) if total_count > 0 else 0
            partial_pct = (partial_count / total_count * 100) if total_count > 0 else 0
            unmatched_pct = (unmatched_count / total_count * 100) if total_count > 0 else 0
            
            channels.append({
                "channel_id": row.channel_id,
                "channel_name": row.channel_name,
                "matched": {
                    "count": matched_count,
                    "amount": f"{matched_amount:.2f}",
                    "percentage": round(matched_pct, 2)
                },
                "partial": {
                    "count": partial_count,
                    "amount": f"{partial_amount:.2f}",
                    "percentage": round(partial_pct, 2)
                },
                "unmatched": {
                    "count": unmatched_count,
                    "amount": f"{unmatched_amount:.2f}",
                    "percentage": round(unmatched_pct, 2)
                },
                "total": {
                    "count": total_count,
                    "amount": f"{total_amount:.2f}"
                }
            })
            
            # Accumulate overall totals
            overall_matched += matched_count
            overall_partial += partial_count
            overall_unmatched += unmatched_count
            overall_total += total_count
            overall_matched_amt += matched_amount
            overall_partial_amt += partial_amount
            overall_unmatched_amt += unmatched_amount
            overall_total_amt += total_amount
        
        # Calculate overall percentages
        overall_matched_pct = (overall_matched / overall_total * 100) if overall_total > 0 else 0
        overall_partial_pct = (overall_partial / overall_total * 100) if overall_total > 0 else 0
        overall_unmatched_pct = (overall_unmatched / overall_total * 100) if overall_total > 0 else 0
        
        return {
            "status": "success",
            "error": False,
            "message": "Summary retrieved successfully",
            "data": {
                "channels": channels,
                "overall": {
                    "matched": {
                        "count": overall_matched,
                        "amount": f"{overall_matched_amt:.2f}",
                        "percentage": round(overall_matched_pct, 2)
                    },
                    "partial": {
                        "count": overall_partial,
                        "amount": f"{overall_partial_amt:.2f}",
                        "percentage": round(overall_partial_pct, 2)
                    },
                    "unmatched": {
                        "count": overall_unmatched,
                        "amount": f"{overall_unmatched_amt:.2f}",
                        "percentage": round(overall_unmatched_pct, 2)
                    },
                    "total": {
                        "count": overall_total,
                        "amount": f"{overall_total_amt:.2f}"
                    }
                }
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": True,
            "message": str(e),
            "data": {}
        }


@router.get("/transactions/count")
async def get_transaction_counts(
    channel_id: int = Query(..., description="Channel ID to filter by"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get transaction counts by match status for a specific channel.
    Counts are grouped by match groups (not individual transactions).
    
    Returns:
    - matched: Count of fully matched groups (match_status = 1)
    - partially_matched: Count of partially matched groups (match_status = 2)
    - not_matched: Count of unmatched transactions (match_status = 0 or NULL)
    
    Example:
    - If 3 transactions (ATM+SWITCH+CBS) match together → counts as 1 matched group
    - If 2 transactions (ATM+SWITCH) partially match → counts as 1 partial match group
    - Unmatched transactions are counted individually
    """
    try:
        # Count FULL matches (match_status = 1) grouped by reference_number
        full_match_query = select(
            func.count(func.distinct(Transaction.reference_number)).label('matched_groups')
        ).where(
            and_(
                Transaction.channel_id == channel_id,
                Transaction.match_status == 1
            )
        )
        
        # Count PARTIAL matches (match_status = 2) grouped by reference_number
        partial_match_query = select(
            func.count(func.distinct(Transaction.reference_number)).label('partial_groups')
        ).where(
            and_(
                Transaction.channel_id == channel_id,
                Transaction.match_status == 2
            )
        )
        
        # Count UNMATCHED transactions (match_status = 0 or NULL)
        unmatched_query = select(
            func.count(Transaction.id).label('unmatched_count')
        ).where(
            and_(
                Transaction.channel_id == channel_id,
                or_(Transaction.match_status == 0, Transaction.match_status == None)
            )
        )
        
        # Execute all queries
        full_result = await db.execute(full_match_query)
        partial_result = await db.execute(partial_match_query)
        unmatched_result = await db.execute(unmatched_query)
        
        full_count = full_result.scalar() or 0
        partial_count = partial_result.scalar() or 0
        unmatched_count = unmatched_result.scalar() or 0
        
        return {
            "status": "success",
            "error": False,
            "message": "Transaction counts retrieved successfully",
            "data": {
                "matched": full_count,
                "partially_matched": partial_count,
                "not_matched": unmatched_count
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": True,
            "message": f"Failed to retrieve transaction counts: {str(e)}",
            "data": {
                "matched": 0,
                "partially_matched": 0,
                "not_matched": 0
            }
        }


@router.get("/transactions/{transaction_id}")
async def get_transaction_details(
    transaction_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed information about a specific transaction including matched transactions.
    """
    try:
        # Get the main transaction
        query = select(
            Transaction,
            ChannelConfig.channel_name,
            SourceConfig.source_name,
            MatchingRuleConfig.rule_name.label('match_rule_name')
        ).outerjoin(
            ChannelConfig, Transaction.channel_id == ChannelConfig.id
        ).outerjoin(
            SourceConfig, Transaction.source_id == SourceConfig.id
        ).outerjoin(
            MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id
        ).where(
            Transaction.id == transaction_id
        )
        
        result = await db.execute(query)
        row = result.one_or_none()
        
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        txn = row[0]
        channel_name = row[1]
        source_name = row[2]
        match_rule_name = row[3]
        
        # Determine match status label
        if txn.match_status == 1:
            match_status_label = "Matched"
        elif txn.match_status == 2:
            match_status_label = "Partially Matched"
        else:
            match_status_label = "Unmatched"
        
        transaction_data = {
            "id": txn.id,
            "recon_reference_number": txn.recon_reference_number,
            "channel_id": txn.channel_id,
            "channel_name": channel_name,
            "source_id": txn.source_id,
            "source_name": source_name,
            "reference_number": txn.reference_number,
            "source_reference_number": txn.source_reference_number,
            "amount": txn.amount,
            "date": txn.date,
            "account_number": txn.account_number,
            "currency": txn.ccy,
            "other_details": txn.otherDetails,
            "match_status": txn.match_status,
            "match_status_label": match_status_label,
            "match_rule_id": txn.match_rule_id,
            "match_rule_name": match_rule_name,
            "match_condition": txn.match_conditon,
            "reconciled_status": txn.reconciled_status,
            "comment": txn.comment,
            "created_at": txn.created_at.isoformat() if txn.created_at else None,
            "updated_at": txn.updated_at.isoformat() if txn.updated_at else None
        }
        
        # Get matched transactions (same reference_number, same channel, matched status)
        matched_transactions = []
        if txn.reference_number and txn.match_status in [1, 2]:
            matched_query = select(
                Transaction,
                SourceConfig.source_name
            ).outerjoin(
                SourceConfig, Transaction.source_id == SourceConfig.id
            ).where(
                and_(
                    Transaction.reference_number == txn.reference_number,
                    Transaction.channel_id == txn.channel_id,
                    Transaction.match_status.in_([1, 2]),
                    Transaction.id != transaction_id  # Exclude the main transaction
                )
            ).order_by(Transaction.source_id)
            
            matched_result = await db.execute(matched_query)
            matched_rows = matched_result.all()
            
            for matched_row in matched_rows:
                matched_txn = matched_row[0]
                matched_source_name = matched_row[1]
                
                matched_transactions.append({
                    "id": matched_txn.id,
                    "source_id": matched_txn.source_id,
                    "source_name": matched_source_name,
                    "reference_number": matched_txn.reference_number,
                    "amount": matched_txn.amount,
                    "date": matched_txn.date,
                    "account_number": matched_txn.account_number,
                    "currency": matched_txn.ccy
                })
        
        return {
            "status": "success",
            "error": False,
            "message": "Transaction details retrieved successfully",
            "data": {
                "transaction": transaction_data,
                "matched_transactions": matched_transactions,
                "match_group_id": f"{txn.reference_number}-{txn.channel_id}" if txn.reference_number else None
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        return {
            "status": "error",
            "error": True,
            "message": str(e),
            "data": {}
        }

@router.get("/all-manual")
async def get_all_manual_transactions(
    db = Depends(get_db)
):
    return await TransactionService.getInvestigatedTxnIds(db)
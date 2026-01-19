"""
Transaction Search Service
"""
from sqlalchemy import select, func, cast, Float, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from typing import Optional
import math

from app.db.models.transactions import Transaction
from app.db.models.source_config import SourceConfig
from app.schemas.transaction_search_schemas import (
    TransactionSearchRequest,
    TransactionSearchResponse,
    TransactionSearchItem
)


class TransactionSearchService:
    """Service for searching transactions with specific fields"""

    @staticmethod
    async def search_transactions(
        search_params: TransactionSearchRequest,
        db: AsyncSession
    ) -> TransactionSearchResponse:
        """
        Search transactions based on reference number, account number, date range, amount range, and source
        
        Args:
            search_params: Search parameters including filters and pagination
            db: Database session
            
        Returns:
            TransactionSearchResponse with matching records and pagination info
        """
        # Build base query with join to source table
        query = select(Transaction, SourceConfig.source_name).outerjoin(
            SourceConfig,
            Transaction.source_id == SourceConfig.id
        )
        
        # Apply filters
        filters = []
        
        # Reference number filter (partial match, case-insensitive)
        if search_params.reference_number:
            filters.append(
                Transaction.reference_number.ilike(f"%{search_params.reference_number}%")
            )
        
        # Account number filter (partial match, case-insensitive)
        if search_params.account_number:
            filters.append(
                Transaction.account_number.ilike(f"%{search_params.account_number}%")
            )
        
        # Date range filter
        if search_params.date_from:
            filters.append(Transaction.date >= search_params.date_from)
        
        if search_params.date_to:
            filters.append(Transaction.date <= search_params.date_to)
        
        # Amount filter (exact match)
        if search_params.amount is not None:
            filters.append(Transaction.amount == search_params.amount)
        
        # Source ID filter
        if search_params.source_id is not None:
            filters.append(Transaction.source_id == search_params.source_id)
        
        # Apply all filters to query
        if filters:
            query = query.where(*filters)
        
        # Get total count
        count_query = select(func.count()).select_from(Transaction)
        if filters:
            count_query = count_query.where(*filters)
        
        result = await db.execute(count_query)
        total = result.scalar_one()
        
        # Calculate pagination
        total_pages = math.ceil(total / search_params.page_size) if total > 0 else 0
        offset = (search_params.page - 1) * search_params.page_size
        
        # Apply sorting (default: most recent first)
        query = query.order_by(Transaction.created_at.desc())
        
        # Apply pagination
        query = query.offset(offset).limit(search_params.page_size)
        
        # Execute query
        result = await db.execute(query)
        rows = result.all()
        
        # Transform results
        data = []
        for row in rows:
            transaction = row[0]  # Transaction object
            source_name = row[1]  # Source name from join
            
            item = TransactionSearchItem(
                id=transaction.id,
                reference_number=transaction.reference_number,
                account_number=transaction.account_number,
                date=transaction.date,
                amount=transaction.amount,
                source_id=transaction.source_id,
                source_name=source_name,
                txn_id=transaction.txn_id,
                recon_reference_number=transaction.recon_reference_number,
                channel_id=transaction.channel_id,
                reconciled_status=transaction.reconciled_status,
                match_status=transaction.match_status,
                created_at=transaction.created_at
            )
            data.append(item)
        
        return TransactionSearchResponse(
            data=data,
            total=total,
            page=search_params.page,
            page_size=search_params.page_size,
            total_pages=total_pages
        )

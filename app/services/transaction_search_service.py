"""
Transaction Search Service
"""
from sqlalchemy import select, func, cast, Float, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from typing import Optional, List, Dict, Any
from collections import defaultdict
import math
import json

from app.db.models.transactions import Transaction
from app.db.models.source_config import SourceConfig
from app.db.models.channel_config import ChannelConfig
from app.db.models.matching_rule_config import MatchingRuleConfig
from app.schemas.transaction_search_schemas import (
    TransactionSearchRequest,
    TransactionSearchResponse,
    TransactionDetailItem,
    SmartSearchRequest,
    GroupedTransactions,
    PaginationMeta,
    SummaryStats
)
from app.utils.smart_search_detector import SmartSearchDetector


class TransactionSearchService:
    """Service for searching transactions with specific fields"""

    @staticmethod
    def _get_match_status_label(match_status: Optional[int]) -> str:
        """Convert match_status integer to label"""
        if match_status == 1:
            return "Matched"
        elif match_status == 2:
            return "Partial"
        elif match_status == 0:
            return "Unmatched"
        return "Unknown"

    @staticmethod
    def _transform_transaction(
        transaction: Transaction, 
        source_name: Optional[str],
        channel_name: Optional[str],
        rule_name: Optional[str]
    ) -> TransactionDetailItem:
        """Transform Transaction model to TransactionDetailItem schema"""
        match_status_label = TransactionSearchService._get_match_status_label(transaction.match_status)
        
        # Build match condition description
        match_condition = None
        if transaction.match_status == 1 and transaction.match_rule_id:
            match_condition = f"Matched by rule {transaction.match_rule_id}"
            if rule_name:
                match_condition += f" ({rule_name})"
            if transaction.match_conditon:
                match_condition += f" - {transaction.match_conditon}"
        
        return TransactionDetailItem(
            id=transaction.id,
            recon_reference_number=transaction.recon_reference_number,
            channel_id=transaction.channel_id,
            channel_name=channel_name,
            source_id=transaction.source_id,
            source_name=source_name,
            reference_number=transaction.reference_number,
            source_reference_number=transaction.source_reference_number,
            amount=transaction.amount,
            date=transaction.date,
            account_number=transaction.account_number,
            currency=transaction.ccy,
            match_status=transaction.match_status,
            other_details=transaction.otherDetails,
            match_status_label=match_status_label,
            match_rule_id=transaction.match_rule_id,
            match_rule_name=rule_name,
            match_condition=match_condition,
            reconciled_status=transaction.reconciled_status,
            reconciled_by=transaction.reconciled_by,
            comment=transaction.comment,
            created_at=transaction.created_at.isoformat() if transaction.created_at else None,
            updated_at=transaction.updated_at.isoformat() if transaction.updated_at else None
        )

    @staticmethod
    def _group_transactions_by_recon_ref(
        transactions: List[tuple]
    ) -> List[GroupedTransactions]:
        """
        Group transactions by recon_reference_number and source type
        
        Args:
            transactions: List of tuples (Transaction, source_name, channel_name, rule_name)
            
        Returns:
            List of GroupedTransactions
        """
        # Group by recon_reference_number
        grouped: Dict[str, Dict[str, List[TransactionDetailItem]]] = defaultdict(lambda: defaultdict(list))
        
        for row in transactions:
            transaction = row[0]
            source_name = row[1]
            channel_name = row[2]
            rule_name = row[3]
            
            # Skip if no recon_reference_number
            if not transaction.recon_reference_number:
                continue
            
            # Transform transaction
            item = TransactionSearchService._transform_transaction(
                transaction, source_name, channel_name, rule_name
            )
            
            # Determine source type key
            source_key = f"{source_name.lower()}_transactions" if source_name else "unknown_transactions"
            
            # Add to grouped structure
            grouped[transaction.recon_reference_number][source_key].append(item)
        
        # Convert to list of GroupedTransactions
        result = []
        for recon_ref, sources in grouped.items():
            group = GroupedTransactions(
                atm_transactions=sources.get('atm_transactions', []),
                switch_transactions=sources.get('switch_transactions', []),
                cbs_transactions=sources.get('cbs_transactions', []),
                network_transactions=sources.get('network_transactions', []),
                settlement_transactions=sources.get('settlement_transactions', []),
                ej_transactions=sources.get('ej_transactions', []),
                platform_transactions=sources.get('platform_transactions', []),
                pos_transactions=sources.get('pos_transactions', [])
            )
            result.append(group)
        
        return result

    @staticmethod
    def _calculate_summary(transactions: List[tuple]) -> SummaryStats:
        """Calculate summary statistics"""
        total_matched = 0
        total_partial = 0
        total_unmatched = 0
        
        for row in transactions:
            transaction = row[0]
            if transaction.match_status == 1:
                total_matched += 1
            elif transaction.match_status == 2:
                total_partial += 1
            elif transaction.match_status == 0:
                total_unmatched += 1
        
        return SummaryStats(
            total_matched=total_matched,
            total_partial=total_partial,
            total_unmatched=total_unmatched
        )

    @staticmethod
    async def smart_search(
        search_params: SmartSearchRequest,
        db: AsyncSession
    ) -> TransactionSearchResponse:
        """
        Smart search that auto-detects field types from comma-separated input
        
        Args:
            search_params: Smart search parameters with auto-detection
            db: Database session
            
        Returns:
            TransactionSearchResponse with matching records grouped by recon_reference_number
        """
        # Build base query with joins
        query = (
            select(
                Transaction,
                SourceConfig.source_name,
                ChannelConfig.channel_name,
                MatchingRuleConfig.rule_name
            )
            .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
            .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
            .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
        )
        
        # Apply filters
        filters = []
        
        # Parse search query if provided
        if search_params.search_query:
            categorized = SmartSearchDetector.parse_search_query(search_params.search_query)
            
            # Build OR conditions for each category
            or_conditions = []
            
            # Add amount filters
            if categorized['amounts']:
                for amount in categorized['amounts']:
                    or_conditions.append(Transaction.amount == amount)
            
            # Add account number filters (partial match)
            if categorized['account_numbers']:
                for account in categorized['account_numbers']:
                    or_conditions.append(Transaction.account_number.ilike(f"%{account}%"))
            
            # Add reference number filters (partial match)
            if categorized['reference_numbers']:
                for rrn in categorized['reference_numbers']:
                    or_conditions.append(Transaction.reference_number.ilike(f"%{rrn}%"))
            
            # Add unknown values as wildcard search across all text fields
            if categorized['unknown']:
                for value in categorized['unknown']:
                    or_conditions.extend([
                        Transaction.reference_number.ilike(f"%{value}%"),
                        Transaction.account_number.ilike(f"%{value}%"),
                        Transaction.txn_id.ilike(f"%{value}%")
                    ])
            
            # Combine all OR conditions
            if or_conditions:
                filters.append(or_(*or_conditions))
        
        # RRN list filter (multi-select from dropdown)
        if search_params.rrn_list:
            rrn_conditions = []
            for rrn in search_params.rrn_list:
                rrn_conditions.append(Transaction.reference_number.ilike(f"%{rrn}%"))
            if rrn_conditions:
                filters.append(or_(*rrn_conditions))
        
        # Date range filter
        if search_params.date_from:
            filters.append(Transaction.date >= search_params.date_from)
        
        if search_params.date_to:
            filters.append(Transaction.date <= search_params.date_to)
        
        # Source ID filter
        if search_params.source_id is not None:
            filters.append(Transaction.source_id == search_params.source_id)
        
        # Apply all filters to query
        if filters:
            query = query.where(*filters)
        
        # Get total count of unique recon_reference_numbers
        count_query = (
            select(func.count(func.distinct(Transaction.recon_reference_number)))
            .select_from(Transaction)
        )
        if filters:
            count_query = count_query.where(*filters)
        
        result = await db.execute(count_query)
        total_groups = result.scalar_one()
        
        # Calculate pagination for groups
        total_pages = math.ceil(total_groups / search_params.page_size) if total_groups > 0 else 0
        offset = (search_params.page - 1) * search_params.page_size
        
        # Get distinct recon_reference_numbers with pagination
        recon_refs_query = (
            select(func.distinct(Transaction.recon_reference_number))
            .select_from(Transaction)
        )
        if filters:
            recon_refs_query = recon_refs_query.where(*filters)
        
        recon_refs_query = (
            recon_refs_query
            .order_by(Transaction.recon_reference_number.desc())
            .offset(offset)
            .limit(search_params.page_size)
        )
        
        result = await db.execute(recon_refs_query)
        paginated_recon_refs = [row[0] for row in result.all()]
        
        # Fetch all transactions for the paginated recon_reference_numbers
        if paginated_recon_refs:
            transactions_query = (
                select(
                    Transaction,
                    SourceConfig.source_name,
                    ChannelConfig.channel_name,
                    MatchingRuleConfig.rule_name
                )
                .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
                .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
                .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
                .where(Transaction.recon_reference_number.in_(paginated_recon_refs))
                .order_by(Transaction.recon_reference_number.desc(), Transaction.source_id)
            )
            
            result = await db.execute(transactions_query)
            all_transactions = result.all()
            
            # Group transactions
            grouped_transactions = TransactionSearchService._group_transactions_by_recon_ref(all_transactions)
            
            # Calculate summary from all matching transactions (not just paginated)
            summary_query = (
                select(
                    Transaction,
                    SourceConfig.source_name,
                    ChannelConfig.channel_name,
                    MatchingRuleConfig.rule_name
                )
                .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
                .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
                .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
            )
            if filters:
                summary_query = summary_query.where(*filters)
            
            result = await db.execute(summary_query)
            all_matching_transactions = result.all()
            summary = TransactionSearchService._calculate_summary(all_matching_transactions)
        else:
            grouped_transactions = []
            summary = SummaryStats()
        
        # Build pagination metadata
        pagination = PaginationMeta(
            page=search_params.page,
            page_size=search_params.page_size,
            total_records=total_groups,
            total_pages=total_pages,
            has_next=search_params.page < total_pages,
            has_previous=search_params.page > 1
        )
        
        return TransactionSearchResponse(
            transactions=grouped_transactions,
            pagination=pagination,
            summary=summary
        )

    @staticmethod
    async def search_transactions(
        search_params: TransactionSearchRequest,
        db: AsyncSession
    ) -> TransactionSearchResponse:
        """
        Search transactions based on reference number, account number, date range, amount, and source
        
        Args:
            search_params: Search parameters including filters and pagination
            db: Database session
            
        Returns:
            TransactionSearchResponse with matching records grouped by recon_reference_number
        """
        # Build base query with joins
        query = (
            select(
                Transaction,
                SourceConfig.source_name,
                ChannelConfig.channel_name,
                MatchingRuleConfig.rule_name
            )
            .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
            .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
            .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
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
        
        # Get total count of unique recon_reference_numbers
        count_query = (
            select(func.count(func.distinct(Transaction.recon_reference_number)))
            .select_from(Transaction)
        )
        if filters:
            count_query = count_query.where(*filters)
        
        result = await db.execute(count_query)
        total_groups = result.scalar_one()
        
        # Calculate pagination for groups
        total_pages = math.ceil(total_groups / search_params.page_size) if total_groups > 0 else 0
        offset = (search_params.page - 1) * search_params.page_size
        
        # Get distinct recon_reference_numbers with pagination
        recon_refs_query = (
            select(func.distinct(Transaction.recon_reference_number))
            .select_from(Transaction)
        )
        if filters:
            recon_refs_query = recon_refs_query.where(*filters)
        
        recon_refs_query = (
            recon_refs_query
            .order_by(Transaction.recon_reference_number.desc())
            .offset(offset)
            .limit(search_params.page_size)
        )
        
        result = await db.execute(recon_refs_query)
        paginated_recon_refs = [row[0] for row in result.all()]
        
        # Fetch all transactions for the paginated recon_reference_numbers
        if paginated_recon_refs:
            transactions_query = (
                select(
                    Transaction,
                    SourceConfig.source_name,
                    ChannelConfig.channel_name,
                    MatchingRuleConfig.rule_name
                )
                .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
                .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
                .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
                .where(Transaction.recon_reference_number.in_(paginated_recon_refs))
                .order_by(Transaction.recon_reference_number.desc(), Transaction.source_id)
            )
            
            result = await db.execute(transactions_query)
            all_transactions = result.all()
            
            # Group transactions
            grouped_transactions = TransactionSearchService._group_transactions_by_recon_ref(all_transactions)
            
            # Calculate summary from all matching transactions (not just paginated)
            summary_query = (
                select(
                    Transaction,
                    SourceConfig.source_name,
                    ChannelConfig.channel_name,
                    MatchingRuleConfig.rule_name
                )
                .outerjoin(SourceConfig, Transaction.source_id == SourceConfig.id)
                .outerjoin(ChannelConfig, Transaction.channel_id == ChannelConfig.id)
                .outerjoin(MatchingRuleConfig, Transaction.match_rule_id == MatchingRuleConfig.id)
            )
            if filters:
                summary_query = summary_query.where(*filters)
            
            result = await db.execute(summary_query)
            all_matching_transactions = result.all()
            summary = TransactionSearchService._calculate_summary(all_matching_transactions)
        else:
            grouped_transactions = []
            summary = SummaryStats()
        
        # Build pagination metadata
        pagination = PaginationMeta(
            page=search_params.page,
            page_size=search_params.page_size,
            total_records=total_groups,
            total_pages=total_pages,
            has_next=search_params.page < total_pages,
            has_previous=search_params.page > 1
        )
        
        return TransactionSearchResponse(
            transactions=grouped_transactions,
            pagination=pagination,
            summary=summary
        )

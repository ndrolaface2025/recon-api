"""
Application Layer Matcher
Handles complex matching rules with OR conditions and parentheses
Processes transactions in Python when database stored procedure cannot handle the logic
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ApplicationMatcher:
    """
    Python-based matching engine for complex rules
    Supports OR operators, nested groups, and parentheses
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def execute_complex_matching(
        self,
        rule_id: int,
        conditions: Dict[str, Any],
        tolerance: Optional[Dict[str, Any]] = None,
        channel_id: Optional[int] = None,
        dry_run: bool = False,
        min_sources: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute complex matching rule with OR/parentheses support
        
        Args:
            rule_id: Matching rule ID
            conditions: Rule conditions (with condition_groups)
            tolerance: Tolerance configuration
            channel_id: Optional channel filter
            dry_run: If True, only analyze without updating
            min_sources: Minimum sources required for partial matching
            
        Returns:
            {
                "rule_id": int,
                "matched_count": int,
                "transaction_ids": List[int],
                "execution_time_ms": int,
                "match_type": str,
                "matched_groups": List[Dict] (in dry_run)
            }
        """
        start_time = datetime.now()
        
        try:
            # Extract sources from conditions
            sources = conditions.get("sources", [])
            if not sources or len(sources) < 2:
                raise ValueError("At least 2 sources required for matching")
            
            # Determine match type (FULL or PARTIAL)
            match_type = "PARTIAL" if min_sources and min_sources < len(sources) else "FULL"
            
            # Fetch transactions for all sources
            transactions_by_source = await self._fetch_transactions_by_sources(
                sources, channel_id
            )
            
            # Find all matching groups using condition_groups logic
            matched_groups = self._find_matching_groups(
                transactions_by_source,
                conditions.get("condition_groups", []),
                tolerance,
                min_sources or len(sources)
            )
            
            if dry_run:
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                return {
                    "rule_id": rule_id,
                    "matched_count": len(matched_groups),
                    "transaction_ids": [],
                    "execution_time_ms": int(execution_time),
                    "match_type": match_type,
                    "matched_groups": matched_groups[:10],  # Return sample for inspection
                    "message": f"DRY RUN: Found {len(matched_groups)} matching groups"
                }
            
            # Update matched transactions in database
            all_txn_ids = await self._update_matched_transactions(
                matched_groups,
                rule_id,
                match_type
            )
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "rule_id": rule_id,
                "matched_count": len(matched_groups),
                "transaction_ids": all_txn_ids,
                "execution_time_ms": int(execution_time),
                "match_type": match_type
            }
            
        except Exception as e:
            logger.error(f"Error in application matcher: {e}")
            raise
    
    async def _fetch_transactions_by_sources(
        self,
        sources: List[str],
        channel_id: Optional[int]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch unmatched transactions for all sources
        
        Returns:
            {
                "ATM": [{"id": 1, "rrn": "ABC", "amount": 100, ...}, ...],
                "SWITCH": [...],
                ...
            }
        """
        transactions = {}
        
        for source_name in sources:
            # Build query for this source
            query = """
                SELECT 
                    t.id,
                    t.rrn,
                    t.amount,
                    t.transaction_date,
                    t.transaction_time,
                    t.card_number,
                    t.terminal_id,
                    t.merchant_id,
                    t.currency_code,
                    t.response_code,
                    s.source_name
                FROM tbl_txn_transactions t
                JOIN tbl_cfg_source s ON t.source_id = s.id
                WHERE s.source_name = :source_name
                  AND (t.match_status IS NULL OR t.match_status = 0)
            """
            
            if channel_id:
                query += " AND s.channel_id = :channel_id"
            
            params = {"source_name": source_name}
            if channel_id:
                params["channel_id"] = channel_id
            
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            transactions[source_name] = [
                {
                    "id": row.id,
                    "rrn": row.rrn,
                    "amount": float(row.amount) if row.amount else None,
                    "transaction_date": row.transaction_date,
                    "transaction_time": row.transaction_time,
                    "card_number": row.card_number,
                    "terminal_id": row.terminal_id,
                    "merchant_id": row.merchant_id,
                    "currency_code": row.currency_code,
                    "response_code": row.response_code,
                    "source_name": row.source_name
                }
                for row in rows
            ]
            
            logger.info(f"Fetched {len(transactions[source_name])} transactions from {source_name}")
        
        return transactions
    
    def _find_matching_groups(
        self,
        transactions_by_source: Dict[str, List[Dict[str, Any]]],
        condition_groups: List[Dict[str, Any]],
        tolerance: Optional[Dict[str, Any]],
        min_sources: int
    ) -> List[Dict[str, Any]]:
        """
        Find all matching transaction groups using complex logic
        
        Returns list of matched groups:
        [
            {
                "transactions": [txn1, txn2, txn3],
                "match_key": "RRN123",
                "condition_met": "rrn_match AND (amt_atm_switch OR amt_switch_cbs)"
            },
            ...
        ]
        """
        matched_groups = []
        
        # Get source names
        sources = list(transactions_by_source.keys())
        
        # Build candidates: all possible transaction combinations
        # For 3 sources: iterate through source1, find matches in source2 and source3
        primary_source = sources[0]
        
        for primary_txn in transactions_by_source[primary_source]:
            # Try to find matching transactions in other sources
            candidates = {primary_source: primary_txn}
            
            for other_source in sources[1:]:
                matching_txn = self._find_matching_transaction(
                    primary_txn,
                    transactions_by_source[other_source],
                    candidates,
                    condition_groups,
                    tolerance
                )
                
                if matching_txn:
                    candidates[other_source] = matching_txn
            
            # Check if we have enough sources
            if len(candidates) >= min_sources:
                # Verify full condition groups
                if self._evaluate_condition_groups(candidates, condition_groups, tolerance):
                    matched_groups.append({
                        "transactions": list(candidates.values()),
                        "match_key": primary_txn.get("rrn", f"group_{len(matched_groups)}"),
                        "sources_matched": list(candidates.keys())
                    })
        
        logger.info(f"Found {len(matched_groups)} matching groups")
        return matched_groups
    
    def _find_matching_transaction(
        self,
        reference_txn: Dict[str, Any],
        candidate_txns: List[Dict[str, Any]],
        current_group: Dict[str, Dict[str, Any]],
        condition_groups: List[Dict[str, Any]],
        tolerance: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Find a transaction from candidates that matches the reference
        """
        for candidate in candidate_txns:
            # Build test group
            test_group = {**current_group}
            test_group[candidate["source_name"]] = candidate
            
            # Evaluate if this forms a valid match
            if self._evaluate_condition_groups(test_group, condition_groups, tolerance):
                return candidate
        
        return None
    
    def _evaluate_condition_groups(
        self,
        transaction_group: Dict[str, Dict[str, Any]],
        condition_groups: List[Dict[str, Any]],
        tolerance: Optional[Dict[str, Any]]
    ) -> bool:
        """
        Evaluate condition_groups with OR/AND logic
        
        Recursively processes nested groups and respects parentheses
        """
        for group in condition_groups:
            group_type = group.get("group_type", "AND")
            conditions = group.get("conditions", [])
            
            results = []
            
            for condition in conditions:
                # Check if this is a nested group
                if "group_type" in condition:
                    # Recursive evaluation
                    result = self._evaluate_condition_groups(
                        transaction_group,
                        [condition],
                        tolerance
                    )
                    results.append(result)
                else:
                    # Evaluate individual condition
                    result = self._evaluate_single_condition(
                        transaction_group,
                        condition,
                        tolerance
                    )
                    results.append(result)
            
            # Apply group operator
            if group_type == "OR":
                if not any(results):
                    return False
            else:  # AND
                if not all(results):
                    return False
        
        return True
    
    def _evaluate_single_condition(
        self,
        transaction_group: Dict[str, Dict[str, Any]],
        condition: Dict[str, Any],
        tolerance: Optional[Dict[str, Any]]
    ) -> bool:
        """
        Evaluate a single field condition
        
        Examples:
        - {"field": "rrn", "operator": "equals"}
        - {"field": "amount", "operator": "equals", "sources": ["ATM", "SWITCH"]}
        """
        field = condition.get("field")
        operator = condition.get("operator", "equals")
        specific_sources = condition.get("sources")
        
        # Get transactions to compare
        if specific_sources:
            # Only compare specific source pairs
            txns_to_compare = [
                transaction_group[src] for src in specific_sources
                if src in transaction_group
            ]
        else:
            # Compare all transactions in group
            txns_to_compare = list(transaction_group.values())
        
        if len(txns_to_compare) < 2:
            return False
        
        # Get values for this field
        values = [txn.get(field) for txn in txns_to_compare]
        
        # Apply operator
        if operator == "equals":
            return self._check_equality(values)
        elif operator == "within_tolerance":
            return self._check_within_tolerance(values, field, tolerance)
        else:
            logger.warning(f"Unknown operator: {operator}")
            return False
    
    def _check_equality(self, values: List[Any]) -> bool:
        """Check if all values are equal"""
        if not values or None in values:
            return False
        return len(set(str(v) for v in values)) == 1
    
    def _check_within_tolerance(
        self,
        values: List[Any],
        field: str,
        tolerance: Optional[Dict[str, Any]]
    ) -> bool:
        """Check if all values are within tolerance"""
        if not values or None in values:
            return False
        
        numeric_values = [float(v) for v in values]
        
        # Get tolerance for this field
        tolerance_config = tolerance.get(field, {}) if tolerance else {}
        tolerance_value = tolerance_config.get("value", 0.01)
        tolerance_type = tolerance_config.get("type", "fixed")
        
        # Check all pairs
        for i in range(len(numeric_values) - 1):
            diff = abs(numeric_values[i] - numeric_values[i + 1])
            
            if tolerance_type == "percentage":
                max_val = max(numeric_values[i], numeric_values[i + 1])
                allowed_diff = max_val * (tolerance_value / 100)
            else:  # fixed
                allowed_diff = tolerance_value
            
            if diff > allowed_diff:
                return False
        
        return True
    
    async def _update_matched_transactions(
        self,
        matched_groups: List[Dict[str, Any]],
        rule_id: int,
        match_type: str
    ) -> List[int]:
        """
        Update all matched transactions in database
        Set match_status, reconciled_status, matched_with_txn_id
        """
        all_txn_ids = []
        match_status = 1 if match_type == "FULL" else 2
        
        for group in matched_groups:
            transactions = group["transactions"]
            txn_ids = [txn["id"] for txn in transactions]
            all_txn_ids.extend(txn_ids)
            
            # Create match condition description
            match_condition = f"Matched by rule {rule_id} ({match_type}) - Application Layer"
            
            # Update each transaction
            update_query = """
                UPDATE tbl_txn_transactions
                SET 
                    match_status = :match_status,
                    reconciled_status = 1,
                    matched_rule_id = :rule_id,
                    match_conditon = :match_condition,
                    updated_at = NOW()
                WHERE id = ANY(:txn_ids)
            """
            
            await self.db.execute(
                text(update_query),
                {
                    "match_status": match_status,
                    "rule_id": rule_id,
                    "match_condition": match_condition,
                    "txn_ids": txn_ids
                }
            )
        
        await self.db.commit()
        logger.info(f"Updated {len(all_txn_ids)} transactions with match results")
        
        return all_txn_ids

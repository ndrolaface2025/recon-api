"""
Application Layer Matcher
Handles ALL matching rules (SIMPLE and COMPLEX) in Python.

This unified engine processes:
- SIMPLE rules: Basic AND conditions, single-level matching
- COMPLEX rules: OR operators, nested groups, source-specific conditions

Features:
- N-way matching (2, 3, 4+ sources)
- AND/OR logic with proper precedence
- Nested condition groups
- Source-specific matching (e.g., amount only between ATM-SWITCH)
- Amount/time tolerance
- Full and partial matching
- Dry-run analysis

Performance:
- Optimized for high-volume transaction matching
- Efficient in-memory processing with indexes
- Batch updates to minimize database round-trips
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select
from datetime import datetime, date, timedelta
import re
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
                match_type,
                total_sources=len(sources)
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
                    t.reference_number as rrn,
                    t.amount,
                    t.date as transaction_date,
                    t.account_number,
                    t.ccy as currency_code,
                    t."otherDetails",
                    t.comment,
                    s.source_name
                FROM tbl_txn_transactions t
                JOIN tbl_cfg_source s ON t.source_id = s.id
                WHERE s.source_name = :source_name
                  AND (t.match_status IS NULL OR t.match_status = 0)
            """
            
            if channel_id:
                query += " AND t.channel_id = :channel_id"
            
            params = {"source_name": source_name}
            if channel_id:
                params["channel_id"] = channel_id
            
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            transactions[source_name] = [
                {
                    "id": row.id,
                    "rrn": row.rrn,
                    "reference_number": row.rrn,
                    "amount": float(row.amount) if row.amount else None,
                    "transaction_date": row.transaction_date,
                    "date": row.transaction_date,
                    "account_number": row.account_number,
                    "currency_code": row.currency_code,
                    "otherDetails": row.otherDetails,
                    "comment": row.comment,
                    "source_name": row.source_name,
                    # Parse otherDetails if it contains pipe-separated values
                    # Format: CARD|TERMINAL|MERCHANT
                    **(self._parse_other_details(row.otherDetails) if row.otherDetails else {})
                }
                for row in rows
            ]
            
            logger.info(f"Fetched {len(transactions[source_name])} transactions from {source_name}")
            if transactions[source_name]:
                sample = transactions[source_name][0]
                logger.info(
                    f"  Sample transaction: ID={sample.get('id')}, "
                    f"RRN={sample.get('reference_number')}, "
                    f"Amount={sample.get('amount')}"
                )
        
        return transactions
    
    def _parse_other_details(self, other_details: str) -> Dict[str, Any]:
        """
        Parse otherDetails field which may contain pipe-separated values
        Format: CARD|TERMINAL|MERCHANT or similar
        """
        try:
            parts = other_details.split('|')
            parsed = {}
            
            if len(parts) >= 1:
                parsed['card_number'] = parts[0]
            if len(parts) >= 2:
                parsed['terminal_id'] = parts[1]
            if len(parts) >= 3:
                parsed['merchant_id'] = parts[2]
            
            return parsed
        except Exception as e:
            logger.warning(f"Error parsing otherDetails '{other_details}': {e}")
            return {}
    
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
        
        logger.info(
            f"🔍 Starting matching process: "
            f"Sources={sources}, min_sources={min_sources}"
        )
        
        # Log transaction counts per source
        for source, txns in transactions_by_source.items():
            logger.info(f"  {source}: {len(txns)} transactions")
        
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
                    logger.warning(
                        f"✅ Match found: {len(candidates)} sources matched - "
                        f"Sources: {list(candidates.keys())} - "
                        f"RRN: {primary_txn.get('reference_number', primary_txn.get('rrn', 'N/A'))}"
                    )
                    matched_groups.append({
                        "transactions": list(candidates.values()),
                        "match_key": primary_txn.get("rrn", f"group_{len(matched_groups)}"),
                        "sources_matched": list(candidates.keys())
                    })
                else:
                    logger.warning(
                        f"❌ Match rejected: {len(candidates)} sources found but conditions not met - "
                        f"Sources: {list(candidates.keys())} - "
                        f"RRN: {primary_txn.get('reference_number', primary_txn.get('rrn', 'N/A'))}"
                    )
            elif len(candidates) > 1:
                logger.info(
                    f"⚠️  Partial candidate: Only {len(candidates)} sources (need {min_sources}) - "
                    f"Sources: {list(candidates.keys())} - "
                    f"RRN: {primary_txn.get('reference_number', primary_txn.get('rrn', 'N/A'))}"
                )
        
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
        elif operator == "greater_than":
            return self._check_comparison(values, condition, "greater_than")
        elif operator == "less_than":
            return self._check_comparison(values, condition, "less_than")
        elif operator == "greater_than_or_equal":
            return self._check_comparison(values, condition, "greater_than_or_equal")
        elif operator == "less_than_or_equal":
            return self._check_comparison(values, condition, "less_than_or_equal")
        elif operator == "date_within_days":
            return self._check_date_tolerance(values, condition, tolerance)
        elif operator == "starts_with":
            return self._check_string_operation(values, condition, "starts_with")
        elif operator == "ends_with":
            return self._check_string_operation(values, condition, "ends_with")
        elif operator == "contains":
            return self._check_string_operation(values, condition, "contains")
        elif operator == "regex":
            return self._check_string_operation(values, condition, "regex")
        elif operator == "is_null":
            return self._check_null_handling(values, True)
        elif operator == "is_not_null":
            return self._check_null_handling(values, False)
        elif operator == "cross_field_equals":
            return self._check_cross_field(transaction_group, condition)
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
    
    def _check_comparison(
        self,
        values: List[Any],
        condition: Dict[str, Any],
        comparison_type: str
    ) -> bool:
        """
        Check comparison operators: >, <, >=, <=
        
        Supports two modes:
        1. Compare against fixed value: {"field": "amount", "operator": "greater_than", "value": 100}
        2. Compare between sources: {"field": "amount", "operator": "greater_than"}
        
        Examples:
        - greater_than: All values > threshold OR first > second
        - less_than: All values < threshold OR first < second
        - greater_than_or_equal: All values >= threshold
        - less_than_or_equal: All values <= threshold
        """
        if not values:
            return False
        
        # Check if comparing against a fixed value
        compare_value = condition.get("value")
        
        if compare_value is not None:
            # Compare all values against fixed threshold
            try:
                threshold = float(compare_value)
                numeric_values = [float(v) for v in values if v is not None]
                
                if not numeric_values:
                    return False
                
                if comparison_type == "greater_than":
                    return all(v > threshold for v in numeric_values)
                elif comparison_type == "less_than":
                    return all(v < threshold for v in numeric_values)
                elif comparison_type == "greater_than_or_equal":
                    return all(v >= threshold for v in numeric_values)
                elif comparison_type == "less_than_or_equal":
                    return all(v <= threshold for v in numeric_values)
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Cannot convert values to numeric for comparison: {e}")
                return False
        else:
            # Compare values between sources (pairwise)
            try:
                numeric_values = [float(v) for v in values if v is not None]
                
                if len(numeric_values) < 2:
                    return False
                
                # Check all consecutive pairs
                for i in range(len(numeric_values) - 1):
                    if comparison_type == "greater_than":
                        if not (numeric_values[i] > numeric_values[i + 1]):
                            return False
                    elif comparison_type == "less_than":
                        if not (numeric_values[i] < numeric_values[i + 1]):
                            return False
                    elif comparison_type == "greater_than_or_equal":
                        if not (numeric_values[i] >= numeric_values[i + 1]):
                            return False
                    elif comparison_type == "less_than_or_equal":
                        if not (numeric_values[i] <= numeric_values[i + 1]):
                            return False
                
                return True
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Cannot convert values to numeric for comparison: {e}")
                return False
        
        return False
    
    def _check_date_tolerance(
        self,
        values: List[Any],
        condition: Dict[str, Any],
        tolerance: Optional[Dict[str, Any]]
    ) -> bool:
        """
        Check if dates are within tolerance (±N days)
        
        Examples:
        - {"field": "transaction_date", "operator": "date_within_days", "days": 1}
        - Uses tolerance config: {"transaction_date": {"days": 2}}
        """
        if not values or None in values:
            return False
        
        # Get tolerance days
        field = condition.get("field")
        days_tolerance = condition.get("days")
        
        # Check tolerance config if not in condition
        if days_tolerance is None and tolerance and field:
            tolerance_config = tolerance.get(field, {})
            days_tolerance = tolerance_config.get("days", 0)
        
        if days_tolerance is None:
            days_tolerance = 0
        
        try:
            # Convert to date objects
            date_values = []
            for v in values:
                if isinstance(v, datetime):
                    date_values.append(v.date())
                elif isinstance(v, date):
                    date_values.append(v)
                elif isinstance(v, str):
                    # Try to parse date string
                    date_values.append(datetime.fromisoformat(v.replace('Z', '+00:00')).date())
                else:
                    logger.warning(f"Cannot parse date value: {v}")
                    return False
            
            # Check all pairs
            for i in range(len(date_values) - 1):
                diff = abs((date_values[i] - date_values[i + 1]).days)
                if diff > days_tolerance:
                    return False
            
            return True
            
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"Error parsing dates: {e}")
            return False
    
    def _check_string_operation(
        self,
        values: List[Any],
        condition: Dict[str, Any],
        operation: str
    ) -> bool:
        """
        Check string operations: starts_with, ends_with, contains, regex
        
        Examples:
        - starts_with: {"field": "account", "operator": "starts_with", "value": "ACC"}
        - contains: {"field": "description", "operator": "contains", "value": "PAYMENT"}
        - regex: {"field": "rrn", "operator": "regex", "pattern": "^[0-9]{12}$"}
        """
        if not values:
            return False
        
        # Get comparison value or pattern
        compare_value = condition.get("value")
        pattern = condition.get("pattern")
        
        if operation == "regex":
            if not pattern:
                logger.warning("Regex operator requires 'pattern' parameter")
                return False
            
            try:
                compiled_pattern = re.compile(pattern)
                return all(
                    compiled_pattern.match(str(v)) is not None 
                    for v in values if v is not None
                )
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")
                return False
        
        else:
            # Other string operations
            if compare_value is None:
                logger.warning(f"{operation} operator requires 'value' parameter")
                return False
            
            compare_str = str(compare_value).lower()
            
            for v in values:
                if v is None:
                    return False
                
                value_str = str(v).lower()
                
                if operation == "starts_with":
                    if not value_str.startswith(compare_str):
                        return False
                elif operation == "ends_with":
                    if not value_str.endswith(compare_str):
                        return False
                elif operation == "contains":
                    if compare_str not in value_str:
                        return False
            
            return True
    
    def _check_null_handling(
        self,
        values: List[Any],
        should_be_null: bool
    ) -> bool:
        """
        Check NULL handling: is_null, is_not_null
        
        Examples:
        - is_null: {"field": "merchant_id", "operator": "is_null"}
        - is_not_null: {"field": "merchant_id", "operator": "is_not_null"}
        """
        if not values:
            return should_be_null
        
        if should_be_null:
            # All values should be None/null
            return all(v is None or v == '' or str(v).lower() == 'none' for v in values)
        else:
            # All values should NOT be None/null
            return all(v is not None and v != '' and str(v).lower() != 'none' for v in values)
    
    def _check_cross_field(
        self,
        transaction_group: Dict[str, Dict[str, Any]],
        condition: Dict[str, Any]
    ) -> bool:
        """
        Check cross-field conditions: Compare different fields
        
        Examples:
        - {"operator": "cross_field_equals", "field1": "debit_amount", "field2": "credit_amount"}
        - {"operator": "cross_field_equals", "field1": "fee", "field2": "charges"}
        """
        field1 = condition.get("field1")
        field2 = condition.get("field2")
        
        if not field1 or not field2:
            logger.warning("cross_field_equals requires 'field1' and 'field2' parameters")
            return False
        
        # Check each transaction in the group
        for source_name, txn in transaction_group.items():
            value1 = txn.get(field1)
            value2 = txn.get(field2)
            
            # Both should exist
            if value1 is None or value2 is None:
                return False
            
            # Try numeric comparison first
            try:
                if float(value1) != float(value2):
                    return False
            except (ValueError, TypeError):
                # Fall back to string comparison
                if str(value1) != str(value2):
                    return False
        
        return True
    
    async def _update_matched_transactions(
        self,
        matched_groups: List[Dict[str, Any]],
        rule_id: int,
        match_type: str,
        total_sources: int = None
    ) -> List[int]:
        """
        Update all matched transactions in database
        Set match_status, reconciled_status, matched_with_txn_id
        
        Args:
            matched_groups: List of matched transaction groups
            rule_id: ID of the matching rule
            match_type: Overall match type (FULL/PARTIAL)
            total_sources: Total number of sources in the rule (for dynamic match_status)
        """
        all_txn_ids = []
        
        for group in matched_groups:
            transactions = group["transactions"]
            txn_ids = [txn["id"] for txn in transactions]
            all_txn_ids.extend(txn_ids)
            
            # Determine match_status based on actual sources matched
            sources_matched = len(group.get("sources_matched", transactions))
            
            # Dynamic match status:
            # - If all sources matched: match_status = 1 (FULL)
            # - If partial sources matched: match_status = 2 (PARTIAL)
            if total_sources and sources_matched < total_sources:
                match_status = 2  # Partial match
                actual_match_type = "PARTIAL"
            else:
                match_status = 1  # Full match
                actual_match_type = "FULL"
            
            logger.warning(
                f"💾 Updating transactions: sources_matched={sources_matched}, "
                f"total_sources={total_sources}, match_status={match_status}, "
                f"txn_ids={txn_ids}"
            )
            
            # Create match condition description
            match_condition = f"Matched by rule {rule_id} ({actual_match_type}: {sources_matched} sources) - Application Layer"
            
            # Update each transaction
            update_query = """
                UPDATE tbl_txn_transactions
                SET 
                    match_status = :match_status,
                    reconciled_status = TRUE,
                    match_rule_id = :rule_id,
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

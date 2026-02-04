# """
# Application Layer Matcher
# Handles ALL matching rules (SIMPLE and COMPLEX) in Python.

# This unified engine processes:
# - SIMPLE rules: Basic AND conditions, single-level matching
# - COMPLEX rules: OR operators, nested groups, source-specific conditions

# Features:
# - N-way matching (2, 3, 4+ sources)
# - AND/OR logic with proper precedence
# - Nested condition groups
# - Source-specific matching (e.g., amount only between ATM-SWITCH)
# - Amount/time tolerance
# - Full and partial matching
# - Dry-run analysis

# Performance:
# - Optimized for high-volume transaction matching
# - Efficient in-memory processing with indexes
# - Batch updates to minimize database round-trips
# """

# from typing import Dict, Any, List, Optional, Set, Tuple
# from sklearn import tree
# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy import text, select
# from datetime import datetime, date, timedelta
# import re
# import random
# import logging
# from types import SimpleNamespace
# from itertools import product
# import ast
# logger = logging.getLogger(__name__)


# class ApplicationMatcher:
#     """
#     Python-based matching engine for complex rules
#     Supports OR operators, nested groups, and parentheses
#     """
    
#     def __init__(self, db: AsyncSession):
#         self.db = db
    
#     async def execute_complex_matching(
#         self,
#         rule_id: int,
#         conditions: Dict[str, Any],
#         tolerance: Optional[Dict[str, Any]] = None,
#         channel_id: Optional[int] = None,
#         dry_run: bool = False,
#         min_sources: Optional[int] = None
#     ) -> Dict[str, Any]:
#         """
#         Execute complex matching rule with OR/parentheses support
        
#         Args:
#             rule_id: Matching rule ID
#             conditions: Rule conditions (with condition_groups)
#             tolerance: Tolerance configuration
#             channel_id: Optional channel filter
#             dry_run: If True, only analyze without updating
#             min_sources: Minimum sources required for partial matching
            
#         Returns:
#             {
#                 "rule_id": int,
#                 "matched_count": int,
#                 "transaction_ids": List[int],
#                 "execution_time_ms": int,
#                 "match_type": str,
#                 "matched_groups": List[Dict] (in dry_run)
#             }
#         """
#         start_time = datetime.now()
        
#         try:
#             # Extract sources from conditions
#             sources = conditions.get("sources", [])
#             if not sources or len(sources) < 2:
#                 raise ValueError("At least 2 sources required for matching")
            
#             # Determine match type (FULL or PARTIAL)
#             match_type = "PARTIAL" if min_sources and min_sources < len(sources) else "FULL"
            
#             # Fetch transactions for all sources
#             transactions_by_source = await self._fetch_transactions_by_sources(
#                 sources, channel_id
#             )
            
#             # Find all matching groups using condition_groups logic
#             matched_groups = self._find_matching_groups(
#                 transactions_by_source,
#                 conditions.get("logic_expression", []),
#                 tolerance,
#                 min_sources or len(sources)
#             )
            
#             if dry_run:
#                 execution_time = (datetime.now() - start_time).total_seconds() * 1000
#                 return {
#                     "rule_id": rule_id,
#                     "matched_count": len(matched_groups),
#                     "transaction_ids": [],
#                     "execution_time_ms": int(execution_time),
#                     "match_type": match_type,
#                     "matched_groups": matched_groups[:10],  # Return sample for inspection
#                     "message": f"DRY RUN: Found {len(matched_groups)} matching groups"
#                 }
            
#             # Update matched transactions in database
#             all_txn_ids = await self._update_matched_transactions(
#                 matched_groups,
#                 rule_id,
#                 match_type,
#                 total_sources=len(sources)
#             )
            
#             execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
#             return {
#                 "rule_id": rule_id,
#                 "matched_count": len(matched_groups),
#                 "transaction_ids": all_txn_ids,
#                 "execution_time_ms": int(execution_time),
#                 "match_type": match_type
#             }
            
#         except Exception as e:
#             logger.error(f"Error in application matcher: {e}")
#             raise
    
#     async def _fetch_transactions_by_sources(
#         self,
#         sources: List[str],
#         channel_id: Optional[int]
#     ) -> Dict[str, List[Dict[str, Any]]]:
#         """
#         Fetch unmatched transactions for all sources
        
#         Returns:
#             {
#                 "ATM": [{"id": 1, "rrn": "ABC", "amount": 100, ...}, ...],
#                 "SWITCH": [...],
#                 ...
#             }
#         """
#         transactions = {}
        
#         for source_name in sources:
#             # Build query for this source
#             query = """
#                 SELECT 
#                     t.id,
#                     t.reference_number as rrn,
#                     t.amount,
#                     t.date as transaction_date,
#                     t.account_number,
#                     t.ccy as currency_code,
#                     t."otherDetails",
#                     t.comment,
#                     s.source_name
#                 FROM tbl_txn_transactions t
#                 JOIN tbl_cfg_source s ON t.source_id = s.id
#                 WHERE s.source_name = :source_name
#                   AND (t.match_status IS NULL OR t.match_status = 0)
#             """
            
#             if channel_id:
#                 query += " AND t.channel_id = :channel_id"
            
#             params = {"source_name": source_name}
#             if channel_id:
#                 params["channel_id"] = channel_id
            
#             result = await self.db.execute(text(query), params)
#             rows = result.fetchall()
            
#             transactions[source_name] = [
#                 {
#                     "id": row.id,
#                     "rrn": row.rrn,
#                     "reference_number": row.rrn,
#                     "amount": float(row.amount) if row.amount else None,
#                     "transaction_date": row.transaction_date,
#                     "date": row.transaction_date,
#                     "account_number": row.account_number,
#                     "currency_code": row.currency_code,
#                     "otherDetails": row.otherDetails,
#                     "comment": row.comment,
#                     "source_name": row.source_name,
#                     # Parse otherDetails if it contains pipe-separated values
#                     # Format: CARD|TERMINAL|MERCHANT
#                     **(self._parse_other_details(row.otherDetails) if row.otherDetails else {})
#                 }
#                 for row in rows
#             ]
            
#             logger.info(f"Fetched {len(transactions[source_name])} transactions from {source_name}")
        
#         return transactions
    
#     def _parse_other_details(self, other_details: str) -> Dict[str, Any]:
#         """
#         Parse otherDetails field which may contain pipe-separated values
#         Format: CARD|TERMINAL|MERCHANT or similar
#         """
#         try:
#             parts = other_details.split('|')
#             parsed = {}
            
#             if len(parts) >= 1:
#                 parsed['card_number'] = parts[0]
#             if len(parts) >= 2:
#                 parsed['terminal_id'] = parts[1]
#             if len(parts) >= 3:
#                 parsed['merchant_id'] = parts[2]
            
#             return parsed
#         except Exception as e:
#             logger.warning(f"Error parsing otherDetails '{other_details}': {e}")
#             return {}
    
#     def _find_matching_groups(
#         self,
#         transactions_by_source: Dict[str, List[Dict[str, Any]]],
#         condition_groups: List[Dict[str, Any]],
#         tolerance: Optional[Dict[str, Any]],
#         min_sources: int
#     ) -> List[Dict[str, Any]]:
#         """
#         Find matching groups of transactions across sources.
        
#         Supports partial matching when min_sources < total_sources.
#         Groups transactions by reference_number (RRN) and validates conditions.
#         """
#         from itertools import combinations
        
#         # Convert single = to == for Python comparison (frontend sends single =)
#         # Example: "ATM.reference_number = SWITCH.reference_number" → "ATM.reference_number == SWITCH.reference_number"
#         if isinstance(condition_groups, str):
#             # Replace single = with == but avoid replacing == with ====
#             condition_expr = condition_groups.replace('==', '@@DOUBLE_EQ@@')  # Protect existing ==
#             condition_expr = condition_expr.replace('=', '==')  # Convert single = to ==
#             condition_expr = condition_expr.replace('@@DOUBLE_EQ@@', '==')  # Restore original ==
#         else:
#             # Fallback for list format (legacy)
#             condition_expr = str(condition_groups)
        
#         # Parse condition to extract required sources
#         tree = ast.parse(condition_expr, mode="eval")
#         ALLOWED_NODES = (
#             ast.Expression,
#             ast.Compare,
#             ast.Eq,
#             ast.BoolOp,
#             ast.And,
#             ast.Or,
#             ast.Name,
#             ast.Load,
#             ast.Attribute,
#         )
        
#         def validate_ast(node):
#             if not isinstance(node, ALLOWED_NODES):
#                 raise ValueError(f"Disallowed expression element: {type(node).__name__}")
#             for child in ast.iter_child_nodes(node):
#                 validate_ast(child)

#         validate_ast(tree)
        
#         class SourceCollector(ast.NodeVisitor):
#             def __init__(self):
#                 self.sources = set()

#             def visit_Name(self, node):
#                 self.sources.add(node.id)

#         collector = SourceCollector()
#         collector.visit(tree)
#         source_names = sorted(collector.sources)
        
#         logger.info(f"🔍 Finding matches: min_sources={min_sources}, total_sources={len(source_names)}, sources={source_names}")
        
#         # Group transactions by RRN for each source
#         txns_by_rrn = {}
#         for source_name in source_names:
#             source_txns = transactions_by_source.get(source_name, [])
#             logger.info(f"   Source {source_name}: {len(source_txns)} transactions")
            
#             for txn in source_txns:
#                 rrn = txn.get("rrn")
#                 if rrn:
#                     if rrn not in txns_by_rrn:
#                         txns_by_rrn[rrn] = {}
#                     if source_name not in txns_by_rrn[rrn]:
#                         txns_by_rrn[rrn][source_name] = []
#                     txns_by_rrn[rrn][source_name].append(txn)
        
#         logger.info(f"   Found {len(txns_by_rrn)} unique RRNs")
        
#         # Find matching groups
#         matched_groups = []
#         compiled_expr = compile(tree, "<rule>", "eval")
        
#         for rrn, sources_dict in txns_by_rrn.items():
#             # Check if we have enough sources for this RRN
#             available_sources = list(sources_dict.keys())
#             num_sources = len(available_sources)
            
#             if num_sources < min_sources:
#                 logger.warning(f"   RRN {rrn}: SKIPPED - only {num_sources} sources (min required: {min_sources}), sources={available_sources}")
#                 continue
            
#             logger.info(f"   RRN {rrn}: {num_sources} sources available ({available_sources}), min_sources={min_sources}, total_sources={len(source_names)}")
            
#             # If we have all sources, do full evaluation
#             if num_sources == len(source_names):
#                 # All sources present - do Cartesian product as before
#                 source_lists = [sources_dict[src] for src in source_names]
                
#                 for txn_tuple in product(*source_lists):
#                     context = {
#                         src: SimpleNamespace(**txn)
#                         for src, txn in zip(source_names, txn_tuple)
#                     }
                    
#                     try:
#                         if eval(compiled_expr, {}, context):
#                             logger.info(f"✅ Match Found (FULL) --> RRN={rrn}, IDs={[context[src].id for src in source_names]}")
#                             matched_groups.append({
#                                 "transactions": [txn for txn in txn_tuple],
#                                 "match_key": rrn,
#                                 "sources_matched": source_names
#                             })
#                             break  # Take first matching combination for this RRN
#                     except Exception as e:
#                         logger.debug(f"Condition evaluation error for RRN {rrn}: {e}")
#                         continue
            
#             else:
#                 # Partial match - we have min_sources <= num_sources < total_sources
#                 logger.info(f"   🔄 RRN {rrn}: Attempting PARTIAL match with {num_sources} sources (need {min_sources}, total possible {len(source_names)})")
#                 # Try all combinations of available sources that meet min_sources
#                 for combo_size in range(num_sources, min_sources - 1, -1):
#                     logger.debug(f"      Trying combinations of size {combo_size}")
#                     for source_combo in combinations(available_sources, combo_size):
#                         source_combo = sorted(source_combo)
                        
#                         # Build a partial condition that only references these sources
#                         source_lists = [sources_dict[src] for src in source_combo]
                        
#                         for txn_tuple in product(*source_lists):
#                             # Build context for only the available sources
#                             context = {
#                                 src: SimpleNamespace(**txn)
#                                 for src, txn in zip(source_combo, txn_tuple)
#                             }
                            
#                             # Build a simplified expression for partial match
#                             # Check if all available sources have matching reference numbers
#                             try:
#                                 # Create a chain of equality checks: src1.reference_number == src2.reference_number == src3.reference_number
#                                 partial_expr = " and ".join([
#                                     f"{source_combo[i]}.reference_number == {source_combo[i+1]}.reference_number"
#                                     for i in range(len(source_combo) - 1)
#                                 ])
                                
#                                 logger.debug(f"         Evaluating: {partial_expr}")
#                                 if eval(partial_expr, {}, context):
#                                     logger.warning(f"✅ Match Found (PARTIAL) --> RRN={rrn}, sources={source_combo}, IDs={[context[src].id for src in source_combo]}")
#                                     matched_groups.append({
#                                         "transactions": [txn for txn in txn_tuple],
#                                         "match_key": rrn,
#                                         "sources_matched": list(source_combo)
#                                     })
#                                     break  # Take first matching combination
#                                 else:
#                                     logger.debug(f"         Expression evaluated to False")
#                             except Exception as e:
#                                 logger.warning(f"Partial match evaluation error for RRN {rrn}: {e}")
#                                 continue
#                         else:
#                             continue
#                         break  # Found a match, stop trying smaller combinations
        
#         logger.info(f"Found {len(matched_groups)} matching groups")
#         return matched_groups
    
#     def _find_matching_transaction(
#         self,
#         reference_txn: Dict[str, Any],
#         candidate_txns: List[Dict[str, Any]],
#         current_group: Dict[str, Dict[str, Any]],
#         condition_groups: List[Dict[str, Any]],
#         tolerance: Optional[Dict[str, Any]]
#     ) -> Optional[Dict[str, Any]]:
#         """
#         Find a transaction from candidates that matches the reference
#         """
#         for candidate in candidate_txns:
#             # Build test group
#             test_group = {**current_group}
#             test_group[candidate["source_name"]] = candidate
            
#             # Evaluate if this forms a valid match
#             if self._evaluate_condition_groups(test_group, condition_groups, tolerance):
#                 return candidate
        
#         return None
    
#     def _evaluate_condition_groups(
#         self,
#         transaction_group: Dict[str, Dict[str, Any]],
#         condition_groups: List[Dict[str, Any]],
#         tolerance: Optional[Dict[str, Any]]
#     ) -> bool:
#         """
#         Evaluate condition_groups with OR/AND logic
        
#         Recursively processes nested groups and respects parentheses
#         """
#         for group in condition_groups:
#             group_type = group.get("group_type", "AND")
#             conditions = group.get("conditions", [])
            
#             results = []
            
#             for condition in conditions:
#                 # Check if this is a nested group
#                 if "group_type" in condition:
#                     # Recursive evaluation
#                     result = self._evaluate_condition_groups(
#                         transaction_group,
#                         [condition],
#                         tolerance
#                     )
#                     results.append(result)
#                 else:
#                     # Evaluate individual condition
#                     result = self._evaluate_single_condition(
#                         transaction_group,
#                         condition,
#                         tolerance
#                     )
#                     results.append(result)
            
#             # Apply group operator
#             if group_type == "OR":
#                 if not any(results):
#                     return False
#             else:  # AND
#                 if not all(results):
#                     return False
        
#         return True
    
#     def _evaluate_single_condition(
#         self,
#         transaction_group: Dict[str, Dict[str, Any]],
#         condition: Dict[str, Any],
#         tolerance: Optional[Dict[str, Any]]
#     ) -> bool:
#         """
#         Evaluate a single field condition
        
#         Examples:
#         - {"field": "rrn", "operator": "equals"}
#         - {"field": "amount", "operator": "equals", "sources": ["ATM", "SWITCH"]}
#         """
#         field = condition.get("field")
#         operator = condition.get("operator", "equals")
#         specific_sources = condition.get("sources")
        
#         # Get transactions to compare
#         if specific_sources:
#             # Only compare specific source pairs
#             txns_to_compare = [
#                 transaction_group[src] for src in specific_sources
#                 if src in transaction_group
#             ]
#         else:
#             # Compare all transactions in group
#             txns_to_compare = list(transaction_group.values())
        
#         if len(txns_to_compare) < 2:
#             return False
        
#         # Get values for this field
#         values = [txn.get(field) for txn in txns_to_compare]
        
#         # Apply operator
#         if operator == "equals":
#             return self._check_equality(values)
#         elif operator == "within_tolerance":
#             return self._check_within_tolerance(values, field, tolerance)
#         elif operator == "greater_than":
#             return self._check_comparison(values, condition, "greater_than")
#         elif operator == "less_than":
#             return self._check_comparison(values, condition, "less_than")
#         elif operator == "greater_than_or_equal":
#             return self._check_comparison(values, condition, "greater_than_or_equal")
#         elif operator == "less_than_or_equal":
#             return self._check_comparison(values, condition, "less_than_or_equal")
#         elif operator == "date_within_days":
#             return self._check_date_tolerance(values, condition, tolerance)
#         elif operator == "starts_with":
#             return self._check_string_operation(values, condition, "starts_with")
#         elif operator == "ends_with":
#             return self._check_string_operation(values, condition, "ends_with")
#         elif operator == "contains":
#             return self._check_string_operation(values, condition, "contains")
#         elif operator == "regex":
#             return self._check_string_operation(values, condition, "regex")
#         elif operator == "is_null":
#             return self._check_null_handling(values, True)
#         elif operator == "is_not_null":
#             return self._check_null_handling(values, False)
#         elif operator == "cross_field_equals":
#             return self._check_cross_field(transaction_group, condition)
#         else:
#             logger.warning(f"Unknown operator: {operator}")
#             return False
    
#     def _check_equality(self, values: List[Any]) -> bool:
#         """Check if all values are equal"""
#         if not values or None in values:
#             return False
#         return len(set(str(v) for v in values)) == 1
    
#     def _check_within_tolerance(
#         self,
#         values: List[Any],
#         field: str,
#         tolerance: Optional[Dict[str, Any]]
#     ) -> bool:
#         """Check if all values are within tolerance"""
#         if not values or None in values:
#             return False
        
#         numeric_values = [float(v) for v in values]
        
#         # Get tolerance for this field
#         tolerance_config = tolerance.get(field, {}) if tolerance else {}
#         tolerance_value = tolerance_config.get("value", 0.01)
#         tolerance_type = tolerance_config.get("type", "fixed")
        
#         # Check all pairs
#         for i in range(len(numeric_values) - 1):
#             diff = abs(numeric_values[i] - numeric_values[i + 1])
            
#             if tolerance_type == "percentage":
#                 max_val = max(numeric_values[i], numeric_values[i + 1])
#                 allowed_diff = max_val * (tolerance_value / 100)
#             else:  # fixed
#                 allowed_diff = tolerance_value
            
#             if diff > allowed_diff:
#                 return False
        
#         return True
    
#     def _check_comparison(
#         self,
#         values: List[Any],
#         condition: Dict[str, Any],
#         comparison_type: str
#     ) -> bool:
#         """
#         Check comparison operators: >, <, >=, <=
        
#         Supports two modes:
#         1. Compare against fixed value: {"field": "amount", "operator": "greater_than", "value": 100}
#         2. Compare between sources: {"field": "amount", "operator": "greater_than"}
        
#         Examples:
#         - greater_than: All values > threshold OR first > second
#         - less_than: All values < threshold OR first < second
#         - greater_than_or_equal: All values >= threshold
#         - less_than_or_equal: All values <= threshold
#         """
#         if not values:
#             return False
        
#         # Check if comparing against a fixed value
#         compare_value = condition.get("value")
        
#         if compare_value is not None:
#             # Compare all values against fixed threshold
#             try:
#                 threshold = float(compare_value)
#                 numeric_values = [float(v) for v in values if v is not None]
                
#                 if not numeric_values:
#                     return False
                
#                 if comparison_type == "greater_than":
#                     return all(v > threshold for v in numeric_values)
#                 elif comparison_type == "less_than":
#                     return all(v < threshold for v in numeric_values)
#                 elif comparison_type == "greater_than_or_equal":
#                     return all(v >= threshold for v in numeric_values)
#                 elif comparison_type == "less_than_or_equal":
#                     return all(v <= threshold for v in numeric_values)
                    
#             except (ValueError, TypeError) as e:
#                 logger.warning(f"Cannot convert values to numeric for comparison: {e}")
#                 return False
#         else:
#             # Compare values between sources (pairwise)
#             try:
#                 numeric_values = [float(v) for v in values if v is not None]
                
#                 if len(numeric_values) < 2:
#                     return False
                
#                 # Check all consecutive pairs
#                 for i in range(len(numeric_values) - 1):
#                     if comparison_type == "greater_than":
#                         if not (numeric_values[i] > numeric_values[i + 1]):
#                             return False
#                     elif comparison_type == "less_than":
#                         if not (numeric_values[i] < numeric_values[i + 1]):
#                             return False
#                     elif comparison_type == "greater_than_or_equal":
#                         if not (numeric_values[i] >= numeric_values[i + 1]):
#                             return False
#                     elif comparison_type == "less_than_or_equal":
#                         if not (numeric_values[i] <= numeric_values[i + 1]):
#                             return False
                
#                 return True
                
#             except (ValueError, TypeError) as e:
#                 logger.warning(f"Cannot convert values to numeric for comparison: {e}")
#                 return False
        
#         return False
    
#     def _check_date_tolerance(
#         self,
#         values: List[Any],
#         condition: Dict[str, Any],
#         tolerance: Optional[Dict[str, Any]]
#     ) -> bool:
#         """
#         Check if dates are within tolerance (±N days)
        
#         Examples:
#         - {"field": "transaction_date", "operator": "date_within_days", "days": 1}
#         - Uses tolerance config: {"transaction_date": {"days": 2}}
#         """
#         if not values or None in values:
#             return False
        
#         # Get tolerance days
#         field = condition.get("field")
#         days_tolerance = condition.get("days")
        
#         # Check tolerance config if not in condition
#         if days_tolerance is None and tolerance and field:
#             tolerance_config = tolerance.get(field, {})
#             days_tolerance = tolerance_config.get("days", 0)
        
#         if days_tolerance is None:
#             days_tolerance = 0
        
#         try:
#             # Convert to date objects
#             date_values = []
#             for v in values:
#                 if isinstance(v, datetime):
#                     date_values.append(v.date())
#                 elif isinstance(v, date):
#                     date_values.append(v)
#                 elif isinstance(v, str):
#                     # Try to parse date string
#                     date_values.append(datetime.fromisoformat(v.replace('Z', '+00:00')).date())
#                 else:
#                     logger.warning(f"Cannot parse date value: {v}")
#                     return False
            
#             # Check all pairs
#             for i in range(len(date_values) - 1):
#                 diff = abs((date_values[i] - date_values[i + 1]).days)
#                 if diff > days_tolerance:
#                     return False
            
#             return True
            
#         except (ValueError, TypeError, AttributeError) as e:
#             logger.warning(f"Error parsing dates: {e}")
#             return False
    
#     def _check_string_operation(
#         self,
#         values: List[Any],
#         condition: Dict[str, Any],
#         operation: str
#     ) -> bool:
#         """
#         Check string operations: starts_with, ends_with, contains, regex
        
#         Examples:
#         - starts_with: {"field": "account", "operator": "starts_with", "value": "ACC"}
#         - contains: {"field": "description", "operator": "contains", "value": "PAYMENT"}
#         - regex: {"field": "rrn", "operator": "regex", "pattern": "^[0-9]{12}$"}
#         """
#         if not values:
#             return False
        
#         # Get comparison value or pattern
#         compare_value = condition.get("value")
#         pattern = condition.get("pattern")
        
#         if operation == "regex":
#             if not pattern:
#                 logger.warning("Regex operator requires 'pattern' parameter")
#                 return False
            
#             try:
#                 compiled_pattern = re.compile(pattern)
#                 return all(
#                     compiled_pattern.match(str(v)) is not None 
#                     for v in values if v is not None
#                 )
#             except re.error as e:
#                 logger.warning(f"Invalid regex pattern '{pattern}': {e}")
#                 return False
        
#         else:
#             # Other string operations
#             if compare_value is None:
#                 logger.warning(f"{operation} operator requires 'value' parameter")
#                 return False
            
#             compare_str = str(compare_value).lower()
            
#             for v in values:
#                 if v is None:
#                     return False
                
#                 value_str = str(v).lower()
                
#                 if operation == "starts_with":
#                     if not value_str.startswith(compare_str):
#                         return False
#                 elif operation == "ends_with":
#                     if not value_str.endswith(compare_str):
#                         return False
#                 elif operation == "contains":
#                     if compare_str not in value_str:
#                         return False
            
#             return True
    
#     def _check_null_handling(
#         self,
#         values: List[Any],
#         should_be_null: bool
#     ) -> bool:
#         """
#         Check NULL handling: is_null, is_not_null
        
#         Examples:
#         - is_null: {"field": "merchant_id", "operator": "is_null"}
#         - is_not_null: {"field": "merchant_id", "operator": "is_not_null"}
#         """
#         if not values:
#             return should_be_null
        
#         if should_be_null:
#             # All values should be None/null
#             return all(v is None or v == '' or str(v).lower() == 'none' for v in values)
#         else:
#             # All values should NOT be None/null
#             return all(v is not None and v != '' and str(v).lower() != 'none' for v in values)
    
#     def _check_cross_field(
#         self,
#         transaction_group: Dict[str, Dict[str, Any]],
#         condition: Dict[str, Any]
#     ) -> bool:
#         """
#         Check cross-field conditions: Compare different fields
        
#         Examples:
#         - {"operator": "cross_field_equals", "field1": "debit_amount", "field2": "credit_amount"}
#         - {"operator": "cross_field_equals", "field1": "fee", "field2": "charges"}
#         """
#         field1 = condition.get("field1")
#         field2 = condition.get("field2")
        
#         if not field1 or not field2:
#             logger.warning("cross_field_equals requires 'field1' and 'field2' parameters")
#             return False
        
#         # Check each transaction in the group
#         for source_name, txn in transaction_group.items():
#             value1 = txn.get(field1)
#             value2 = txn.get(field2)
            
#             # Both should exist
#             if value1 is None or value2 is None:
#                 return False
            
#             # Try numeric comparison first
#             try:
#                 if float(value1) != float(value2):
#                     return False
#             except (ValueError, TypeError):
#                 # Fall back to string comparison
#                 if str(value1) != str(value2):
#                     return False
        
#         return True
    
#     def generate_reference(self):
#         timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
#         rand = random.randint(1000, 9999)
#         return f"REF-{timestamp}-{rand}"

#     def generate_recon_run_group(self, prefix="RECON"):
#         import uuid
#         """
#         Generates a unique recon run group value for a single run.
#         Example: RECON_20260122_103045_8f3c2a
#         """
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         unique_id = uuid.uuid4().hex[:6]
#         return f"{prefix}_{timestamp}_{unique_id}"
    
#     async def _update_matched_transactions(
#         self,
#         matched_groups: List[Dict[str, Any]],
#         rule_id: int,
#         match_type: str,
#         total_sources: int = None
#     ) -> List[int]:
#         """
#         Update all matched transactions in database
#         Set match_status, reconciled_status, matched_with_txn_id
        
#         Args:
#             matched_groups: List of matched transaction groups
#             rule_id: ID of the matching rule
#             match_type: Overall match type (FULL/PARTIAL)
#             total_sources: Total number of sources in the rule (for dynamic match_status)
#         """
#         recon_group_number = self.generate_recon_run_group()
#         all_txn_ids = []
        
#         for group in matched_groups:
#             transactions = group["transactions"]
#             txn_ids = [txn["id"] for txn in transactions]
#             all_txn_ids.extend(txn_ids)
            
#             # Determine match_status based on actual sources matched
#             sources_matched = len(group.get("sources_matched", transactions))
            
#             # Dynamic match status:
#             # - If all sources matched: match_status = 1 (FULL)
#             # - If partial sources matched: match_status = 2 (PARTIAL)
#             if total_sources and sources_matched < total_sources:
#                 match_status = 2  # Partial match
#                 actual_match_type = "PARTIAL"
#                 reconciled_mode = None
#                 recon_reference_number = None
#                 reconciled_status = None
#             else:
#                 match_status = 1  # Full match
#                 actual_match_type = "FULL"
#                 reconciled_mode = True
#                 recon_reference_number = self.generate_reference()
#                 reconciled_status = True
            
#             logger.warning(
#                 f"💾 Updating {len(txn_ids)} transactions: sources_matched={sources_matched}, "
#                 f"total_sources={total_sources}, match_status={match_status} ({actual_match_type})"
#             )
            
#             # Create match condition description
#             match_condition = f"Matched by rule {rule_id} ({actual_match_type}: {sources_matched} sources) - Application Layer"
            
#             # Update each transaction
#             update_query = """
#                 UPDATE tbl_txn_transactions
#                 SET 
#                     match_status = :match_status,
#                     reconciled_status = :reconciled_status,
#                     reconciled_mode = :reconciled_mode,
#                     recon_reference_number = :recon_reference_number,
#                     match_rule_id = :rule_id,
#                     match_conditon = :match_condition,
#                     recon_group_number = :recon_group_number,
#                     updated_at = NOW()
#                 WHERE id = ANY(:txn_ids)
#             """
            
#             await self.db.execute(
#                 text(update_query),
#                 {
#                     "match_status": match_status,
#                     "reconciled_mode": reconciled_mode,
#                     "reconciled_status": reconciled_status,
#                     "recon_reference_number": recon_reference_number,
#                     "rule_id": rule_id,
#                     "match_condition": match_condition,
#                     "recon_group_number": recon_group_number,
#                     "txn_ids": txn_ids
#                 }
#             )
        
#         await self.db.commit()
#         logger.info(f"Updated {len(all_txn_ids)} transactions with match results")
        
#         return all_txn_ids

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
from sklearn import tree
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select
from datetime import datetime, date, timedelta
import re
import random
import logging
from types import SimpleNamespace
from itertools import product
import ast
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
                "unmatched_count": int,
                "transaction_ids": List[int],
                "execution_time_ms": int,
                "match_type": str,
                "matched_groups": List[Dict] (in dry_run)
            }
        """
        start_time = datetime.now()
        
        # ✅ Generate recon_group_number ONCE for this entire run
        recon_group_number = self.generate_recon_run_group()
        
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
                conditions.get("logic_expression", []),
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
            
            # ✅ Update matched transactions in database
            all_matched_txn_ids = await self._update_matched_transactions(
                matched_groups,
                rule_id,
                match_type,
                total_sources=len(sources),
                recon_group_number=recon_group_number
            )
            
            # ✅ Update unmatched transactions with the same recon_group_number
            unmatched_count = await self._update_unmatched_transactions(
                transactions_by_source,
                all_matched_txn_ids,
                rule_id,
                recon_group_number
            )
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "rule_id": rule_id,
                "matched_count": len(matched_groups),
                "unmatched_count": unmatched_count,
                "transaction_ids": all_matched_txn_ids,
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
        Find matching groups of transactions across sources.
        
        Supports partial matching when min_sources < total_sources.
        Groups transactions by reference_number (RRN) and validates conditions.
        """
        from itertools import combinations
        
        # Convert single = to == for Python comparison (frontend sends single =)
        # Example: "ATM.reference_number = SWITCH.reference_number" → "ATM.reference_number == SWITCH.reference_number"
        if isinstance(condition_groups, str):
            # Replace single = with == but avoid replacing == with ====
            condition_expr = condition_groups.replace('==', '@@DOUBLE_EQ@@')  # Protect existing ==
            condition_expr = condition_expr.replace('equalto', '=')  # Convert equalto to =
            condition_expr = condition_expr.replace('=', '==')  # Convert single = to ==
            condition_expr = condition_expr.replace('@@DOUBLE_EQ@@', '==')  # Restore original ==
            
            # FIX: Auto-wrap chained comparisons with AND/OR to prevent syntax errors
            # AND convert uppercase AND/OR to lowercase and/or (Python requirement)
            # Example: "A == B == C AND D == E == F" → "(A == B == C) and (D == E == F)"
            # Split by logical operators while preserving them
            import re
            
            # Split by AND/OR while preserving the operators
            parts = re.split(r'\s+(AND|OR)\s+', condition_expr, flags=re.IGNORECASE)
            
            # If we have multiple parts (logical operators present)
            if len(parts) > 1:
                fixed_parts = []
                for i, part in enumerate(parts):
                    # Logical operators (AND/OR) - convert to lowercase
                    if part.upper() in ['AND', 'OR']:
                        fixed_parts.append(f' {part.lower()} ')
                    # Wrap comparison expressions in parentheses if they contain ==
                    elif '==' in part:
                        fixed_parts.append(f'({part.strip()})')
                    else:
                        fixed_parts.append(part)
                
                condition_expr = ''.join(fixed_parts)
                logger.info(f"🔧 Auto-fixed logic expression: {condition_expr}")
        else:
            # Fallback for list format (legacy)
            condition_expr = str(condition_groups)
        
        # Parse condition to extract required sources
        tree = ast.parse(condition_expr, mode="eval")
        ALLOWED_NODES = (
            ast.Expression,
            ast.Compare,
            ast.Eq,
            ast.BoolOp,
            ast.And,
            ast.Or,
            ast.Name,
            ast.Load,
            ast.Attribute,
        )
        
        def validate_ast(node):
            if not isinstance(node, ALLOWED_NODES):
                raise ValueError(f"Disallowed expression element: {type(node).__name__}")
            for child in ast.iter_child_nodes(node):
                validate_ast(child)

        validate_ast(tree)
        
        class SourceCollector(ast.NodeVisitor):
            def __init__(self):
                self.sources = set()

            def visit_Name(self, node):
                self.sources.add(node.id)

        collector = SourceCollector()
        collector.visit(tree)
        source_names = sorted(collector.sources)
        
        logger.info(f"🔍 Finding matches: min_sources={min_sources}, total_sources={len(source_names)}, sources={source_names}")
        
        # Group transactions by matching key
        # CRITICAL FIX: For OR conditions, group by primary field only (usually reference_number)
        # For AND conditions, group by ALL fields for composite key
        matching_fields = set()
        
        # Parse the expression to find ALL equality fields
        # Look for patterns like "source1.field == source2.field"
        import re
        logic_expr_str = ast.unparse(tree) if hasattr(ast, 'unparse') else condition_expr
        
        # Check if expression contains OR operator
        has_or = ' or ' in logic_expr_str.lower()
        
        # Find all field comparisons - this captures any field used in equality checks
        # Pattern matches: "source.field ==" (captures "field")
        field_patterns = re.findall(r'\.(\w+)\s*==', logic_expr_str)
        
        if has_or:
            # OR logic: Only group by the FIRST field (primary key - usually reference_number)
            # This allows matching even when secondary fields differ
            # Example: "ref == ref OR amount == amount" → group by reference_number only
            if field_patterns:
                primary_field = field_patterns[0]  # Take first field as primary
                matching_fields.add(primary_field)
                logger.info(f"   ⚠️  OR condition detected - grouping by PRIMARY field only: {primary_field}")
                logger.info(f"   Other fields {field_patterns[1:]} will be evaluated in condition, not used for grouping")
        else:
            # AND logic: Group by ALL fields for composite key
            # Example: "ref == ref AND amount == amount" → group by (reference_number, amount)
            matching_fields.update(field_patterns)
        
        # If no fields found (shouldn't happen, but safeguard), at least use reference_number
        if not matching_fields:
            logger.warning("⚠️  No equality fields found in condition, defaulting to reference_number only")
            matching_fields.add("reference_number")
        
        logger.info(f"   Grouping transactions by composite key with fields: {sorted(matching_fields)}")

        
        # Group transactions by composite key using ALL matching fields
        txns_by_key = {}
        for source_name in source_names:
            source_txns = transactions_by_source.get(source_name, [])
            logger.info(f"   Source {source_name}: {len(source_txns)} transactions")
            
            for txn in source_txns:
                # Build composite key from ALL matching fields
                # Sort fields for consistent key generation
                key_parts = []
                
                for field in sorted(matching_fields):
                    # Map common aliases to standard field names
                    field_name = field
                    if field in ["rrn", "reference_number"]:
                        field_value = txn.get("rrn") or txn.get("reference_number")
                    else:
                        field_value = txn.get(field)
                    
                    # Skip if field value is None/missing
                    if field_value is None:
                        continue
                    
                    key_parts.append(f"{field_name}={field_value}")
                
                # Skip transactions without any matching fields
                if not key_parts:
                    logger.debug(f"   Skipping transaction {txn.get('id')} - no matching field values")
                    continue
                
                composite_key = "|".join(key_parts)
                
                if composite_key not in txns_by_key:
                    txns_by_key[composite_key] = {}
                if source_name not in txns_by_key[composite_key]:
                    txns_by_key[composite_key][source_name] = []
                txns_by_key[composite_key][source_name].append(txn)
        
        logger.info(f"   Found {len(txns_by_key)} unique matching keys (RRN + other fields)")
        
        # Find matching groups
        matched_groups = []
        compiled_expr = compile(tree, "<rule>", "eval")
        
        for composite_key, sources_dict in txns_by_key.items():
            # Check if we have enough sources for this key
            available_sources = list(sources_dict.keys())
            num_sources = len(available_sources)
            
            if num_sources < min_sources:
                logger.warning(f"   Key {composite_key}: SKIPPED - only {num_sources} sources (min required: {min_sources}), sources={available_sources}")
                continue
            
            logger.info(f"   Key {composite_key}: {num_sources} sources available ({available_sources}), min_sources={min_sources}, total_sources={len(source_names)}")
            
            # If we have all sources, do full evaluation
            if num_sources == len(source_names):
                # All sources present - do Cartesian product as before
                source_lists = [sources_dict[src] for src in source_names]
                
                for txn_tuple in product(*source_lists):
                    context = {
                        src: SimpleNamespace(**txn)
                        for src, txn in zip(source_names, txn_tuple)
                    }
                    
                    try:
                        if eval(compiled_expr, {}, context):
                            # Extract RRN from composite key for logging
                            rrn_value = composite_key.split("|")[0] if "|" in composite_key else composite_key
                            logger.info(f"✅ Match Found (FULL) --> Key={composite_key}, IDs={[context[src].id for src in source_names]}")
                            matched_groups.append({
                                "transactions": [txn for txn in txn_tuple],
                                "match_key": composite_key,
                                "sources_matched": source_names
                            })
                            break  # Take first matching combination for this key
                    except Exception as e:
                        logger.debug(f"Condition evaluation error for key {composite_key}: {e}")
                        continue
            
            else:
                # Partial match - we have min_sources <= num_sources < total_sources
                logger.info(f"   🔄 Key {composite_key}: Attempting PARTIAL match with {num_sources} sources (need {min_sources}, total possible {len(source_names)})")
                # Try all combinations of available sources that meet min_sources
                for combo_size in range(num_sources, min_sources - 1, -1):
                    logger.debug(f"      Trying combinations of size {combo_size}")
                    for source_combo in combinations(available_sources, combo_size):
                        source_combo = sorted(source_combo)
                        
                        # Build a partial condition that only references these sources
                        source_lists = [sources_dict[src] for src in source_combo]
                        
                        for txn_tuple in product(*source_lists):
                            # Build context for only the available sources
                            context = {
                                src: SimpleNamespace(**txn)
                                for src, txn in zip(source_combo, txn_tuple)
                            }
                            
                            # Build a simplified expression for partial match
                            # Check if all available sources have matching reference numbers
                            try:
                                # Create a chain of equality checks: src1.reference_number == src2.reference_number == src3.reference_number
                                partial_expr = " and ".join([
                                    f"{source_combo[i]}.reference_number == {source_combo[i+1]}.reference_number"
                                    for i in range(len(source_combo) - 1)
                                ])
                                
                                logger.debug(f"         Evaluating: {partial_expr}")
                                if eval(partial_expr, {}, context):
                                    logger.warning(f"✅ Match Found (PARTIAL) --> Key={composite_key}, sources={source_combo}, IDs={[context[src].id for src in source_combo]}")
                                    matched_groups.append({
                                        "transactions": [txn for txn in txn_tuple],
                                        "match_key": composite_key,
                                        "sources_matched": list(source_combo)
                                    })
                                    break  # Take first matching combination
                                else:
                                    logger.debug(f"         Expression evaluated to False")
                            except Exception as e:
                                logger.warning(f"Partial match evaluation error for key {composite_key}: {e}")
                                continue
                        else:
                            continue
                        break  # Found a match, stop trying smaller combinations
        
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
    
    def generate_reference(self):
        """Generate a unique reference number using timestamp + microseconds + UUID"""
        import uuid
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        microseconds = datetime.now().microsecond
        unique_id = uuid.uuid4().hex[:6]  # 6-char hex for extra uniqueness
        return f"REF-{timestamp}-{microseconds:06d}-{unique_id}"

    def generate_recon_run_group(self, prefix="RECON"):
        import uuid
        """
        Generates a unique recon run group value for a single run.
        Example: RECON_20260122_103045_8f3c2a
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:6]
        return f"{prefix}_{timestamp}_{unique_id}"
    
    async def _update_matched_transactions(
        self,
        matched_groups: List[Dict[str, Any]],
        rule_id: int,
        match_type: str,
        total_sources: int = None,
        recon_group_number: str = None
    ) -> List[int]:
        """
        Update all matched transactions in database
        Set match_status, reconciled_status, matched_with_txn_id
        
        Args:
            matched_groups: List of matched transaction groups
            rule_id: ID of the matching rule
            match_type: Overall match type (FULL/PARTIAL)
            total_sources: Total number of sources in the rule (for dynamic match_status)
            recon_group_number: Recon group number for this run
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
                reconciled_mode = None
                recon_reference_number = None
                reconciled_status = None
            else:
                match_status = 1  # Full match
                actual_match_type = "FULL"
                reconciled_mode = True
                recon_reference_number = self.generate_reference()
                reconciled_status = True
            
            logger.warning(
                f"💾 Updating {len(txn_ids)} transactions: sources_matched={sources_matched}, "
                f"total_sources={total_sources}, match_status={match_status} ({actual_match_type})"
            )
            
            # Create match condition description
            match_condition = f"Matched by rule {rule_id} ({actual_match_type}: {sources_matched} sources) - Application Layer"
            
            # Update each transaction
            update_query = """
                UPDATE tbl_txn_transactions
                SET 
                    match_status = :match_status,
                    reconciled_status = :reconciled_status,
                    reconciled_mode = :reconciled_mode,
                    recon_reference_number = :recon_reference_number,
                    match_rule_id = :rule_id,
                    match_conditon = :match_condition,
                    recon_group_number = :recon_group_number,
                    updated_at = NOW()
                WHERE id = ANY(:txn_ids)
            """
            
            await self.db.execute(
                text(update_query),
                {
                    "match_status": match_status,
                    "reconciled_mode": reconciled_mode,
                    "reconciled_status": reconciled_status,
                    "recon_reference_number": recon_reference_number,
                    "rule_id": rule_id,
                    "match_condition": match_condition,
                    "recon_group_number": recon_group_number,
                    "txn_ids": txn_ids
                }
            )
        
        await self.db.commit()
        logger.info(f"Updated {len(all_txn_ids)} matched transactions")
        
        return all_txn_ids
    
    async def _update_unmatched_transactions(
        self,
        transactions_by_source: Dict[str, List[Dict[str, Any]]],
        matched_txn_ids: List[int],
        rule_id: int,
        recon_group_number: str
    ) -> int:
        """
        Update all unmatched transactions with recon_group_number
        
        Args:
            transactions_by_source: All fetched transactions
            matched_txn_ids: IDs of transactions that were matched
            rule_id: ID of the matching rule
            recon_group_number: Recon group number for this run
            
        Returns:
            Count of unmatched transactions updated
        """
        # Collect all transaction IDs from all sources
        all_txn_ids = []
        for source_txns in transactions_by_source.values():
            all_txn_ids.extend([txn["id"] for txn in source_txns])
        
        # Find unmatched transactions (those NOT in matched_txn_ids)
        unmatched_txn_ids = [tid for tid in all_txn_ids if tid not in matched_txn_ids]
        
        if not unmatched_txn_ids:
            logger.info("No unmatched transactions to update")
            return 0
        
        logger.info(f"📝 Updating {len(unmatched_txn_ids)} unmatched transactions with recon_group_number")
        
        # Update unmatched transactions
        update_query = """
            UPDATE tbl_txn_transactions
            SET 
                match_status = 0,
                reconciled_status = NULL,
                reconciled_mode = NULL,
                recon_reference_number = NULL,
                match_rule_id = NULL,
                match_conditon = NULL,
                recon_group_number = :recon_group_number,
                updated_at = NOW()
            WHERE id = ANY(:txn_ids)
        """
        
        # match_condition = f"Processed by rule {rule_id} (UNMATCHED) - Application Layer"
        
        await self.db.execute(
            text(update_query),
            {
                "recon_group_number": recon_group_number,
                "txn_ids": unmatched_txn_ids
            }
        )
        
        await self.db.commit()
        logger.info(f"✅ Updated {len(unmatched_txn_ids)} unmatched transactions")
        
        return len(unmatched_txn_ids)
"""
Matching Rule Dispatcher
Routes all matching rules to the application layer (Python) execution engine.

ARCHITECTURE CHANGE:
- Previous: SIMPLE rules → Stored Procedure, COMPLEX rules → Application Layer
- Current: ALL rules → Application Layer (unified approach)

Benefits:
- Consistency: All rules use the same execution path
- Flexibility: Easier to add features and modify logic
- Maintainability: Single codebase to maintain
- Portability: No database-specific stored procedures needed

The complexity analysis is still performed for:
- Performance monitoring and optimization
- Rule analysis and debugging
- Future optimization opportunities
"""

from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

from app.new_engine.rule_complexity_analyzer import RuleComplexityAnalyzer, RuleComplexity
from app.new_engine.application_matcher import ApplicationMatcher

logger = logging.getLogger(__name__)


class MatchingRuleDispatcher:
    """
    Intelligent dispatcher that routes matching rules based on complexity
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.application_matcher = ApplicationMatcher(db)
    
    async def execute_matching_rule(
        self,
        rule_id: int,
        channel_id: Optional[int] = None,
        dry_run: bool = False,
        min_sources: Optional[int] = None
    ) -> Dict[str, Any]:
       
        try:
            # Step 1: Fetch matching rule
            rule = await self._fetch_matching_rule(rule_id, channel_id)
            logger.info(f"Fetched matching rule {rule_id}: {rule}")
            if not rule:
                raise ValueError(f"Matching rule {rule_id} not found or inactive")
            
            # Step 2: Analyze complexity
            complexity = RuleComplexityAnalyzer.analyze(rule["conditions"])
            strategy = RuleComplexityAnalyzer.get_execution_strategy(rule["conditions"])
            
            logger.info(
                f"Rule {rule_id} analyzed: {complexity} complexity, "
                f"routing to {strategy['executor']}"
            )
            
            # Log strategy details if dry_run
            if dry_run:
                logger.info(f"Execution strategy: {strategy['reason']}")
                logger.info(f"Features detected: {strategy['features_detected']}")
            
            # Step 3: Route to appropriate executor
            # NOTE: We now use application layer for ALL rules (SIMPLE and COMPLEX)
            # This provides consistency and flexibility without needing stored procedures
            # The complexity analysis is kept for informational purposes
            
            # Previous implementation used stored procedure for SIMPLE rules:
            # if complexity == RuleComplexity.SIMPLE:
            #     result = await self._execute_via_stored_procedure(...)
            # else:
            #     result = await self._execute_via_application_layer(...)
            
            # New unified approach - always use application layer
            result = await self._execute_via_application_layer(
                rule_id=rule_id,
                conditions=rule["conditions"],
                tolerance=rule["tolerance"],
                channel_id=channel_id,
                dry_run=dry_run,
                min_sources=min_sources
            )
            result["executor"] = "application_layer"
            
            # Add complexity info for monitoring/optimization
            result["complexity"] = complexity
            
            # Add analysis metadata
            result["complexity"] = complexity
            result["execution_strategy"] = strategy
            
            return result
            
        except Exception as e:
            logger.error(f"Error in matching rule dispatcher: {e}")
            raise
    
    async def _fetch_matching_rule(
        self,
        rule_id: int,
        channel_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch matching rule from database
        """
        query = """
            SELECT 
                id,
                rule_name,
                channel_id,
                conditions,
                tolerance,
                status
            FROM tbl_cfg_matching_rule
            WHERE id = :rule_id
              AND status = 1
        """
        
        if channel_id:
            query += " AND channel_id = :channel_id"
        
        params = {"rule_id": rule_id}
        if channel_id:
            params["channel_id"] = channel_id
        
        result = await self.db.execute(text(query), params)
        row = result.fetchone()
        
        if not row:
            return None
        
        return {
            "id": row.id,
            "rule_name": row.rule_name,
            "channel_id": row.channel_id,
            "conditions": row.conditions,
            "tolerance": row.tolerance,
            "status": row.status
        }
    
    async def _execute_via_stored_procedure(
        self,
        rule_id: int,
        channel_id: Optional[int],
        dry_run: bool,
        min_sources: Optional[int]
    ) -> Dict[str, Any]:
        """
        Execute via PostgreSQL stored procedure
        Optimal for simple AND conditions
        """
        logger.info(f"Executing rule {rule_id} via STORED PROCEDURE")
        
        # Call the stored procedure
        query = """
            SELECT * FROM fn_execute_matching_rule(
                p_rule_id := :rule_id,
                p_channel_id := :channel_id,
                p_dry_run := :dry_run,
                p_min_sources := :min_sources
            )
        """
        
        result = await self.db.execute(
            text(query),
            {
                "rule_id": rule_id,
                "channel_id": channel_id,
                "dry_run": dry_run,
                "min_sources": min_sources
            }
        )
        
        row = result.fetchone()
        
        if not row:
            raise ValueError("Stored procedure returned no results")
        
        return {
            "rule_id": row.rule_id,
            "matched_count": row.matched_count,
            "transaction_ids": row.transaction_ids or [],
            "execution_time_ms": row.execution_time_ms,
            "match_type": row.match_type
        }
    
    async def _execute_via_application_layer(
        self,
        rule_id: int,
        conditions: Dict[str, Any],
        tolerance: Optional[Dict[str, Any]],
        channel_id: Optional[int],
        dry_run: bool,
        min_sources: Optional[int]
    ) -> Dict[str, Any]:
        """
        Execute via Python application layer
        Required for complex OR/parentheses logic
        """
        logger.info(f"Executing rule {rule_id} via APPLICATION LAYER")
        
        result = await self.application_matcher.execute_complex_matching(
            rule_id=rule_id,
            conditions=conditions,
            tolerance=tolerance,
            channel_id=channel_id,
            dry_run=dry_run,
            min_sources=min_sources
        )
        
        return result
    
    async def analyze_rule(self, rule_id: int) -> Dict[str, Any]:
        """
        Analyze a rule without executing it
        Useful for debugging and optimization planning
        
        Returns complete analysis:
        - Complexity level
        - Execution strategy
        - Features detected
        - Estimated performance
        """
        rule = await self._fetch_matching_rule(rule_id, channel_id=None)
        
        if not rule:
            raise ValueError(f"Rule {rule_id} not found")
        
        strategy = RuleComplexityAnalyzer.get_execution_strategy(rule["conditions"])
        
        # Get transaction counts for estimation
        sources = rule["conditions"].get("sources", [])
        txn_counts = await self._estimate_transaction_counts(sources, rule["channel_id"])
        
        # Estimate execution time
        estimated_time_ms = self._estimate_execution_time(
            strategy["complexity"],
            txn_counts
        )
        
        return {
            "rule_id": rule_id,
            "rule_name": rule["rule_name"],
            "complexity": strategy["complexity"],
            "executor": strategy["executor"],
            "reason": strategy["reason"],
            "features_detected": strategy["features_detected"],
            "sources": sources,
            "transaction_counts": txn_counts,
            "estimated_execution_time_ms": estimated_time_ms,
            "conditions": rule["conditions"]
        }
    
    async def _estimate_transaction_counts(
        self,
        sources: list,
        channel_id: int
    ) -> Dict[str, int]:
        """
        Get approximate transaction counts for each source
        """
        counts = {}
        
        for source_name in sources:
            query = """
                SELECT COUNT(*) as count
                FROM tbl_txn_transactions t
                JOIN tbl_cfg_source s ON t.source_id = s.id
                WHERE s.source_name = :source_name
                  AND t.channel_id = :channel_id
                  AND (t.match_status IS NULL OR t.match_status = 0)
            """
            
            result = await self.db.execute(
                text(query),
                {"source_name": source_name, "channel_id": channel_id}
            )
            row = result.fetchone()
            counts[source_name] = row.count if row else 0
        
        return counts
    
    def _estimate_execution_time(
        self,
        complexity: str,
        txn_counts: Dict[str, int]
    ) -> int:
        """
        Estimate execution time based on complexity and transaction volume
        
        Returns estimated time in milliseconds
        """
        total_txns = sum(txn_counts.values())
        
        if complexity == RuleComplexity.SIMPLE:
            # Stored procedure is very fast
            # ~1ms per 100 transactions
            return int(total_txns / 100) + 10
        else:
            # Application layer is slower due to Python processing
            # ~10ms per 100 transactions
            return int(total_txns / 10) + 50


# Convenience function for direct usage
async def execute_rule(
    db: AsyncSession,
    rule_id: int,
    channel_id: Optional[int] = None,
    dry_run: bool = False,
    min_sources: Optional[int] = None
) -> Dict[str, Any]:
    """
    Convenience function to execute a matching rule
    """
    dispatcher = MatchingRuleDispatcher(db)
    return await dispatcher.execute_matching_rule(
        rule_id=rule_id,
        channel_id=channel_id,
        dry_run=dry_run,
        min_sources=min_sources
    )

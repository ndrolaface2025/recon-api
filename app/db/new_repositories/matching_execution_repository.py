from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional, Dict, Any, List
import logging

from app.new_engine.matching_dispatcher import MatchingRuleDispatcher

logger = logging.getLogger(__name__)


class MatchingExecutionRepository:
    """
    Repository for executing matching rules

    Uses unified application layer to execute ALL matching rules:
    - SIMPLE rules: Basic AND conditions
    - COMPLEX rules: OR operators, nested groups, source-specific matching

    All rules are processed through the Python matching engine for consistency,
    flexibility, and maintainability.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dispatcher = MatchingRuleDispatcher(db)

    async def execute_matching_rule(
        self,
        rule_id: int,
        channel_id: Optional[int] = None,
        dry_run: bool = False,
        min_sources: Optional[int] = None,
    ) -> Dict[str, Any]:

        try:
            # If min_sources not specified, auto-detect based on rule type
            if min_sources is None:
                # Fetch rule to determine source count
                query = """
                    SELECT 
                        conditions,
                        json_array_length(conditions::json->'sources') as source_count
                    FROM tbl_cfg_matching_rule
                    WHERE id = :rule_id AND status = 1
                """
                result = await self.db.execute(text(query), {"rule_id": rule_id})
                rule_data = result.fetchone()

                if rule_data:
                    source_count = rule_data[1]
                    # For 3-way or higher, enable partial matching (2 sources minimum)
                    if source_count >= 3:
                        min_sources = 2
                        logger.info(
                            f"Auto-enabling partial matching for {source_count}-way rule (min_sources=2)"
                        )
                    # For 2-way, keep None (requires both sources)

            # Use dispatcher to intelligently route the rule
            result = await self.dispatcher.execute_matching_rule(
                rule_id=rule_id,
                channel_id=channel_id,
                dry_run=dry_run,
                min_sources=min_sources,
            )

            # Log execution details
            if not dry_run:
                logger.info(
                    f"Rule {rule_id} executed via {result.get('executor', 'unknown')} "
                    f"({result.get('complexity', 'unknown')} complexity): "
                    f"{result['matched_count']} matches in {result['execution_time_ms']}ms"
                )

            return result

        except Exception as e:
            logger.error(f"Error executing matching rule {rule_id}: {str(e)}")
            raise

    async def analyze_rule(self, rule_id: int) -> Dict[str, Any]:
        """
        Analyze a rule without executing it

        Shows:
        - Rule complexity (SIMPLE or COMPLEX)
        - Execution strategy (stored_procedure or application_layer)
        - Features detected (OR operators, nested groups, etc.)
        - Transaction counts and estimated execution time

        Args:
            rule_id: ID of the matching rule to analyze

        Returns:
            Dictionary with complete rule analysis
        """
        try:
            return await self.dispatcher.analyze_rule(rule_id)
        except Exception as e:
            logger.error(f"Error analyzing rule {rule_id}: {str(e)}")
            raise

    async def get_matching_statistics(
        self, rule_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get statistics about matched transactions

        Args:
            rule_id: Optional rule ID to filter statistics

        Returns:
            Dictionary containing matching statistics
        """
        try:
            if rule_id:
                sql = text(
                    """
                    SELECT 
                        match_rule_id,
                        COUNT(*) as total_matched,
                        COUNT(DISTINCT source_id) as sources_count,
                        MIN(created_at) as first_match,
                        MAX(created_at) as last_match
                    FROM tbl_txn_transactions
                    WHERE match_status = 1
                      AND match_rule_id = :rule_id
                    GROUP BY match_rule_id
                """
                )
                result = await self.db.execute(sql, {"rule_id": rule_id})
            else:
                sql = text(
                    """
                    SELECT 
                        match_rule_id,
                        COUNT(*) as total_matched,
                        COUNT(DISTINCT source_id) as sources_count,
                        MIN(created_at) as first_match,
                        MAX(created_at) as last_match
                    FROM tbl_txn_transactions
                    WHERE match_status = 1
                    GROUP BY match_rule_id
                    ORDER BY match_rule_id
                """
                )
                result = await self.db.execute(sql)

            rows = result.fetchall()

            if not rows:
                return {"statistics": [], "total_rules": 0}

            statistics = []
            for row in rows:
                statistics.append(
                    {
                        "rule_id": row[0],
                        "total_matched": row[1],
                        "sources_count": row[2],
                        "first_match": row[3].isoformat() if row[3] else None,
                        "last_match": row[4].isoformat() if row[4] else None,
                    }
                )

            return {"statistics": statistics, "total_rules": len(statistics)}

        except Exception as e:
            logger.error(f"Error getting matching statistics: {str(e)}")
            raise

    async def get_matched_transactions(
        self, rule_id: int, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get list of transactions matched by a specific rule

        Args:
            rule_id: ID of the matching rule
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of matched transactions
        """
        try:
            sql = text(
                """
                SELECT 
                    id,
                    reference_number,
                    source_id,
                    amount,
                    date,
                    reconciliation_status,
                    created_at
                FROM tbl_txn_transactions
                WHERE match_status = 1
                  AND match_rule_id = :rule_id
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """
            )

            result = await self.db.execute(
                sql, {"rule_id": rule_id, "limit": limit, "offset": offset}
            )

            rows = result.fetchall()

            transactions = []
            for row in rows:
                transactions.append(
                    {
                        "id": row[0],
                        "reference_number": row[1],
                        "source_id": row[2],
                        "amount": row[3],
                        "date": row[4],
                        "reconciliation_status": row[5],
                        "created_at": row[6].isoformat() if row[6] else None,
                    }
                )

            return transactions

        except Exception as e:
            logger.error(f"Error getting matched transactions: {str(e)}")
            raise

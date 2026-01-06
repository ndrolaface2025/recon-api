from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class MatchingExecutionRepository:
    """Repository for executing matching rules via stored procedure"""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def execute_matching_rule(
        self,
        rule_id: int,
        channel_id: Optional[int] = None,
        dry_run: bool = False,
        min_sources: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute the matching rule stored procedure
        
        Args:
            rule_id: ID of the matching rule to execute
            channel_id: Optional channel ID filter
            dry_run: If True, only returns generated SQL without execution
            min_sources: Minimum sources required (None=all sources, 2=partial 2 out of 3, etc.)
            
        Returns:
            Dictionary containing execution results or dry run information
            
        Raises:
            Exception: If the stored procedure fails or rule not found
        """
        try:
            # Build the SQL call
            sql = text("""
                SELECT * FROM fn_execute_matching_rule(
                    :rule_id,
                    :channel_id,
                    :dry_run,
                    :min_sources
                )
            """)
            
            # Execute the stored procedure
            result = await self.db.execute(
                sql,
                {
                    "rule_id": rule_id,
                    "channel_id": channel_id,
                    "dry_run": dry_run,
                    "min_sources": min_sources
                }
            )
            
            # Fetch the result
            row = result.fetchone()
            
            if dry_run:
                # For dry run, check PostgreSQL notices/logs
                # Note: In dry run mode, the function returns immediately with RETURN
                # The RAISE NOTICE output would be in PostgreSQL logs
                return {
                    "rule_id": rule_id,
                    "dry_run": True,
                    "match_type": "FULL" if min_sources is None else "PARTIAL",
                    "message": "Dry run completed. Check PostgreSQL logs for generated SQL.",
                    "note": "Use dry_run=false to execute actual matching"
                }
            
            if row is None:
                return {
                    "rule_id": rule_id,
                    "matched_count": 0,
                    "transaction_ids": [],
                    "execution_time_ms": 0,
                    "match_type": "FULL" if min_sources is None else "PARTIAL",
                    "message": "No matches found"
                }
            
            # Parse the result
            result_dict = {
                "rule_id": row[0],
                "matched_count": row[1] if row[1] else 0,
                "transaction_ids": list(row[2]) if row[2] else [],
                "execution_time_ms": row[3] if row[3] else 0,
                "match_type": row[4] if len(row) > 4 else "FULL"
            }
            
            # Commit the transaction updates
            if not dry_run and result_dict["matched_count"] > 0:
                await self.db.commit()
                logger.info(
                    f"Matched {result_dict['matched_count']} transactions "
                    f"using rule {rule_id} ({result_dict['match_type']}) in {result_dict['execution_time_ms']}ms"
                )
            
            return result_dict
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error executing matching rule {rule_id}: {str(e)}")
            raise

    async def get_matching_statistics(self, rule_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get statistics about matched transactions
        
        Args:
            rule_id: Optional rule ID to filter statistics
            
        Returns:
            Dictionary containing matching statistics
        """
        try:
            if rule_id:
                sql = text("""
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
                """)
                result = await self.db.execute(sql, {"rule_id": rule_id})
            else:
                sql = text("""
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
                """)
                result = await self.db.execute(sql)
            
            rows = result.fetchall()
            
            if not rows:
                return {"statistics": [], "total_rules": 0}
            
            statistics = []
            for row in rows:
                statistics.append({
                    "rule_id": row[0],
                    "total_matched": row[1],
                    "sources_count": row[2],
                    "first_match": row[3].isoformat() if row[3] else None,
                    "last_match": row[4].isoformat() if row[4] else None
                })
            
            return {
                "statistics": statistics,
                "total_rules": len(statistics)
            }
            
        except Exception as e:
            logger.error(f"Error getting matching statistics: {str(e)}")
            raise

    async def get_matched_transactions(
        self,
        rule_id: int,
        limit: int = 100,
        offset: int = 0
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
            sql = text("""
                SELECT 
                    id,
                    reference_number,
                    source_id,
                    amount,
                    date,
                    reconciled_status,
                    created_at
                FROM tbl_txn_transactions
                WHERE match_status = 1
                  AND match_rule_id = :rule_id
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await self.db.execute(
                sql,
                {"rule_id": rule_id, "limit": limit, "offset": offset}
            )
            
            rows = result.fetchall()
            
            transactions = []
            for row in rows:
                transactions.append({
                    "id": row[0],
                    "reference_number": row[1],
                    "source_id": row[2],
                    "amount": row[3],
                    "date": row[4],
                    "reconciled_status": row[5],
                    "created_at": row[6].isoformat() if row[6] else None
                })
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error getting matched transactions: {str(e)}")
            raise

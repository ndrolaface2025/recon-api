"""
Auto-Matching Service
Automatically triggers matching rules when file uploads complete
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Dict, List, Any

from app.new_engine.matching_dispatcher import MatchingRuleDispatcher

logger = logging.getLogger(__name__)


class AutoMatchingService:
    """
    Service to automatically execute matching rules based on channel configuration
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.dispatcher = MatchingRuleDispatcher(db)
    
    async def trigger_matching_for_channel(
        self,
        channel_id: int,
        source_id: int,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Automatically trigger all active matching rules for a channel
        
        Args:
            channel_id: Channel ID that just received new transactions
            source_id: Source ID of the uploaded file
            dry_run: If True, only analyze without updating
            
        Returns:
            {
                "status": "success",
                "channel_id": int,
                "rules_executed": int,
                "results": List[Dict],
                "message": str
            }
        """
        try:
            logger.info(
                f"Auto-matching triggered for channel {channel_id}, "
                f"source {source_id}"
            )
            
            # Step 1: Get all active matching rules for this channel
            rules = await self._get_active_rules_for_channel(channel_id)
            
            if not rules or len(rules) == 0:
                logger.warning(f"No active matching rules found for channel {channel_id}")
                return {
                    "status": "success",
                    "channel_id": channel_id,
                    "rules_executed": 0,
                    "results": [],
                    "message": f"No active matching rules configured for channel {channel_id}"
                }
            
            logger.info(f"Found {len(rules)} active rules for channel {channel_id}")
            
            # Step 2: Execute each rule
            results = []
            for rule in rules:
                rule_id = rule["id"]
                rule_name = rule["rule_name"]
                match_type = rule["match_type"]
                
                # Determine min_sources for partial matching
                # For 3-way and higher, allow partial matches (2 out of 3, etc.)
                min_sources = None
                if match_type == "3-way":
                    min_sources = 2  # Allow 2-source partial matches
                elif match_type == "4-way":
                    min_sources = 2  # Allow 2-source partial matches
                # For 2-way, keep None (requires both sources)
                
                try:
                    logger.info(
                        f"Executing rule {rule_id} ({rule_name}) "
                        f"- {match_type} matching (min_sources={min_sources})"
                    )
                    
                    # Execute the matching rule
                    result = await self.dispatcher.execute_matching_rule(
                        rule_id=rule_id,
                        channel_id=channel_id,
                        dry_run=dry_run,
                        min_sources=min_sources
                    )
                    
                    results.append({
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "match_type": match_type,
                        "status": "success",
                        "matched_count": result.get("matched_count", 0),
                        "execution_time_ms": result.get("execution_time_ms", 0)
                    })
                    
                    logger.info(
                        f"Rule {rule_id} executed successfully: "
                        f"{result.get('matched_count', 0)} matches found"
                    )
                    
                except Exception as rule_error:
                    logger.error(f"Error executing rule {rule_id}: {str(rule_error)}")
                    results.append({
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "match_type": match_type,
                        "status": "error",
                        "error": str(rule_error)
                    })
            
            # Step 3: Return summary
            successful_rules = sum(1 for r in results if r["status"] == "success")
            total_matches = sum(r.get("matched_count", 0) for r in results if r["status"] == "success")
            
            return {
                "status": "success",
                "channel_id": channel_id,
                "rules_executed": successful_rules,
                "total_matches": total_matches,
                "results": results,
                "message": f"Executed {successful_rules}/{len(rules)} rules, found {total_matches} matches"
            }
            
        except Exception as e:
            logger.error(f"Error in auto-matching for channel {channel_id}: {str(e)}")
            return {
                "status": "error",
                "channel_id": channel_id,
                "rules_executed": 0,
                "results": [],
                "message": f"Auto-matching failed: {str(e)}",
                "error": str(e)
            }
    
    async def _get_active_rules_for_channel(
        self,
        channel_id: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch all active matching rules for a channel
        
        Returns:
            List of rules with id, rule_name, and match_type (2-way, 3-way, etc.)
        """
        query = """
            SELECT 
                id,
                rule_name,
                channel_id,
                conditions,
                tolerance,
                COALESCE(
                    conditions::json->>'matching_type',
                    CASE 
                        WHEN json_array_length(conditions::json->'sources') = 2 THEN '2-way'
                        WHEN json_array_length(conditions::json->'sources') = 3 THEN '3-way'
                        WHEN json_array_length(conditions::json->'sources') = 4 THEN '4-way'
                        ELSE CONCAT(json_array_length(conditions::json->'sources'), '-way')
                    END
                ) as match_type,
                status
            FROM tbl_cfg_matching_rule
            WHERE channel_id = :channel_id
            AND status = 1
            ORDER BY id ASC
        """
        
        result = await self.db.execute(
            text(query),
            {"channel_id": channel_id}
        )
        
        rows = result.fetchall()
        
        return [
            {
                "id": row.id,
                "rule_name": row.rule_name,
                "channel_id": row.channel_id,
                "match_type": row.match_type,
                "conditions": row.conditions,
                "tolerance": row.tolerance
            }
            for row in rows
        ]
    
    async def check_source_readiness(
        self,
        channel_id: int,
        rule_id: int
    ) -> Dict[str, Any]:
        """
        Check if all required sources have data for a specific rule
        
        This helps determine if a rule should be executed or needs to wait
        for more source uploads.
        
        Returns:
            {
                "ready": bool,
                "required_sources": List[str],
                "available_sources": List[str],
                "missing_sources": List[str],
                "transaction_counts": Dict[str, int]
            }
        """
        # Get rule configuration
        query_rule = """
            SELECT conditions
            FROM tbl_cfg_matching_rule
            WHERE id = :rule_id AND channel_id = :channel_id
        """
        
        result = await self.db.execute(
            text(query_rule),
            {"rule_id": rule_id, "channel_id": channel_id}
        )
        
        row = result.fetchone()
        if not row:
            return {
                "ready": False,
                "error": f"Rule {rule_id} not found"
            }
        
        conditions = row.conditions
        required_sources = conditions.get("sources", [])
        
        # Check transaction counts for each source
        query_counts = """
            SELECT 
                s.source_name,
                COUNT(t.id) as txn_count
            FROM tbl_cfg_source s
            LEFT JOIN tbl_txn_transactions t ON t.source_id = s.id 
                AND t.channel_id = :channel_id
                AND t.match_status IS NULL  -- Only unmatched transactions
            WHERE s.source_name = ANY(:sources)
            GROUP BY s.source_name
        """
        
        result = await self.db.execute(
            text(query_counts),
            {
                "channel_id": channel_id,
                "sources": required_sources
            }
        )
        
        counts = result.fetchall()
        
        transaction_counts = {row.source_name: row.txn_count for row in counts}
        available_sources = [
            source for source in required_sources 
            if transaction_counts.get(source, 0) > 0
        ]
        missing_sources = [
            source for source in required_sources
            if transaction_counts.get(source, 0) == 0
        ]
        
        return {
            "ready": len(missing_sources) == 0,
            "required_sources": required_sources,
            "available_sources": available_sources,
            "missing_sources": missing_sources,
            "transaction_counts": transaction_counts
        }

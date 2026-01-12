from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.db.new_repositories.matching_execution_repository import MatchingExecutionRepository
from app.api.v2.schemas.matching_execution import (
    MatchingExecutionRequest,
    MatchingExecutionResponse,
    MatchingExecutionError
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/new/matching-execution", tags=["New Matching Execution"])


@router.post("/execute",response_model=MatchingExecutionResponse, status_code=status.HTTP_200_OK, summary="Execute matching rule",
        description="""
            Execute a matching rule to find and match transactions.
            
            **Unified Application Layer:**
                All rules (SIMPLE and COMPLEX) are processed through the Python matching engine
                for consistency, flexibility, and maintainability.
            
            **Features:**
                - N-way matching (2, 3, 4+ sources)
                - AND/OR logic with proper precedence  
                - Nested condition groups
                - Source-specific matching
                - Amount/time tolerance
                - Full and partial matching
                - Dry-run analysis
            
            **Process:**
                1. Fetches active matching rule from database
                2. Analyzes rule complexity (informational)
                3. Executes via application layer matching engine
                4. Updates matched transactions with match_status and reconciled_status
                5. Returns results with execution metrics
            
            **Parameters:**
                - `rule_id`: ID of the matching rule to execute (required)
                - `channel_id`: Optional channel filter
                - `dry_run`: If true, analyzes without executing
                - `min_sources`: Minimum sources for partial matching (None = all sources required)
        """,
        responses={
            200: {
                "description": "Matching executed successfully",
                "content": {
                    "application/json": {
                        "example": {
                            "rule_id": 1,
                            "matched_count": 15,
                            "transaction_ids": [101, 102, 103, 201, 202, 203],
                            "execution_time_ms": 245,
                            "match_type": "FULL",
                            "message": "Successfully matched 15 transactions using rule 1 (FULL match)"
                        }
                    }
                }
            },
            404: {
                "description": "Matching rule not found or inactive",
                "model": MatchingExecutionError
            },
            500: {
                "description": "Internal server error during execution",
                "model": MatchingExecutionError
            }
        }
    )
async def execute_matching_rule(request: MatchingExecutionRequest, db: AsyncSession = Depends(get_db)):
    """
    Execute a matching rule to find and reconcile transactions
    """
    try:
        repo = MatchingExecutionRepository(db)
        
        result = await repo.execute_matching_rule(
            rule_id=request.rule_id,
            channel_id=request.channel_id,
            dry_run=request.dry_run,
            min_sources=request.min_sources
        )
        
        # Handle dry run response
        if request.dry_run:
            return {
                "rule_id": result["rule_id"],
                "matched_count": 0,
                "transaction_ids": [],
                "execution_time_ms": 0,
                "match_type": result.get("match_type", "FULL"),
                "message": result["message"]
            }
        
        # Build success message
        match_type = result.get("match_type", "FULL")
        message = f"Successfully matched {result['matched_count']} transactions using rule {result['rule_id']} ({match_type} match)"
        if result['matched_count'] == 0:
            message = f"No matching transactions found for rule {result['rule_id']}"
        
        return MatchingExecutionResponse(
            rule_id=result["rule_id"],
            matched_count=result["matched_count"],
            transaction_ids=result["transaction_ids"],
            execution_time_ms=result["execution_time_ms"],
            match_type=match_type,
            message=message
        )
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error executing matching rule {request.rule_id}: {error_msg}")
        
        # Handle specific PostgreSQL errors
        if "not found or inactive" in error_msg.lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Rule not found",
                    "detail": f"Matching rule {request.rule_id} not found or inactive",
                    "rule_id": request.rule_id
                }
            )
        elif "at least 2 sources required" in error_msg.lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid rule configuration",
                    "detail": "At least 2 sources are required for matching",
                    "rule_id": request.rule_id
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error": "Execution failed",
                    "detail": error_msg,
                    "rule_id": request.rule_id
                }
            )
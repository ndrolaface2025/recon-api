from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.db.repositories.matching_execution_repository import MatchingExecutionRepository
from app.api.v1.schemas.matching_execution import (
    MatchingExecutionRequest,
    MatchingExecutionResponse,
    DryRunResponse,
    MatchingExecutionError
)
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/matching-execution", tags=["Matching Execution"])


@router.post(
    "/execute",
    response_model=MatchingExecutionResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute matching rule",
    description="""
    Execute a matching rule stored procedure to find and match transactions.
    
    **Features:**
    - Dynamically reads rule conditions from database
    - Supports N-way matching (2, 3, or more sources)
    - Applies tolerance for amount matching
    - Updates matched transactions with match_status and reconciled_status
    - Returns execution metrics
    
    **Process:**
    1. Fetches active matching rule from database
    2. Extracts match conditions and sources
    3. Dynamically builds SQL query
    4. Executes matching logic
    5. Updates matched transactions
    6. Returns results with execution time
    
    **Parameters:**
    - `rule_id`: ID of the matching rule to execute (required)
    - `channel_id`: Optional channel filter
    - `dry_run`: If true, shows generated SQL without executing
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
                        "message": "Successfully matched 15 transactions using rule 1"
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
async def execute_matching_rule(
    request: MatchingExecutionRequest,
    db: AsyncSession = Depends(get_db)
):
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


@router.get(
    "/statistics",
    response_model=Dict[str, Any],
    summary="Get matching statistics",
    description="Retrieve statistics about matched transactions, optionally filtered by rule ID"
)
async def get_matching_statistics(
    rule_id: Optional[int] = Query(None, description="Optional rule ID to filter statistics"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get statistics about matched transactions
    """
    try:
        repo = MatchingExecutionRepository(db)
        statistics = await repo.get_matching_statistics(rule_id)
        return statistics
        
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve statistics: {str(e)}"
        )


@router.get(
    "/matched-transactions/{rule_id}",
    response_model=List[Dict[str, Any]],
    summary="Get matched transactions",
    description="Retrieve list of transactions matched by a specific rule with pagination"
)
async def get_matched_transactions(
    rule_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of transactions matched by a specific rule
    """
    try:
        repo = MatchingExecutionRepository(db)
        transactions = await repo.get_matched_transactions(
            rule_id=rule_id,
            limit=limit,
            offset=offset
        )
        return transactions
        
    except Exception as e:
        logger.error(f"Error getting matched transactions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve matched transactions: {str(e)}"
        )


@router.post(
    "/bulk-execute",
    response_model=List[MatchingExecutionResponse],
    summary="Execute multiple matching rules",
    description="Execute all active matching rules for a channel or all channels"
)
async def bulk_execute_matching_rules(
    channel_id: Optional[int] = Query(None, description="Optional channel ID to filter rules"),
    db: AsyncSession = Depends(get_db)
):
    """
    Execute all active matching rules, optionally filtered by channel
    """
    try:
        from app.db.repositories.matching_rule_repository import MatchingRuleRepository
        
        # Get all active rules
        rule_repo = MatchingRuleRepository(db)
        
        if channel_id:
            rules = await rule_repo.get_by_channel(channel_id)
        else:
            rules_response = await rule_repo.get_all(page=1, page_size=1000)
            rules = rules_response["data"]
        
        # Execute each rule
        execution_repo = MatchingExecutionRepository(db)
        results = []
        
        for rule in rules:
            try:
                result = await execution_repo.execute_matching_rule(
                    rule_id=rule.id,
                    channel_id=channel_id,
                    dry_run=False
                )
                
                message = f"Successfully matched {result['matched_count']} transactions using rule {result['rule_id']}"
                if result['matched_count'] == 0:
                    message = f"No matching transactions found for rule {result['rule_id']}"
                
                results.append(MatchingExecutionResponse(
                    rule_id=result["rule_id"],
                    matched_count=result["matched_count"],
                    transaction_ids=result["transaction_ids"],
                    execution_time_ms=result["execution_time_ms"],
                    message=message
                ))
                
            except Exception as rule_error:
                logger.error(f"Error executing rule {rule.id}: {str(rule_error)}")
                # Continue with next rule
                results.append(MatchingExecutionResponse(
                    rule_id=rule.id,
                    matched_count=0,
                    transaction_ids=[],
                    execution_time_ms=0,
                    message=f"Failed: {str(rule_error)}"
                ))
        
        return results
        
    except Exception as e:
        logger.error(f"Error in bulk execution: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bulk execution failed: {str(e)}"
        )

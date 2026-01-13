"""
Auto-Matching API Router
Endpoints for automatic matching rule execution
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.session import get_db
from app.services.auto_matching_service import AutoMatchingService
from app.workers.tasks import auto_trigger_matching

router = APIRouter(prefix="/api/v1/auto-matching", tags=["Auto Matching"])


@router.post("/trigger/{channel_id}")
async def trigger_auto_matching(
    channel_id: int,
    source_id: Optional[int] = Query(None, description="Optional source ID filter"),
    dry_run: bool = Query(False, description="If true, only analyze without updating"),
    async_mode: bool = Query(False, description="If true, queue as background task"),
    db: AsyncSession = Depends(get_db)
):
    """
    Manually trigger auto-matching for a channel.
    
    This endpoint allows you to manually run all matching rules for a channel
    without waiting for a file upload.
    
    **Use Cases:**
    - Re-run matching after fixing data issues
    - Test matching rules after configuration changes
    - Trigger matching for channels with existing unmatched transactions
    
    **Parameters:**
    - `channel_id`: Channel ID to run matching for (required)
    - `source_id`: Optional source ID filter
    - `dry_run`: If true, only analyze without updating database
    - `async_mode`: If true, queue as background task (returns immediately)
    
    **Example:**
    ```
    POST /api/v1/auto-matching/trigger/1?dry_run=false
    ```
    
    **Response:**
    ```json
    {
      "status": "success",
      "channel_id": 1,
      "rules_executed": 3,
      "total_matches": 150,
      "message": "Executed 3/3 rules, found 150 matches",
      "results": [
        {
          "rule_id": 1,
          "rule_name": "ATM-2WAY-RRN",
          "match_type": "2-way",
          "status": "success",
          "matched_count": 50,
          "execution_time_ms": 245
        },
        ...
      ]
    }
    ```
    """
    try:
        # If async mode, queue as Celery task
        if async_mode:
            task = auto_trigger_matching.delay(
                channel_id=channel_id,
                source_id=source_id or 0,
                file_id=0  # No file_id for manual trigger
            )
            
            return {
                "status": "queued",
                "message": "Auto-matching queued as background task",
                "task_id": task.id,
                "channel_id": channel_id
            }
        
        # Otherwise, execute synchronously
        service = AutoMatchingService(db)
        result = await service.trigger_matching_for_channel(
            channel_id=channel_id,
            source_id=source_id,
            dry_run=dry_run
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error triggering auto-matching: {str(e)}"
        )


@router.get("/check-readiness/{channel_id}/{rule_id}")
async def check_source_readiness(
    channel_id: int,
    rule_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Check if all required sources have data for a specific matching rule.
    
    This endpoint helps determine if a rule is ready to execute or needs
    to wait for more source uploads.
    
    **Parameters:**
    - `channel_id`: Channel ID
    - `rule_id`: Matching rule ID
    
    **Example:**
    ```
    GET /api/v1/auto-matching/check-readiness/1/4
    ```
    
    **Response:**
    ```json
    {
      "ready": true,
      "required_sources": ["ATM", "SWITCH", "CBS"],
      "available_sources": ["ATM", "SWITCH", "CBS"],
      "missing_sources": [],
      "transaction_counts": {
        "ATM": 150,
        "SWITCH": 150,
        "CBS": 150
      }
    }
    ```
    
    **Use Cases:**
    - Pre-flight check before running matching
    - Dashboard showing which rules are ready to run
    - Identify which sources need data uploads
    """
    try:
        service = AutoMatchingService(db)
        result = await service.check_source_readiness(
            channel_id=channel_id,
            rule_id=rule_id
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error checking source readiness: {str(e)}"
        )


@router.get("/rules/{channel_id}")
async def get_channel_rules(
    channel_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get all active matching rules for a channel.
    
    Shows which rules will be executed when auto-matching triggers.
    
    **Parameters:**
    - `channel_id`: Channel ID
    
    **Example:**
    ```
    GET /api/v1/auto-matching/rules/1
    ```
    
    **Response:**
    ```json
    {
      "channel_id": 1,
      "channel_name": "ATM",
      "rules": [
        {
          "id": 1,
          "rule_name": "ATM-2WAY-RRN",
          "match_type": "2-way",
          "sources": ["ATM", "SWITCH"]
        },
        {
          "id": 4,
          "rule_name": "ATM-3WAY-RRN",
          "match_type": "3-way",
          "sources": ["ATM", "SWITCH", "CBS"]
        }
      ]
    }
    ```
    """
    try:
        service = AutoMatchingService(db)
        rules = await service._get_active_rules_for_channel(channel_id)
        
        return {
            "channel_id": channel_id,
            "rules_count": len(rules),
            "rules": [
                {
                    "id": rule["id"],
                    "rule_name": rule["rule_name"],
                    "match_type": rule["match_type"],
                    "sources": rule["conditions"].get("sources", [])
                }
                for rule in rules
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching channel rules: {str(e)}"
        )

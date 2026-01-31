from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.db.session import get_db
from app.db.repositories.matching_rule_repository import MatchingRuleRepository
from app.api.v1.schemas.matching_rule import (
    MatchingRuleCreate,
    MatchingRuleUpdate,
    MatchingRuleResponse,
    MatchingRuleList
)

router = APIRouter(prefix="/api/v1/matching-rules", tags=["Matching Rules"])


@router.post("/", response_model=MatchingRuleResponse, status_code=201)
async def create_matching_rule(
    rule: MatchingRuleCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new matching rule.
    
    - **rule_name**: Name of the matching rule (required)
    - **channel_id**: ID of the channel (required)
    - **rule_desc**: Description of the rule
    - **conditions**: JSON string of matching conditions
    - **tolerance**: JSON string of tolerance settings
    - **status**: 1=Active, 0=Inactive
    """
    created_rule = await MatchingRuleRepository.create(db, rule)
    return created_rule


@router.get("/", response_model=MatchingRuleList)
async def list_matching_rules(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page"),
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
    status: Optional[int] = Query(None, description="Filter by status (0=Inactive, 1=Active)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get a paginated list of matching rules.
    
    - **page**: Page number (default: 1)
    - **page_size**: Number of items per page (default: 10, max: 100)
    - **channel_id**: Optional filter by channel ID
    - **status**: Optional filter by status
    """
    skip = (page - 1) * page_size
    rules, total = await MatchingRuleRepository.get_all(
        db,
        skip=skip,
        limit=page_size,
        channel_id=channel_id,
        status=status
    )
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "rules": rules
    }


@router.get("/search", response_model=list[MatchingRuleResponse])
async def get_matching_rules_by_filters(
    channel_id: int = Query(..., description="Channel ID (required)"),
    network_id: Optional[int] = Query(None, description="Network ID (optional)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all active matching rules by channel_id and optional network_id.
    
    This endpoint allows you to retrieve matching rules filtered by:
    - **channel_id**: ID of the channel (required)
    - **network_id**: ID of the network (optional)
    
    If network_id is provided, only rules for that specific network will be returned.
    If network_id is not provided, all rules for the channel will be returned.
    
    Example usage:
    - GET /api/v1/matching-rules/search?channel_id=1 (get all rules for channel 1)
    - GET /api/v1/matching-rules/search?channel_id=1&network_id=5 (get rules for channel 1 and network 5)
    """
    rules = await MatchingRuleRepository.get_by_channel_and_network(
        db, 
        channel_id=channel_id, 
        network_id=network_id
    )
    
    if not rules:
        raise HTTPException(
            status_code=404, 
            detail=f"No matching rules found for channel_id={channel_id}" + 
                   (f" and network_id={network_id}" if network_id else "")
        )
    
    return rules


@router.get("/channel/{channel_id}", response_model=list[MatchingRuleResponse])
async def get_rules_by_channel(
    channel_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get all active matching rules for a specific channel.
    
    - **channel_id**: ID of the channel
    """
    rules = await MatchingRuleRepository.get_by_channel(db, channel_id)
    return rules


@router.get("/{rule_id}", response_model=MatchingRuleResponse)
async def get_matching_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get a specific matching rule by ID.
    
    - **rule_id**: ID of the matching rule
    """
    rule = await MatchingRuleRepository.get_by_id(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Matching rule with ID {rule_id} not found")
    return rule


@router.put("/{rule_id}", response_model=MatchingRuleResponse)
async def update_matching_rule(
    rule_id: int,
    rule_update: MatchingRuleUpdate,
    db: AsyncSession = Depends(get_db)
):
    """
    Update an existing matching rule.
    
    - **rule_id**: ID of the matching rule to update
    - All fields are optional - only provided fields will be updated
    """
    updated_rule = await MatchingRuleRepository.update(db, rule_id, rule_update)
    if not updated_rule:
        raise HTTPException(status_code=404, detail=f"Matching rule with ID {rule_id} not found")
    return updated_rule


@router.delete("/{rule_id}", status_code=200)
async def delete_matching_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Delete a matching rule.
    
    - **rule_id**: ID of the matching rule to delete
    """
    deleted = await MatchingRuleRepository.delete(db, rule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Matching rule with ID {rule_id} not found")
    return HTTPException(status_code=200, detail=f"The matching rule has been deleted successfully")

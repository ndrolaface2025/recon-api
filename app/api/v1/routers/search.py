"""
Generic Search API Router
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List

from app.db.session import get_db
from app.schemas.search_schemas import (
    SearchRequest,
    SearchResponse,
    QuickSearchRequest,
    EntityFieldsResponse
)
from app.services.search_service import SearchService

router = APIRouter(prefix="/api/v1/search", tags=["search"])


@router.post("/", response_model=SearchResponse)
async def search(
    search_request: SearchRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Generic search endpoint with advanced filtering, sorting, and pagination.
    
    ## Features:
    - **Multiple filters** with AND/OR logic
    - **Flexible operators**: eq, ne, gt, gte, lt, lte, like, ilike, in, not_in, is_null, is_not_null, between
    - **Multiple sort fields** with asc/desc order
    - **Pagination** with configurable page size
    
    ## Example Request:
    ```json
    {
        "entity": "transactions",
        "filters": [
            {"field": "channel_id", "operator": "eq", "value": 1},
            {"field": "match_status", "operator": "in", "value": [1, 2]},
            {"field": "amount", "operator": "between", "value": 1000, "value2": 5000},
            {"field": "date", "operator": "gte", "value": "2026-01-01"}
        ],
        "logic": "AND",
        "sort": [
            {"field": "created_at", "order": "desc"},
            {"field": "amount", "order": "asc"}
        ],
        "page": 1,
        "page_size": 30
    }
    ```
    
    ## Supported Entities:
    - transactions
    - manual_transactions
    - upload_files
    - channels
    - sources
    - users
    - matching_rules
    - journal_entries
    - general_ledger
    """
    try:
        return await SearchService.execute_search(search_request, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.post("/quick", response_model=SearchResponse)
async def quick_search(
    search_request: QuickSearchRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Quick text search across multiple fields of an entity.
    
    Searches for the query text in multiple fields simultaneously using ILIKE (case-insensitive).
    
    ## Example Request:
    ```json
    {
        "entity": "transactions",
        "query": "ABC123",
        "search_fields": ["txn_id", "reference_number", "account_number"],
        "page": 1,
        "page_size": 30
    }
    ```
    
    If `search_fields` is not provided, uses default searchable fields for the entity:
    - **transactions**: txn_id, reference_number, source_reference_number, account_number, recon_reference_number
    - **manual_transactions**: manual_txn_id, reference_number, source_reference_number, account_number, recon_reference_number
    - **upload_files**: file_name, file_path
    - **channels**: channel_name, channel_code
    - **sources**: source_name, source_code
    - **users**: username, email, first_name, last_name
    - **matching_rules**: rule_name, description
    - **journal_entries**: account_number, account_brn, recon_reference_number
    - **general_ledger**: general_ledger, gl_role, gl_description
    """
    try:
        return await SearchService.execute_quick_search(search_request, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quick search failed: {str(e)}")


@router.get("/entities")
async def get_available_entities():
    """
    Get list of all searchable entities.
    
    Returns a list of entity names that can be used in search requests.
    """
    return {
        "entities": SearchService.get_available_entities(),
        "total": len(SearchService.get_available_entities())
    }


@router.get("/entities/{entity_name}/fields", response_model=EntityFieldsResponse)
async def get_entity_fields(entity_name: str):
    """
    Get metadata about available fields for a specific entity.
    
    Returns:
    - All fields with their types and properties
    - Searchable fields (for quick search)
    - Filterable fields (for advanced filters)
    - Sortable fields (for sorting)
    
    ## Example:
    ```
    GET /api/v1/search/entities/transactions/fields
    ```
    """
    try:
        return SearchService.get_entity_metadata(entity_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get entity fields: {str(e)}")


@router.get("/examples")
async def get_search_examples():
    """
    Get example search requests for different use cases.
    """
    return {
        "basic_filter": {
            "entity": "transactions",
            "filters": [
                {"field": "channel_id", "operator": "eq", "value": 1}
            ],
            "page": 1,
            "page_size": 30
        },
        "multiple_filters_and": {
            "entity": "transactions",
            "filters": [
                {"field": "match_status", "operator": "eq", "value": 1},
                {"field": "channel_id", "operator": "eq", "value": 2}
            ],
            "logic": "AND",
            "page": 1,
            "page_size": 30
        },
        "multiple_filters_or": {
            "entity": "transactions",
            "filters": [
                {"field": "channel_id", "operator": "eq", "value": 1},
                {"field": "channel_id", "operator": "eq", "value": 2}
            ],
            "logic": "OR",
            "page": 1,
            "page_size": 30
        },
        "text_search": {
            "entity": "transactions",
            "filters": [
                {"field": "reference_number", "operator": "ilike", "value": "ABC"}
            ],
            "page": 1,
            "page_size": 30
        },
        "range_filter": {
            "entity": "transactions",
            "filters": [
                {"field": "amount", "operator": "between", "value": 1000, "value2": 5000}
            ],
            "page": 1,
            "page_size": 30
        },
        "date_range": {
            "entity": "transactions",
            "filters": [
                {"field": "date", "operator": "gte", "value": "2026-01-01"},
                {"field": "date", "operator": "lte", "value": "2026-01-31"}
            ],
            "logic": "AND",
            "page": 1,
            "page_size": 30
        },
        "with_sorting": {
            "entity": "transactions",
            "filters": [
                {"field": "match_status", "operator": "eq", "value": 1}
            ],
            "sort": [
                {"field": "created_at", "order": "desc"},
                {"field": "amount", "order": "asc"}
            ],
            "page": 1,
            "page_size": 30
        },
        "quick_search": {
            "entity": "transactions",
            "query": "ABC123",
            "page": 1,
            "page_size": 30
        },
        "quick_search_specific_fields": {
            "entity": "transactions",
            "query": "12345",
            "search_fields": ["txn_id", "reference_number"],
            "page": 1,
            "page_size": 30
        }
    }

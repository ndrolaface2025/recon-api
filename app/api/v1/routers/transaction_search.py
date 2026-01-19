"""
Transaction Search API Router
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any

from app.db.session import get_db
from app.schemas.transaction_search_schemas import (
    TransactionSearchRequest,
    TransactionSearchResponse,
    SmartSearchRequest
)
from app.services.transaction_search_service import TransactionSearchService
from app.utils.smart_search_detector import SmartSearchDetector

router = APIRouter(prefix="/api/v1/transactions/search", tags=["transactions-search"])


def create_api_response(
    data: Any,
    message: str = "Success",
    status: str = "success",
    error: bool = False
) -> Dict[str, Any]:
    """Create standardized API response format"""
    return {
        "status": status,
        "error": error,
        "message": message,
        "data": data
    }


@router.post("/smart")
async def smart_search_transactions(
    search_params: SmartSearchRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Smart search with auto-detection of field types from comma-separated input.
    
    ## Features:
    - **Auto-Detection**: Automatically detects if values are RRN, account number, or amount
    - **Comma-Separated**: Input multiple values separated by commas
    - **Multi-Select RRNs**: Support for RRN dropdown multi-select
    - **Date Range**: Filter by date_from and date_to
    - **Source Filter**: Filter by source_id
    
    ## Detection Rules:
    - **Amount**: 2-6 digit numbers (e.g., "2000", "5000")
    - **Account Number**: Contains 'x' or 7-11 digits (e.g., "xxxxxx7890", "1234567890")
    - **Reference Number (RRN)**: 12+ digit numbers (e.g., "427654421259")
    - **Unknown**: Searches across all text fields
    
    ## Example Request:
    ```json
    {
        "search_query": "427654421259, xxxxxx7890, 2000, 5000",
        "rrn_list": ["427654421259", "390447500669"],
        "date_from": "2026-01-01",
        "date_to": "2026-01-31",
        "source_id": 1,
        "page": 1,
        "page_size": 30
    }
    ```
    
    ## How It Works:
    1. Parse `search_query` and split by commas
    2. Auto-detect each value type (amount/account/RRN)
    3. Apply appropriate filters for each type
    4. Combine with `rrn_list` (from multi-select dropdown)
    5. Apply date range and source filters
    6. Return paginated results grouped by recon_reference_number
    
    ## Frontend Integration:
    - **Search Box**: Send value as `search_query`
    - **RRN Dropdown**: Send selected RRNs as `rrn_list` array
    - **Date Picker**: Send as `date_from` and `date_to`
    - **Filters**: Send as `source_id`
    """
    try:
        # Show detection summary if search_query is provided
        if search_params.search_query:
            summary = SmartSearchDetector.get_detection_summary(search_params.search_query)
            print(f"🔍 Smart Search Detection: {summary}")
        
        result = await TransactionSearchService.smart_search(search_params, db)
        return create_api_response(
            data=result.model_dump(),
            message="Transactions retrieved successfully"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Smart search failed: {str(e)}")


@router.post("/")
async def search_transactions(
    search_params: TransactionSearchRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Search transactions by reference number, account number, date range, amount, and source.
    
    ## Features:
    - **Reference Number**: Partial match, case-insensitive
    - **Account Number**: Partial match, case-insensitive  
    - **Date Range**: Filter by date_from and date_to (format: YYYY-MM-DD)
    - **Amount**: Exact amount match
    - **Source**: Filter by source_id
    - **Pagination**: Configurable page and page_size
    
    ## Example Request:
    ```json
    {
        "reference_number": "REF123",
        "account_number": "1234567890",
        "date_from": "2026-01-01",
        "date_to": "2026-01-31",
        "amount": "2000",
        "source_id": 1,
        "page": 1,
        "page_size": 30
    }
    ```
    
    ## Response:
    Returns matching transactions grouped by recon_reference_number with:
    - Transactions separated by source type (ATM, SWITCH, CBS, etc.)
    - Complete transaction details including match status and reconciliation info
    - Pagination metadata
    - Summary statistics (matched, partial, unmatched)
    
    ## Query Parameters:
    All parameters are optional. If no parameters provided, returns all transactions (paginated).
    """
    try:
        result = await TransactionSearchService.search_transactions(search_params, db)
        return create_api_response(
            data=result.model_dump(),
            message="Transactions retrieved successfully"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction search failed: {str(e)}")


@router.get("/")
async def search_transactions_get(
    reference_number: Optional[str] = Query(None, description="Transaction reference number (partial match)"),
    account_number: Optional[str] = Query(None, description="Account number (partial match)"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    amount: Optional[str] = Query(None, description="Exact amount"),
    source_id: Optional[int] = Query(None, description="Source ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(30, ge=1, le=100, description="Records per page"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search transactions using GET method with query parameters.
    
    Alternative endpoint that accepts search parameters as query strings instead of JSON body.
    
    ## Example:
    ```
    GET /api/v1/transactions/search/?reference_number=427654421259&date_from=2026-01-01&date_to=2026-01-31&page=1&page_size=30
    ```
    
    ## Query Parameters:
    - **reference_number**: Partial match, case-insensitive
    - **account_number**: Partial match, case-insensitive
    - **date_from**: Start date in YYYY-MM-DD format
    - **date_to**: End date in YYYY-MM-DD format
    - **amount**: Exact amount value
    - **source_id**: Filter by specific source
    - **page**: Page number (default: 1)
    - **page_size**: Records per page (default: 30, max: 100)
    
    All parameters are optional.
    """
    try:
        search_params = TransactionSearchRequest(
            reference_number=reference_number,
            account_number=account_number,
            date_from=date_from,
            date_to=date_to,
            amount=amount,
            source_id=source_id,
            page=page,
            page_size=page_size
        )
        result = await TransactionSearchService.search_transactions(search_params, db)
        return create_api_response(
            data=result.model_dump(),
            message="Transactions retrieved successfully"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction search failed: {str(e)}")


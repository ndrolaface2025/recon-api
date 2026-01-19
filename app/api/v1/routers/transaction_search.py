"""
Transaction Search API Router
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.session import get_db
from app.schemas.transaction_search_schemas import (
    TransactionSearchRequest,
    TransactionSearchResponse
)
from app.services.transaction_search_service import TransactionSearchService

router = APIRouter(prefix="/api/v1/transactions/search", tags=["transactions-search"])


@router.post("/", response_model=TransactionSearchResponse)
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
    - **Amount Range**: Filter by amount_min and amount_max
    - **Source**: Filter by source_id
    - **Pagination**: Configurable page and page_size
    
    ## Example Request:
    ```json
    {
        "reference_number": "REF123",
        "account_number": "1234567890",
        "date_from": "2026-01-01",
        "date_to": "2026-01-31",
        "amount_min": 1000.0,
        "amount_max": 5000.0,
        "source_id": 1,
        "page": 1,
        "page_size": 20
    }
    ```
    
    ## Response:
    Returns matching transactions with:
    - Reference Number
    - Account Number
    - Date
    - Amount
    - Source (ID and Name)
    - Additional transaction details (txn_id, reconciliation status, etc.)
    - Pagination metadata (total records, pages, etc.)
    
    ## Query Parameters:
    All parameters are optional. If no parameters provided, returns all transactions (paginated).
    """
    try:
        return await TransactionSearchService.search_transactions(search_params, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction search failed: {str(e)}")


@router.get("/", response_model=TransactionSearchResponse)
async def search_transactions_get(
    reference_number: Optional[str] = Query(None, description="Transaction reference number (partial match)"),
    account_number: Optional[str] = Query(None, description="Account number (partial match)"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    amount_min: Optional[float] = Query(None, description="Minimum amount"),
    amount_max: Optional[float] = Query(None, description="Maximum amount"),
    source_id: Optional[int] = Query(None, description="Source ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Records per page"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search transactions using GET method with query parameters.
    
    Alternative endpoint that accepts search parameters as query strings instead of JSON body.
    
    ## Example:
    ```
    GET /api/v1/transactions/search/?reference_number=REF123&date_from=2026-01-01&date_to=2026-01-31&page=1&page_size=20
    ```
    
    ## Query Parameters:
    - **reference_number**: Partial match, case-insensitive
    - **account_number**: Partial match, case-insensitive
    - **date_from**: Start date in YYYY-MM-DD format
    - **date_to**: End date in YYYY-MM-DD format
    - **amount_min**: Minimum amount value
    - **amount_max**: Maximum amount value
    - **source_id**: Filter by specific source
    - **page**: Page number (default: 1)
    - **page_size**: Records per page (default: 20, max: 100)
    
    All parameters are optional.
    """
    try:
        search_params = TransactionSearchRequest(
            reference_number=reference_number,
            account_number=account_number,
            date_from=date_from,
            date_to=date_to,
            amount_min=amount_min,
            amount_max=amount_max,
            source_id=source_id,
            page=page,
            page_size=page_size
        )
        return await TransactionSearchService.search_transactions(search_params, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction search failed: {str(e)}")

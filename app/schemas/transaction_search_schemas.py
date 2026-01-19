"""
Transaction Search API Schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class TransactionSearchRequest(BaseModel):
    """Request model for transaction search"""
    reference_number: Optional[str] = Field(None, description="Transaction reference number (partial match supported)")
    account_number: Optional[str] = Field(None, description="Account number (partial match supported)")
    date_from: Optional[str] = Field(None, description="Start date for date range filter (format: YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="End date for date range filter (format: YYYY-MM-DD)")
    amount_min: Optional[float] = Field(None, description="Minimum amount")
    amount_max: Optional[float] = Field(None, description="Maximum amount")
    source_id: Optional[int] = Field(None, description="Source ID")
    page: int = Field(1, ge=1, description="Page number (starts from 1)")
    page_size: int = Field(20, ge=1, le=100, description="Number of records per page")

    class Config:
        json_schema_extra = {
            "example": {
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
        }


class TransactionSearchItem(BaseModel):
    """Individual transaction item in search results"""
    id: int
    reference_number: Optional[str] = None
    account_number: Optional[str] = None
    date: Optional[str] = None
    amount: Optional[str] = None
    source_id: Optional[int] = None
    source_name: Optional[str] = None  # Joined from source table
    txn_id: Optional[str] = None
    recon_reference_number: Optional[str] = None
    channel_id: Optional[int] = None
    reconciled_status: Optional[bool] = None
    match_status: Optional[int] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class TransactionSearchResponse(BaseModel):
    """Response model for transaction search"""
    data: List[TransactionSearchItem]
    total: int = Field(..., description="Total number of matching records")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Records per page")
    total_pages: int = Field(..., description="Total number of pages")

    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {
                        "id": 1,
                        "reference_number": "REF123456",
                        "account_number": "1234567890",
                        "date": "2026-01-15",
                        "amount": "2500.00",
                        "source_id": 1,
                        "source_name": "CBS",
                        "txn_id": "TXN001",
                        "recon_reference_number": "RECON001",
                        "channel_id": 1,
                        "reconciled_status": False,
                        "match_status": 1,
                        "created_at": "2026-01-15T10:30:00"
                    }
                ],
                "total": 100,
                "page": 1,
                "page_size": 20,
                "total_pages": 5
            }
        }

"""
Transaction Search API Schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class SmartSearchRequest(BaseModel):
    """Smart search request that auto-detects field types from comma-separated input"""
    search_query: Optional[str] = Field(None, description="Comma-separated values that will be auto-detected as RRN, account, or amount")
    rrn_list: Optional[List[str]] = Field(None, description="List of Reference Numbers (RRNs) for multi-select")
    date_from: Optional[str] = Field(None, description="Start date for date range filter (format: YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="End date for date range filter (format: YYYY-MM-DD)")
    source_id: Optional[int] = Field(None, description="Source ID")
    page: int = Field(1, ge=1, description="Page number (starts from 1)")
    page_size: int = Field(30, ge=1, le=100, description="Number of records per page")

    class Config:
        json_schema_extra = {
            "example": {
                "search_query": "427654421259, xxxxxx7890, 2000, 5000",
                "rrn_list": ["427654421259", "390447500669"],
                "date_from": "2026-01-01",
                "date_to": "2026-01-31",
                "source_id": 1,
                "page": 1,
                "page_size": 30
            }
        }


class TransactionSearchRequest(BaseModel):
    """Request model for transaction search"""
    reference_number: Optional[str] = Field(None, description="Transaction reference number (partial match supported)")
    account_number: Optional[str] = Field(None, description="Account number (partial match supported)")
    date_from: Optional[str] = Field(None, description="Start date for date range filter (format: YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="End date for date range filter (format: YYYY-MM-DD)")
    amount: Optional[str] = Field(None, description="Exact amount to search for")
    source_id: Optional[int] = Field(None, description="Source ID")
    page: int = Field(1, ge=1, description="Page number (starts from 1)")
    page_size: int = Field(30, ge=1, le=100, description="Number of records per page")

    class Config:
        json_schema_extra = {
            "example": {
                "reference_number": "REF123",
                "account_number": "1234567890",
                "date_from": "2026-01-01",
                "date_to": "2026-01-31",
                "amount": "2000",
                "source_id": 1,
                "page": 1,
                "page_size": 30
            }
        }


class TransactionDetailItem(BaseModel):
    """Detailed transaction item with all fields"""
    id: int
    recon_reference_number: Optional[str] = None
    channel_id: Optional[int] = None
    channel_name: Optional[str] = None
    source_id: Optional[int] = None
    source_name: Optional[str] = None
    reference_number: Optional[str] = None
    source_reference_number: Optional[str] = None
    amount: Optional[str] = None
    date: Optional[str] = None
    account_number: Optional[str] = None
    currency: Optional[str] = None
    match_status: Optional[int] = None
    other_details: Optional[str] = None
    match_status_label: Optional[str] = None
    match_rule_id: Optional[int] = None
    match_rule_name: Optional[str] = None
    match_condition: Optional[str] = None
    reconciled_status: Optional[bool] = None
    reconciled_by: Optional[int] = None
    comment: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True


class GroupedTransactions(BaseModel):
    """Grouped transactions by source type"""
    atm_transactions: List[TransactionDetailItem] = []
    switch_transactions: List[TransactionDetailItem] = []
    cbs_transactions: List[TransactionDetailItem] = []
    network_transactions: List[TransactionDetailItem] = []
    settlement_transactions: List[TransactionDetailItem] = []
    ej_transactions: List[TransactionDetailItem] = []
    platform_transactions: List[TransactionDetailItem] = []
    pos_transactions: List[TransactionDetailItem] = []


class PaginationMeta(BaseModel):
    """Pagination metadata"""
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool
    has_previous: bool


class SummaryStats(BaseModel):
    """Summary statistics"""
    total_matched: int = 0
    total_partial: int = 0
    total_unmatched: int = 0


class TransactionSearchResponse(BaseModel):
    """Response model for transaction search with grouped data"""
    transactions: List[GroupedTransactions]
    pagination: PaginationMeta
    summary: SummaryStats

    class Config:
        json_schema_extra = {
            "example": {
                "transactions": [
                    {
                        "atm_transactions": [
                            {
                                "id": 949943,
                                "recon_reference_number": "REF-20260116-151857-4858",
                                "reference_number": "427654421259",
                                "amount": "5000",
                                "account_number": "xxxxxx7890",
                                "currency": "inr",
                                "match_status": 1,
                                "source_name": "ATM",
                                "channel_name": "ATM"
                            }
                        ],
                        "switch_transactions": [],
                        "cbs_transactions": []
                    }
                ],
                "pagination": {
                    "page": 1,
                    "page_size": 30,
                    "total_records": 99,
                    "total_pages": 4,
                    "has_next": True,
                    "has_previous": False
                },
                "summary": {
                    "total_matched": 300,
                    "total_partial": 0,
                    "total_unmatched": 0
                }
            }
        }


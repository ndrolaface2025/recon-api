from pydantic import BaseModel
from datetime import datetime
from typing import Any, Optional


class ReconRunResponse(BaseModel):
    report_id: str
    channel_id: int
    channel_name: str

    recon_date: Optional[str]
    start_time: datetime
    end_time: datetime
    execution_seconds: int

    status: str

    total: int
    matched: int
    partial: int
    unmatched: int
    match_rate: float

    maker_id: Optional[int]
    maker_name: Optional[str]
    checker_id: Optional[int]
    checker_name: Optional[str]


class PaginatedReconResponse(BaseModel):
    page: int
    size: int
    total_records: int
    data: list[ReconRunResponse]


class ReconUploadedFileResponse(BaseModel):
    file_id: int
    file_name: str
    source: str
    uploaded_at: Optional[datetime]
    uploaded_by: str
    total: int
    processed: int
    success: int
    duplicate: int
    failed: int


class ReconRuleResponse(BaseModel):
    rule_id: int
    rule_name: str
    channel_id: int
    channel_name: str
    rule_desc: Optional[str]
    conditions: Optional[Any]
    tolerance: Optional[Any]
    status: int
    created_at: Optional[datetime]


class ReconModeBreakdownResponse(BaseModel):
    type: str  # AUTOMATIC | MANUAL
    total: int
    matched: int
    partial: int
    unmatched: int
    match_rate: float


class ReconTransactionResponse(BaseModel):
    id: int
    txn_id: Optional[str]
    reference_number: Optional[str]
    amount: Optional[str]
    ccy: Optional[str]
    account_number: Optional[str]

    match_status: Optional[int]
    reconciled_mode: int

    created_at: Optional[datetime]

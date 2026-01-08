from pydantic import BaseModel, Field
from typing import Optional, List


class MatchingExecutionRequest(BaseModel):
    """Request schema for executing matching rules"""
    rule_id: int = Field(..., description="ID of the matching rule to execute", gt=0)
    channel_id: Optional[int] = Field(None, description="Optional channel ID filter", gt=0)
    dry_run: bool = Field(False, description="If true, only shows generated SQL without executing")
    min_sources: Optional[int] = Field(None, description="Minimum sources required (NULL=all, 2=partial 2-way, etc.)", ge=2)

    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": 1,
                "channel_id": 2,
                "dry_run": False,
                "min_sources": 2
            }
        }


class MatchingExecutionResponse(BaseModel):
    """Response schema for matching execution results"""
    rule_id: int = Field(..., description="ID of the executed rule")
    matched_count: int = Field(..., description="Number of transactions matched")
    transaction_ids: List[int] = Field(..., description="Array of matched transaction IDs")
    execution_time_ms: int = Field(..., description="Execution time in milliseconds")
    match_type: str = Field(..., description="Match type: FULL or PARTIAL")
    message: str = Field(..., description="Execution status message")

    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": 1,
                "matched_count": 15,
                "transaction_ids": [101, 102, 103, 201, 202, 203],
                "execution_time_ms": 245,
                "match_type": "FULL",
                "message": "Successfully matched 15 transactions using rule 1 (FULL match)"
            }
        }


class DryRunResponse(BaseModel):
    """Response schema for dry run execution"""
    rule_id: int
    generated_sql: str = Field(..., description="SQL that would be executed")
    source_ids: List[int] = Field(..., description="Source IDs involved in matching")
    message: str = Field(..., description="Dry run status")

    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": 1,
                "generated_sql": "SELECT ... FROM tbl_txn_transactions ...",
                "source_ids": [1, 2, 3],
                "message": "Dry run completed. Check generated_sql for the query that would be executed."
            }
        }


class MatchingExecutionError(BaseModel):
    """Error response schema"""
    error: str
    detail: str
    rule_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Rule not found",
                "detail": "Matching rule 999 not found or inactive",
                "rule_id": 999
            }
        }

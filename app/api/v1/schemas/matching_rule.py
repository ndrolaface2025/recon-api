from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class MatchingRuleBase(BaseModel):
    rule_name: Optional[str] = Field(None, max_length=255, description="Name of the matching rule")
    channel_id: Optional[int] = Field(None, description="ID of the channel this rule belongs to")
    rule_desc: Optional[str] = Field(None, description="Description of the matching rule")
    conditions: Optional[str] = Field(None, description="JSON string of matching conditions")
    tolerance: Optional[str] = Field(None, description="JSON string of tolerance settings")
    status: Optional[int] = Field(1, description="Status: 1=Active, 0=Inactive")


class MatchingRuleCreate(MatchingRuleBase):
    """Schema for creating a new matching rule"""
    rule_name: str = Field(..., min_length=1, max_length=255, description="Name of the matching rule")
    channel_id: int = Field(..., description="ID of the channel this rule belongs to")
    created_by: Optional[int] = Field(None, description="User ID who created the rule")


class MatchingRuleUpdate(MatchingRuleBase):
    """Schema for updating an existing matching rule"""
    updated_by: Optional[int] = Field(None, description="User ID who updated the rule")


class MatchingRuleResponse(MatchingRuleBase):
    """Schema for matching rule response"""
    id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    version_number: Optional[int] = None

    class Config:
        from_attributes = True


class MatchingRuleList(BaseModel):
    """Schema for paginated list response"""
    total: int
    page: int
    page_size: int
    rules: list[MatchingRuleResponse]

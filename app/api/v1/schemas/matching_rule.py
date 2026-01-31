from pydantic import BaseModel, Field, field_serializer
from typing import Optional, Union, Dict, Any
from datetime import datetime
import json


class ChannelInfo(BaseModel):
    """Channel information schema"""
    id: int
    channel_name: Optional[str] = None
    channel_description: Optional[str] = None
    status: Optional[bool] = None
    
    class Config:
        from_attributes = True


class MatchingRuleBase(BaseModel):
    rule_name: Optional[str] = Field(None, max_length=255, description="Name of the matching rule")
    channel_id: Optional[int] = Field(None, description="ID of the channel this rule belongs to")
    rule_desc: Optional[str] = Field(None, description="Description of the matching rule")
    conditions: Optional[Union[str, Dict[str, Any]]] = Field(None, description="Matching conditions (JSON string or object)")
    tolerance: Optional[Union[str, Dict[str, Any]]] = Field(None, description="Tolerance settings (JSON string or object)")
    status: Optional[int] = Field(1, description="Status: 1=Active, 0=Inactive")
    
    @field_serializer('conditions', 'tolerance')
    def serialize_json_field(self, value: Optional[Union[str, Dict[str, Any]]]) -> Optional[str]:
        """Serialize JSON fields to string for API response"""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value)


class MatchingRuleCreate(MatchingRuleBase):
    """Schema for creating a new matching rule"""
    rule_name: str = Field(..., min_length=1, max_length=255, description="Name of the matching rule")
    channel_id: int = Field(..., description="ID of the channel this rule belongs to")
    created_by: Optional[int] = Field(None, description="User ID who created the rule")
    network_id: Optional[int] = Field(None, description="Network ID associated with the rule")


class MatchingRuleUpdate(MatchingRuleBase):
    """Schema for updating an existing matching rule"""
    updated_by: Optional[int] = Field(None, description="User ID who updated the rule")
    network_id: Optional[int] = Field(None, description="Network ID associated with the rule")


class MatchingRuleResponse(BaseModel):
    """Schema for matching rule response with channel details"""
    id: int
    rule_name: str = Field(..., description="Name of the matching rule")
    channel: Optional[ChannelInfo] = None  # Include channel details instead of channel_id
    rule_desc: Optional[str] = None
    conditions: Optional[Union[str, Dict[str, Any]]] = None
    tolerance: Optional[Union[str, Dict[str, Any]]] = None
    status: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    version_number: Optional[int] = None
    network_id: Optional[int] = None
    
    @field_serializer('conditions', 'tolerance')
    def serialize_json_field(self, value: Optional[Union[str, Dict[str, Any]]]) -> Optional[str]:
        """Serialize JSON fields to string for API response"""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value)

    class Config:
        from_attributes = True


class MatchingRuleList(BaseModel):
    """Schema for paginated list response"""
    total: int
    page: int
    page_size: int
    rules: list[MatchingRuleResponse]

"""
Schemas for generic search functionality
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime


class SearchFilter(BaseModel):
    """Individual search filter"""
    field: str = Field(..., description="Field name to filter on")
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "like", "ilike", "in", "not_in", "is_null", "is_not_null", "between"] = Field(
        ..., 
        description="Comparison operator"
    )
    value: Optional[Any] = Field(None, description="Value to compare against")
    value2: Optional[Any] = Field(None, description="Second value for 'between' operator")


class SearchSort(BaseModel):
    """Sorting configuration"""
    field: str = Field(..., description="Field name to sort by")
    order: Literal["asc", "desc"] = Field("asc", description="Sort order")


class SearchRequest(BaseModel):
    """Generic search request"""
    entity: str = Field(..., description="Entity to search (transactions, manual_transactions, upload_files, etc.)")
    filters: Optional[List[SearchFilter]] = Field(default=[], description="List of filters to apply")
    logic: Literal["AND", "OR"] = Field("AND", description="Logic to combine filters")
    sort: Optional[List[SearchSort]] = Field(default=[], description="Sorting configuration")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(30, ge=1, le=500, description="Records per page")
    include_related: Optional[List[str]] = Field(default=[], description="Related entities to include (joins)")


class SearchResponse(BaseModel):
    """Generic search response"""
    entity: str
    total: int
    page: int
    page_size: int
    total_pages: int
    data: List[Dict[str, Any]]
    filters_applied: List[SearchFilter]
    sort_applied: List[SearchSort]


class QuickSearchRequest(BaseModel):
    """Quick text search across multiple fields"""
    entity: str = Field(..., description="Entity to search")
    query: str = Field(..., min_length=1, description="Search query text")
    search_fields: Optional[List[str]] = Field(None, description="Fields to search in (if None, uses default fields)")
    page: int = Field(1, ge=1)
    page_size: int = Field(30, ge=1, le=500)


class AdvancedSearchRequest(BaseModel):
    """Advanced search with complex conditions"""
    entity: str
    conditions: Dict[str, Any] = Field(..., description="Complex search conditions using MongoDB-like syntax")
    page: int = Field(1, ge=1)
    page_size: int = Field(30, ge=1, le=500)
    sort: Optional[List[SearchSort]] = Field(default=[])


class EntityFieldsResponse(BaseModel):
    """Response for available fields on an entity"""
    entity: str
    fields: List[Dict[str, str]] = Field(..., description="List of field metadata")
    searchable_fields: List[str] = Field(..., description="Fields that support text search")
    filterable_fields: List[str] = Field(..., description="Fields that can be filtered")
    sortable_fields: List[str] = Field(..., description="Fields that can be sorted")

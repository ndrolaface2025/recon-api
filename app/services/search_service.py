"""
Generic search service
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Dict, Any, List
import math

from app.utils.search_builder import SearchQueryBuilder, get_entity_fields, ENTITY_MAP
from app.schemas.search_schemas import (
    SearchRequest, 
    SearchResponse, 
    QuickSearchRequest,
    EntityFieldsResponse
)


class SearchService:
    """Service for handling generic search operations"""
    
    @staticmethod
    async def execute_search(search_req: SearchRequest, db: AsyncSession) -> SearchResponse:
        """Execute a generic search"""
        builder = SearchQueryBuilder(search_req.entity)
        
        # Apply filters
        builder.apply_filters(search_req.filters, search_req.logic)
        
        # Apply sorting
        if search_req.sort:
            builder.apply_sorting(search_req.sort)
        
        # Get total count
        count_query = builder.get_count_query()
        
        # Apply same filters to count query
        if search_req.filters:
            count_builder = SearchQueryBuilder(search_req.entity)
            count_builder.apply_filters(search_req.filters, search_req.logic)
            count_query = count_builder.get_count_query()
            
            # Get the WHERE clause from the main query
            for condition in builder.query.whereclause:
                count_query = count_query.where(condition)
        
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0
        
        # Apply pagination
        builder.apply_pagination(search_req.page, search_req.page_size)
        
        # Execute query
        result = await db.execute(builder.build())
        records = result.scalars().all()
        
        # Convert to dict
        data = []
        for record in records:
            record_dict = {}
            for column in record.__table__.columns:
                value = getattr(record, column.name)
                # Convert datetime to ISO string
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                record_dict[column.name] = value
            data.append(record_dict)
        
        total_pages = math.ceil(total / search_req.page_size) if total > 0 else 0
        
        return SearchResponse(
            entity=search_req.entity,
            total=total,
            page=search_req.page,
            page_size=search_req.page_size,
            total_pages=total_pages,
            data=data,
            filters_applied=search_req.filters,
            sort_applied=search_req.sort or []
        )
    
    @staticmethod
    async def execute_quick_search(search_req: QuickSearchRequest, db: AsyncSession) -> SearchResponse:
        """Execute a quick text search"""
        builder = SearchQueryBuilder(search_req.entity)
        
        # Apply quick search
        builder.apply_quick_search(search_req.query, search_req.search_fields)
        
        # Get total count with search filter
        count_builder = SearchQueryBuilder(search_req.entity)
        count_builder.apply_quick_search(search_req.query, search_req.search_fields)
        count_query = count_builder.get_count_query()
        
        # Apply WHERE clause to count
        if builder.query.whereclause is not None:
            count_query = count_query.where(builder.query.whereclause)
        
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0
        
        # Apply pagination
        builder.apply_pagination(search_req.page, search_req.page_size)
        
        # Execute query
        result = await db.execute(builder.build())
        records = result.scalars().all()
        
        # Convert to dict
        data = []
        for record in records:
            record_dict = {}
            for column in record.__table__.columns:
                value = getattr(record, column.name)
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                record_dict[column.name] = value
            data.append(record_dict)
        
        total_pages = math.ceil(total / search_req.page_size) if total > 0 else 0
        
        return SearchResponse(
            entity=search_req.entity,
            total=total,
            page=search_req.page,
            page_size=search_req.page_size,
            total_pages=total_pages,
            data=data,
            filters_applied=[],
            sort_applied=[]
        )
    
    @staticmethod
    def get_entity_metadata(entity_name: str) -> EntityFieldsResponse:
        """Get metadata about an entity's fields"""
        metadata = get_entity_fields(entity_name)
        return EntityFieldsResponse(**metadata)
    
    @staticmethod
    def get_available_entities() -> List[str]:
        """Get list of all searchable entities"""
        return list(ENTITY_MAP.keys())

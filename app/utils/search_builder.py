"""
Generic search utility for building dynamic SQLAlchemy queries
"""
from sqlalchemy import select, func, and_, or_, cast, String, Numeric, Date, TIMESTAMP
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional, Type
from datetime import datetime, date
import re

from app.db.models.transactions import Transaction
from app.db.models.manualTransaction import ManualTransaction
from app.db.models.upload_file import UploadFile
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from app.db.models.user_config import UserConfig
from app.db.models.matching_rule_config import MatchingRuleConfig
from app.db.models.txnJournalEntry import TxnJournalEntry
from app.db.models.glLedger import GeneralLedger
from app.schemas.search_schemas import SearchFilter, SearchSort


# Entity mapping
ENTITY_MAP = {
    "transactions": Transaction,
    "manual_transactions": ManualTransaction,
    "upload_files": UploadFile,
    "channels": ChannelConfig,
    "sources": SourceConfig,
    "users": UserConfig,
    "matching_rules": MatchingRuleConfig,
    "journal_entries": TxnJournalEntry,
    "general_ledger": GeneralLedger,
}


# Default searchable fields for each entity
DEFAULT_SEARCHABLE_FIELDS = {
    "transactions": ["txn_id", "reference_number", "source_reference_number", "account_number", "recon_reference_number"],
    "manual_transactions": ["manual_txn_id", "reference_number", "source_reference_number", "account_number", "recon_reference_number"],
    "upload_files": ["file_name", "file_path"],
    "channels": ["channel_name", "channel_code"],
    "sources": ["source_name", "source_code"],
    "users": ["username", "email", "first_name", "last_name"],
    "matching_rules": ["rule_name", "description"],
    "journal_entries": ["account_number", "account_brn", "recon_reference_number"],
    "general_ledger": ["general_ledger", "gl_role", "gl_description"],
}


class SearchQueryBuilder:
    """Build dynamic SQLAlchemy queries from search filters"""
    
    def __init__(self, entity_name: str):
        if entity_name not in ENTITY_MAP:
            raise ValueError(f"Unknown entity: {entity_name}. Available: {list(ENTITY_MAP.keys())}")
        
        self.entity_name = entity_name
        self.model = ENTITY_MAP[entity_name]
        self.query = select(self.model)
    
    def apply_filters(self, filters: List[SearchFilter], logic: str = "AND") -> 'SearchQueryBuilder':
        """Apply filters to the query"""
        if not filters:
            return self
        
        conditions = []
        for filter_obj in filters:
            condition = self._build_condition(filter_obj)
            if condition is not None:
                conditions.append(condition)
        
        if conditions:
            if logic == "OR":
                self.query = self.query.where(or_(*conditions))
            else:
                self.query = self.query.where(and_(*conditions))
        
        return self
    
    def _build_condition(self, filter_obj: SearchFilter):
        """Build a single filter condition"""
        # Get the column from the model
        if not hasattr(self.model, filter_obj.field):
            raise ValueError(f"Field '{filter_obj.field}' does not exist on {self.entity_name}")
        
        column = getattr(self.model, filter_obj.field)
        operator = filter_obj.operator
        value = filter_obj.value
        
        # Build condition based on operator
        if operator == "eq":
            return column == value
        elif operator == "ne":
            return column != value
        elif operator == "gt":
            return column > value
        elif operator == "gte":
            return column >= value
        elif operator == "lt":
            return column < value
        elif operator == "lte":
            return column <= value
        elif operator == "like":
            return cast(column, String).like(f"%{value}%")
        elif operator == "ilike":
            return cast(column, String).ilike(f"%{value}%")
        elif operator == "in":
            if not isinstance(value, list):
                value = [value]
            return column.in_(value)
        elif operator == "not_in":
            if not isinstance(value, list):
                value = [value]
            return column.notin_(value)
        elif operator == "is_null":
            return column.is_(None)
        elif operator == "is_not_null":
            return column.isnot(None)
        elif operator == "between":
            if filter_obj.value2 is None:
                raise ValueError("'between' operator requires value2")
            return column.between(value, filter_obj.value2)
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    def apply_quick_search(self, query_text: str, search_fields: Optional[List[str]] = None) -> 'SearchQueryBuilder':
        """Apply text search across multiple fields"""
        if not search_fields:
            search_fields = DEFAULT_SEARCHABLE_FIELDS.get(self.entity_name, [])
        
        if not search_fields:
            return self
        
        conditions = []
        for field_name in search_fields:
            if hasattr(self.model, field_name):
                column = getattr(self.model, field_name)
                conditions.append(cast(column, String).ilike(f"%{query_text}%"))
        
        if conditions:
            self.query = self.query.where(or_(*conditions))
        
        return self
    
    def apply_sorting(self, sorts: List[SearchSort]) -> 'SearchQueryBuilder':
        """Apply sorting to the query"""
        for sort_obj in sorts:
            if not hasattr(self.model, sort_obj.field):
                raise ValueError(f"Field '{sort_obj.field}' does not exist on {self.entity_name}")
            
            column = getattr(self.model, sort_obj.field)
            if sort_obj.order == "desc":
                self.query = self.query.order_by(column.desc())
            else:
                self.query = self.query.order_by(column.asc())
        
        return self
    
    def apply_pagination(self, page: int, page_size: int) -> 'SearchQueryBuilder':
        """Apply pagination"""
        offset = (page - 1) * page_size
        self.query = self.query.offset(offset).limit(page_size)
        return self
    
    def get_count_query(self):
        """Get count query for total records"""
        return select(func.count()).select_from(self.model)
    
    def build(self):
        """Return the built query"""
        return self.query


def get_entity_fields(entity_name: str) -> Dict[str, Any]:
    """Get metadata about fields for an entity"""
    if entity_name not in ENTITY_MAP:
        raise ValueError(f"Unknown entity: {entity_name}")
    
    model = ENTITY_MAP[entity_name]
    fields = []
    
    for column in model.__table__.columns:
        field_info = {
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "primary_key": column.primary_key,
        }
        fields.append(field_info)
    
    return {
        "entity": entity_name,
        "fields": fields,
        "searchable_fields": DEFAULT_SEARCHABLE_FIELDS.get(entity_name, []),
        "filterable_fields": [f["name"] for f in fields],
        "sortable_fields": [f["name"] for f in fields],
    }

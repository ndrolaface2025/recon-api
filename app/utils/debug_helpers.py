"""
Debug Helper Utilities for SQLAlchemy Models

Provides utility functions for debugging and pretty-printing SQLAlchemy model objects.
"""

from sqlalchemy import inspect
from typing import Any, Dict, List, Optional
import json
from datetime import datetime, date


def model_to_dict(obj: Any) -> Dict:
    """
    Convert SQLAlchemy model instance to dictionary.
    
    Args:
        obj: SQLAlchemy model instance
        
    Returns:
        Dictionary with all column values
        
    Example:
        channel = await ChannelRepository.get_by_id(db, 1)
        data = model_to_dict(channel)
        print(data)  # {'id': 1, 'channel_name': 'ATM', ...}
    """
    if obj is None:
        return {}
    
    return {c.key: getattr(obj, c.key) 
            for c in inspect(obj).mapper.column_attrs}


def model_to_json(obj: Any, indent: int = 2) -> str:
    """
    Convert SQLAlchemy model to pretty JSON string.
    
    Args:
        obj: SQLAlchemy model instance
        indent: JSON indentation (default: 2)
        
    Returns:
        Pretty-printed JSON string
        
    Example:
        channel = await ChannelRepository.get_by_id(db, 1)
        print(model_to_json(channel))
    """
    if obj is None:
        return json.dumps(None, indent=indent)
    
    data = model_to_dict(obj)
    
    # Convert datetime objects to ISO format strings
    for key, value in data.items():
        if isinstance(value, (datetime, date)):
            data[key] = value.isoformat()
    
    return json.dumps(data, indent=indent, default=str)


def print_model(obj: Any, title: str = "Model") -> None:
    """
    Pretty print a single SQLAlchemy model.
    
    Args:
        obj: SQLAlchemy model instance
        title: Title for the output (default: "Model")
        
    Example:
        channel = await ChannelRepository.get_by_id(db, 1)
        print_model(channel, "Channel Details")
    """
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    
    if obj is None:
        print("  ❌ Object is None")
        print(f"{'='*70}\n")
        return
    
    # Print using __str__ if available
    print(f"  {obj}")
    print(f"{'-'*70}")
    
    # Print all attributes
    data = model_to_dict(obj)
    max_key_length = max(len(str(key)) for key in data.keys()) if data else 20
    
    for key, value in data.items():
        # Format datetime objects
        if isinstance(value, (datetime, date)):
            value = value.isoformat()
        print(f"  {key:<{max_key_length}} : {value}")
    
    print(f"{'='*70}\n")


def print_models(objects: List[Any], title: str = "Models", show_details: bool = True) -> None:
    """
    Pretty print a list of SQLAlchemy models.
    
    Args:
        objects: List of SQLAlchemy model instances
        title: Title for the output (default: "Models")
        show_details: Whether to show detailed attributes (default: True)
        
    Example:
        channels = await ChannelRepository.get_all(db)
        print_models(channels, "All Channels")
    """
    print(f"\n{'='*70}")
    print(f"  {title} ({len(objects)} items)")
    print(f"{'='*70}")
    
    if not objects:
        print("  ℹ️  No objects found")
        print(f"{'='*70}\n")
        return
    
    for i, obj in enumerate(objects, 1):
        if show_details:
            print(f"\n  [{i}] {obj}")
            print(f"  {'-'*66}")
            
            data = model_to_dict(obj)
            max_key_length = max(len(str(key)) for key in data.keys()) if data else 20
            
            for key, value in data.items():
                if isinstance(value, (datetime, date)):
                    value = value.isoformat()
                print(f"      {key:<{max_key_length}} : {value}")
        else:
            print(f"  [{i}] {obj}")
    
    print(f"\n{'='*70}\n")


def debug_query_result(result: Any, query_name: str = "Query") -> None:
    """
    Debug helper for query results - handles single objects, lists, or None.
    
    Args:
        result: Query result (single object, list, or None)
        query_name: Name of the query for logging
        
    Example:
        result = await ChannelRepository.get_by_id(db, 1)
        debug_query_result(result, "Get Channel by ID")
    """
    print(f"\n{'🔍'*35}")
    print(f"  DEBUG: {query_name}")
    print(f"{'🔍'*35}")
    
    if result is None:
        print("  ❌ Result is None")
    elif isinstance(result, list):
        print(f"  ✅ Found {len(result)} items")
        if result:
            print(f"  📋 First item: {result[0]}")
            if len(result) > 1:
                print(f"  📋 Last item: {result[-1]}")
    else:
        print(f"  ✅ Found: {result}")
        print(f"  📊 Type: {type(result).__name__}")
        if hasattr(result, 'id'):
            print(f"  🆔 ID: {result.id}")
    
    print(f"{'🔍'*35}\n")


def compare_models(obj1: Any, obj2: Any, title: str = "Model Comparison") -> None:
    """
    Compare two SQLAlchemy model instances and show differences.
    
    Args:
        obj1: First model instance
        obj2: Second model instance
        title: Title for comparison output
        
    Example:
        old_channel = await ChannelRepository.get_by_id(db, 1)
        # ... update channel ...
        new_channel = await ChannelRepository.get_by_id(db, 1)
        compare_models(old_channel, new_channel, "Channel Updates")
    """
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    
    if obj1 is None or obj2 is None:
        print("  ⚠️  One or both objects are None")
        print(f"  obj1: {obj1}")
        print(f"  obj2: {obj2}")
        print(f"{'='*70}\n")
        return
    
    data1 = model_to_dict(obj1)
    data2 = model_to_dict(obj2)
    
    all_keys = set(data1.keys()) | set(data2.keys())
    
    differences = []
    for key in sorted(all_keys):
        val1 = data1.get(key)
        val2 = data2.get(key)
        
        if val1 != val2:
            differences.append((key, val1, val2))
    
    if differences:
        print(f"  ⚠️  Found {len(differences)} differences:")
        print(f"{'-'*70}")
        for key, val1, val2 in differences:
            print(f"  {key}:")
            print(f"    Old: {val1}")
            print(f"    New: {val2}")
    else:
        print("  ✅ Objects are identical")
    
    print(f"{'='*70}\n")


# Logging helper
def log_model(obj: Any, logger, level: str = "info", message: Optional[str] = None) -> None:
    """
    Log SQLAlchemy model using Python logger.
    
    Args:
        obj: SQLAlchemy model instance
        logger: Python logger instance
        level: Log level ('debug', 'info', 'warning', 'error')
        message: Optional message prefix
        
    Example:
        import logging
        logger = logging.getLogger(__name__)
        
        channel = await ChannelRepository.get_by_id(db, 1)
        log_model(channel, logger, "info", "Retrieved channel")
    """
    log_func = getattr(logger, level.lower(), logger.info)
    
    if message:
        log_func(message)
    
    if obj is None:
        log_func("Model: None")
        return
    
    log_func(f"Model: {obj}")
    log_func(f"Details: {repr(obj)}")
    
    data = model_to_dict(obj)
    for key, value in data.items():
        log_func(f"  {key}: {value}")


# Example usage in code:
"""
# In your repository or service:
from app.utils.debug_helpers import print_model, debug_query_result

class ChannelRepository:
    @staticmethod
    async def get_by_id(db: AsyncSession, channel_id: int):
        stmt = select(ChannelConfig).where(ChannelConfig.id == channel_id)
        result = await db.execute(stmt)
        channel = result.scalars().first()
        
        # Debug output
        debug_query_result(channel, f"Get Channel by ID {channel_id}")
        
        return channel

# In your API route:
@router.get("/{channel_id}")
async def get_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    channel = await ChannelRepository.get_by_id(db, channel_id)
    
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    
    # Debug
    print_model(channel, f"Channel {channel_id} Details")
    
    return channel
"""

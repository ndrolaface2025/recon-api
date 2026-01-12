from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from typing import Optional, List
from app.db.models.matching_rule_config import MatchingRuleConfig
from app.api.v1.schemas.matching_rule import MatchingRuleCreate, MatchingRuleUpdate


class MatchingRuleRepository:
    """Repository for matching rule CRUD operations"""

    @staticmethod
    async def create(db: AsyncSession, rule_data: MatchingRuleCreate) -> MatchingRuleConfig:
        """Create a new matching rule"""
        rule = MatchingRuleConfig(
            rule_name=rule_data.rule_name,
            channel_id=rule_data.channel_id,
            rule_desc=rule_data.rule_desc,
            conditions=rule_data.conditions,
            tolerance=rule_data.tolerance,
            status=rule_data.status,
            created_by=rule_data.created_by,
            version_number=1
        )
        db.add(rule)
        await db.commit()
        await db.refresh(rule)
        return rule

    @staticmethod
    async def get_by_id(db: AsyncSession, rule_id: int) -> Optional[MatchingRuleConfig]:
        """Get a matching rule by ID"""
        result = await db.execute(
            select(MatchingRuleConfig).where(MatchingRuleConfig.id == rule_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_all(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100,
        channel_id: Optional[int] = None,
        status: Optional[int] = None
    ) -> tuple[List[MatchingRuleConfig], int]:
        """Get all matching rules with pagination and filters"""
        query = select(MatchingRuleConfig)
        
        # Apply filters
        if channel_id is not None:
            query = query.where(MatchingRuleConfig.channel_id == channel_id)
        if status is not None:
            query = query.where(MatchingRuleConfig.status == status)
        
        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.offset(skip).limit(limit).order_by(MatchingRuleConfig.created_at.desc())
        
        result = await db.execute(query)
        rules = result.scalars().all()
        
        return list(rules), total

    @staticmethod
    async def update(
        db: AsyncSession,
        rule_id: int,
        rule_data: MatchingRuleUpdate
    ) -> Optional[MatchingRuleConfig]:
        """Update a matching rule"""
        # Get existing rule
        rule = await MatchingRuleRepository.get_by_id(db, rule_id)
        if not rule:
            return None
        
        # Update fields
        update_data = rule_data.model_dump(exclude_unset=True)
        
        # Increment version number
        if update_data:
            update_data['version_number'] = (rule.version_number or 0) + 1
            
            await db.execute(
                update(MatchingRuleConfig)
                .where(MatchingRuleConfig.id == rule_id)
                .values(**update_data)
            )
            await db.commit()
            
            # Refresh to get updated data
            await db.refresh(rule)
        
        return rule

    @staticmethod
    async def delete(db: AsyncSession, rule_id: int) -> bool:
        """Delete a matching rule"""
        result = await db.execute(
            delete(MatchingRuleConfig).where(MatchingRuleConfig.id == rule_id)
        )
        await db.commit()
        return result.rowcount > 0

    @staticmethod
    async def get_by_channel(db: AsyncSession, channel_id: int) -> List[MatchingRuleConfig]:
        """Get all active matching rules for a specific channel"""
        result = await db.execute(
            select(MatchingRuleConfig)
            .where(MatchingRuleConfig.channel_id == channel_id)
            .where(MatchingRuleConfig.status == 1)
            .order_by(MatchingRuleConfig.created_at.desc())
        )
        return list(result.scalars().all())

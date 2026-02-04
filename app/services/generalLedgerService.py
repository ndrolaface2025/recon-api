from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from app.db.models.glLedger import GeneralLedger
from app.db.repositories.generalLedgerRequest import GeneralLedgerCreateRequest
from typing import List
from sqlalchemy import select
from sqlalchemy import select, func

class GeneralLedgerService:

    @staticmethod
    async def create_general_ledger(
        db: AsyncSession,
        payload: GeneralLedgerCreateRequest,
        user_id: int
    ) -> GeneralLedger:
        
        existing_gl = await db.execute(
        select(GeneralLedger).where(
            GeneralLedger.general_ledger == payload.general_ledger
        )
        )
        if existing_gl.scalars().first():
            raise HTTPException(
                status_code=400,
                detail="General Ledger already exists"
            )

        if payload.apply_to_all_channels:
            channel_id = None
        else:
            if not payload.channel_id:
                raise HTTPException(
                    status_code=400,
                    detail="channel_id is required when 'Apply to all channels' is unchecked"
                )

        gl = GeneralLedger(
            general_ledger=payload.general_ledger,
            gl_role=payload.gl_role,
            channel_id= payload.channel_id,
            status = payload.status,
            apply_to_all_channels=payload.apply_to_all_channels,
            gl_description=payload.gl_description,
            created_by=user_id,
            updated_by=user_id,
        )

        db.add(gl)
        await db.commit()          
        await db.refresh(gl)       

        return gl
    
    @staticmethod
    async def get_all_general_ledgers(
        db: AsyncSession,
        offset: int,
        limit: int
    ):
        # total count
        total_stmt = select(func.count()).select_from(GeneralLedger)
        total = await db.execute(total_stmt)
        total_records = total.scalar()

        # paginated data
        stmt = (
            select(GeneralLedger)
            .order_by(GeneralLedger.created_at.desc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(stmt)
        items = result.scalars().all()

        return {
            "items": items,
            "total": total_records,
            "offset": offset,
            "limit": limit,
        }
    

    @staticmethod
    async def delete_general_ledger_by_id(
        db: AsyncSession,
        gl_id: int
    ) -> None:

        stmt = select(GeneralLedger).where(GeneralLedger.id == gl_id)
        result = await db.execute(stmt)
        gl = result.scalar_one_or_none()

        if not gl:
            raise HTTPException(
                status_code=404,
                detail="General Ledger not found"
            )

        await db.delete(gl)
        await db.commit()

    @staticmethod
    async def update_general_ledger_by_id(
        db: AsyncSession,
        gl_id: int,
        payload: GeneralLedgerCreateRequest,
        user_id: int,
    ) -> GeneralLedger:

        stmt = select(GeneralLedger).where(GeneralLedger.id == gl_id)
        result = await db.execute(stmt)
        gl = result.scalar_one_or_none()

        if not gl:
            raise HTTPException(
                status_code=404,
                detail="General Ledger not found"
            )

        if payload.apply_to_all_channels is not None:
            if payload.apply_to_all_channels:
                gl.channel_id = None
                gl.apply_to_all_channels = True
            else:
                if payload.channel_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail="channel_id is required when 'Apply to all channels' is unchecked"
                    )
                gl.channel_id = payload.channel_id
                gl.apply_to_all_channels = False

        if payload.status is not None:
            gl.status = payload.status

        if payload.general_ledger is not None:
            gl.general_ledger = payload.general_ledger

        if payload.gl_role is not None:
            gl.gl_role = payload.gl_role

        if payload.gl_description is not None:
            gl.gl_description = payload.gl_description

        gl.updated_by = user_id

        await db.commit()
        await db.refresh(gl)

        return gl
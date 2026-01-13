from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from app.db.models.glLedger import GeneralLedger
from app.db.repositories.generalLedgerRequest import GeneralLedgerCreateRequest
from typing import List
from sqlalchemy import select

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
            channel_id = payload.channel_id

        gl = GeneralLedger(
            general_ledger=payload.general_ledger,
            gl_role=payload.gl_role,
            channel_id=channel_id,
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
        db: AsyncSession
    ) -> List[GeneralLedger]:

        stmt = select(GeneralLedger).order_by(GeneralLedger.created_at.desc())
        result = await db.execute(stmt)

        return result.scalars().all()
    

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

        # Apply-to-all logic
        if payload.apply_to_all_channels:
            gl.channel_id = None
        else:
            if not payload.channel_id:
                raise HTTPException(
                    status_code=400,
                    detail="channel_id is required when 'Apply to all channels' is unchecked"
                )
            gl.channel_id = payload.channel_id

        # Update fields
        gl.general_ledger = payload.general_ledger
        gl.gl_role = payload.gl_role
        gl.apply_to_all_channels = payload.apply_to_all_channels
        gl.gl_description = payload.gl_description
        gl.updated_by = user_id

        await db.commit()
        await db.refresh(gl)

        return gl
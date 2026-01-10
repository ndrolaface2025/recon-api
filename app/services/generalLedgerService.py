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

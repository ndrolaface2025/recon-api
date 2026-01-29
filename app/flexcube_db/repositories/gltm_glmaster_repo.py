from sqlalchemy import select, func
from sqlalchemy.orm import Session
from app.flexcube_db.models.gltm_glmaster import GltmGlmaster


class GltmGlmasterRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_paginated(self, page: int, page_size: int):
        count_query = select(func.count()).select_from(GltmGlmaster)
        total_records = self.db.execute(count_query).scalar() or 0

        offset = (page - 1) * page_size

        query = (
            select(GltmGlmaster)
            .order_by(GltmGlmaster.gl_code)
            .offset(offset)
            .limit(page_size)
        )

        records = self.db.execute(query).scalars().all()

        return records, total_records

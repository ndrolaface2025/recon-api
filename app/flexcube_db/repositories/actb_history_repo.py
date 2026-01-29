from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session
from datetime import date
from app.flexcube_db.models.actb_history import ActbHistory


class ActbHistoryRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_filtered(
        self,
        page: int,
        page_size: int,
        ac_no: str | None = None,
        drcr_ind: str | None = None,
        ccy: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ):
        conditions = []

        if ac_no:
            conditions.append(ActbHistory.ac_no == ac_no)

        if drcr_ind:
            conditions.append(ActbHistory.drcr_ind == drcr_ind)

        if ccy:
            conditions.append(ActbHistory.ac_ccy == ccy)

        if date_from:
            conditions.append(ActbHistory.trn_date >= date_from)

        if date_to:
            conditions.append(ActbHistory.trn_date <= date_to)

        base_query = select(ActbHistory)

        if conditions:
            base_query = base_query.where(and_(*conditions))

        count_query = select(func.count()).select_from(base_query.subquery())
        total_records = self.db.execute(count_query).scalar() or 0

        offset = (page - 1) * page_size

        query = (
            base_query.order_by(ActbHistory.trn_date.desc())
            .offset(offset)
            .limit(page_size)
        )

        records = self.db.execute(query).scalars().all()

        return records, total_records

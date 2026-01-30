from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session
from datetime import date

from app.flexcube_db.models.acvw_all_ac_entries import AcvwAllAcEntries


class AcvwAllAcEntriesRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_filtered(
        self,
        page: int,
        page_size: int,
        ac_no: str | None = None,
        drcr_ind: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        search: str | None = None,
    ):
        conditions = []

        if ac_no:
            conditions.append(AcvwAllAcEntries.ac_no == ac_no)

        if drcr_ind:
            conditions.append(AcvwAllAcEntries.drcr_ind == drcr_ind)

        if date_from:
            conditions.append(AcvwAllAcEntries.trn_dt >= date_from)

        if date_to:
            conditions.append(AcvwAllAcEntries.trn_dt <= date_to)

        if search:
            conditions.append(
                or_(
                    AcvwAllAcEntries.trn_ref_no == search,
                    AcvwAllAcEntries.external_ref_no == search,
                )
            )

        base_query = select(AcvwAllAcEntries)

        if conditions:
            base_query = base_query.where(and_(*conditions))

        count_query = select(func.count()).select_from(base_query.subquery())
        total_records = self.db.execute(count_query).scalar() or 0

        offset = (page - 1) * page_size

        query = (
            base_query.order_by(
                AcvwAllAcEntries.trn_dt.desc(),
                AcvwAllAcEntries.trn_ref_no,
                AcvwAllAcEntries.ac_entry_sr_no,
            )
            .offset(offset)
            .limit(page_size)
        )

        records = self.db.execute(query).scalars().all()

        return records, total_records

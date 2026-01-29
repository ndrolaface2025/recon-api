from sqlalchemy import (
    Column,
    String,
    Numeric,
    Date,
    CHAR,
    TIMESTAMP,
    Index,
)
from app.flexcube_db.base import FlexcubeBase


class ActbDailyLog(FlexcubeBase):
    __tablename__ = "ACTB_DAILY_LOG"

    trn_ref_no = Column("TRN_REF_NO", String(30), primary_key=True)
    event_sr_no = Column("EVENT_SR_NO", Numeric(5, 0), primary_key=True)

    event = Column("EVENT", String(10))
    ac_branch = Column("AC_BRANCH", Numeric(6, 0))
    ac_no = Column("AC_NO", String(20))
    ac_ccy = Column("AC_CCY", String(3))
    drcr_ind = Column("DRCR_IND", CHAR(1))
    trn_code = Column("TRN_CODE", String(10))

    amount = Column("AMOUNT", Numeric(18, 2))
    lcy_amount = Column("LCY_AMOUNT", Numeric(18, 2))

    value_date = Column("VALUE_DATE", Date)
    trn_date = Column("TRN_DATE", Date)

    user_id = Column("USER_ID", String(30))
    auth_id = Column("AUTH_ID", String(30))
    batch_no = Column("BATCH_NO", String(30))
    posting_status = Column("POSTING_STATUS", String(20))

    created_at = Column("CREATED_AT", TIMESTAMP)

    __table_args__ = (
        Index("IDX_ACTB_DAILY_TRNREF", "TRN_REF_NO"),
        Index("IDX_ACTB_DAILY_ACNO", "AC_NO"),
    )

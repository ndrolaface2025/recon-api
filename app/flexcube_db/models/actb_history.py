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


class ActbHistory(FlexcubeBase):
    __tablename__ = "ACTB_HISTORY"

    trn_ref_no = Column("TRN_REF_NO", String(30), primary_key=True)
    event_sr_no = Column("EVENT_SR_NO", Numeric(5, 0), primary_key=True)

    event = Column("EVENT", String(10))
    ac_branch = Column("AC_BRANCH", Numeric(6, 0))
    ac_no = Column("AC_NO", String(20))
    ac_ccy = Column("AC_CCY", String(3))
    drcr_ind = Column("DRCR_IND", CHAR(1))
    trn_code = Column("TRN_CODE", String(10))
    amount_tag = Column("AMOUNT_TAG", String(20))

    amount = Column("AMOUNT", Numeric(18, 2))
    lcy_amount = Column("LCY_AMOUNT", Numeric(18, 2))
    fcy_amount = Column("FCY_AMOUNT", Numeric(18, 2))
    exch_rate = Column("EXCH_RATE", Numeric(15, 8))

    value_date = Column("VALUE_DATE", Date)
    trn_date = Column("TRN_DATE", Date)

    financial_cycle = Column("FINANCIAL_CYCLE", String(10))
    period_code = Column("PERIOD_CODE", String(10))

    user_id = Column("USER_ID", String(30))
    auth_id = Column("AUTH_ID", String(30))
    product = Column("PRODUCT", String(20))

    glmis_val_upd_flag = Column("GLMIS_VAL_UPD_FLAG", CHAR(1))
    external_ref_no = Column("EXTERNAL_REF_NO", String(50))

    dont_showin_stmt = Column("DONT_SHOWIN_STMT", CHAR(1))
    ic_bal_inclusion = Column("IC_BAL_INCLUSION", CHAR(1))
    aml_exception = Column("AML_EXCEPTION", CHAR(1))
    pl_split_reqd = Column("PL_SPLIT_REQD", CHAR(1))

    intraday_posting_gl = Column("INTRADAY_POSTING_GL", String(20))
    created_at = Column("CREATED_AT", TIMESTAMP)

    __table_args__ = (
        Index("IDX_ACTB_HIST_TRNREF", "TRN_REF_NO"),
        Index("IDX_ACTB_HIST_ACNO", "AC_NO"),
    )

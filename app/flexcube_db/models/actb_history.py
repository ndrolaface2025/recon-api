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

    trn_ref_no = Column("TRN_REF_NO", String(50), primary_key=True)
    event_sr_no = Column("EVENT_SR_NO", Numeric(5, 0), primary_key=True)
    ac_entry_sr_no = Column("AC_ENTRY_SR_NO", Numeric(20, 0), primary_key=True)

    event = Column("EVENT", String(20))
    ac_branch = Column("AC_BRANCH", String(20))
    ac_no = Column("AC_NO", String(35))
    ac_ccy = Column("AC_CCY", String(3))
    drcr_ind = Column("DRCR_IND", CHAR(1))
    trn_code = Column("TRN_CODE", String(20))
    amount_tag = Column("AMOUNT_TAG", String(50))

    fcy_amount = Column("FCY_AMOUNT", Numeric(20, 4))
    exch_rate = Column("EXCH_RATE", Numeric(15, 8))
    lcy_amount = Column("LCY_AMOUNT", Numeric(20, 4))

    related_customer = Column("RELATED_CUSTOMER", String(50))
    related_account = Column("RELATED_ACCOUNT", String(50))
    related_reference = Column("RELATED_REFERENCE", String(50))

    mis_flag = Column("MIS_FLAG", CHAR(1))
    mis_head = Column("MIS_HEAD", String(50))

    trn_dt = Column("TRN_DT", Date)
    value_dt = Column("VALUE_DT", Date)
    txn_init_date = Column("TXN_INIT_DATE", Date)
    stmt_dt = Column("STMT_DT", TIMESTAMP)
    glmis_update_flag = Column("GLMIS_UPDATE_FLAG", Date)

    financial_cycle = Column("FINANCIAL_CYCLE", String(20))
    period_code = Column("PERIOD_CODE", String(20))
    instrument_code = Column("INSTRUMENT_CODE", String(20))
    bank_code = Column("BANK_CODE", String(20))

    type = Column("TYPE", String(20))
    category = Column("CATEGORY", String(20))
    cust_gl = Column("CUST_GL", String(50))
    module = Column("MODULE", String(20))
    ib = Column("IB", CHAR(1))
    flg_position_status = Column("FLG_POSITION_STATUS", CHAR(1))

    user_id = Column("USER_ID", String(50))
    curr_no = Column("CURR_NO", Numeric(5, 0))
    batch_no = Column("BATCH_NO", String(20))
    print_stat = Column("PRINT_STAT", CHAR(1))
    product_accrual = Column("PRODUCT_ACCRUAL", String(50))
    auth_id = Column("AUTH_ID", String(50))
    product = Column("PRODUCT", String(20))
    glmis_val_upd_flag = Column("GLMIS_VAL_UPD_FLAG", CHAR(1))
    external_ref_no = Column("EXTERNAL_REF_NO", String(50))
    dont_showin_stmt = Column("DONT_SHOWIN_STMT", CHAR(1))
    ic_bal_inclusion = Column("IC_BAL_INCLUSION", CHAR(1))
    aml_exception = Column("AML_EXCEPTION", CHAR(1))
    orig_pnl_gl = Column("ORIG_PNL_GL", String(50))
    entry_seq_no = Column("ENTRY_SEQ_NO", Numeric(5, 0))

    __table_args__ = (
        Index("IDX_ACTB_HIST_TRNREF", "TRN_REF_NO"),
        Index("IDX_ACTB_HIST_ACNO", "AC_NO"),
    )

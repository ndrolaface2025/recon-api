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

    # Primary Key based on the CREATE TABLE statement:
    # CONSTRAINT "PK01_ACTB_HISTORY" PRIMARY KEY ("AC_ENTRY_SR_NO")
    ac_entry_sr_no = Column("AC_ENTRY_SR_NO", Numeric(20, 0), primary_key=True)

    trn_ref_no = Column("TRN_REF_NO", String(16))
    event_sr_no = Column("EVENT_SR_NO", Numeric)
    event = Column("EVENT", String(4))
    ac_branch = Column("AC_BRANCH", String(3))
    ac_no = Column("AC_NO", String(20))
    ac_ccy = Column("AC_CCY", String(3))
    drcr_ind = Column("DRCR_IND", CHAR(1))
    trn_code = Column("TRN_CODE", String(3))
    amount_tag = Column("AMOUNT_TAG", String(35))

    fcy_amount = Column("FCY_AMOUNT", Numeric(22, 3))
    exch_rate = Column("EXCH_RATE", Numeric(24, 12))
    lcy_amount = Column("LCY_AMOUNT", Numeric(22, 3))

    related_customer = Column("RELATED_CUSTOMER", String(9))
    related_account = Column("RELATED_ACCOUNT", String(20))
    related_reference = Column("RELATED_REFERENCE", String(16))

    mis_flag = Column("MIS_FLAG", CHAR(1))
    mis_head = Column("MIS_HEAD", String(9))

    trn_dt = Column("TRN_DT", Date)
    value_dt = Column("VALUE_DT", Date)
    txn_init_date = Column("TXN_INIT_DATE", Date)

    financial_cycle = Column("FINANCIAL_CYCLE", String(9))
    period_code = Column("PERIOD_CODE", String(3))
    instrument_code = Column("INSTRUMENT_CODE", String(16))
    bank_code = Column("BANK_CODE", String(12))

    type = Column("TYPE", CHAR(1))
    category = Column("CATEGORY", CHAR(1))
    cust_gl = Column("CUST_GL", CHAR(1))
    module = Column("MODULE", String(2))

    ib = Column("IB", CHAR(1))
    flg_position_status = Column("FLG_POSITION_STATUS", CHAR(1))

    # In Create Table, GLMIS_UPDATE_FLAG is CHAR(1), not Date
    glmis_update_flag = Column("GLMIS_UPDATE_FLAG", CHAR(1))

    user_id = Column("USER_ID", String(12))
    curr_no = Column("CURR_NO", Numeric(6, 0))
    batch_no = Column("BATCH_NO", String(4))

    print_stat = Column("PRINT_STAT", CHAR(1), default="Y")
    product_accrual = Column("PRODUCT_ACCRUAL", String(1))
    auth_id = Column("AUTH_ID", String(12))
    product = Column("PRODUCT", String(4))

    glmis_val_upd_flag = Column("GLMIS_VAL_UPD_FLAG", CHAR(1), default="N")
    external_ref_no = Column("EXTERNAL_REF_NO", String(35))

    dont_showin_stmt = Column("DONT_SHOWIN_STMT", CHAR(1))
    ic_bal_inclusion = Column("IC_BAL_INCLUSION", CHAR(1), default="Y")

    aml_exception = Column("AML_EXCEPTION", String(1))
    orig_pnl_gl = Column("ORIG_PNL_GL", String(9))

    stmt_dt = Column("STMT_DT", Date)
    entry_seq_no = Column("ENTRY_SEQ_NO", Numeric)

    __table_args__ = (
        Index("IDX_ACTB_HIST_TRNREF", "TRN_REF_NO"),
        Index("IDX_ACTB_HIST_ACNO", "AC_NO"),
    )

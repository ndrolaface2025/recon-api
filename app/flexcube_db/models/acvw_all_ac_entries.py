from sqlalchemy import (
    Column,
    String,
    Numeric,
    Date,
    CHAR,
    TIMESTAMP,
)
from app.flexcube_db.base import FlexcubeBase


class AcvwAllAcEntries(FlexcubeBase):
    __tablename__ = "ACVW_ALL_AC_ENTRIES"

    # SQLAlchemy requires a primary key even for Views to map objects.
    # We create a composite key using the unique transaction identifiers + the source flag.
    trn_ref_no = Column("TRN_REF_NO", String(16), primary_key=True)
    ac_entry_sr_no = Column("AC_ENTRY_SR_NO", Numeric(20, 0), primary_key=True)
    event_sr_no = Column("EVENT_SR_NO", Numeric, primary_key=True)
    dly_hist = Column(
        "DLY_HIST", CHAR(1), primary_key=True
    )  # 'D' for Daily, 'H' for History

    event = Column("EVENT", String(4))
    ac_branch = Column("AC_BRANCH", String(3))
    ac_no = Column("AC_NO", String(20))
    ac_ccy = Column("AC_CCY", String(3))
    category = Column("CATEGORY", CHAR(1))
    drcr_ind = Column("DRCR_IND", CHAR(1))
    trn_code = Column("TRN_CODE", String(3))

    fcy_amount = Column("FCY_AMOUNT", Numeric(22, 3))
    exch_rate = Column("EXCH_RATE", Numeric(24, 12))
    lcy_amount = Column("LCY_AMOUNT", Numeric(22, 3))

    trn_dt = Column("TRN_DT", Date)
    value_dt = Column("VALUE_DT", Date)
    txn_init_date = Column("TXN_INIT_DATE", Date)
    stmt_dt = Column("STMT_DT", Date)

    amount_tag = Column("AMOUNT_TAG", String(35))
    related_account = Column("RELATED_ACCOUNT", String(20))
    related_customer = Column("RELATED_CUSTOMER", String(9))
    related_reference = Column("RELATED_REFERENCE", String(16))

    mis_head = Column("MIS_HEAD", String(9))
    mis_flag = Column("MIS_FLAG", CHAR(1))
    instrument_code = Column("INSTRUMENT_CODE", String(16))
    bank_code = Column("BANK_CODE", String(12))
    balance_upd = Column("BALANCE_UPD", CHAR(1))
    auth_stat = Column("AUTH_STAT", CHAR(1))
    module = Column("MODULE", String(2))
    cust_gl = Column("CUST_GL", CHAR(1))

    financial_cycle = Column("FINANCIAL_CYCLE", String(9))
    period_code = Column("PERIOD_CODE", String(3))
    batch_no = Column("BATCH_NO", String(4))
    user_id = Column("USER_ID", String(12))
    curr_no = Column("CURR_NO", Numeric(6, 0))
    print_stat = Column("PRINT_STAT", CHAR(1))
    auth_id = Column("AUTH_ID", String(12))

    glmis_val_upd_flag = Column("GLMIS_VAL_UPD_FLAG", CHAR(1))
    external_ref_no = Column("EXTERNAL_REF_NO", String(35))
    dont_showin_stmt = Column("DONT_SHOWIN_STMT", CHAR(1))
    ic_bal_inclusion = Column("IC_BAL_INCLUSION", CHAR(1))
    aml_exception = Column("AML_EXCEPTION", String(1))
    ib = Column("IB", CHAR(1))
    glmis_update_flag = Column("GLMIS_UPDATE_FLAG", CHAR(1))
    product_accrual = Column("PRODUCT_ACCRUAL", String(1))
    orig_pnl_gl = Column("ORIG_PNL_GL", String(9))
    entry_seq_no = Column("ENTRY_SEQ_NO", Numeric)

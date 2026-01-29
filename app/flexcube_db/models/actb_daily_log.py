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

    trn_ref_no = Column("TRN_REF_NO", String(50), primary_key=True)
    event_sr_no = Column("EVENT_SR_NO", Numeric(5, 0), primary_key=True)
    ac_entry_sr_no = Column("AC_ENTRY_SR_NO", Numeric(20, 0), primary_key=True)

    module = Column("MODULE", String(20))
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

    # Dates
    trn_dt = Column("TRN_DT", Date)
    value_dt = Column("VALUE_DT", Date)
    txn_init_date = Column("TXN_INIT_DATE", Date)
    stmt_dt = Column("STMT_DT", TIMESTAMP)
    cust_gl_update = Column("CUST_GL_UPDATE", Date)
    glmis_update_status = Column("GLMIS_UPDATE_STATUS", Date)
    vdbal_update_flag = Column("VDBAL_UPDATE_FLAG", Date)

    financial_cycle = Column("FINANCIAL_CYCLE", String(20))
    period_code = Column("PERIOD_CODE", String(20))
    instrument_code = Column("INSTRUMENT_CODE", String(20))

    batch_no = Column("BATCH_NO", String(20))
    curr_no = Column("CURR_NO", Numeric(5, 0))
    user_id = Column("USER_ID", String(50))
    bank_code = Column("BANK_CODE", String(20))
    avldays = Column("AVLDAYS", Numeric(5, 0))
    balance_upd = Column("BALANCE_UPD", CHAR(1))
    type = Column("TYPE", String(20))
    auth_id = Column("AUTH_ID", String(50))
    print_stat = Column("PRINT_STAT", CHAR(1))
    auth_stat = Column("AUTH_STAT", CHAR(1))
    category = Column("CATEGORY", String(20))
    cust_gl = Column("CUST_GL", String(50))
    distributed = Column("DISTRIBUTED", CHAR(1))
    node = Column("NODE", String(50))
    delete_stat = Column("DELETE_STAT", CHAR(1))
    on_line = Column("ON_LINE", CHAR(1))
    updact = Column("UPDACT", CHAR(1))
    node_sr_no = Column("NODE_SR_NO", Numeric(5, 0))
    netting_ind = Column("NETTING_IND", CHAR(1))
    ib = Column("IB", CHAR(1))
    flg_position_status = Column("FLG_POSITION_STATUS", CHAR(1))
    glmis_update_flag = Column("GLMIS_UPDATE_FLAG", Date)
    product_accrual = Column("PRODUCT_ACCRUAL", String(50))
    product = Column("PRODUCT", String(20))
    glmis_val_upd_flag = Column("GLMIS_VAL_UPD_FLAG", CHAR(1))
    external_ref_no = Column("EXTERNAL_REF_NO", String(50))
    processed_flag = Column("PROCESSED_FLAG", String(1))
    mis_spread = Column("MIS_SPREAD", Numeric(10, 4))
    dont_showin_stmt = Column("DONT_SHOWIN_STMT", CHAR(1))
    ic_bal_inclusion = Column("IC_BAL_INCLUSION", CHAR(1))
    aml_exception = Column("AML_EXCEPTION", CHAR(1))
    orig_pnl_gl = Column("ORIG_PNL_GL", String(50))
    entry_seq_no = Column("ENTRY_SEQ_NO", Numeric(5, 0))
    il_bvt_processed = Column("IL_BVT_PROCESSED", String(1))

    # Custom Object Columns
    ac_obj_custom_entry_sr_no = Column(
        "AC_OBJ_CUSTOM.AC_ENTRY_SR_NO", Numeric(20, 0), quote=True
    )
    rel_event_sr_no = Column("REL_EVENT_SR_NO", Numeric(5, 0))

    __table_args__ = (
        Index("IDX_ACTB_DAILY_TRNREF", "TRN_REF_NO"),
        Index("IDX_ACTB_DAILY_ACNO", "AC_NO"),
    )

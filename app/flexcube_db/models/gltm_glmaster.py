from sqlalchemy import (
    Column,
    String,
    Numeric,
    Date,
    CHAR,
    TIMESTAMP,
)
from app.flexcube_db.base import FlexcubeBase


class GltmGlmaster(FlexcubeBase):
    __tablename__ = "GLTM_GLMASTER"

    gl_code = Column("GL_CODE", String(9), primary_key=True)
    gl_desc = Column("GL_DESC", String(105))
    parent_gl = Column("PARENT_GL", String(9))
    ccy_res = Column("CCY_RES", CHAR(1))
    res_ccy = Column("RES_CCY", String(3))
    leaf = Column("LEAF", CHAR(1))
    type = Column("TYPE", CHAR(1))
    customer = Column("CUSTOMER", CHAR(1))
    category = Column("CATEGORY", CHAR(1))
    ho_res = Column("HO_RES", CHAR(1))
    reval = Column("REVAL", CHAR(1))
    profit_acc = Column("PROFIT_ACC", String(9))
    loss_acc = Column("LOSS_ACC", String(9))
    cbank_line_dr = Column("CBANK_LINE_DR", String(16))
    cbank_line_cr = Column("CBANK_LINE_CR", String(16))
    ho_line_dr = Column("HO_LINE_DR", String(16))
    ho_line_cr = Column("HO_LINE_CR", String(16))
    first_auth = Column("FIRST_AUTH", CHAR(1))
    ulti_parent = Column("ULTI_PARENT", String(9))
    block = Column("BLOCK", CHAR(1))
    posting_res = Column("POSTING_RES", CHAR(1))
    recon = Column("RECON", CHAR(1))
    record_stat = Column("RECORD_STAT", CHAR(1))
    once_auth = Column("ONCE_AUTH", CHAR(1))
    auth_stat = Column("AUTH_STAT", CHAR(1))
    mod_no = Column("MOD_NO", Numeric(4, 0))
    maker_id = Column("MAKER_ID", String(12))
    maker_dt_stamp = Column("MAKER_DT_STAMP", Date)
    checker_id = Column("CHECKER_ID", String(12))
    checker_dt_stamp = Column("CHECKER_DT_STAMP", Date)
    flg_position_ac = Column("FLG_POSITION_AC", CHAR(1))
    ccy_pos_gl = Column("CCY_POS_GL", CHAR(1))
    ac_stmt_required = Column("AC_STMT_REQUIRED", CHAR(1))
    gen_stmt_only_on_mvmt = Column("GEN_STMT_ONLY_ON_MVMT", CHAR(1))
    media = Column("MEDIA", String(15))
    ac_stmt_day = Column("AC_STMT_DAY", Numeric(2, 0))
    ac_stmt_cycle = Column("AC_STMT_CYCLE", CHAR(1))
    ac_stmt_type = Column("AC_STMT_TYPE", CHAR(1), default="N")
    address1 = Column("ADDRESS1", String(105))
    address2 = Column("ADDRESS2", String(105))
    address3 = Column("ADDRESS3", String(105))
    address4 = Column("ADDRESS4", String(105))
    is_accrual = Column("IS_ACCRUAL", String(1), default="N")
    bal_trfr_on_mis_reclass = Column("BAL_TRFR_ON_MIS_RECLASS", String(1), default="N")
    allow_back_period_entry = Column("ALLOW_BACK_PERIOD_ENTRY", CHAR(1), default="Y")
    ed_fcyrvl_singleentry = Column("ED_FCYRVL_SINGLEENTRY", CHAR(1))
    ed_fcyrvl_entrypair = Column("ED_FCYRVL_ENTRYPAIR", CHAR(1))
    eod_fcyrvl_revr = Column("EOD_FCYRVL_REVR", CHAR(1))
    avg_bal_reqd = Column("AVG_BAL_REQD", String(1), default="N")
    prev_yr_adj_gl = Column("PREV_YR_ADJ_GL", String(9))
    repost_fcy_entries = Column("REPOST_FCY_ENTRIES", CHAR(1))
    pl_split_reqd = Column("PL_SPLIT_REQD", String(1))
    intraday_posting_gl = Column("INTRADAY_POSTING_GL", String(9))

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


class GltmGlmaster(FlexcubeBase):
    __tablename__ = "GLTM_GLMASTER"

    gl_code = Column("GL_CODE", String(20), primary_key=True)
    gl_desc = Column("GL_DESC", String(255))
    parent_gl = Column("PARENT_GL", String(20))

    ccy_res = Column("CCY_RES", CHAR(1))
    res_ccy = Column("RES_CCY", String(3))

    leaf = Column("LEAF", CHAR(1))
    type = Column("TYPE", String(20))
    customer = Column("CUSTOMER", CHAR(1))
    category = Column("CATEGORY", String(20))
    ho_res = Column("HO_RES", CHAR(1))
    reval = Column("REVAL", CHAR(1))

    profit_acc = Column("PROFIT_ACC", String(20))
    loss_acc = Column("LOSS_ACC", String(20))

    cbank_line_dr = Column("CBANK_LINE_DR", Numeric(20, 0))
    cbank_line_cr = Column("CBANK_LINE_CR", Numeric(20, 0))
    ho_line_dr = Column("HO_LINE_DR", Numeric(20, 0))
    ho_line_cr = Column("HO_LINE_CR", Numeric(20, 0))

    first_auth = Column("FIRST_AUTH", CHAR(1))
    ulti_parent = Column("ULTI_PARENT", String(20))
    block = Column("BLOCK", CHAR(1))
    posting_res = Column("POSTING_RES", CHAR(1))
    recon = Column("RECON", CHAR(1))
    record_stat = Column("RECORD_STAT", CHAR(1))
    once_auth = Column("ONCE_AUTH", CHAR(1))
    auth_stat = Column("AUTH_STAT", CHAR(1))

    mod_no = Column("MOD_NO", Numeric(5, 0))
    maker_id = Column("MAKER_ID", String(50))
    maker_dt_stamp = Column("MAKER_DT_STAMP", TIMESTAMP)
    checker_id = Column("CHECKER_ID", String(50))
    checker_dt_stamp = Column("CHECKER_DT_STAMP", TIMESTAMP)

    flg_position_ac = Column("FLG_POSITION_AC", CHAR(1))
    ccy_pos_gl = Column("CCY_POS_GL", CHAR(1))
    ac_stmt_required = Column("AC_STMT_REQUIRED", CHAR(1))
    gen_stmt_only_on_mvmt = Column("GEN_STMT_ONLY_ON_MVMT", CHAR(1))

    media = Column("MEDIA", String(50))
    ac_stmt_day = Column("AC_STMT_DAY", Numeric(5, 0))
    ac_stmt_cycle = Column("AC_STMT_CYCLE", String(20))
    ac_stmt_type = Column("AC_STMT_TYPE", String(20))

    address1 = Column("ADDRESS1", String(255))
    address2 = Column("ADDRESS2", String(255))
    address3 = Column("ADDRESS3", String(255))
    address4 = Column("ADDRESS4", String(255))

    is_accrual = Column("IS_ACCRUAL", CHAR(1))
    bal_trfr_on_mis_reclass = Column("BAL_TRFR_ON_MIS_RECLASS", CHAR(1))
    allow_back_period_entry = Column("ALLOW_BACK_PERIOD_ENTRY", CHAR(1))
    ed_fcyrvl_singleentry = Column("ED_FCYRVL_SINGLEENTRY", CHAR(1))
    ed_fcyrvl_entrypair = Column("ED_FCYRVL_ENTRYPAIR", CHAR(1))
    eod_fcyrvl_revr = Column("EOD_FCYRVL_REVR", CHAR(1))
    avg_bal_reqd = Column("AVG_BAL_REQD", CHAR(1))
    prev_yr_adj_gl = Column("PREV_YR_ADJ_GL", String(20))
    repost_fcy_entries = Column("REPOST_FCY_ENTRIES", CHAR(1))
    pl_split_reqd = Column("PL_SPLIT_REQD", CHAR(1))
    intraday_posting_gl = Column("INTRADAY_POSTING_GL", CHAR(1))

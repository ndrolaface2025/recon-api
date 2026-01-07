from sqlalchemy import Column, BigInteger, String, Date, Numeric, Text, TIMESTAMP, CHAR
from sqlalchemy.sql import func
from app.db.base import Base

class TxnJournalEntry(Base):
    __tablename__ = "tbl_txn_journal_entries"

    id = Column(BigInteger, primary_key=True, index=True)
    recon_reference_number = Column(String(50))
    account_brn = Column(String(20))
    account_number = Column(String(50))
    account_desc = Column(Text)
    cif = Column(String(50))
    drcr_ind = Column(CHAR(2))
    ccy = Column(String(10))
    exch_rate = Column(Numeric)
    fcy_amount = Column(Numeric)
    lcy_amount = Column(Numeric)
    trn_dt = Column(Date)
    post_status = Column(String(20))
    post_date = Column(Date)
    batch_no = Column(String(50))
    source_id = Column(String(50))
    auth_stat = Column(String(20))
    maker_id = Column(String(50))
    maker_dt_stamp = Column(TIMESTAMP)
    checker_id = Column(String(50))
    checker_dt_stamp = Column(TIMESTAMP)
    comments = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

from sqlalchemy import Column, BigInteger, String, Date, Numeric, Boolean, Text, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.db.base import Base

class ManualTransaction(Base):
    __tablename__ = "tbl_txn_manuals"
    id = Column(BigInteger, primary_key=True, index=True)
    manual_txn_id = Column(String(50), index=True)
    recon_reference_number = Column(String(50), index=True)
    channel_id = Column(String(50))
    source_id = Column(String(50))
    source_reference_number = Column(String(50), index=True)
    reference_number = Column(String(50), index=True)
    txn_date = Column(Date)
    account_number = Column(String(50))
    cif = Column(String(50))
    ccy = Column(String(10))
    amount = Column(Numeric)
    json_file = Column(JSONB)
    file_transactions_id = Column(BigInteger)
    reconciled_status = Column(String(50))
    reconciled_by = Column(BigInteger)
    comment = Column(Text)
    is_journal_entry = Column(Boolean)
    journal_entry_status = Column(String(50))
    created_at = Column(TIMESTAMP, server_default=func.now())
    created_by = Column(String(50))
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    updated_by = Column(String(50))

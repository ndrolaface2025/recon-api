from sqlalchemy import Column, BigInteger, String, Date, Numeric, Boolean, Text, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.db.base import Base

class ManualTransaction(Base):
    __tablename__ = "tbl_txn_manual"
    id = Column(BigInteger, primary_key=True, index=True)
    manual_txn_id = Column(String(50), index=True)
    recon_reference_number = Column(String(255), nullable=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    source_reference_number = Column(String(255), nullable=True)
    reference_number = Column(String(50), index=True)
    # txn_date = Column(Date)
    txn_date = Column(String(50), nullable=True)
    account_number = Column(String(50))
    cif = Column(String(50))
    ccy = Column(String(10))
    amount = Column(Numeric)
    json_file = Column(JSONB)
    file_transactions_id = Column(BigInteger, ForeignKey("tbl_upload_files.id"), nullable=True)
    reconciled_status = Column(Boolean, nullable=True)
    reconciled_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    comment = Column(Text)
    is_journal_entry = Column(Boolean)
    journal_entry_status = Column(String(50))
    created_at = Column(TIMESTAMP, server_default=func.now())
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)

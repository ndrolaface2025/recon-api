from sqlalchemy import Column, BigInteger, String, Boolean, Integer, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class Transaction(Base):
    __tablename__ = "tbl_txn_transactions"

    id = Column(BigInteger, primary_key=True)
    txn_id = Column(String(50), index=True)
    recon_reference_number = Column(String(255), nullable=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    reference_number = Column(String(255), nullable=True)
    source_reference_number = Column(String(255), nullable=True)
    amount = Column(String(50), nullable=True)
    date = Column(String(50), nullable=True)
    account_number = Column(String(50), nullable=True)
    ccy = Column(String(10), nullable=True)
    otherDetails = Column("otherDetails", Text, nullable=True)  # Explicitly quoted to match DB column case
    recon_group_number = Column(String(255), nullable=True)
    file_transactions_id = Column(BigInteger, ForeignKey("tbl_upload_files.id"), nullable=True)
    network_id = Column(BigInteger, ForeignKey("tbl_cfg_networks.id"), nullable=True)
    reconciled_status = Column(Boolean, nullable=True)
    reconciled_mode = Column(Boolean, nullable=True)
    reconciled_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    match_rule_id = Column(BigInteger, nullable=True)
    match_conditon = Column(Text, nullable=True)
    match_status = Column(Integer, nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)
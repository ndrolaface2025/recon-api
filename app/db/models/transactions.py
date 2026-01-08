from sqlalchemy import Column, BigInteger, String, Boolean, Integer, Numeric, Text, TIMESTAMP, func, Date
from sqlalchemy.dialects.postgresql import JSONB
from app.db.base import Base

class Transaction(Base):
    __tablename__ = "tbl_txn_transactions"

    id = Column(BigInteger, primary_key=True, index=True)
    txn_id = Column(String(50), index=True)
    recon_reference_number = Column(String(50), index=True)
    channel_id = Column(String(50))
    source_id = Column(String(50))
    source_reference_number = Column(String(50), index=True)
    reference_number = Column(String(50), index=True)

    date = Column(Date)
    account_number = Column(String(50))
    cif = Column(String(50))
    ccy = Column(String(10))

    amount = Column(Numeric)
    json_file = Column(JSONB)
    file_transactions_id = Column(BigInteger)

    reconciled_status = Column(String(50))
    reconciled_by = Column(BigInteger)
    reconciled_mode = Column(Boolean)

    match_rule_id = Column(BigInteger)
    match_conditon = Column(Text)
    match_status = Column(Integer)

    comment = Column(Text)

    created_at = Column(TIMESTAMP, server_default=func.now())
    created_by = Column(String(50))
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    updated_by = Column(String(50))

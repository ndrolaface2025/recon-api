from sqlalchemy import Column, BigInteger, String, Boolean, Integer, Numeric, Text, TIMESTAMP, func, Date, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from app.db.base import Base

class GeneralLedger(Base):
    __tablename__ = "tbl_cfg_general_ledger"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    general_ledger = Column(String(50), index=True)
    gl_role = Column(String(255), nullable=True)
    channel_id = Column(String(50), nullable=True, index=True)
    apply_to_all_channels = Column(Boolean, nullable=True, default=False)
    gl_description = Column(String(255), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, nullable=True)

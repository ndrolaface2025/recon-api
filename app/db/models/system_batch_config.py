from sqlalchemy import Column, BigInteger, String, Integer, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class SystemBatchConfig(Base):
    __tablename__ = "tbl_cfg_system_batch"

    id = Column(BigInteger, primary_key=True)
    record_per_job = Column(BigInteger, nullable=True)
    system_id = Column(BigInteger, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)

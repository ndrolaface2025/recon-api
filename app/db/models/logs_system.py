from sqlalchemy import Column, BigInteger, String, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class SystemLog(Base):
    __tablename__ = "tbl_logs_system"

    id = Column(BigInteger, primary_key=True)
    channel_id = Column(BigInteger, nullable=True)
    log_type = Column(String(255), nullable=True)
    log_details = Column(Text, nullable=True)
    added_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)

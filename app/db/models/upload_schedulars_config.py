from sqlalchemy import Column, BigInteger, Integer, String, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class UploadSchedulerConfig(Base):
    __tablename__ = "tbl_cfg_upload_schedulars"

    id = Column(BigInteger, primary_key=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    schedular_name = Column(String(255), nullable=True)
    schedular_time = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)

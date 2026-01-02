from sqlalchemy import Column, BigInteger, Integer, String, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class UploadAPIConfig(Base):
    __tablename__ = "tbl_cfg_upload_api"

    id = Column(BigInteger, primary_key=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    api_name = Column(String(255), nullable=True)
    method = Column(String(50), nullable=True)
    base_url = Column(Text, nullable=True)
    responce_formate = Column(Text, nullable=True)
    auth_type = Column(String(50), nullable=True)
    auth_token = Column(Text, nullable=True)
    api_time_out = Column(String(50), nullable=True)
    max_try = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)

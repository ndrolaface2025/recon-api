from sqlalchemy import Column, BigInteger, String, Integer, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class UploadFile(Base):
    __tablename__ = "tbl_upload_files"

    id = Column(BigInteger, primary_key=True)
    file_name = Column(String(255), nullable=True)
    file_details = Column(Text, nullable=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    status = Column(Integer, nullable=True)
    record_details = Column(String(255), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    version_number = Column(Integer, nullable=True)

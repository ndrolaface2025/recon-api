from sqlalchemy import Column, BigInteger, String, Boolean, Integer, ForeignKey, Text, TIMESTAMP, func
from app.db.base import Base

class ChannelConfig(Base):
    __tablename__ = "tbl_cfg_channels"

    id = Column(BigInteger, primary_key=True)
    channel_name = Column(String(255), nullable=True)
    channel_description = Column(Text, nullable=True)
    cannel_source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    network_source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    cbs_source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    switch_source_id = Column(BigInteger, ForeignKey("tbl_cfg_source.id"), nullable=True)
    status = Column(Boolean, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)

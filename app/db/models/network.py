from sqlalchemy import Column, BigInteger, Integer, String, ForeignKey, Text, TIMESTAMP, Float, func
from app.db.base import Base

class Network(Base):
    __tablename__ = "tbl_cfg_networks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    network_name = Column(String(255), nullable=False, unique=True, index=True)
    network_desc = Column(Text, nullable=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True, index=True)
    status = Column(Integer, nullable=False, default=1, index=True)  # 1=Active, 0=Inactive
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=False, default=1)
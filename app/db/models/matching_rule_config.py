from sqlalchemy import Column, BigInteger, Integer, String, ForeignKey, Text, TIMESTAMP, func, JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.base import Base


class MatchingRuleConfig(Base):
    __tablename__ = "tbl_cfg_matching_rule"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    rule_name = Column(String(255), nullable=False, index=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=False, index=True)
    rule_desc = Column(Text, nullable=True)
    conditions = Column(JSON, nullable=True)  # Changed to JSON for better query support
    tolerance = Column(JSON, nullable=True)   # Changed to JSON for better query support
    status = Column(Integer, nullable=False, default=1, index=True)  # 1=Active, 0=Inactive
    network_id = Column(BigInteger, ForeignKey("tbl_cfg_networks.id"), nullable=True, index=True)
    created_at = Column(TIMESTAMP, nullable=True, default=datetime.utcnow, server_default=func.now())
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True, default=datetime.utcnow, server_default=func.now(), onupdate=func.now())
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=False, default=1)
    
    # Relationship to channel
    channel = relationship("ChannelConfig", foreign_keys=[channel_id], lazy="joined")
    network = relationship("Network", foreign_keys=[network_id], lazy="joined")

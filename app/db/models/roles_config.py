from sqlalchemy import Column, BigInteger, String, Text, ForeignKey, TIMESTAMP, func
from app.db.base import Base

class RoleConfig(Base):
    __tablename__ = "tbl_cfg_roles"

    id = Column(BigInteger, primary_key=True)
    module_id = Column(BigInteger, ForeignKey("tbl_cfg_module.id"), nullable=True)
    name = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    permission_json = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)

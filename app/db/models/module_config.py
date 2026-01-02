from sqlalchemy import Column, BigInteger, String, Text, TIMESTAMP, func
from app.db.base import Base


class ModuleConfig(Base):
    __tablename__ = "tbl_cfg_module"

    id = Column(BigInteger, primary_key=True)
    module_name = Column(String(255), nullable=True)
    module_description = Column(Text, nullable=True)
    module_permission_json = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)

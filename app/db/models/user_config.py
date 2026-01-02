from sqlalchemy import Column, BigInteger, String, Boolean, Integer, ForeignKey, TIMESTAMP, func
from app.db.base import Base

class UserConfig(Base):
    __tablename__ = "tbl_cfg_users"

    id = Column(BigInteger, primary_key=True)
    f_name = Column(String(100), nullable=True)
    m_name = Column(String(100), nullable=True)
    l_name = Column(String(100), nullable=True)
    gender = Column(Boolean, nullable=True)
    phone = Column(String(20), nullable=True)
    birth_date = Column(TIMESTAMP, nullable=True)
    email = Column(String(255), nullable=True)
    username = Column(String(100), nullable=True)
    role = Column(BigInteger, ForeignKey("tbl_cfg_roles.id"), nullable=True)
    status = Column(Boolean, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    version_number = Column(Integer, nullable=True)

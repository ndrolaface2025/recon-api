from sqlalchemy import Column, BigInteger, String, Integer, Text, TIMESTAMP, func
from app.db.base import Base

class SourceConfig(Base):
    __tablename__ = "tbl_cfg_source"

    id = Column(BigInteger, primary_key=True)
    source_name = Column(String(255), nullable=True)
    source_type = Column(Integer, nullable=True)
    source_json = Column(Text, nullable=True)
    status = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    version_number = Column(Integer, nullable=True)
    
    def __repr__(self):
        return (
            f"SourceConfig(id={self.id}, "
            f"source_name='{self.source_name}', "
            f"source_type={self.source_type}, "
            f"status={self.status})"
        )
    
    def __str__(self):
        return f"Source: {self.source_name} (ID: {self.id}, Type: {self.source_type})"

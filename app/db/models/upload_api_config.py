from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    ForeignKey,
    Text,
    TIMESTAMP,
    func,
)
from app.db.base import Base


class UploadAPIConfig(Base):
    __tablename__ = "tbl_cfg_upload_api"

    id = Column(BigInteger, primary_key=True)

    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=False)

    api_name = Column(String(255), nullable=False)
    method = Column(String(50), nullable=False)
    base_url = Column(Text, nullable=False)

    response_format = Column(String(50))
    auth_type = Column(String(50), nullable=False)
    auth_token = Column(Text)

    api_time_out = Column(Integer, default=30)
    max_try = Column(Integer, default=1)

    is_active = Column(Integer, default=1)

    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)

    updated_at = Column(
        TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)

    version_number = Column(Integer, default=1)

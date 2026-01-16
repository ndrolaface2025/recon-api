from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    ForeignKey,
    TIMESTAMP,
    func,
)
from app.db.base import Base


class UploadSchedulerConfig(Base):
    __tablename__ = "tbl_cfg_upload_schedulers"

    id = Column(BigInteger, primary_key=True)

    upload_api_id = Column(
        BigInteger, ForeignKey("tbl_cfg_upload_api.id"), nullable=False
    )

    scheduler_name = Column(String(255), nullable=False)

    # Cron expression (e.g. */15 * * * *)
    cron_expression = Column(String(100), nullable=False)

    timezone = Column(String(50), default="UTC")

    is_active = Column(Integer, default=1)

    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)

    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)

    updated_at = Column(
        TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    updated_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)

    version_number = Column(Integer, default=1)

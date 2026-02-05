from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    SmallInteger,
    String,
    ForeignKey,
    TIMESTAMP,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from app.db.base import Base


class UploadSchedulerHistory(Base):
    __tablename__ = "tbl_upload_scheduler_history"

    id = Column(BigInteger, primary_key=True)

    scheduler_id = Column(
        BigInteger,
        ForeignKey("tbl_cfg_upload_schedulers.id"),
        nullable=False,
    )

    started_at = Column(TIMESTAMP, nullable=False)
    finished_at = Column(TIMESTAMP, nullable=True)

    status = Column(SmallInteger, nullable=False, default=0)

    total_files = Column(Integer, default=0)
    failed_files = Column(Integer, default=0)

    file_names = Column(JSONB, nullable=True)

    error_message = Column(String(1000), nullable=True)

    created_at = Column(
        TIMESTAMP,
        server_default=func.now(),
        nullable=False,
    )

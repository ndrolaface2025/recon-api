from sqlalchemy import Column, BigInteger, String, Integer, ForeignKey, Text, TIMESTAMP, Float, func
from app.db.base import Base

class UploadFile(Base):
    __tablename__ = "tbl_upload_files"

    id = Column(BigInteger, primary_key=True)
    file_name = Column(String(255), nullable=True)
    file_details = Column(Text, nullable=True)
    channel_id = Column(BigInteger, ForeignKey("tbl_cfg_channels.id"), nullable=True)
    status = Column(Integer, nullable=True)
    record_details = Column(String(255), nullable=True)
    
    # Progress tracking fields for large file uploads
    total_records = Column(BigInteger, nullable=True, comment="Total records in uploaded file")
    processed_records = Column(BigInteger, default=0, nullable=True, comment="Number of records processed so far")
    success_records = Column(BigInteger, default=0, nullable=True, comment="Number of successfully inserted records")
    failed_records = Column(BigInteger, default=0, nullable=True, comment="Number of failed records")
    duplicate_records = Column(BigInteger, default=0, nullable=True, comment="Number of duplicate records skipped")
    progress_percentage = Column(Float, default=0.0, nullable=True, comment="Upload progress (0-100)")
    
    # Timing fields
    upload_started_at = Column(TIMESTAMP, nullable=True, comment="When upload processing started")
    upload_completed_at = Column(TIMESTAMP, nullable=True, comment="When upload processing completed")
    processing_time_seconds = Column(Integer, nullable=True, comment="Total processing time in seconds")
    
    # Error tracking
    error_message = Column(Text, nullable=True, comment="Error message if upload failed")
    error_details = Column(Text, nullable=True, comment="Detailed error information (JSON format)")
    
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=True)
    created_by = Column(BigInteger, ForeignKey("tbl_cfg_users.id"), nullable=True)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=True)
    version_number = Column(Integer, nullable=True)


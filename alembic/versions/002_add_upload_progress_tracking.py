"""Add progress tracking fields to upload_file table

Revision ID: add_progress_tracking
Revises: add_upload_indexes
Create Date: 2026-01-07

This migration adds progress tracking fields to tbl_upload_files:
- Record counts (total, processed, success, failed, duplicate)
- Progress percentage
- Timing information (started_at, completed_at, processing_time)
- Error tracking (error_message, error_details)

These fields enable real-time progress monitoring for large file uploads.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_progress_tracking'
down_revision = 'add_upload_indexes'
branch_labels = None
depends_on = None


def upgrade():
    """Add progress tracking fields"""
    
    # Record count fields
    op.add_column('tbl_upload_files', 
        sa.Column('total_records', sa.BigInteger(), nullable=True, 
                  comment='Total records in uploaded file'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('processed_records', sa.BigInteger(), nullable=True, server_default='0',
                  comment='Number of records processed so far'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('success_records', sa.BigInteger(), nullable=True, server_default='0',
                  comment='Number of successfully inserted records'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('failed_records', sa.BigInteger(), nullable=True, server_default='0',
                  comment='Number of failed records'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('duplicate_records', sa.BigInteger(), nullable=True, server_default='0',
                  comment='Number of duplicate records skipped'))
    
    # Progress percentage
    op.add_column('tbl_upload_files', 
        sa.Column('progress_percentage', sa.Float(), nullable=True, server_default='0.0',
                  comment='Upload progress (0-100)'))
    
    # Timing fields
    op.add_column('tbl_upload_files', 
        sa.Column('upload_started_at', sa.TIMESTAMP(), nullable=True,
                  comment='When upload processing started'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('upload_completed_at', sa.TIMESTAMP(), nullable=True,
                  comment='When upload processing completed'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('processing_time_seconds', sa.Integer(), nullable=True,
                  comment='Total processing time in seconds'))
    
    # Error tracking
    op.add_column('tbl_upload_files', 
        sa.Column('error_message', sa.Text(), nullable=True,
                  comment='Error message if upload failed'))
    
    op.add_column('tbl_upload_files', 
        sa.Column('error_details', sa.Text(), nullable=True,
                  comment='Detailed error information (JSON format)'))


def downgrade():
    """Remove progress tracking fields"""
    
    op.drop_column('tbl_upload_files', 'error_details')
    op.drop_column('tbl_upload_files', 'error_message')
    op.drop_column('tbl_upload_files', 'processing_time_seconds')
    op.drop_column('tbl_upload_files', 'upload_completed_at')
    op.drop_column('tbl_upload_files', 'upload_started_at')
    op.drop_column('tbl_upload_files', 'progress_percentage')
    op.drop_column('tbl_upload_files', 'duplicate_records')
    op.drop_column('tbl_upload_files', 'failed_records')
    op.drop_column('tbl_upload_files', 'success_records')
    op.drop_column('tbl_upload_files', 'processed_records')
    op.drop_column('tbl_upload_files', 'total_records')

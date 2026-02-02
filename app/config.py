import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import AnyUrl, field_validator
from typing import Optional, List


load_dotenv()


class Settings(BaseSettings):
    APP_NAME: str = os.getenv("APP_NAME")
    ENV: str = os.getenv("ENV")
    DEBUG: bool = True
    DATABASE_URL: str
    # Flexcube DB (Oracle)
    FLEXCUBE_DB_USER: str
    FLEXCUBE_DB_PASSWORD: str
    FLEXCUBE_DB_HOST: str = "localhost"
    FLEXCUBE_DB_PORT: int = 1521
    FLEXCUBE_DB_SERVICE: str

    SYSTEM_USER_ID: int

    REDIS_URL: Optional[str] = None
    OPENAI_API_KEY: str
    JWT_SECRET: str = "change-me"
    S3_ENDPOINT: Optional[AnyUrl] = None
    S3_BUCKET: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None  # Add OpenAI API key for LLM detection
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 24
    ENCRYPTION_KEY: str
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USERNAME: str
    SMTP_PASSWORD: str
    SMTP_FROM_EMAIL: str
    SMTP_FROM_NAME: str = "Reconciliation System"
    FRONTEND_URL: str = "http://localhost:3000"
    RESET_TOKEN_EXPIRE_MINUTES: int = 30

    # Celery Configuration
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    CELERY_TASK_TRACK_STARTED: bool = True
    CELERY_TASK_TIME_LIMIT: int = 1800  # 30 minutes
    CELERY_TASK_SOFT_TIME_LIMIT: int = 1500  # 25 minutes

    # Upload duplicate check batch size (tune based on Postgres max_stack_depth)
    UPLOAD_DUPLICATE_CHECK_BATCH_SIZE: int = (
        2000  # Safe default for max_stack_depth=2048kB
    )
    # Recommended values:
    # - 2000: Safe for default Postgres (2048kB stack)
    # - 5000: For max_stack_depth=4096kB (2.5x faster)
    # - 8000: For max_stack_depth=6144kB (4x faster, requires tuning)

    @field_validator("S3_ENDPOINT", mode="before")
    def _s3_endpoint_empty_to_none(cls, v):
        # allow empty string in .env to mean unset
        if v == "":
            return None
        return v

    @field_validator("S3_BUCKET", mode="before")
    def _s3_bucket_empty_to_none(cls, v):
        if v == "":
            return None
        return v

    ALLOWED_HOSTS: List[str] = ["*"]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()


def get_settings() -> Settings:
    """Helper function to get settings instance"""
    return settings

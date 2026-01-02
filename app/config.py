import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import AnyUrl, field_validator
from typing import Optional, List


load_dotenv()
class Settings(BaseSettings):
    APP_NAME: str = os.getenv("APP_NAME")  # can override via APP_NAME in .env
    ENV: str = os.getenv("ENV")
    DEBUG: bool = True
    DATABASE_URL: str
    REDIS_URL: Optional[str] = None
    JWT_SECRET: str = "change-me"
    S3_ENDPOINT: Optional[AnyUrl] = None
    S3_BUCKET: Optional[str] = None

    @field_validator('S3_ENDPOINT', mode='before')
    def _s3_endpoint_empty_to_none(cls, v):
        # allow empty string in .env to mean unset
        if v == '':
            return None
        return v

    @field_validator('S3_BUCKET', mode='before')
    def _s3_bucket_empty_to_none(cls, v):
        if v == '':
            return None
        return v
    ALLOWED_HOSTS: List[str] = ["*"]
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
settings = Settings()

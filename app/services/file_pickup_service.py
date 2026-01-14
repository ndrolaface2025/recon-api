"""
file_pickup_service.py
----------------------

Picks files from external locations (LOCAL / HTTP / FTP),
runs detect-source on them, then uploads using upload logic.

Called ONLY by scheduler execution.
"""

import os
import base64
import requests
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse
from ftplib import FTP
from tempfile import SpooledTemporaryFile

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class UploadApiConfig:
    id: int  # UploadAPIConfig.id
    channel_id: int
    api_name: str
    method: str  # LOCAL | HTTP | FTP
    base_url: str
    auth_type: str  # NONE | BASIC | BEARER | API_KEY | JWT
    auth_token: Optional[str]
    api_time_out: int
    max_try: int


class AuthHandler:
    def build_headers(self, config: UploadApiConfig) -> dict:
        token = config.auth_token

        if config.auth_type == "NONE":
            return {}

        if config.auth_type == "BASIC":
            encoded = base64.b64encode(token.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        if config.auth_type in ("BEARER", "JWT"):
            return {"Authorization": f"Bearer {token}"}

        if config.auth_type == "API_KEY":
            return {"x-api-key": token}

        raise ValueError(f"Unsupported auth type: {config.auth_type}")


class FilePickupService:
    """
    Responsibilities:
    - Fetch file bytes
    - Build UploadFile correctly
    - Call detect-source
    - Rewind file
    - Call UploadService.fileUpload(...)
    """

    def __init__(self, upload_service, db: AsyncSession):
        if isinstance(upload_service, type) or not hasattr(
            upload_service, "fileUpload"
        ):
            raise RuntimeError("UploadService must be an INSTANCE")

        self.upload_service = upload_service
        self.db = db
        self.auth = AuthHandler()

    async def pickup(self, config: UploadApiConfig):
        """
        Called by scheduler runner.
        """
        print(f"\n=== FILE PICKUP START: {config.api_name} (API ID={config.id}) ===")

        if config.method == "LOCAL":
            await self._pickup_local(config)

        elif config.method == "HTTP":
            await self._pickup_http(config)

        elif config.method == "FTP":
            await self._pickup_ftp(config)

        else:
            raise ValueError(f"Unsupported pickup method: {config.method}")

        print(f"=== FILE PICKUP END: {config.api_name} ===\n")

    async def _process_file(self, upload_file: UploadFile):
        """
        1. Detect source
        2. Rewind file
        3. Upload via UploadService
        """
        from app.api.v1.routers.reconciliation import detect_source

        # Detect source (consumes file stream)
        detection = await detect_source(upload_file, self.db)

        if detection.get("status") != "success":
            raise RuntimeError("Source detection failed")

        data = detection["data"]
        channel = data.get("channel_details")
        source = data.get("source_details")
        mappings = data.get("column_mapping")

        if not channel or not source:
            raise RuntimeError("Channel or Source not detected")

        # Rewind after detect-source
        await upload_file.seek(0)

        # Delegate to real upload logic
        await self.upload_service.fileUpload(
            upload_file,
            channel["id"],
            source["id"],
            mappings,
        )

    @staticmethod
    def _build_upload_file(filename: str, content: bytes) -> UploadFile:
        """
        Correct way to construct UploadFile programmatically.
        """
        tmp = SpooledTemporaryFile()
        tmp.write(content)
        tmp.seek(0)

        return UploadFile(filename=filename, file=tmp)

    async def _pickup_local(self, config: UploadApiConfig):
        if not os.path.isdir(config.base_url):
            raise RuntimeError(f"Directory not found: {config.base_url}")

        for name in sorted(os.listdir(config.base_url)):
            path = os.path.join(config.base_url, name)

            if not os.path.isfile(path):
                continue

            with open(path, "rb") as f:
                upload_file = self._build_upload_file(name, f.read())

            await self._process_file(upload_file)

    async def _pickup_http(self, config: UploadApiConfig):
        headers = self.auth.build_headers(config)

        files = requests.get(
            f"{config.base_url}/files",
            headers=headers,
            timeout=config.api_time_out,
        ).json()

        for f in files:
            name = f["name"]

            r = requests.get(
                f"{config.base_url}/files/{name}",
                headers=headers,
                timeout=config.api_time_out,
            )
            r.raise_for_status()

            upload_file = self._build_upload_file(name, r.content)
            await self._process_file(upload_file)

    async def _pickup_ftp(self, config: UploadApiConfig):
        parsed = urlparse(config.base_url)

        if parsed.scheme != "ftp":
            raise ValueError("FTP base_url must start with ftp://")

        if config.auth_type == "BASIC" and config.auth_token:
            user, password = config.auth_token.split(":", 1)
        else:
            user, password = "anonymous", ""

        ftp = FTP(timeout=config.api_time_out)
        ftp.connect(parsed.hostname, parsed.port or 21)
        ftp.login(user=user, passwd=password)
        ftp.cwd(parsed.path or "/")

        try:
            for name in ftp.nlst():
                buf = bytearray()
                ftp.retrbinary(f"RETR {name}", buf.extend)

                upload_file = self._build_upload_file(name, bytes(buf))
                await self._process_file(upload_file)

        finally:
            ftp.quit()

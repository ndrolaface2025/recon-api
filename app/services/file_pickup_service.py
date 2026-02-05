"""
file_pickup_service.py
----------------------

Picks files from external locations (LOCAL / HTTP / FTP / SFTP),
runs detect-source on them, then uploads using upload logic.

Called ONLY by scheduler execution.
"""

import os
import base64
import requests
from dataclasses import dataclass
from typing import Optional, List
from urllib.parse import urlparse
from ftplib import FTP
from tempfile import SpooledTemporaryFile
from datetime import datetime
import re
import paramiko

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update

from app.config import settings
from app.db.models.upload_scheduler_history import UploadSchedulerHistory
from app.services.upload_scheduler_history_service import (
    UploadSchedulerHistoryService,
)
from app.utils.enums.scheduler import SchedulerStatus

ALLOWED_EXTENSIONS = (".csv", ".txt", ".xlsx")


@dataclass
class UploadApiConfig:
    id: int
    channel_id: int
    api_name: str
    method: str  # LOCAL | HTTP | FTP | SFTP
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
    def __init__(
        self,
        upload_service,
        db: AsyncSession,
        scheduler_id: int,
    ):
        if isinstance(upload_service, type) or not hasattr(
            upload_service, "fileUpload"
        ):
            raise RuntimeError("UploadService must be an INSTANCE")

        self.upload_service = upload_service
        self.db = db
        self.scheduler_id = scheduler_id
        self.auth = AuthHandler()
        self.history_service = UploadSchedulerHistoryService(db)

    @staticmethod
    def _is_valid_file(name: str) -> bool:
        if name.startswith("."):
            return False
        return name.lower().endswith(ALLOWED_EXTENSIONS)

    @staticmethod
    def _build_upload_file(filename: str, content: bytes) -> UploadFile:
        tmp = SpooledTemporaryFile()
        tmp.write(content)
        tmp.seek(0)
        return UploadFile(filename=filename, file=tmp)

    def _extract_filename(self, response, config: UploadApiConfig) -> str:
        cd = response.headers.get("Content-Disposition")
        if cd:
            match = re.search(r'filename="?([^"]+)"?', cd)
            if match:
                return match.group(1)

        path = urlparse(config.base_url).path
        if path and path != "/":
            return os.path.basename(path)

        return f"pickup_{config.api_name}.dat"

    async def pickup(self, config: UploadApiConfig):
        history_resp = await self.history_service.create_entry(
            scheduler_id=self.scheduler_id
        )

        history = history_resp.get("data")

        attempted: List[str] = []
        successful: List[str] = []
        failed_files = 0
        error_message = None

        try:
            if config.method == "LOCAL":
                await self._pickup_local(config, attempted, successful)

            elif config.method == "HTTP":
                await self._pickup_http(config, attempted, successful)

            elif config.method == "FTP":
                await self._pickup_ftp(config, attempted, successful)

            elif config.method == "SFTP":
                await self._pickup_sftp(config, attempted, successful)

            else:
                raise ValueError(f"Unsupported pickup method: {config.method}")

            failed_files = len(attempted) - len(successful)

            if failed_files == 0:
                status = SchedulerStatus.SUCCESS.value
            elif successful:
                status = SchedulerStatus.PARTIAL.value
            else:
                status = SchedulerStatus.FAILED.value

        except Exception as e:
            status = SchedulerStatus.FAILED.value
            failed_files = len(attempted)
            error_message = str(e)

        await self._finalize_history(
            history_id=history.id,
            status=status,
            picked_files=successful,
            failed_files=failed_files,
            error_message=error_message,
        )

    async def _process_file(self, upload_file: UploadFile):
        from app.api.v1.routers.reconciliation import detect_source

        detection = await detect_source(upload_file, self.db)

        if detection.get("status") != "success":
            raise RuntimeError("Source detection failed")

        data = detection["data"]
        channel = data.get("channel_details")
        source = data.get("source_details")
        mappings = data.get("column_mapping")

        if not channel or not source:
            raise RuntimeError("Channel or Source not detected")

        await upload_file.seek(0)

        await self.upload_service.fileUpload(
            upload_file,
            channel["id"],
            source["id"],
            mappings,
            settings.SYSTEM_USER_ID,
        )

    async def _pickup_local(
        self,
        config: UploadApiConfig,
        attempted: list[str],
        successful: list[str],
    ):
        if not os.path.isdir(config.base_url):
            raise RuntimeError(f"Directory not found: {config.base_url}")

        for name in sorted(os.listdir(config.base_url)):
            if not self._is_valid_file(name):
                continue

            path = os.path.join(config.base_url, name)
            if not os.path.isfile(path):
                continue

            attempted.append(name)

            try:
                with open(path, "rb") as f:
                    upload_file = self._build_upload_file(name, f.read())
                await self._process_file(upload_file)
                successful.append(name)
            except Exception:
                continue

        if not attempted:
            raise RuntimeError("No valid files found to process")

    async def _pickup_http(
        self,
        config: UploadApiConfig,
        attempted: list[str],
        successful: list[str],
    ):
        headers = self.auth.build_headers(config)

        r = requests.get(
            config.base_url,
            headers=headers,
            timeout=config.api_time_out,
        )
        r.raise_for_status()

        filename = self._extract_filename(r, config)

        if not self._is_valid_file(filename):
            raise RuntimeError(f"Invalid file received: {filename}")

        attempted.append(filename)

        upload_file = self._build_upload_file(filename, r.content)
        await self._process_file(upload_file)
        successful.append(filename)

    async def _pickup_ftp(
        self,
        config: UploadApiConfig,
        attempted: list[str],
        successful: list[str],
    ):
        parsed = urlparse(config.base_url)
        if parsed.scheme != "ftp":
            raise ValueError("FTP base_url must start with ftp://")

        user, password = (
            config.auth_token.split(":", 1)
            if config.auth_type == "BASIC" and config.auth_token
            else ("anonymous", "")
        )

        ftp = FTP(timeout=config.api_time_out)
        ftp.set_pasv(True)
        ftp.use_epsv = False
        ftp.connect(parsed.hostname, parsed.port or 21)
        ftp.login(user=user, passwd=password)

        path = parsed.path.lstrip("/") or "."
        ftp.cwd(path)

        try:
            names = []
            ftp.retrlines("NLST", names.append)

            for name in names:
                if not self._is_valid_file(name):
                    continue

                attempted.append(name)

                try:
                    tmp = SpooledTemporaryFile()
                    ftp.retrbinary(f"RETR {name}", tmp.write)
                    tmp.seek(0)

                    upload_file = UploadFile(filename=name, file=tmp)
                    await self._process_file(upload_file)
                    successful.append(name)
                except Exception:
                    continue
        finally:
            ftp.quit()

    async def _pickup_sftp(
        self,
        config: UploadApiConfig,
        attempted: list[str],
        successful: list[str],
    ):
        parsed = urlparse(config.base_url)
        if parsed.scheme != "sftp":
            raise ValueError("SFTP base_url must start with sftp://")

        user, password = config.auth_token.split(":", 1)

        transport = paramiko.Transport((parsed.hostname, parsed.port or 22))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.chdir(parsed.path or "/")
            for name in sftp.listdir():
                if not self._is_valid_file(name):
                    continue

                attempted.append(name)

                try:
                    remote_path = (
                        f"{parsed.path.rstrip('/')}/{name}" if parsed.path else name
                    )
                    with sftp.open(remote_path, "rb") as f:
                        content = f.read()

                    upload_file = self._build_upload_file(name, content)
                    await self._process_file(upload_file)
                    successful.append(name)
                except Exception:
                    continue
        finally:
            sftp.close()
            transport.close()

    async def _finalize_history(
        self,
        history_id: int,
        status: int,
        picked_files: list[str],
        failed_files: int,
        error_message: Optional[str],
    ):
        stmt = (
            update(UploadSchedulerHistory)
            .where(UploadSchedulerHistory.id == history_id)
            .values(
                finished_at=datetime.utcnow(),
                status=status,
                total_files=len(picked_files) + failed_files,
                failed_files=failed_files,
                file_names=picked_files,
                error_message=error_message,
            )
        )

        await self.db.execute(stmt)
        await self.db.commit()

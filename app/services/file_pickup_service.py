"""
file_pickup_service.py
----------------------

Industry-grade file pickup service.

Design guarantees:
- File-level isolation
- Scheduler-level isolation
- Zero silent failures
- Structured, actionable logs (Loguru)
"""

import os
import base64
import re
import asyncio
from dataclasses import dataclass
import ssl
from typing import Optional, List
from urllib.parse import urlparse
from tempfile import SpooledTemporaryFile
from datetime import datetime, timezone

import requests
import paramiko
from ftplib import FTP, FTP_TLS
from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from loguru import logger

from app.config import settings
from app.db.models.upload_scheduler_history import UploadSchedulerHistory
from app.services.upload_scheduler_history_service import (
    UploadSchedulerHistoryService,
)
from app.utils.enums.scheduler import SchedulerStatus

ALLOWED_EXTENSIONS = (".csv", ".txt", ".xlsx")
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB safety cap


@dataclass
class UploadApiConfig:
    id: int
    channel_id: int
    api_name: str
    method: str  # LOCAL | HTTP | FTP | SFTP
    base_url: str
    auth_type: str
    auth_token: Optional[str]
    api_time_out: int
    max_try: int


class AuthHandler:
    def build_headers(self, config: UploadApiConfig) -> dict:
        if config.auth_type == "NONE":
            return {}

        token = config.auth_token or ""

        if config.auth_type == "BASIC":
            encoded = base64.b64encode(token.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        if config.auth_type in ("BEARER", "JWT"):
            return {"Authorization": f"Bearer {token}"}

        if config.auth_type == "API_KEY":
            return {"x-api-key": token}

        raise ValueError(f"Unsupported auth type: {config.auth_type}")


class FilePickupService:
    def __init__(self, upload_service, db: AsyncSession, scheduler_id: int):
        if isinstance(upload_service, type):
            raise RuntimeError("UploadService must be an instance")

        self.upload_service = upload_service
        self.db = db
        self.scheduler_id = scheduler_id
        self.auth = AuthHandler()
        self.history_service = UploadSchedulerHistoryService(db)

        # base logger context
        self.log = logger.bind(
            component="file_pickup",
            scheduler_id=scheduler_id,
        )

    @staticmethod
    def _is_valid_file(name: str) -> bool:
        return (
            not name.startswith(".")
            and ".." not in name
            and name.lower().endswith(ALLOWED_EXTENSIONS)
        )

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
        log = self.log.bind(
            api_id=config.id,
            api_name=config.api_name,
            method=config.method,
            base_url=config.base_url,
        )

        log.info("Pickup started")

        history = (
            await self.history_service.create_entry(scheduler_id=self.scheduler_id)
        )["data"]

        attempted: List[str] = []
        successful: List[str] = []
        status = SchedulerStatus.FAILED.value
        error_message = None

        try:
            if config.method == "LOCAL":
                log.debug("Using LOCAL pickup")
                await self._pickup_local(config, attempted, successful)

            elif config.method == "HTTP":
                log.debug("Using HTTP pickup")
                await self._pickup_http(config, attempted, successful)

            elif config.method == "FTP":
                log.debug("Using FTP pickup")
                await self._pickup_ftp(config, attempted, successful)

            elif config.method == "SFTP":
                log.debug("Using SFTP pickup")
                await self._pickup_sftp(config, attempted, successful)

            else:
                raise ValueError(f"Unsupported pickup method: {config.method}")

            failed = len(attempted) - len(successful)

            if failed == 0:
                status = SchedulerStatus.SUCCESS.value
            elif successful:
                status = SchedulerStatus.PARTIAL.value
            else:
                status = SchedulerStatus.FAILED.value

            log.info(
                "Pickup completed",
                attempted=len(attempted),
                successful=len(successful),
                failed=failed,
                status=status,
            )

        except Exception as e:
            error_message = str(e)
            log.exception("Pickup crashed")

        finally:
            try:
                await self._finalize_history(
                    history_id=history.id,
                    status=status,
                    picked_files=successful,
                    failed_files=len(attempted) - len(successful),
                    error_message=error_message,
                )
            except Exception:
                self.log.critical(
                    "Failed to finalize scheduler history (isolated session)",
                    history_id=history.id,
                    exc_info=True,
                )

    async def _process_file(self, upload_file: UploadFile, file_log):
        from app.api.v1.routers.reconciliation import detect_source

        file_log.debug("Detecting source")

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

        file_log.debug(
            "Uploading file",
            channel_id=channel["id"],
            source_id=source["id"],
        )

        await self.upload_service.fileUpload(
            upload_file,
            channel["id"],
            source["id"],
            mappings,
            settings.SYSTEM_USER_ID,
        )

        file_log.success("File processed successfully")

    async def _pickup_local(self, config, attempted, successful):
        if not os.path.isdir(config.base_url):
            raise RuntimeError(f"Directory not found: {config.base_url}")

        for name in sorted(os.listdir(config.base_url)):
            if not self._is_valid_file(name):
                continue

            attempted.append(name)
            file_log = self.log.bind(file=name, method="LOCAL")

            try:
                file_log.info("Processing file")
                with open(os.path.join(config.base_url, name), "rb") as f:
                    upload_file = self._build_upload_file(name, f.read())

                await self._process_file(upload_file, file_log)
                successful.append(name)

            except Exception:
                file_log.exception("File failed")
                continue

        if not attempted:
            raise RuntimeError("No valid files found to process")

    async def _pickup_http(self, config, attempted, successful):
        headers = self.auth.build_headers(config)

        response = await asyncio.to_thread(
            requests.get,
            config.base_url,
            headers=headers,
            timeout=config.api_time_out,
        )
        response.raise_for_status()

        filename = self._extract_filename(response, config)
        attempted.append(filename)

        file_log = self.log.bind(file=filename, method="HTTP")

        if not self._is_valid_file(filename):
            file_log.warning("Invalid file extension received")
            return

        try:
            upload_file = self._build_upload_file(filename, response.content)
            await self._process_file(upload_file, file_log)
            successful.append(filename)
        except Exception:
            file_log.exception("File failed")

    async def _pickup_ftp(self, config, attempted, successful):
        parsed = urlparse(config.base_url)

        log = self.log.bind(
            method="FTP",
            host=parsed.hostname,
            port=parsed.port or 21,
            path=parsed.path or "/",
        )
        if config.auth_type == "BASIC" and config.auth_token:
            try:
                user, password = config.auth_token.split(":", 1)
            except ValueError:
                raise RuntimeError("FTP auth_token must be 'user:password'")
        else:
            user, password = "anonymous", ""

        log.info("Connecting to FTP", user=user)

        ftp = FTP(timeout=config.api_time_out)

        try:
            await asyncio.to_thread(
                ftp.connect,
                parsed.hostname,
                parsed.port or 21,
            )

            await asyncio.to_thread(
                ftp.login,
                user=user,
                passwd=password,
            )

            ftp.set_pasv(True)
            ftp.use_epsv = False

            ftp.cwd(parsed.path.lstrip("/") or ".")

            names: list[str] = []
            ftp.retrlines("NLST", names.append)

            log.info("Directory listed", file_count=len(names))

            for name in names:
                if not self._is_valid_file(name):
                    log.debug("Skipping invalid file", file=name)
                    continue

                attempted.append(name)
                file_log = log.bind(file=name)

                try:
                    file_log.info("Downloading file")

                    tmp = SpooledTemporaryFile()
                    ftp.retrbinary(f"RETR {name}", tmp.write)
                    tmp.seek(0)

                    await self._process_file(
                        UploadFile(filename=name, file=tmp),
                        file_log,
                    )

                    successful.append(name)
                    file_log.success("File processed")

                except Exception:
                    file_log.exception("File failed")

        finally:
            try:
                ftp.quit()
                log.debug("FTP connection closed")
            except Exception:
                log.warning("FTP quit failed (connection may already be closed)")

    async def _pickup_ftps(self, config, attempted, successful):
        parsed = urlparse(config.base_url)

        log = self.log.bind(
            method="FTPS",
            host=parsed.hostname,
            port=parsed.port or 21,
            path=parsed.path or "/",
        )

        # ---------------- AUTH ----------------
        if config.auth_type == "BASIC" and config.auth_token:
            try:
                user, password = config.auth_token.split(":", 1)
            except ValueError:
                raise RuntimeError("FTPS auth_token must be 'user:password'")
        else:
            raise RuntimeError("FTPS requires BASIC auth")

        log.info("Connecting to FTPS", user=user)

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE  # ← common in bank FTPS servers

        ftps = FTP_TLS(context=ssl_ctx, timeout=config.api_time_out)

        try:
            await asyncio.to_thread(
                ftps.connect,
                parsed.hostname,
                parsed.port or 21,
            )

            await asyncio.to_thread(ftps.auth)
            await asyncio.to_thread(
                ftps.login,
                user=user,
                passwd=password,
            )

            await asyncio.to_thread(ftps.prot_p)

            ftps.set_pasv(True)
            ftps.cwd(parsed.path.lstrip("/") or ".")

            names: list[str] = []
            ftps.retrlines("NLST", names.append)

            log.info("Directory listed", file_count=len(names))

            for name in names:
                if not self._is_valid_file(name):
                    log.debug("Skipping invalid file", file=name)
                    continue

                attempted.append(name)
                file_log = log.bind(file=name)

                try:
                    file_log.info("Downloading file")

                    tmp = SpooledTemporaryFile()
                    ftps.retrbinary(f"RETR {name}", tmp.write)
                    tmp.seek(0)

                    await self._process_file(
                        UploadFile(filename=name, file=tmp),
                        file_log,
                    )

                    successful.append(name)
                    file_log.success("File processed")

                except Exception:
                    file_log.exception("File failed")

        finally:
            try:
                ftps.quit()
                log.debug("FTPS connection closed")
            except Exception:
                log.warning("FTPS quit failed (connection already closed)")

    async def _pickup_sftp(self, config, attempted, successful):
        parsed = urlparse(config.base_url)

        if not config.auth_token:
            raise RuntimeError("SFTP auth_token required (user:password)")

        try:
            user, password = config.auth_token.split(":", 1)
        except ValueError:
            raise RuntimeError("SFTP auth_token must be 'user:password'")

        log = self.log.bind(
            method="SFTP",
            host=parsed.hostname,
            port=parsed.port or 22,
            path=parsed.path or "/",
            user=user,
        )

        log.info("Connecting to SFTP")

        transport = paramiko.Transport((parsed.hostname, parsed.port or 22))

        try:
            await asyncio.to_thread(
                transport.connect,
                username=user,
                password=password,
            )

            sftp = paramiko.SFTPClient.from_transport(transport)

            directory = parsed.path or "/"
            sftp.chdir(directory)

            names = sftp.listdir()
            log.info("Directory listed", file_count=len(names))

            for name in names:
                if not self._is_valid_file(name):
                    log.debug("Skipping invalid file", file=name)
                    continue

                attempted.append(name)
                file_log = log.bind(file=name)

                try:
                    file_log.info("Downloading file")

                    remote_path = f"{directory.rstrip('/')}/{name}"

                    with sftp.open(remote_path, "rb") as f:
                        upload_file = self._build_upload_file(name, f.read())

                    await self._process_file(upload_file, file_log)

                    successful.append(name)
                    file_log.success("File processed")

                except Exception:
                    file_log.exception("File failed")

        finally:
            try:
                transport.close()
                log.debug("SFTP connection closed")
            except Exception:
                log.warning("SFTP transport close failed")

    async def _finalize_history(
        self,
        history_id: int,
        status: int,
        picked_files: list[str],
        failed_files: int,
        error_message: str | None,
    ):
        from datetime import datetime
        from app.db.session import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
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

            result = await db.execute(stmt)

            if result.rowcount == 0:
                raise RuntimeError(
                    f"Finalize failed: history_id {history_id} not found"
                )

            await db.commit()

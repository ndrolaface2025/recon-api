from datetime import datetime
from math import ceil
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.upload_scheduler_history_repository import (
    UploadSchedulerHistoryRepository,
)
from app.utils.enums.scheduler import SchedulerStatus


class UploadSchedulerHistoryService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_entry(self, scheduler_id: int):
        """
        Create scheduler history entry at execution start.
        MUST either return a persisted entity or raise.
        """

        payload = {
            "scheduler_id": scheduler_id,
            "started_at": datetime.utcnow(),
            "status": SchedulerStatus.IN_PROGRESS.value,
            "total_files": 0,
            "failed_files": 0,
            "file_names": [],
        }

        result = await UploadSchedulerHistoryRepository.create(self.db, payload)

        if result.get("status") != "success" or not result.get("data"):
            raise RuntimeError(
                f"Failed to create scheduler history for scheduler_id={scheduler_id}. "
                f"Reason: {result.get('message')}"
            )

        return {
            "success": True,
            "message": "Scheduler history created",
            "data": result["data"],
            "meta": None,
        }

    async def get_all(
        self,
        scheduler_id: int | None = None,
        status: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 1,
        page_size: int = 20,
    ):
        """
        Fetch scheduler history with filters
        and return standardized API response.
        """

        # -----------------------------
        # Validate date range
        # -----------------------------
        if date_from and date_to and date_from > date_to:
            return self._error_response(
                message="date_from cannot be greater than date_to",
                page=page,
                page_size=page_size,
            )

        filters = {
            "scheduler_id": scheduler_id,
            "status": SchedulerStatus(status) if status is not None else None,
            "date_from": date_from,
            "date_to": date_to,
            "page": page,
            "page_size": page_size,
        }

        result = await UploadSchedulerHistoryRepository.get_all(self.db, filters)

        if result.get("status") != "success":
            return self._error_response(
                message=result.get("message", "Failed to fetch scheduler history"),
                page=page,
                page_size=page_size,
            )

        records = result.get("data", [])
        total_records = result.get("total", 0)
        total_pages = ceil(total_records / page_size) if page_size else 1

        return {
            "success": True,
            "message": "Scheduler history fetched successfully",
            "data": records,
            "meta": {
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_records": total_records,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1,
                }
            },
        }

    @staticmethod
    def _error_response(message: str, page: int, page_size: int):
        return {
            "success": False,
            "message": message,
            "data": [],
            "meta": {
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_records": 0,
                    "total_pages": 0,
                    "has_next": False,
                    "has_previous": False,
                }
            },
        }

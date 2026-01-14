from app.celery_app import celery_app
import asyncio, json
from app.engine.reconciliation_engine import ReconciliationEngine
from app.db.session import AsyncSessionLocal
from app.db.repositories.upload import UploadRepository

import pandas as pd
from app.services.data_normalize_service import ReconDataNormalizer

import asyncio
from datetime import datetime
from croniter import croniter
import pytz


SCHEMA_V1 = {
    "datetime": "datetime",
    "terminalid": "string",
    "location": "string",
    "pan_masked": "string",
    "account_masked": "string",
    "transactiontype": "string",
    "currency": "string",
    "amount": "float",
    "rrn": "string",
    "stan": "string",
    "auth": "string",
    "responsecode": "string",
    "atm_index": "int",
}
# Global event loop for this worker process
_worker_loop = None


def get_or_create_event_loop():
    """Get or create a persistent event loop for this worker process"""
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
    return _worker_loop


@celery_app.task(bind=True, max_retries=3, name="app.workers.tasks.start_recon_job")
def start_recon_job(self, channel: str, inputs: dict):
    print("Channel:", channel)
    try:
        loop = get_or_create_event_loop()
        engine = ReconciliationEngine()
        result = loop.run_until_complete(engine.run(channel, inputs))

        out_path = list(inputs.values())[0] + "_result.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, default=str)

        return {"status": "ok", "result_path": out_path}

    except Exception as exc:
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(
    bind=True, max_retries=3, name="app.workers.tasks.process_upload_batch"
)
def process_upload_batch(
    self,
    batch_data: list,
    file_json: dict,
    file_id: int,
    batch_number: int,
    total_batches: int,
    column_mappings: dict,
):
    """
    Process a batch of uploaded transactions.

    OPTIMIZED:
    - Batch size 5000 (500x improvement from 10)
    - Bulk duplicate detection (100,000x faster)
    - Progress tracking after each batch

    Args:
        batch_data: List of transaction dicts to process
        file_json: File metadata (channel_id, source_id, etc.)
        file_id: Upload file ID for progress tracking
        batch_number: Current batch number (1-indexed)
        total_batches: Total number of batches
        column_mappings: Dict with keys: date, amount, account_number, currency
    """
    print(
        f"Processing batch {batch_number}/{total_batches} for file {file_id} ({len(batch_data)} records)"
    )

    try:

        async def process_batch():
            async with AsyncSessionLocal() as db:
                try:
                    # Update status to processing on first batch
                    if batch_number == 1:
                        await UploadRepository.updateFileStatus(db, file_id, status=1)

                    print(
                        f"Normalized data for batch {batch_number}... with {len(batch_data)} records and data sample: {batch_data[0] if batch_data else 'No records'}"
                    )
                    df = pd.DataFrame(batch_data)

                    # df_normalized = (
                    #     ReconDataNormalizer(df)
                    #     .run_all(
                    #         datetime_column=column_mappings.get("date"),
                    #         amount_column=column_mappings.get("amount"),
                    #         schema=SCHEMA_V1,
                    #         tz="UTC"
                    #     )
                    # )

                    df_normalized = (
                        ReconDataNormalizer(df)
                        .normalize_columns()
                        .sanitize_strings()
                        .normalize_datetime("datetime")
                        .clean_amount("amount")
                        .get_df()
                    )

                    normalized_batch_data = df_normalized.to_dict(orient="records")

                    print(
                        f"Normalized batch {batch_number}, first record: {normalized_batch_data[0] if normalized_batch_data else 'No records'}"
                    )

                    # Process the batch with bulk duplicate detection
                    result = await UploadRepository.saveFileDetailsBatch(
                        db=db,
                        fileData=normalized_batch_data,
                        fileJson=file_json,
                        column_mappings=column_mappings,
                    )

                    if result.get("error"):
                        raise Exception(result.get("message", "Unknown error"))

                    # Calculate cumulative progress
                    records_saved = result.get("recordsSaved", 0)
                    duplicates_count = len(result.get("duplicateRecords", []))

                    # Get current progress from database
                    progress_info = await UploadRepository.getUploadProgress(
                        db, file_id
                    )

                    current_processed = progress_info.get("processed_records", 0)
                    current_success = progress_info.get("success_records", 0)
                    current_duplicates = progress_info.get("duplicate_records", 0)
                    total_records = progress_info.get("total_records", 0)

                    # Update cumulative counts
                    new_processed = current_processed + len(batch_data)
                    new_success = current_success + records_saved
                    new_duplicates = current_duplicates + duplicates_count

                    # Update progress
                    await UploadRepository.updateUploadProgress(
                        db=db,
                        file_id=file_id,
                        processed=new_processed,
                        success=new_success,
                        failed=0,  # Update if you track failed records
                        duplicates=new_duplicates,
                        total=total_records,
                    )

                    # Mark as completed if this is the last batch
                    if batch_number == total_batches:
                        await UploadRepository.updateFileStatus(db, file_id, status=2)

                    return {
                        "status": "success",
                        "batch_number": batch_number,
                        "records_processed": len(batch_data),
                        "records_saved": records_saved,
                        "duplicates": duplicates_count,
                    }

                except Exception as e:
                    # Mark upload as failed
                    await UploadRepository.updateFileStatus(
                        db=db,
                        file_id=file_id,
                        status=3,
                        error_message=f"Batch {batch_number} failed: {str(e)}",
                        error_details=json.dumps(
                            {"batch_number": batch_number, "error": str(e)}
                        ),
                    )
                    raise

        # Run async function using persistent event loop
        loop = get_or_create_event_loop()
        result = loop.run_until_complete(process_batch())
        return result

    except Exception as exc:
        print(f"Error processing batch {batch_number}: {str(exc)}")
        raise self.retry(exc=exc, countdown=30)


@celery_app.task(
    name="app.workers.scheduler_tasks.run_scheduler_tick",
    queue="scheduler",
)
def run_scheduler_tick():
    """
    Entry point for scheduler worker.
    """
    loop = get_or_create_event_loop()
    loop.run_until_complete(_run())


async def _run():
    from app.db.session import AsyncSessionLocal
    from app.services.upload_scheduler_config_service import (
        UploadSchedulerConfigService,
    )
    from app.services.upload_api_config_service import UploadAPIConfigService
    from app.services.file_pickup_service import FilePickupService, UploadApiConfig
    from app.services.upload_service import UploadService

    utc_now = datetime.utcnow()

    async with AsyncSessionLocal() as db:
        scheduler_service = UploadSchedulerConfigService(db)
        upload_api_service = UploadAPIConfigService(db)

        upload_service = UploadService(db)
        pickup_service = FilePickupService(upload_service, db)

        schedulers = (
            (await scheduler_service.get_all(filters={"is_active": 1}))
            .get("result", {})
            .get("data", [])
        )

        print(f"📦 TOTAL ACTIVE SCHEDULERS FOUND: {len(schedulers)}")

        for scheduler in schedulers:
            cron_expr = scheduler["cron_expression"]
            timezone = scheduler.get("timezone", "UTC")

            raw_tz = scheduler.get("timezone", "UTC")
            timezone = raw_tz.strip()

            try:
                tz = pytz.timezone(timezone)
            except Exception:
                print(
                    "❌❌❌ INVALID TIMEZONE AFTER STRIP — FALLING BACK TO UTC ❌❌❌"
                )
                tz = pytz.UTC

            now = pytz.utc.localize(utc_now).astimezone(tz)

            try:
                match = croniter.match(cron_expr, now)
                print("✅ CRON MATCH RESULT:", match)
            except Exception as e:
                print("❌❌❌ CRON PARSE ERROR ❌❌❌")
                print("❌ ERROR:", str(e))
                continue

            if not match:
                print("⛔⛔⛔ CRON DID NOT MATCH — SKIPPING ⛔⛔⛔")
                continue

            print("🔥🔥🔥 CRON MATCHED — FIRING SCHEDULER 🔥🔥🔥")

            upload_api_id = scheduler["upload_api_id"]

            api_result = await upload_api_service.get_by_id(upload_api_id)
            api_cfg = api_result.get("result", {}).get("data")

            if not api_cfg:
                print("❌❌❌ UPLOAD API NOT FOUND — SKIPPING ❌❌❌")
                continue

            if api_cfg.get("is_active") != 1:
                print("⛔⛔⛔ UPLOAD API IS DISABLED — SKIPPING ⛔⛔⛔")
                continue

            print(
                f"\n🚀🚀🚀 EXECUTING PICKUP 🚀🚀🚀\n"
                f"📛 API NAME: {api_cfg['api_name']}\n"
                f"📂 METHOD: {api_cfg['method']}\n"
                f"🌐 BASE URL: {api_cfg['base_url']}\n"
            )

            await pickup_service.pickup(
                UploadApiConfig(
                    id=api_cfg["id"],
                    channel_id=api_cfg["channel_id"],
                    api_name=api_cfg["api_name"],
                    method=api_cfg["method"],
                    base_url=api_cfg["base_url"],
                    auth_type=api_cfg["auth_type"],
                    auth_token=api_cfg.get("auth_token"),
                    api_time_out=int(api_cfg.get("api_time_out", 30)),
                    max_try=int(api_cfg.get("max_try", 1)),
                )
            )

            print("✅✅✅ PICKUP COMPLETED SUCCESSFULLY ✅✅✅")

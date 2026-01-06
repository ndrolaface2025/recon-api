from celery_app import celery_app
import asyncio, json
from app.engine.reconciliation_engine import ReconciliationEngine

@celery_app.task(bind=True, max_retries=3, name="app.workers.tasks.start_recon_job")
def start_recon_job(self, channel: str, inputs: dict):
    print("Channel:", channel)
    try:
        engine = ReconciliationEngine()
        result = asyncio.run(engine.run(channel, inputs))

        out_path = list(inputs.values())[0] + "_result.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, default=str)

        return {"status": "ok", "result_path": out_path}

    except Exception as exc:
        raise self.retry(exc=exc, countdown=60)

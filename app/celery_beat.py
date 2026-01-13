from datetime import timedelta
from app.celery_app import celery_app

celery_app.conf.beat_schedule = {
    "scheduler-tick-every-10-seconds": {
        "task": "app.workers.scheduler_tasks.run_scheduler_tick",
        "schedule": timedelta(seconds=30),
        "options": {"queue": "scheduler"},
    }
}

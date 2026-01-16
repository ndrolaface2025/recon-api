from app.celery_app import celery_app

from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    "file-pickup-scheduler-dispatch": {
        "task": "app.workers.tasks.file_pickup_scheduler.run",
        "schedule": crontab(minute="*"),
        "options": {"queue": "file-pickup-scheduler"},
    }
}

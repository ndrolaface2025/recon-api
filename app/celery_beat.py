from app.celery_app import celery_app

from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    "scheduler-tick-every-minute": {
        "task": "app.workers.scheduler_tasks.run_scheduler_tick",
        "schedule": crontab(minute="*"),
        "options": {"queue": "scheduler"},
    }
}

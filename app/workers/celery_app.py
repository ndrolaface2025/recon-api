from celery import Celery
from app.config import settings
broker = settings.REDIS_URL or "redis://redis:6379/0"
cel = Celery("recon_workers", broker=broker, backend=broker)
cel.conf.task_routes = {"app.workers.tasks.*": {"queue": "recon"}}

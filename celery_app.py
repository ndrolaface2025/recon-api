# celery_app.py
from celery import Celery
import logging

# Configure logging BEFORE creating Celery app
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create Celery instance
celery_app = Celery(
    "recon_workers",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

# Configure routes
celery_app.conf.task_routes = {
    "app.workers.tasks.*": {"queue": "recon"}
}

# Important: Don't let Celery hijack the root logger
celery_app.conf.worker_hijack_root_logger = True
# celery_app.py
from celery import Celery, signals
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging BEFORE creating Celery app
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get Celery configuration from environment
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Create Celery instance
celery_app = Celery(
    "recon_workers",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

# Celery Configuration
celery_app.conf.update(
    task_track_started=True,
    task_time_limit=int(os.getenv("CELERY_TASK_TIME_LIMIT", 1800)),  # 30 minutes
    task_soft_time_limit=int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", 1500)),  # 25 minutes
    worker_prefetch_multiplier=1,  # Fetch one task at a time
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks (prevent memory leaks)
    result_expires=3600,  # Results expire after 1 hour
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

# Use default 'celery' queue (no custom routing needed)
# Tasks will be automatically registered when app.workers.tasks imports celery_app

# Important: Don't let Celery hijack the root logger
celery_app.conf.worker_hijack_root_logger = True

# Add signal handlers for proper async cleanup
@signals.worker_process_init.connect
def worker_process_init(**kwargs):
    """Called when a worker process initializes"""
    print("🔧 Worker process initialized")

@signals.worker_process_shutdown.connect
def worker_process_shutdown(**kwargs):
    """Called when a worker process shuts down - cleanup database connections"""
    print("🔧 Worker process shutting down, cleaning up connections")
    try:
        from app.db.session import engine
        import asyncio
        # Close all database connections in this worker process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(engine.dispose())
        loop.close()
    except Exception as e:
        print(f"⚠️  Error during cleanup: {e}")

# Import tasks to register them with Celery
# This must be done AFTER celery_app is created to avoid circular imports
try:
    from app.workers import tasks
    print("✅ Tasks imported and registered successfully")
except ImportError as e:
    print(f"⚠️  Warning: Could not import tasks: {e}")
    print("   Tasks will be registered when worker starts")

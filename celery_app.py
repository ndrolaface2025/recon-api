# celery_app.py
from celery import Celery, signals
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

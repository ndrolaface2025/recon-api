# Recon Backend - Enterprise Scaffold

Production-ready FastAPI boilerplate for channel/source-aware reconciliation.
---

## 🚀 Quick Start (Docker)

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Edit .env with your configuration
# 3. Start all services
docker compose up --build

# API available at: http://localhost:8000
# Redis available at: localhost:6379
```

---

## 💻 Local Development Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Redis 6+

### Installation

```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# 3. Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 4. Copy and configure environment file
cp .env.example .env
## 🗄️ Database Setup

### Run Migrations

```bash
# Activate virtual environment first
source venv/bin/activate  # macOS/Linux
# OR
.\.venv\Scripts\Activate.ps1  # Windows PowerShell

# Set DATABASE_URL (or use .env)
export DATABASE_URL="postgresql+asyncpg://user:password@localhost:5432/test_db"

# Generate migration (auto-detect schema changes)
alembic revision --autogenerate -m "describe your changes"

# Apply migrations
alembic upgrade head

# Rollback last migration (if needed)
alembic downgrade -1
```

### Windows PowerShell Migration

```powershell
.\.venv\Scripts\Activate.ps1
$env:DATABASE_URL="postgresql+asyncpg://user:password@localhost:5432/recon_backend_db"
alembic revision --autogenerate -m "describe-change"
alembic upgrade head
```

---

## 🔧 Running the Application

### 1. Start Redis Server

```bash
# macOS (using Homebrew)
brew services start redis

# Linux
sudo systemctl start redis

# Docker
docker run -d -p 6379:6379 redis:alpine

# Verify Redis is running
redis-cli ping
# Should return: PONG
```

### 2. Start FastAPI Application

```bash
# Activate virtual environment
source venv/bin/activate  # macOS/Linux

# Start development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# API will be available at:
# - Swagger UI: http://localhost:8000/docs
# - ReDoc: http://localhost:8000/redoc
```

### 3. Start Celery Worker

Open a **NEW terminal window** and run:

```bash
# Activate virtual environment
source venv/bin/activate  # macOS/Linux

# Start Celery worker
celery -A celery_app worker --loglevel=info --pool=solo

# Options:
# --loglevel=info    : Show INFO level logs
# --loglevel=debug   : Show DEBUG level logs (verbose)
# --pool=solo        : Single-threaded (good for development/debugging)
# --concurrency=4    : Number of concurrent workers (production)
```

**Windows Users:**

```powershell
# Windows requires 'solo' or 'gevent' pool
celery -A celery_app worker --loglevel=info --pool=solo
```

**Production (Linux/macOS):**

```bash
celery -A celery_app worker --loglevel=info --concurrency=4
```

### 4. Start Celery Flower (Optional Monitoring)

Open **another terminal** for monitoring:

```bash
# Install flower (if not already installed)
pip install flower

# Start Flower dashboard
celery -A celery_app flower --port=5555




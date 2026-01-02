# Recon Backend - Enterprise Scaffold
Production-ready FastAPI boilerplate for channel/source-aware reconciliation.
Includes:
- Multiple channels: ATM, POS, CARDS, WALLET, MOBILE_MONEY
- Source-aware parsers (SWITCH, CBS, EJ, NETWORK, SETTLEMENT)
- Channel-level normalizers and matchers
- Async SQLAlchemy, Celery worker, Alembic skeleton
- Docker + docker-compose, Kubernetes manifests (example)
- CI: GitHub Actions workflow skeleton

How to run (local dev)
1. Copy `.env.example` to `.env` and fill DB/Redis values.
2. `docker compose up --build`
3. Web API available at http://localhost:8000


Installtion Commands:

# create venv
python -m venv .venv

# activate (PowerShell)
.\.venv\Scripts\Activate.ps1

# upgrade pip and install deps
python -m pip install --upgrade pip
pip install -r requirements.txt

# copy example to a local .env you will edit
Copy-Item .env.example .env

# migration Command
- .\.venv\Scripts\Activate.ps1; 
- $env:DATABASE_URL="postgresql+asyncpg://postgres:1234@localhost:5432/recon_database";
- alembic revision --autogenerate -m "describe-change"; 
- alembic upgrade head




#!/usr/bin/env bash
# POSIX script to activate a Python venv and run Alembic autogenerate + upgrade
# Usage: ./scripts/run_migrations.sh ["message"]
MSG=${1:-auto-migration}
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# activate venv if it exists
if [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
  # shellcheck source=/dev/null
  . "$ROOT_DIR/.venv/bin/activate"
else
  echo "Warning: venv activate not found at $ROOT_DIR/.venv/bin/activate"
fi

# load .env
if [ -f "$ROOT_DIR/.env" ]; then
  echo "Loading .env"
  set -o allexport
  # shellcheck source=/dev/null
  . "$ROOT_DIR/.env"
  set +o allexport
else
  echo ".env not found — ensure DATABASE_URL is exported in your environment"
fi

TS=$(date +%Y%m%d%H%M%S)
REVMSG="$MSG-$TS"

echo "Running: alembic revision --autogenerate -m '$REVMSG'"
alembic revision --autogenerate -m "$REVMSG"

echo "Running: alembic upgrade head"
alembic upgrade head

echo "Migrations complete."

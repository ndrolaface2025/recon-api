# Run this script to auto-generate and apply Alembic migrations.
# It activates the repository venv, loads .env into env vars, runs
# `alembic revision --autogenerate` and `alembic upgrade head`.

param(
    [string]$Message = "auto-migration"
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
Push-Location $repoRoot

# Activate virtualenv if available
$activate = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $activate) {
    Write-Host "Activating venv: $activate"
    & $activate
} else {
    Write-Host "Warning: virtualenv activate script not found at $activate"
}

# Load .env file into environment variables (simple parser)
$envFile = Join-Path $repoRoot ".env"
if (Test-Path $envFile) {
    Write-Host "Loading environment variables from .env"
    Get-Content $envFile | ForEach-Object {
        if ($_ -and ($_ -notmatch '^[#;]') -and ($_ -match '^\s*([^=]+)=(.*)$')) {
            $k = $matches[1].Trim()
            $v = $matches[2].Trim().Trim('"')
            Write-Host "  $k = $v"
            $env:$k = $v
        }
    }
} else {
    Write-Host ".env file not found; ensure DATABASE_URL is set in the environment."
}

# Ensure alembic is available in the venv
$alembicExe = Join-Path $repoRoot ".venv\Scripts\alembic.exe"
if (-not (Test-Path $alembicExe)) {
    Write-Host "Using alembic from PATH (if available)."
}

# Timestamped message
$ts = (Get-Date).ToString('yyyyMMddHHmmss')
$revMsg = "$Message-$ts"

Write-Host "Running: alembic revision --autogenerate -m '$revMsg'"
# Run alembic revision --autogenerate
& alembic revision --autogenerate -m $revMsg

# Apply migrations
Write-Host "Running: alembic upgrade head"
& alembic upgrade head

Pop-Location
Write-Host "Migrations complete."

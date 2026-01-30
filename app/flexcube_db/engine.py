from sqlalchemy import create_engine
from urllib.parse import quote_plus
from app.config import settings

user = quote_plus(settings.FLEXCUBE_DB_USER)
password = quote_plus(settings.FLEXCUBE_DB_PASSWORD)
host = settings.FLEXCUBE_DB_HOST
port = settings.FLEXCUBE_DB_PORT
service = settings.FLEXCUBE_DB_SERVICE

FLEXCUBE_SQLALCHEMY_URL = (
    f"oracle+oracledb://{user}:{password}@" f"{host}:{port}/?service_name={service}"
)

flexcube_engine = create_engine(
    FLEXCUBE_SQLALCHEMY_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

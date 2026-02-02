from sqlalchemy import create_engine
from urllib.parse import quote_plus
from typing import Optional
from app.config import settings


def create_flexcube_engine():
    if not all(
        (
            settings.FLEXCUBE_DB_USER,
            settings.FLEXCUBE_DB_PASSWORD,
            settings.FLEXCUBE_DB_HOST,
            settings.FLEXCUBE_DB_PORT,
            settings.FLEXCUBE_DB_SERVICE,
        )
    ):
        return None

    user = quote_plus(settings.FLEXCUBE_DB_USER)
    password = quote_plus(settings.FLEXCUBE_DB_PASSWORD)
    host = settings.FLEXCUBE_DB_HOST
    port = settings.FLEXCUBE_DB_PORT
    service = settings.FLEXCUBE_DB_SERVICE

    url = (
        f"oracle+oracledb://{user}:{password}@" f"{host}:{port}/?service_name={service}"
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=False,
    )


flexcube_engine = create_flexcube_engine()

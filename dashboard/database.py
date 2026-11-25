
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def get_engine():
    """Create a SQL Server database engine."""

    server = os.getenv("SQL_SERVER")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")
    database = os.getenv("DW_DB", "HospitalDW")
    port = os.getenv("SQL_PORT")

    if not all([server, username, password]):
        raise ValueError(
            "Missing SQL_SERVER, SQL_USERNAME "
            "or SQL_PASSWORD in .env"
        )

    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=server,
        port=int(port) if port else None,
        database=database,
        query={
            "driver": os.getenv(
                "SQL_ODBC_DRIVER",
                "ODBC Driver 18 for SQL Server"
            ),
            "Encrypt": "yes",
            "TrustServerCertificate": "yes"
        }
    )

    return create_engine(
        connection_url,
        pool_pre_ping=True
    )


def run_query(engine, query, params=None):
    """Execute a read-only SELECT query into a DataFrame."""

    with engine.connect() as connection:
        return pd.read_sql_query(
            text(query),
            connection,
            params=params or {}
        )

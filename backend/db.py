"""
Database connection helpers for Azure PostgreSQL.

Expected environment variable:
    DATABASE_URL

Example:
    postgresql://pgadmin:<password>@pg-specdiff-prod.postgres.database.azure.com:5432/postgres?sslmode=require

Alternative individual variables:
    PGHOST
    PGPORT
    PGDATABASE
    PGUSER
    PGPASSWORD
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg
from psycopg.rows import dict_row


def database_url() -> Optional[str]:
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    host = os.getenv("PGHOST")
    database = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not (host and database and user and password):
        return None

    return (
        f"postgresql://{user}:{password}@{host}:{port}/{database}"
        "?sslmode=require"
    )


def db_enabled() -> bool:
    return bool(database_url())


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    url = database_url()

    if not url:
        raise RuntimeError(
            "Database is not configured. Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

    conn = psycopg.connect(url, row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ping_db() -> dict:
    if not db_enabled():
        return {
            "enabled": False,
            "status": "not_configured",
        }

    try:
        with get_conn() as conn:
            row = conn.execute("SELECT version() AS version").fetchone()
            return {
                "enabled": True,
                "status": "ok",
                "version": row["version"] if row else None,
            }
    except Exception as exc:
        return {
            "enabled": True,
            "status": "error",
            "error": str(exc),
        }

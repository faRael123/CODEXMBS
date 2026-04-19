import os
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
from psycopg import errors
from psycopg.rows import dict_row


BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"


def _env_first(*names, default=""):
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def _normalize_database_url(url):
    if not url:
        return ""
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


DATABASE_URL = _normalize_database_url(_env_first("DATABASE_URL", "POSTGRES_URL", default=""))
DB_CONFIG = {
    "host": _env_first("DB_HOST", "PGHOST", default="localhost"),
    "user": _env_first("DB_USER", "PGUSER", default="postgres"),
    "password": _env_first("DB_PASSWORD", "PGPASSWORD", default=""),
    "port": int(_env_first("DB_PORT", "PGPORT", default="5432")),
    "dbname": _env_first("DB_NAME", "PGDATABASE", default="codexmbs_db"),
}


def _safe_database_name(name):
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError("DB_NAME may only contain letters, numbers, and underscores.")
    return name


def _connection_kwargs():
    if DATABASE_URL:
        return {"conninfo": DATABASE_URL}
    return dict(DB_CONFIG)


def _server_connection_kwargs():
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        server_path = "/postgres"
        return {"conninfo": urlunparse(parsed._replace(path=server_path, query="", fragment=""))}
    config = dict(DB_CONFIG)
    config["dbname"] = "postgres"
    return config


class CursorResult:
    def __init__(self, cursor, lastrowid=None):
        self.cursor = cursor
        self._lastrowid = lastrowid

    @property
    def lastrowid(self):
        return self._lastrowid

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class DBConnection:
    def __init__(self, conn):
        self.conn = conn

    def _normalize_query(self, query):
        return query.replace("?", "%s")

    def execute(self, query, params=()):
        normalized = self._normalize_query(query)
        cursor = self.conn.cursor(row_factory=dict_row)
        cursor.execute(normalized, tuple(params))
        lastrowid = None
        if cursor.description and normalized.lstrip().upper().startswith("INSERT") and "RETURNING" in normalized.upper():
            row = cursor.fetchone()
            if row and "id" in row:
                lastrowid = row["id"]
        return CursorResult(cursor, lastrowid)

    def executemany(self, query, seq_of_params):
        cursor = self.conn.cursor()
        cursor.executemany(self._normalize_query(query), seq_of_params)
        return cursor

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def _split_sql_statements(sql_text):
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def bootstrap_db():
    if not DATABASE_URL:
        database_name = _safe_database_name(DB_CONFIG["dbname"])
        try:
            with psycopg.connect(**_server_connection_kwargs(), autocommit=True) as server_conn:
                exists = server_conn.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (database_name,),
                ).fetchone()
                if not exists:
                    server_conn.execute(f'CREATE DATABASE "{database_name}"')
        except errors.DuplicateDatabase:
            pass

    with psycopg.connect(**_connection_kwargs()) as db_conn:
        with db_conn.cursor() as db_cursor:
            sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
            for statement in _split_sql_statements(sql_text):
                try:
                    db_cursor.execute(statement)
                except (
                    errors.DuplicateColumn,
                    errors.DuplicateObject,
                    errors.DuplicateTable,
                    errors.DuplicateAlias,
                ):
                    db_conn.rollback()
                else:
                    db_conn.commit()


def get_db():
    return DBConnection(psycopg.connect(**_connection_kwargs()))

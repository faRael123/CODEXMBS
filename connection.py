import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"


def _env_first(*names, default=""):
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def _database_url():
    url = _env_first("DATABASE_URL", "POSTGRES_URL", "POSTGRESQL_URL")
    if url:
        return url
    host = _env_first("DB_HOST", "PGHOST", default="localhost")
    port = _env_first("DB_PORT", "PGPORT", default="5432")
    user = _env_first("DB_USER", "PGUSER", default="postgres")
    password = _env_first("DB_PASSWORD", "PGPASSWORD", default="")
    database = _env_first("DB_NAME", "PGDATABASE", default="codexmbs_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _postgres_modules():
    try:
        import psycopg2
        from psycopg2 import errors
        from psycopg2.extras import RealDictCursor
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2-binary is required for PostgreSQL. Run `pip install -r requirements.txt` before starting the app."
        ) from exc
    return psycopg2, errors, RealDictCursor


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

    def _with_returning_id(self, query):
        stripped = query.strip()
        lowered = stripped.lower()
        if not lowered.startswith("insert into") or " returning " in lowered:
            return query
        return f"{query.rstrip()} RETURNING id"

    def execute(self, query, params=()):
        _, _, RealDictCursor = _postgres_modules()
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        normalized_query = self._normalize_query(query)
        statement = self._with_returning_id(normalized_query)
        cursor.execute(statement, params)
        lastrowid = None
        if statement is not normalized_query and cursor.description:
            returned = cursor.fetchone()
            if returned:
                lastrowid = returned.get("id")
        return CursorResult(cursor, lastrowid)

    def executemany(self, query, seq_of_params):
        cursor = self.conn.cursor()
        cursor.executemany(self._normalize_query(query), seq_of_params)
        return cursor

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def bootstrap_db():
    psycopg2, errors, _ = _postgres_modules()
    db_conn = psycopg2.connect(_database_url())
    db_cursor = db_conn.cursor()

    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    for statement in statements:
        try:
            db_cursor.execute(statement)
            db_conn.commit()
        except (errors.DuplicateColumn, errors.DuplicateObject, errors.DuplicateTable):
            db_conn.rollback()
            continue

    db_cursor.close()
    db_conn.close()


def get_db():
    psycopg2, _, _ = _postgres_modules()
    return DBConnection(psycopg2.connect(_database_url()))

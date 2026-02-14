"""
Data Access Layer - работа с PostgreSQL
"""
from typing import List, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values

from backend.layers.data.type_mapping import to_postgres_type, from_postgres_type


class PostgresRepository:
    """Репозиторий для работы с PostgreSQL"""

    def __init__(
        self,
        connection: dict,
        database: str,
        table_name: str,
        field_name: str | None = None,
        field_type: str | None = None,
        columns: List[Tuple[str, str]] | None = None,
    ):
        self._conn_params = {
            "host": connection.get("host", "localhost"),
            "port": int(connection.get("port", 5433)),
            "user": connection.get("username", "postgres"),
            "password": connection.get("password", ""),
            "dbname": connection.get("database", database),
        }
        self._database = database
        self._table_name = table_name
        self._field_name = field_name
        self._field_type = field_type
        self._columns = columns  # [(name, type), ...] для многополевого режима
        self._conn = None

    def _connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self._conn_params)
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def ensure_database(self) -> None:
        base_params = {k: v for k, v in self._conn_params.items() if k != "dbname"}
        base_params["dbname"] = "postgres"
        conn = psycopg2.connect(**base_params)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT 1 FROM pg_database WHERE datname = %s', (self._database,))
                if not cur.fetchone():
                    cur.execute(f'CREATE DATABASE "{self._database}"')
        finally:
            conn.close()

    def ensure_table(self) -> None:
        if not self._field_name or not self._field_type:
            raise ValueError("field_name и field_type требуются для ensure_table")
        conn = self._connect()
        pg_type = to_postgres_type(self._field_type)
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{self._table_name}" ("{self._field_name}" {pg_type})'
            )
        conn.commit()

    def ensure_table_with_columns(self, columns: List[Tuple[str, str]]) -> None:
        """Создать таблицу с несколькими колонками. columns: [(name, ch_type), ...]"""
        if not columns:
            raise ValueError("Укажите хотя бы одну колонку")
        conn = self._connect()
        col_defs = ", ".join(
            f'"{name}" {to_postgres_type(ch_type)}' for name, ch_type in columns
        )
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{self._table_name}" ({col_defs})')
        conn.commit()

    def insert_rows(self, rows: Sequence[Sequence], column_names: List[str]) -> None:
        conn = self._connect()
        cols = ", ".join(f'"{c}"' for c in column_names)
        with conn.cursor() as cur:
            execute_values(cur, f'INSERT INTO "{self._table_name}" ({cols}) VALUES %s', rows)
        conn.commit()

    @property
    def table_name(self) -> str:
        return f"{self._database}.{self._table_name}"

    @property
    def column_names(self) -> List[str]:
        if self._columns:
            return [c[0] for c in self._columns]
        if self._field_name:
            return [self._field_name]
        return self.get_table_columns(self._table_name)

    def get_table_columns(self, table_name: str) -> List[str]:
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (tbl,))
            return [r[0] for r in cur.fetchall()]

    def describe_table(self, table_name: str) -> List[Tuple[str, str]]:
        """Схема таблицы: список (имя_колонки, тип_clickhouse)"""
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (tbl,))
            return [(r[0], from_postgres_type(r[1])) for r in cur.fetchall()]

    def get_table_count(self, table_name: str) -> int:
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{tbl}"')
            return cur.fetchone()[0]

    def truncate_table(self, table_name: str) -> None:
        """Очистить таблицу (удалить все строки)"""
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "{tbl}"')
        conn.commit()

    def drop_table(self, table_name: str) -> None:
        """Удалить таблицу (DROP TABLE IF EXISTS)"""
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{tbl}"')
        conn.commit()

    def list_tables(self) -> List[str]:
        """Список таблиц в текущей базе (schema public)"""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
            return [r[0] for r in cur.fetchall()]

    def fetch_table_data(
        self, table_name: str, limit: int, shuffle: bool = False,
        float_precision: int = 2
    ) -> List[List]:
        tbl = table_name.split(".")[-1] if "." in table_name else table_name
        conn = self._connect()
        order = "ORDER BY random()" if shuffle else ""
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{tbl}" {order} LIMIT %s', (limit,))
            rows = []
            for row in cur.fetchall():
                cells = []
                for val in row:
                    if isinstance(val, float):
                        cells.append(f"{val:.{float_precision}f}")
                    else:
                        cells.append(str(val) if val is not None else "")
                rows.append(cells)
        return rows

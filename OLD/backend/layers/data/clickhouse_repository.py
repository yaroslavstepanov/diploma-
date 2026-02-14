from typing import List, Sequence
from ch_synth.profile import Profile
from ch_synth.client import ClickHouseService


class ClickHouseRepository:
    def __init__(self, profile: Profile):
        self._profile = profile
        self._service = ClickHouseService(profile)

    def ensure_database(self) -> None:
        self._service.ensure_database()

    def ensure_table(self) -> None:
        self._service.ensure_table()

    def insert_rows(self, rows: Sequence[Sequence], column_names: List[str]) -> None:
        self._service.insert_rows(rows, column_names)

    def close(self) -> None:
        self._service.close()

    @property
    def table_name(self) -> str:
        return self._profile.fq_table()

    @property
    def column_names(self) -> List[str]:
        return self._profile.column_names()

    def get_table_columns(self, table_name: str) -> List[str]:
        result = self._service._client.query(f"DESCRIBE TABLE {table_name}")
        return [row[0] for row in result.result_rows]

    def get_table_count(self, table_name: str) -> int:
        result = self._service._client.query(f"SELECT count() FROM {table_name}")
        return result.result_rows[0][0] if result.result_rows else 0

    def truncate_table(self, table_name: str) -> None:
        self._service._client.command(f"TRUNCATE TABLE {table_name}")

    def fetch_table_data(self, table_name: str, limit: int, shuffle: bool = False, float_precision: int = 2) -> List[List]:
        q = f"SELECT * FROM {table_name} ORDER BY rand() LIMIT {limit}" if shuffle else f"SELECT * FROM {table_name} LIMIT {limit}"
        result = self._service._client.query(q)
        rows = []
        for row in result.result_rows:
            cells = []
            for val in row:
                cells.append(f"{val:.{float_precision}f}" if isinstance(val, float) else str(val))
            rows.append(cells)
        return rows

"""
Data Access Layer - работа с ClickHouse
"""
from typing import List, Sequence, Tuple
from ch_synth.profile import Profile
from ch_synth.client import ClickHouseService


class ClickHouseRepository:
    """Репозиторий для работы с ClickHouse"""
    
    def __init__(self, profile: Profile):
        self._profile = profile
        self._service = ClickHouseService(profile)
    
    def ensure_database(self) -> None:
        """Создать базу данных если не существует"""
        self._service.ensure_database()
    
    def ensure_table(self) -> None:
        """Создать таблицу если не существует"""
        self._service.ensure_table()
    
    def insert_rows(self, rows: Sequence[Sequence], column_names: List[str]) -> None:
        """Вставить строки в таблицу"""
        self._service.insert_rows(rows, column_names)
    
    def close(self) -> None:
        """Закрыть подключение"""
        self._service.close()
    
    @property
    def table_name(self) -> str:
        """Полное название таблицы"""
        return self._profile.fq_table()
    
    @property
    def column_names(self) -> List[str]:
        """Список колонок"""
        return self._profile.column_names()
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Получить список колонок таблицы"""
        result = self._service._client.query(f"DESCRIBE TABLE {table_name}")
        return [row[0] for row in result.result_rows]

    def describe_table(self, table_name: str) -> List[Tuple[str, str]]:
        """Схема таблицы: список (имя_колонки, тип)"""
        result = self._service._client.query(f"DESCRIBE TABLE {table_name}")
        return [(row[0], row[1]) for row in result.result_rows] if result.result_rows else []
    
    def get_table_count(self, table_name: str) -> int:
        """Получить количество строк в таблице"""
        result = self._service._client.query(f"SELECT count() FROM {table_name}")
        return result.result_rows[0][0] if result.result_rows else 0
    
    def truncate_table(self, table_name: str) -> None:
        """Очистить таблицу (удалить все строки)"""
        self._service._client.command(f"TRUNCATE TABLE {table_name}")

    def drop_table(self, table_name: str) -> None:
        """Удалить таблицу (DROP TABLE IF EXISTS)"""
        self._service._client.command(f"DROP TABLE IF EXISTS {table_name}")

    def list_tables(self, database: str) -> List[str]:
        """Список таблиц в базе"""
        result = self._service._client.query(f"SHOW TABLES FROM {database}")
        return [row[0] for row in result.result_rows] if result.result_rows else []

    def fetch_table_data(
        self, table_name: str, limit: int, shuffle: bool = False,
        float_precision: int = 2
    ) -> List[List]:
        """Получить данные из таблицы.
        
        При shuffle=True использует ORDER BY rand() чтобы показать перемешанный порядок.
        float_precision — знаков после запятой (0–10) для отображения float.
        """
        if shuffle:
            query = f"SELECT * FROM {table_name} ORDER BY rand() LIMIT {limit}"
        else:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result = self._service._client.query(query)
        rows = []
        for row in result.result_rows:
            cells = []
            for val in row:
                if isinstance(val, float):
                    cells.append(f"{val:.{float_precision}f}")
                else:
                    cells.append(str(val))
            rows.append(cells)
        return rows

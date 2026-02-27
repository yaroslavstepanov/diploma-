"""
Presentation Layer - модели запросов и ответов
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, model_validator


class FieldSpec(BaseModel):
    """Описание поля для генерации (много-полевой режим)"""
    name: str = Field(..., description="Имя поля", example="id")
    type: str = Field(
        ...,
        description="Тип: String, Int32, DateTime, UUID",
        example="UUID"
    )
    generator_kind: str = Field(..., description="Тип генератора", example="uuid4")
    generator_params: Dict[str, Any] = Field(default_factory=dict, description="Параметры генератора")


class GenerateRequest(BaseModel):
    """
    Универсальный запрос на генерацию данных.
    
    Режимы:
    - Один столбец: generator_kind + generator_params (field_name, field_type внутри params)
    - Несколько столбцов: fields[] — массив полей (name, type, generator_kind, generator_params)
    """
    generator_kind: Optional[str] = Field(
        None,
        description="Тип генератора (однополевой режим)",
        example="random_int"
    )
    generator_params: Optional[Dict[str, Any]] = Field(
        None,
        description="Параметры генератора (однополевой режим)",
        example={"min": 0, "max": 100, "field_name": "value", "field_type": "Int32"}
    )
    fields: Optional[List[FieldSpec]] = Field(
        None,
        description="Список полей (многополевой режим). Если указан — игнорируются generator_kind и generator_params.",
        example=[{"name": "id", "type": "UUID", "generator_kind": "uuid4", "generator_params": {}}]
    )
    connection: Optional[Dict[str, Any]] = Field(
        None,
        description="Параметры подключения. engine: clickhouse|postgres. ClickHouse: port 18123. PostgreSQL: port 5433 (5432 часто занят)",
        example={
            "engine": "clickhouse",
            "host": "localhost",
            "port": 18123,
            "username": "default",
            "password": "",
            "database": "default",
            "secure": False
        }
    )
    target_table: Optional[str] = Field(
        None,
        description="Название целевой таблицы (требуется для генерации в БД)",
        example="my_table"
    )
    rows: int = Field(
        10,
        description="Количество строк для генерации",
        ge=1,
        example=1000
    )
    batch_size: int = Field(
        1000,
        description="Размер батча для вставки в БД",
        ge=1,
        example=1000
    )
    create_table: bool = Field(
        False,
        description="Создать таблицу автоматически, если её нет",
        example=True
    )
    preview_only: bool = Field(
        False,
        description="Если True - только предпросмотр (не писать в БД). Если False и указаны connection/target_table - генерация в БД",
        example=False
    )

    @model_validator(mode='after')
    def check_fields_or_single(self):
        has_fields = self.fields and len(self.fields) > 0
        has_single = self.generator_kind is not None
        if not has_fields and not has_single:
            raise ValueError("Укажите fields[] или generator_kind + generator_params")
        if has_fields and has_single:
            # fields имеет приоритет
            pass
        return self

    model_config = {
        "json_schema_extra": {
            "examples": [{
                "generator_kind": "random_int",
                "generator_params": {"min": 0, "max": 100, "use_float": True, "precision": 2, "field_name": "value", "field_type": "Int32"},
                "connection": {"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default", "secure": False},
                "target_table": "preview_table",
                "rows": 10,
                "batch_size": 1000,
                "create_table": True,
                "preview_only": False
            }]
        }
    }


class GenerateResponse(BaseModel):
    """Ответ с результатами генерации"""
    success: bool = Field(..., description="Успешность операции", example=True)
    data: Optional[List[str]] = None  # Для предпросмотра
    rows_inserted: Optional[int] = None  # Для генерации в БД
    table: Optional[str] = None  # Название таблицы
    message: Optional[str] = None


class FetchDataRequest(BaseModel):
    """Запрос на получение данных из БД"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: 'clickhouse' (port 18123) или 'postgres' (port 5433). Таблица должна существовать — сначала вызовите POST /api/generate.",
        example={
            "engine": "clickhouse",
            "host": "localhost",
            "port": 18123,
            "username": "default",
            "password": "ch_pass",
            "database": "default",
            "secure": False
        }
    )
    table: str = Field(
        ...,
        description="Имя таблицы. Таблица создаётся через /api/generate с create_table: true.",
        example="preview_table"
    )
    limit: int = Field(
        10,
        description="Макс. строк в ответе (1–100)",
        ge=1,
        le=100,
        example=10
    )
    shuffle: bool = Field(
        False,
        description="true — случайный порядок (ORDER BY random/rand()), false — порядок таблицы",
        example=False
    )
    float_precision: int = Field(
        2,
        description="Знаков после запятой для float в ячейках (0–10)",
        ge=0,
        le=10,
        example=2
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "connection": {"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default", "secure": False},
                    "table": "preview_table",
                    "limit": 10,
                    "shuffle": False,
                    "float_precision": 2
                },
                {
                    "connection": {"engine": "postgres", "host": "localhost", "port": 5433, "username": "postgres", "password": "postgres", "database": "postgres"},
                    "table": "preview_table",
                    "limit": 10,
                    "shuffle": True,
                    "float_precision": 2
                }
            ]
        }
    }


class TestConnectionRequest(BaseModel):
    """Запрос на проверку подключения к БД"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: clickhouse (18123) | postgres (5433)",
        example={"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"connection": {"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}},
                {"connection": {"engine": "postgres", "host": "localhost", "port": 5433, "username": "postgres", "password": "postgres", "database": "postgres"}}
            ]
        }
    }


class TestConnectionResponse(BaseModel):
    """Ответ на проверку подключения"""
    success: bool = Field(..., description="Подключение успешно", example=True)
    message: str = Field(..., description="Сообщение", example="Подключение к ClickHouse успешно")
    engine: str = Field(..., description="Проверенный движок", example="clickhouse")


class ClearTableRequest(BaseModel):
    """Запрос на очистку таблицы (TRUNCATE)"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: clickhouse (18123) | postgres (5433)",
        example={"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}
    )
    table: str = Field(..., description="Имя таблицы для очистки (TRUNCATE)", example="preview_table")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"connection": {"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}, "table": "preview_table"},
                {"connection": {"engine": "postgres", "host": "localhost", "port": 5433, "username": "postgres", "password": "postgres", "database": "postgres"}, "table": "preview_table"}
            ]
        }
    }


class ClearTableResponse(BaseModel):
    """Ответ на очистку таблицы"""
    success: bool = Field(..., description="Успешность операции", example=True)
    message: str = Field(..., description="Сообщение", example="Таблица preview_table очищена")


class DropTableRequest(BaseModel):
    """Запрос на удаление таблицы (DROP TABLE)"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: clickhouse | postgres",
        example={"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}
    )
    table: str = Field(..., description="Имя таблицы для удаления", example="preview_table")


class DropTableResponse(BaseModel):
    """Ответ на удаление таблицы"""
    success: bool = Field(..., description="Успешность операции", example=True)
    message: str = Field(..., description="Сообщение", example="Таблица preview_table удалена")


class ListTablesRequest(BaseModel):
    """Запрос списка таблиц в базе"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: clickhouse | postgres",
        example={"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}
    )


class ListTablesResponse(BaseModel):
    """Ответ со списком таблиц"""
    success: bool = Field(..., description="Успешность операции", example=True)
    engine: str = Field(..., description="СУБД: clickhouse | postgres", example="clickhouse")
    database: str = Field(..., description="Имя базы данных", example="default")
    tables: List[str] = Field(..., description="Список имён таблиц", example=["preview_table", "my_table"])


class FetchDataResponse(BaseModel):
    """Ответ с данными из БД"""
    success: bool = Field(..., description="Успешность операции", example=True)
    data: List[List[str]] = Field(..., description="Данные из таблицы (строки)", example=[["1", "value1"], ["2", "value2"]])
    columns: List[str] = Field(..., description="Названия колонок", example=["#", "field_name"])
    total_rows: Optional[int] = Field(None, description="Общее количество строк в таблице", example=1000)


# --- GET /api/generators, GET /api/supported-types, POST /api/describe-table, GET /api/health ---

class GeneratorParamSchema(BaseModel):
    """Параметр генератора"""
    name: str = Field(..., description="Имя параметра", example="min")
    type: str = Field(..., description="Тип: number, string, boolean, array", example="number")
    required: bool = Field(False, description="Обязателен ли параметр", example=True)
    default: Optional[Any] = Field(None, description="Значение по умолчанию")
    description: Optional[str] = Field(None, description="Описание")
    placeholder: Optional[str] = Field(None, description="Подсказка в поле ввода")
    min: Optional[float] = Field(None, description="Минимальное значение (для number)")
    max: Optional[float] = Field(None, description="Максимальное значение (для number)")
    options: Optional[List[str]] = Field(None, description="Варианты для select")
    option_labels: Optional[Dict[str, str]] = Field(None, description="Подписи для options (value -> label)")


class GeneratorSchema(BaseModel):
    """Описание генератора"""
    kind: str = Field(..., description="Идентификатор генератора", example="random_int")
    description: str = Field(..., description="Краткое описание", example="Случайные целые числа в диапазоне [min, max]")
    compatible_types: List[str] = Field(
        ...,
        description="Поддерживаемые типы полей: String, Int32, DateTime, UUID",
        example=["Int32"]
    )
    params: List[GeneratorParamSchema] = Field(default_factory=list, description="Параметры генератора")


class GeneratorsResponse(BaseModel):
    """Список доступных генераторов и их параметров"""
    generators: List[GeneratorSchema] = Field(
        ...,
        description="Список генераторов",
        example=[]
    )


class SupportedTypeSchema(BaseModel):
    """Поддерживаемый тип данных"""
    id: str = Field(..., description="Идентификатор типа (для API)", example="String")
    label: str = Field(..., description="Отображаемое название", example="Строка")


class SupportedTypesResponse(BaseModel):
    """Поддерживаемые типы данных"""
    types: List[SupportedTypeSchema] = Field(
        ...,
        description="Список типов: Строка (String), Число (Int32), Дата (DateTime), UUID",
        example=[{"id": "String", "label": "Строка"}, {"id": "Int32", "label": "Число"}]
    )


class DescribeTableRequest(BaseModel):
    """Запрос схемы таблицы"""
    connection: Dict[str, Any] = Field(
        ...,
        description="Подключение. engine: clickhouse | postgres",
        example={"engine": "clickhouse", "host": "localhost", "port": 18123, "username": "default", "password": "ch_pass", "database": "default"}
    )
    table: str = Field(..., description="Имя таблицы", example="preview_table")


class TableColumnSchema(BaseModel):
    """Колонка таблицы"""
    name: str = Field(..., description="Имя колонки", example="value")
    type: str = Field(..., description="Тип данных (ClickHouse-style)", example="Int32")


class DescribeTableResponse(BaseModel):
    """Схема таблицы: колонки и типы"""
    success: bool = Field(..., description="Успешность операции", example=True)
    columns: List[TableColumnSchema] = Field(
        ...,
        description="Колонки таблицы (для добавления полей в существующую таблицу)",
        example=[{"name": "id", "type": "Int32"}, {"name": "value", "type": "String"}]
    )
    table: str = Field(..., description="Имя таблицы", example="preview_table")
    database: str = Field(..., description="Имя базы", example="default")


class HealthResponse(BaseModel):
    """Проверка работоспособности сервиса"""
    status: str = Field(..., description="Статус", example="ok")

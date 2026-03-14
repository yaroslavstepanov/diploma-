"""
Presentation Layer - обработчики HTTP-запросов (routes)
"""
import json
import os
import tempfile
from typing import List, Tuple

from fastapi import HTTPException

from backend.layers.presentation.models import (
    GenerateRequest, GenerateResponse,
    FetchDataRequest, FetchDataResponse,
    TestConnectionRequest, TestConnectionResponse,
    ClearTableRequest, ClearTableResponse,
    DropTableRequest, DropTableResponse,
    ListTablesRequest, ListTablesResponse,
    GeneratorsResponse, GeneratorSchema, GeneratorParamSchema,
    SupportedTypesResponse, SupportedTypeSchema,
    DescribeTableRequest, DescribeTableResponse,
    TableColumnSchema,
)
from backend.layers.business.generator_service import GeneratorService
from backend.layers.data.clickhouse_repository import ClickHouseRepository
from backend.layers.data.postgres_repository import PostgresRepository
from ch_synth.profile import Profile


def _make_ch_repo(connection: dict, database: str, table: str) -> ClickHouseRepository:
    """Создать ClickHouse репозиторий для операций fetch/clear/drop/list/describe."""
    pd = {
        "connection": connection,
        "target": {"database": database, "table": table, "order_by": "tuple()", "partition_by": None},
        "fields": []
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(pd, f, indent=2)
        pf = f.name
    try:
        profile = Profile.load(pf)
        return ClickHouseRepository(profile)
    finally:
        if os.path.exists(pf):
            os.unlink(pf)


def _resolve_fields_and_generators(request: GenerateRequest):
    """Определить fields_spec и single-field параметры."""
    if request.fields and len(request.fields) > 0:
        fields_spec = [
            {"name": f.name, "type": f.type, "generator_kind": f.generator_kind, "generator_params": f.generator_params or {}}
            for f in request.fields
        ]
        return fields_spec, None
    # Single-field
    params = request.generator_params or {}
    field_name = params.get("field_name", "value")
    field_type = params.get("field_type", "String")
    fields_spec = [{"name": field_name, "type": field_type, "generator_kind": request.generator_kind, "generator_params": params}]
    return fields_spec, (request.generator_kind, params)


class GenerateHandler:
    @staticmethod
    async def handle_generate(request: GenerateRequest) -> GenerateResponse:
        try:
            fields_spec, single = _resolve_fields_and_generators(request)
            generators = [
                GeneratorService.create_generator(f["generator_kind"], {**f["generator_params"], "field_name": f["name"], "field_type": f["type"]})
                for f in fields_spec
            ]

            if request.preview_only or not request.connection or not request.target_table:
                preview_count = min(request.rows, 10)
                data = []
                for i in range(preview_count):
                    row = []
                    for gen in generators:
                        v = gen.next(i)
                        row.append(str(v))
                    data.append(row[0] if len(row) == 1 else row)
                if data and isinstance(data[0], list) and len(data[0]) > 1:
                    data = [", ".join(str(x) for x in r) for r in data]
                elif data and isinstance(data[0], list):
                    data = [str(r[0]) for r in data]
                return GenerateResponse(success=True, data=data, message=f"Сгенерировано {len(data)} значений")

            conn = request.connection
            engine = conn.get("engine", "clickhouse")
            database = conn.get("database", "default")
            target_table = request.target_table

            if engine == "postgres":
                columns = [(f["name"], f["type"]) for f in fields_spec]
                repository = PostgresRepository(
                    connection=conn, database=database, table_name=target_table,
                    columns=columns
                )
                if request.create_table:
                    repository.ensure_database()
                    repository.ensure_table_with_columns(columns)
            else:
                profile = GeneratorService.create_profile_from_fields(
                    fields_spec, conn, target_table
                )
                repository = ClickHouseRepository(profile)
                if request.create_table:
                    repository.ensure_database()
                    repository.ensure_table()

            try:
                cols = repository.column_names
                total = 0
                batch = []
                for i in range(request.rows):
                    row = []
                    for gen, f in zip(generators, fields_spec):
                        v = gen.next(i)
                        ft = f.get("type", "String")
                        if ft in ("Int32", "Int64", "Int8", "Int16") and isinstance(v, float):
                            v = int(v)
                        row.append(v)
                    batch.append(row)
                    if len(batch) >= request.batch_size:
                        repository.insert_rows(batch, cols)
                        total += len(batch)
                        batch = []
                if batch:
                    repository.insert_rows(batch, cols)
                    total += len(batch)
                return GenerateResponse(
                    success=True, rows_inserted=total, table=repository.table_name,
                    message=f"Сгенерировано {total} строк"
                )
            finally:
                repository.close()
        except HTTPException:
            raise
        except Exception as e:
            import traceback
            raise HTTPException(status_code=500, detail={"error": str(e), "traceback": traceback.format_exc()})


class FetchDataHandler:
    @staticmethod
    async def handle_fetch(request: FetchDataRequest) -> FetchDataResponse:
        try:
            conn = request.connection
            engine = conn.get("engine", "clickhouse")
            database = conn.get("database", "default")
            tbl = request.table

            if engine == "postgres":
                repo = PostgresRepository(
                    connection=conn, database=database, table_name=tbl,
                    field_name="_dummy", field_type="TEXT"
                )
            else:
                tbl = f"{database}.{request.table}"
                repo = _make_ch_repo(conn, database, request.table)

            try:
                cols = repo.get_table_columns(tbl)
                total = repo.get_table_count(tbl)
                data = repo.fetch_table_data(tbl, request.limit, request.shuffle, request.float_precision)
                data_with_index = [[str(i + 1)] + row for i, row in enumerate(data)]
                return FetchDataResponse(
                    success=True, data=data_with_index, columns=["#"] + cols, total_rows=total
                )
            finally:
                repo.close()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})


class TestConnectionHandler:
    @staticmethod
    async def handle_test(request: TestConnectionRequest) -> TestConnectionResponse:
        conn = request.connection
        engine = conn.get("engine", "clickhouse")
        if engine == "postgres":
            import psycopg2
            try:
                psycopg2.connect(
                    host=conn.get("host", "localhost"),
                    port=int(conn.get("port", 5433)),
                    user=conn.get("username", "postgres"),
                    password=conn.get("password", ""),
                    dbname=conn.get("database", "postgres")
                )
                return TestConnectionResponse(success=True, message="Подключение к PostgreSQL успешно", engine="postgres")
            except Exception as e:
                raise HTTPException(status_code=502, detail={"error": str(e)})
        else:
            import clickhouse_connect
            try:
                clickhouse_connect.get_client(
                    host=conn.get("host", "localhost"),
                    port=int(conn.get("port", 18123)),
                    username=conn.get("username", "default"),
                    password=conn.get("password", ""),
                    database=conn.get("database", "default"),
                    secure=bool(conn.get("secure", False))
                ).query("SELECT 1")
                return TestConnectionResponse(success=True, message="Подключение к ClickHouse успешно", engine="clickhouse")
            except Exception as e:
                raise HTTPException(status_code=502, detail={"error": str(e)})


class ClearTableHandler:
    @staticmethod
    async def handle_clear(request: ClearTableRequest) -> ClearTableResponse:
        conn = request.connection
        engine = conn.get("engine", "clickhouse")
        database = conn.get("database", "default")
        tbl = request.table

        if engine == "postgres":
            repo = PostgresRepository(conn, database, request.table, "_dummy", "TEXT")
        else:
            tbl = f"{database}.{request.table}"
            repo = _make_ch_repo(conn, database, request.table)

        try:
            repo.truncate_table(tbl)
            return ClearTableResponse(success=True, message=f"Таблица {request.table} очищена")
        finally:
            repo.close()


class DropTableHandler:
    @staticmethod
    async def handle_drop(request: DropTableRequest) -> DropTableResponse:
        conn = request.connection
        engine = conn.get("engine", "clickhouse")
        database = conn.get("database", "default")
        tbl = request.table

        if engine == "postgres":
            repo = PostgresRepository(conn, database, request.table, "_dummy", "TEXT")
        else:
            tbl = f"{database}.{request.table}"
            repo = _make_ch_repo(conn, database, request.table)

        try:
            repo.drop_table(tbl)
            return DropTableResponse(success=True, message=f"Таблица {request.table} удалена")
        finally:
            repo.close()


class ListTablesHandler:
    @staticmethod
    async def handle_list(request: ListTablesRequest) -> ListTablesResponse:
        conn = request.connection
        engine = conn.get("engine", "clickhouse")
        database = conn.get("database", "default")

        if engine == "postgres":
            repo = PostgresRepository(conn, database, "_dummy", "_dummy", "TEXT")
            try:
                tables = repo.list_tables()
                return ListTablesResponse(success=True, engine="postgres", database=database, tables=tables)
            finally:
                repo.close()
        else:
            repo = _make_ch_repo(conn, database, "_dummy")
            try:
                tables = repo.list_tables(database)
                return ListTablesResponse(success=True, engine="clickhouse", database=database, tables=tables)
            finally:
                repo.close()


class GeneratorsHandler:
    GENERATORS = [
        {"kind": "random_int", "description": "Случайные целые числа в диапазоне [min, max]", "compatible_types": ["Int32"], "params": [
            {"name": "min", "type": "number", "default": 0, "description": "Минимум"},
            {"name": "max", "type": "number", "default": 100, "description": "Максимум"},
            {"name": "use_float", "type": "boolean", "default": False, "description": "Генерировать вещественные числа"},
            {"name": "precision", "type": "number", "default": 2, "min": 0, "max": 10, "description": "Знаков после запятой (при use_float)"},
        ]},
        {"kind": "sequence_int", "description": "Последовательность", "compatible_types": ["Int32"], "params": [
            {"name": "start", "type": "number", "default": 0},
            {"name": "step", "type": "number", "default": 1},
            {"name": "probability", "type": "number", "default": 100, "min": 0, "max": 100, "step": 0.01, "description": "Вероятность последовательного значения (%)"},
        ]},
        {"kind": "timestamp_asc", "description": "Даты по возрастанию", "compatible_types": ["DateTime"], "params": [
            {"name": "start", "type": "string", "default": "now", "placeholder": "now или ISO-8601"},
            {"name": "step", "type": "string", "default": "1s", "placeholder": "1s, 5m, 2h, 1d"},
        ]},
        {"kind": "timestamp_desc", "description": "Даты по убыванию", "compatible_types": ["DateTime"], "params": [
            {"name": "start", "type": "string", "default": "now"},
            {"name": "step", "type": "string", "default": "1s"},
        ]},
        {"kind": "random_digits", "description": "Случайные цифры", "compatible_types": ["String"], "params": [
            {"name": "length", "type": "number", "default": 8, "min": 1, "max": 100},
        ]},
        {"kind": "uuid4", "description": "UUID", "compatible_types": ["String", "UUID"], "params": []},
        {"kind": "url_template", "description": "URL шаблон", "compatible_types": ["String"], "params": [
            {"name": "pattern", "type": "string", "default": "https://example.com/item/{row}?uuid={uuid}", "placeholder": "https://example.com/item/{row}?uuid={uuid}"},
        ]},
        {"kind": "enum_choice", "description": "Выбор из списка: случайно или по очереди", "compatible_types": ["String", "Int32", "DateTime", "UUID"], "params": [
            {"name": "mode", "type": "select", "default": "random", "description": "Режим", "options": ["random", "sequential"], "option_labels": {"random": "Случайный", "sequential": "По очереди"}},
            {"name": "values", "type": "array", "description": "Значения (по строке)", "placeholder": "value1\nvalue2"},
            {"name": "dictionary", "type": "string", "description": "Имя словаря из dictionaries/"},
            {"name": "weights", "type": "array", "description": "Вероятности % (только для случайного)"},
        ]},
        {"kind": "regex", "description": "Случайная строка по regex. Выберите пресет или введите свой.", "compatible_types": ["String"], "params": [
            {"name": "preset", "type": "select", "default": "", "description": "Готовый формат", "options": ["", "ru_passport", "ru_phone", "mac_address"], "option_labels": {"": "Свой regex", "ru_passport": "Паспорт РФ", "ru_phone": "Телефон РФ (+7)", "mac_address": "MAC-адрес"}},
            {"name": "pattern", "type": "string", "default": "[a-z0-9]{8}", "description": "Регулярное выражение (если пресет не выбран)", "placeholder": "[A-Z]{3}-\\d{4}"},
        ]},
    ]

    @staticmethod
    async def handle() -> GeneratorsResponse:
        generators = [
            GeneratorSchema(
                kind=g["kind"],
                description=g["description"],
                compatible_types=g["compatible_types"],
                params=[GeneratorParamSchema(**p) for p in g["params"]]
            )
            for g in GeneratorsHandler.GENERATORS
        ]
        return GeneratorsResponse(generators=generators)


class SupportedTypesHandler:
    @staticmethod
    async def handle() -> SupportedTypesResponse:
        return SupportedTypesResponse(types=[
            SupportedTypeSchema(id="String", label="Строка"),
            SupportedTypeSchema(id="Int32", label="Число"),
            SupportedTypeSchema(id="DateTime", label="Дата"),
            SupportedTypeSchema(id="UUID", label="UUID"),
        ])


class DescribeTableHandler:
    @staticmethod
    async def handle(request: DescribeTableRequest) -> DescribeTableResponse:
        conn = request.connection
        engine = conn.get("engine", "clickhouse")
        database = conn.get("database", "default")
        tbl = request.table

        if engine == "postgres":
            repo = PostgresRepository(conn, database, request.table, "_dummy", "TEXT")
        else:
            tbl = f"{database}.{request.table}"
            repo = _make_ch_repo(conn, database, request.table)

        try:
            cols = repo.describe_table(tbl)
            return DescribeTableResponse(
                success=True,
                columns=[TableColumnSchema(name=c[0], type=c[1]) for c in cols],
                table=request.table,
                database=database
            )
        finally:
            repo.close()

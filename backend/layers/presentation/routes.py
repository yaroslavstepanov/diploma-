"""
Presentation Layer - API endpoints
"""
from fastapi import HTTPException
from typing import Dict, Any, Union

from backend.layers.presentation.models import (
    GenerateRequest, GenerateResponse,
    FetchDataRequest, FetchDataResponse,
    TestConnectionRequest, TestConnectionResponse,
    ClearTableRequest, ClearTableResponse,
    DropTableRequest, DropTableResponse,
    ListTablesRequest, ListTablesResponse,
    GeneratorsResponse, SupportedTypesResponse,
    DescribeTableRequest, DescribeTableResponse,
    GeneratorSchema, GeneratorParamSchema,
    SupportedTypeSchema, TableColumnSchema
)
from backend.layers.business.generator_service import GeneratorService
from backend.layers.data.clickhouse_repository import ClickHouseRepository
from backend.layers.data.postgres_repository import PostgresRepository


class GenerateHandler:
    """Обработчик запросов на генерацию данных"""
    
    @staticmethod
    async def handle_generate(request: GenerateRequest) -> GenerateResponse:
        """
        Обработать запрос на генерацию данных
        
        Args:
            request: Запрос на генерацию
        
        Returns:
            Ответ с результатами генерации
        
        Raises:
            HTTPException: При ошибках генерации
        """
        try:
            use_multi = request.fields and len(request.fields) > 0
            conn = request.connection or {}
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')

            if use_multi:
                # Многополевой режим
                if request.preview_only or not conn or not request.target_table:
                    # Предпросмотр только для первого поля
                    f0 = request.fields[0]
                    gen = GeneratorService.create_generator(f0.generator_kind, f0.generator_params)
                    data = GeneratorService.generate_preview(gen, request.rows)
                    return GenerateResponse(
                        success=True,
                        data=data,
                        message=f"Предпросмотр {len(data)} значений (поле {f0.name})"
                    )
                fields_spec = [
                    {"name": f.name, "type": f.type, "generator_kind": f.generator_kind, "generator_params": f.generator_params}
                    for f in request.fields
                ]
                if engine == 'postgres':
                    columns = [(f.name, f.type) for f in request.fields]
                    repository = PostgresRepository(
                        connection=conn,
                        database=database,
                        table_name=request.target_table,
                        columns=columns,
                    )
                    field_generators = [
                        GeneratorService.create_generator(f.generator_kind, f.generator_params)
                        for f in request.fields
                    ]
                    fields_for_row = [{"type": f.type} for f in request.fields]
                else:
                    profile = GeneratorService.create_profile_from_fields(
                        fields_spec, conn, request.target_table
                    )
                    repository = ClickHouseRepository(profile)
                    field_generators = GeneratorService.generate_rows(
                        profile, request.rows, request.batch_size
                    )
                    fields_for_row = profile.fields
            else:
                # Однополевой режим
                generator = GeneratorService.create_generator(
                    request.generator_kind,
                    request.generator_params or {}
                )
                if request.preview_only or not conn or not request.target_table:
                    data = GeneratorService.generate_preview(generator, request.rows)
                    return GenerateResponse(
                        success=True,
                        data=data,
                        message=f"Сгенерировано {len(data)} значений для предпросмотра"
                    )
                field_name = (request.generator_params or {}).get('field_name', 'field_name')
                field_type = (request.generator_params or {}).get('field_type', 'String')
                if engine == 'postgres':
                    repository = PostgresRepository(
                        connection=conn,
                        database=database,
                        table_name=request.target_table,
                        field_name=field_name,
                        field_type=field_type,
                    )
                    field_generators = [GeneratorService.create_generator(
                        request.generator_kind,
                        {**(request.generator_params or {}), 'field_name': field_name, 'field_type': field_type}
                    )]
                    fields_for_row = [{'type': field_type}]
                else:
                    profile = GeneratorService.create_profile(
                        field_name=field_name,
                        field_type=field_type,
                        generator_kind=request.generator_kind,
                        generator_params=request.generator_params,
                        connection=conn,
                        target_table=request.target_table
                    )
                    repository = ClickHouseRepository(profile)
                    field_generators = GeneratorService.generate_rows(
                        profile, request.rows, request.batch_size
                    )
                    fields_for_row = profile.fields

            try:
                repository.ensure_database()
                if request.create_table:
                    if use_multi and engine == 'postgres':
                        repository.ensure_table_with_columns(
                            [(f.name, f.type) for f in request.fields]
                        )
                    else:
                        repository.ensure_table()
                
                total_inserted = 0
                current_batch = []
                column_names = repository.column_names
                
                for row_index in range(request.rows):
                    row = []
                    for gen, field in zip(field_generators, fields_for_row):
                        val = gen.next(row_index)
                        # При Int32 и float — приводим к int (избегаем ошибки в clickhouse-connect)
                        ft = field.type if hasattr(field, 'type') else field.get('type', '')
                        if ft in ('Int32', 'Int64', 'Int8', 'Int16') and isinstance(val, float):
                            val = int(val)
                        row.append(val)
                    current_batch.append(row)
                    
                    if len(current_batch) >= request.batch_size:
                        repository.insert_rows(current_batch, column_names)
                        total_inserted += len(current_batch)
                        current_batch = []
                
                # Вставляем оставшиеся строки
                if current_batch:
                    repository.insert_rows(current_batch, column_names)
                    total_inserted += len(current_batch)
                
                return GenerateResponse(
                    success=True,
                    rows_inserted=total_inserted,
                    table=repository.table_name,
                    message=f"Успешно сгенерировано {total_inserted} строк в таблицу {repository.table_name}"
                )
            finally:
                repository.close()
                
        except Exception as e:
            import traceback
            raise HTTPException(
                status_code=500,
                detail={
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
            )


class FetchDataHandler:
    """Обработчик запросов на получение данных из БД"""
    
    @staticmethod
    async def handle_fetch(request: FetchDataRequest) -> FetchDataResponse:
        """
        Получить данные из таблицы ClickHouse
        
        Args:
            request: Запрос на получение данных
        
        Returns:
            Ответ с данными из таблицы
        
        Raises:
            HTTPException: При ошибках получения данных
        """
        try:
            import os
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            profile_path = None

            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn,
                    database=database,
                    table_name=request.table,
                )
                table_name = request.table
            else:
                from ch_synth.profile import Profile
                import tempfile
                import json
                profile_data = {
                    "connection": conn,
                    "target": {
                        "database": database,
                        "table": request.table,
                        "order_by": "tuple()",
                        "partition_by": None
                    },
                    "fields": []
                }
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(profile_data, f, indent=2)
                    profile_path = f.name
                profile = Profile.load(profile_path)
                repository = ClickHouseRepository(profile)
                table_name = f"{database}.{request.table}"

            try:
                columns = repository.get_table_columns(table_name)
                total_rows = repository.get_table_count(table_name)
                data = repository.fetch_table_data(
                    table_name, request.limit,
                    shuffle=request.shuffle,
                    float_precision=request.float_precision
                )
                columns_with_index = ["#"] + columns
                data_with_index = [[str(i + 1)] + row for i, row in enumerate(data)]
                return FetchDataResponse(
                    success=True,
                    data=data_with_index,
                    columns=columns_with_index,
                    total_rows=total_rows
                )
            finally:
                repository.close()
                if profile_path and os.path.exists(profile_path):
                    os.unlink(profile_path)
                    
        except HTTPException:
            raise
        except Exception as e:
            err_str = str(e).lower()
            if "does not exist" in err_str or "undefinedtable" in err_str:
                raise HTTPException(
                    status_code=404,
                    detail={"error": f"Таблица «{request.table}» не найдена. Сначала сгенерируйте данные через POST /api/generate"}
                )
            import traceback
            raise HTTPException(
                status_code=500,
                detail={
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
            )


class TestConnectionHandler:
    """Обработчик проверки подключения к БД"""

    @staticmethod
    async def handle_test(request: TestConnectionRequest) -> TestConnectionResponse:
        conn = request.connection
        engine = conn.get('engine', 'clickhouse')

        if engine == 'postgres':
            import psycopg2
            try:
                pg_params = {
                    "host": conn.get("host", "localhost"),
                    "port": int(conn.get("port", 5433)),
                    "user": conn.get("username", "postgres"),
                    "password": conn.get("password", ""),
                    "dbname": conn.get("database", "postgres"),
                }
                client = psycopg2.connect(**pg_params)
                with client.cursor() as cur:
                    cur.execute("SELECT 1")
                client.close()
                return TestConnectionResponse(
                    success=True,
                    message="Подключение к PostgreSQL успешно",
                    engine="postgres"
                )
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail={"error": f"Ошибка подключения к PostgreSQL: {str(e)}"}
                )
        else:
            import clickhouse_connect
            try:
                ch_client = clickhouse_connect.get_client(
                    host=conn.get("host", "localhost"),
                    port=int(conn.get("port", 18123)),
                    username=conn.get("username", "default"),
                    password=conn.get("password", ""),
                    database=conn.get("database", "default"),
                    secure=bool(conn.get("secure", False)),
                )
                ch_client.query("SELECT 1")
                ch_client.close()
                return TestConnectionResponse(
                    success=True,
                    message="Подключение к ClickHouse успешно",
                    engine="clickhouse"
                )
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail={"error": f"Ошибка подключения к ClickHouse: {str(e)}"}
                )


class ClearTableHandler:
    """Обработчик очистки таблицы (TRUNCATE)"""

    @staticmethod
    async def handle_clear(request: ClearTableRequest) -> ClearTableResponse:
        try:
            import os
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            profile_path = None

            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn,
                    database=database,
                    table_name=request.table,
                )
                table_name = request.table
            else:
                from ch_synth.profile import Profile
                import tempfile
                import json
                profile_data = {
                    "connection": conn,
                    "target": {"database": database, "table": request.table, "order_by": "tuple()", "partition_by": None},
                    "fields": []
                }
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(profile_data, f, indent=2)
                    profile_path = f.name
                profile = Profile.load(profile_path)
                repository = ClickHouseRepository(profile)
                table_name = f"{database}.{request.table}"

            try:
                repository.truncate_table(table_name)
                return ClearTableResponse(success=True, message=f"Таблица {request.table} очищена")
            finally:
                repository.close()
                if profile_path and os.path.exists(profile_path):
                    os.unlink(profile_path)

        except HTTPException:
            raise
        except Exception as e:
            err_str = str(e).lower()
            if "does not exist" in err_str or "undefinedtable" in err_str:
                raise HTTPException(status_code=404, detail={"error": f"Таблица «{request.table}» не найдена"})
            import traceback
            raise HTTPException(status_code=500, detail={"error": str(e), "traceback": traceback.format_exc()})


class DropTableHandler:
    """Обработчик удаления таблицы (DROP TABLE IF EXISTS)"""

    @staticmethod
    async def handle_drop(request: DropTableRequest) -> DropTableResponse:
        try:
            import os
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            profile_path = None

            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn,
                    database=database,
                    table_name=request.table,
                )
                table_name = request.table
            else:
                from ch_synth.profile import Profile
                import tempfile
                import json
                profile_data = {
                    "connection": conn,
                    "target": {"database": database, "table": request.table, "order_by": "tuple()", "partition_by": None},
                    "fields": []
                }
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(profile_data, f, indent=2)
                    profile_path = f.name
                profile = Profile.load(profile_path)
                repository = ClickHouseRepository(profile)
                table_name = f"{database}.{request.table}"

            try:
                repository.drop_table(table_name)
                return DropTableResponse(success=True, message=f"Таблица {request.table} удалена")
            finally:
                repository.close()
                if profile_path and os.path.exists(profile_path):
                    os.unlink(profile_path)

        except HTTPException:
            raise
        except Exception as e:
            import traceback
            raise HTTPException(status_code=500, detail={"error": str(e), "traceback": traceback.format_exc()})


class ListTablesHandler:
    """Обработчик списка таблиц в базе"""

    @staticmethod
    async def handle_list(request: ListTablesRequest) -> ListTablesResponse:
        try:
            import os
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            profile_path = None

            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn,
                    database=database,
                    table_name="_list_tables_dummy",
                )
                try:
                    tables = repository.list_tables()
                    return ListTablesResponse(success=True, engine="postgres", database=database, tables=tables)
                finally:
                    repository.close()
            else:
                from ch_synth.profile import Profile
                import tempfile
                import json
                profile_data = {
                    "connection": conn,
                    "target": {"database": database, "table": "_dummy", "order_by": "tuple()", "partition_by": None},
                    "fields": []
                }
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(profile_data, f, indent=2)
                    profile_path = f.name
                profile = Profile.load(profile_path)
                repository = ClickHouseRepository(profile)
                try:
                    tables = repository.list_tables(database)
                    return ListTablesResponse(success=True, engine="clickhouse", database=database, tables=tables)
                finally:
                    repository.close()
                    if profile_path and os.path.exists(profile_path):
                        os.unlink(profile_path)

        except HTTPException:
            raise
        except Exception as e:
            import traceback
            raise HTTPException(status_code=500, detail={"error": str(e), "traceback": traceback.format_exc()})


# --- Статические данные для GET /api/generators ---
GENERATORS_SPEC = [
    GeneratorSchema(
        kind="random_int",
        description="Случайные целые числа в диапазоне [min, max]. Опция use_float — вещественные с precision.",
        compatible_types=["Int32"],
        params=[
            GeneratorParamSchema(name="min", type="number", required=True, default=0, description="Минимум"),
            GeneratorParamSchema(name="max", type="number", required=True, default=100, description="Максимум"),
            GeneratorParamSchema(name="use_float", type="boolean", required=False, default=False, description="Генерировать вещественные числа"),
            GeneratorParamSchema(name="precision", type="number", required=False, default=2, min=0, max=10, description="Знаков после запятой (при use_float)"),
        ],
    ),
    GeneratorSchema(
        kind="sequence_int",
        description="Детерминированная последовательность целых чисел (start, step). Опционально probability.",
        compatible_types=["Int32"],
        params=[
            GeneratorParamSchema(name="start", type="number", required=False, default=0),
            GeneratorParamSchema(name="step", type="number", required=False, default=1),
            GeneratorParamSchema(name="probability", type="number", required=False, default=None, min=0, max=1),
        ],
    ),
    GeneratorSchema(
        kind="timestamp_asc",
        description="Возрастающие временные метки. start: 'now' или ISO-8601. step: '1s', '5m', '2h', '1d'.",
        compatible_types=["DateTime"],
        params=[
            GeneratorParamSchema(name="start", type="string", required=False, default="now", placeholder="now или ISO-8601"),
            GeneratorParamSchema(name="step", type="string", required=False, default="1s", placeholder="1s, 5m, 2h, 1d"),
        ],
    ),
    GeneratorSchema(
        kind="timestamp_desc",
        description="Убывающие временные метки. Параметры как у timestamp_asc.",
        compatible_types=["DateTime"],
        params=[
            GeneratorParamSchema(name="start", type="string", required=False, default="now", placeholder="now или ISO-8601"),
            GeneratorParamSchema(name="step", type="string", required=False, default="1s", placeholder="1s, 5m, 2h, 1d"),
        ],
    ),
    GeneratorSchema(
        kind="random_digits",
        description="Строка случайных цифр фиксированной длины.",
        compatible_types=["String"],
        params=[
            GeneratorParamSchema(name="length", type="number", required=False, default=8, min=1, max=100),
        ],
    ),
    GeneratorSchema(
        kind="uuid4",
        description="UUID v4 (RFC 4122).",
        compatible_types=["String", "UUID"],
        params=[],
    ),
    GeneratorSchema(
        kind="url_template",
        description="Шаблон с плейсхолдерами {row} и {uuid}.",
        compatible_types=["String"],
        params=[
            GeneratorParamSchema(name="pattern", type="string", required=True, default="https://example.com/item/{row}?uuid={uuid}", description="Шаблон URL", placeholder="https://example.com/item/{row}?uuid={uuid}"),
        ],
    ),
    GeneratorSchema(
        kind="enum_choice",
        description="Случайный выбор из списка. Опционально weights (вероятности).",
        compatible_types=["String", "Int32", "DateTime", "UUID"],
        params=[
            GeneratorParamSchema(name="values", type="array", required=True, description="Список значений (по одному на строку)", placeholder="value1\nvalue2\nvalue3"),
            GeneratorParamSchema(name="weights", type="array", required=False, description="Вероятности % (опционально)", placeholder="50\n30\n20"),
        ],
    ),
    GeneratorSchema(
        kind="regex",
        description="Случайная строка по регулярному выражению. Выберите пресет или введите свой regex.",
        compatible_types=["String"],
        params=[
            GeneratorParamSchema(
                name="preset",
                type="select",
                required=False,
                description="Готовый формат или свой шаблон",
                default="",
                options=["", "ru_passport", "ru_phone", "mac_address"],
                option_labels={"": "Свой regex", "ru_passport": "Паспорт РФ", "ru_phone": "Телефон РФ (+7)", "mac_address": "MAC-адрес"},
            ),
            GeneratorParamSchema(
                name="pattern",
                type="string",
                required=False,
                description="Свой regex (если пресет не выбран)",
                placeholder=r"[A-Z]{3}-\d{4}",
            ),
        ],
    ),
]

SUPPORTED_TYPES_SPEC = [
    SupportedTypeSchema(id="String", label="Строка"),
    SupportedTypeSchema(id="Int32", label="Число"),
    SupportedTypeSchema(id="DateTime", label="Дата"),
    SupportedTypeSchema(id="UUID", label="UUID"),
]


class GeneratorsHandler:
    @staticmethod
    async def handle() -> GeneratorsResponse:
        return GeneratorsResponse(generators=GENERATORS_SPEC)


class SupportedTypesHandler:
    @staticmethod
    async def handle() -> SupportedTypesResponse:
        return SupportedTypesResponse(types=SUPPORTED_TYPES_SPEC)


class DescribeTableHandler:
    """Обработчик схемы таблицы (колонки и типы)"""

    @staticmethod
    async def handle(request: DescribeTableRequest) -> DescribeTableResponse:
        try:
            import os
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            profile_path = None

            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn,
                    database=database,
                    table_name=request.table,
                )
                table_name = request.table
            else:
                from ch_synth.profile import Profile
                import tempfile
                import json
                profile_data = {
                    "connection": conn,
                    "target": {"database": database, "table": request.table, "order_by": "tuple()", "partition_by": None},
                    "fields": []
                }
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(profile_data, f, indent=2)
                    profile_path = f.name
                profile = Profile.load(profile_path)
                repository = ClickHouseRepository(profile)
                table_name = f"{database}.{request.table}"

            try:
                schema = repository.describe_table(table_name)
                columns = [TableColumnSchema(name=name, type=col_type) for name, col_type in schema]
                return DescribeTableResponse(
                    success=True,
                    columns=columns,
                    table=request.table,
                    database=database,
                )
            finally:
                repository.close()
                if profile_path and os.path.exists(profile_path):
                    os.unlink(profile_path)

        except HTTPException:
            raise
        except Exception as e:
            err_str = str(e).lower()
            if "does not exist" in err_str or "undefinedtable" in err_str:
                raise HTTPException(status_code=404, detail={"error": f"Таблица «{request.table}» не найдена"})
            import traceback
            raise HTTPException(status_code=500, detail={"error": str(e), "traceback": traceback.format_exc()})

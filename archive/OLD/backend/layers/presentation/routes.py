from fastapi import HTTPException
from typing import Union
from OLD.backend.layers.presentation.models import (
    GenerateRequest, GenerateResponse, FetchDataRequest, FetchDataResponse,
    TestConnectionRequest, TestConnectionResponse, ClearTableRequest, ClearTableResponse
)
from OLD.backend.layers.business.generator_service import GeneratorService
from OLD.backend.layers.data.clickhouse_repository import ClickHouseRepository
from OLD.backend.layers.data.postgres_repository import PostgresRepository


class GenerateHandler:
    @staticmethod
    async def handle_generate(request: GenerateRequest) -> GenerateResponse:
        try:
            generator = GeneratorService.create_generator(request.generator_kind, request.generator_params)
            if request.preview_only or not request.connection or not request.target_table:
                data = GeneratorService.generate_preview(generator, request.rows)
                return GenerateResponse(success=True, data=data, message=f"Сгенерировано {len(data)} значений")
            conn = request.connection
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            field_name = request.generator_params.get('field_name', 'field_name')
            field_type = request.generator_params.get('field_type', 'String')
            if engine == 'postgres':
                repository = PostgresRepository(
                    connection=conn, database=database, table_name=request.target_table,
                    field_name=field_name, field_type=field_type
                )
                field_generators = [GeneratorService.create_generator(
                    request.generator_kind,
                    {**request.generator_params, 'field_name': field_name, 'field_type': field_type}
                )]
                fields_for_row = [{'type': field_type}]
            else:
                profile = GeneratorService.create_profile(
                    field_name, field_type, request.generator_kind, request.generator_params,
                    conn, request.target_table
                )
                repository = ClickHouseRepository(profile)
                field_generators = GeneratorService.generate_rows(profile, request.rows, request.batch_size)
                fields_for_row = profile.fields
            try:
                repository.ensure_database()
                if request.create_table:
                    repository.ensure_table()
                total = 0
                batch = []
                cols = repository.column_names
                for i in range(request.rows):
                    row = []
                    for gen, f in zip(field_generators, fields_for_row):
                        v = gen.next(i)
                        ft = f.type if hasattr(f, 'type') else f.get('type', '')
                        if ft in ('Int32', 'Int64', 'Int8', 'Int16') and isinstance(v, float):
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
                return GenerateResponse(success=True, rows_inserted=total, table=repository.table_name,
                                       message=f"Сгенерировано {total} строк")
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
            engine = conn.get('engine', 'clickhouse')
            database = conn.get('database', 'default')
            tbl = request.table
            if engine == 'postgres':
                repo = PostgresRepository(conn, database, request.table, "_dummy", "String")
            else:
                from ch_synth.profile import Profile
                import tempfile, json, os
                pd = {"connection": conn, "target": {"database": database, "table": request.table, "order_by": "tuple()", "partition_by": None}, "fields": []}
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(pd, f, indent=2)
                    pf = f.name
                try:
                    profile = Profile.load(pf)
                    repo = ClickHouseRepository(profile)
                    tbl = f"{database}.{request.table}"
                finally:
                    if os.path.exists(pf):
                        os.unlink(pf)
            try:
                cols = repo.get_table_columns(tbl)
                total = repo.get_table_count(tbl)
                data = repo.fetch_table_data(tbl, request.limit, request.shuffle, request.float_precision)
                data_with_index = [[str(i + 1)] + row for i, row in enumerate(data)]
                return FetchDataResponse(success=True, data=data_with_index, columns=["#"] + cols, total_rows=total)
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
        engine = conn.get('engine', 'clickhouse')
        if engine == 'postgres':
            import psycopg2
            try:
                psycopg2.connect(
                    host=conn.get('host', 'localhost'),
                    port=int(conn.get('port', 5433)),
                    user=conn.get('username', 'postgres'),
                    password=conn.get('password', ''),
                    dbname=conn.get('database', 'postgres')
                )
                return TestConnectionResponse(success=True, message="OK", engine="postgres")
            except Exception as e:
                raise HTTPException(status_code=502, detail={"error": str(e)})
        else:
            import clickhouse_connect
            try:
                clickhouse_connect.get_client(
                    host=conn.get('host', 'localhost'),
                    port=int(conn.get('port', 18123)),
                    username=conn.get('username', 'default'),
                    password=conn.get('password', ''),
                    database=conn.get('database', 'default'),
                    secure=bool(conn.get('secure', False))
                ).query("SELECT 1")
                return TestConnectionResponse(success=True, message="OK", engine="clickhouse")
            except Exception as e:
                raise HTTPException(status_code=502, detail={"error": str(e)})


class ClearTableHandler:
    @staticmethod
    async def handle_clear(request: ClearTableRequest) -> ClearTableResponse:
        import os
        conn = request.connection
        engine = conn.get('engine', 'clickhouse')
        database = conn.get('database', 'default')
        if engine == 'postgres':
            repo = PostgresRepository(conn, database, request.table, "_dummy", "TEXT")
            tbl = request.table
        else:
            from ch_synth.profile import Profile
            import tempfile, json
            pd = {"connection": conn, "target": {"database": database, "table": request.table, "order_by": "tuple()", "partition_by": None}, "fields": []}
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(pd, f, indent=2)
                pf = f.name
            try:
                profile = Profile.load(pf)
                repo = ClickHouseRepository(profile)
                tbl = f"{database}.{request.table}"
            finally:
                if os.path.exists(pf):
                    os.unlink(pf)
            tbl = f"{database}.{request.table}"
        try:
            repo.truncate_table(tbl)
            return ClearTableResponse(success=True, message=f"Таблица {request.table} очищена")
        finally:
            repo.close()

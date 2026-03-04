"""
FastAPI сервер для фронтенда генератора данных.
Трехслойная архитектура:
- Presentation Layer: API endpoints
- Business Logic Layer: сервисы
- Data Access Layer: репозитории

Запуск: uvicorn backend.server:app --reload --port 5000
"""
from pathlib import Path
from fastapi import FastAPI, Body
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# Импорты из слоев
from backend.layers.presentation.models import (
    GenerateRequest, GenerateResponse,
    FetchDataRequest, FetchDataResponse,
    TestConnectionRequest, TestConnectionResponse,
    ClearTableRequest, ClearTableResponse,
    DropTableRequest, DropTableResponse,
    ListTablesRequest, ListTablesResponse,
    GeneratorsResponse, SupportedTypesResponse,
    DescribeTableRequest, DescribeTableResponse,
    HealthResponse, DictionariesResponse
)
from backend.layers.presentation.routes import (
    GenerateHandler, FetchDataHandler, TestConnectionHandler, ClearTableHandler,
    DropTableHandler, ListTablesHandler,
    GeneratorsHandler, SupportedTypesHandler, DescribeTableHandler
)

app = FastAPI(
    title="Field Generator API",
    description="API для генерации синтетических данных. Поддержка ClickHouse и PostgreSQL. Типы: Строка (String), Число (Int32), Дата (DateTime), UUID.",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get(
    "/api/health",
    response_model=HealthResponse,
    summary="Проверка работоспособности",
    description="Проверка работоспособности сервиса. Возвращает status: ok.",
    tags=["Служебные"]
)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/")
async def index():
    """Главная страница"""
    return FileResponse("frontend/index.html")


@app.post(
    "/api/generate",
    response_model=GenerateResponse,
    summary="Генерация данных",
    description="""
    Универсальный endpoint для генерации данных.
    
    **Режимы работы:**
    - **Предпросмотр**: `preview_only=true` или не указаны `connection`/`target_table`
      - Генерирует до 10 значений для предпросмотра
      - Не записывает в БД
      - Возвращает поле `data` с результатами
    
    - **Генерация в БД**: `preview_only=false` и указаны `connection`/`target_table`
      - Генерирует указанное количество строк в ClickHouse или PostgreSQL
      - Может создать таблицу автоматически
      - Возвращает поле `rows_inserted` с количеством вставленных строк
    
    **Поддерживаемые генераторы:**
    - `random_int` - случайные числа (params: min, max, use_float, precision при use_float)
    - `timestamp_asc` - возрастающие временные метки (params: start, step)
    - `timestamp_desc` - убывающие временные метки (params: start, step)
    - `random_digits` - случайные цифры (params: length)
    - `uuid4` - UUID v4 (params: нет)
    - `url_template` - URL с плейсхолдерами (params: pattern)
    - `enum_choice` - выбор из списка (params: values)
    """,
    tags=["Генерация"]
)
async def generate(request: GenerateRequest) -> GenerateResponse:
    """
    Универсальный endpoint для генерации данных.
    
    Использует трехслойную архитектуру:
    - Presentation Layer: обработка HTTP запроса
    - Business Logic Layer: логика генерации
    - Data Access Layer: работа с БД
    """
    return await GenerateHandler.handle_generate(request)


FETCH_DATA_EXAMPLE = {
    "connection": {
        "engine": "clickhouse",
        "host": "localhost",
        "port": 18123,
        "username": "default",
        "password": "ch_pass",
        "database": "default",
        "secure": False
    },
    "table": "preview_table",
    "limit": 10,
    "shuffle": False,
    "float_precision": 2
}


@app.post(
    "/api/fetch-data",
    response_model=FetchDataResponse,
    summary="Получение данных из БД",
    description="""
    Получить строки из таблицы ClickHouse или PostgreSQL для предпросмотра.
    
    **Порядок вызова:**
    1. Сначала `POST /api/generate` с `create_table: true` — создаёт и заполняет таблицу
    2. Затем `POST /api/fetch-data` — получает данные для отображения
    
    **Параметры connection:**
    - `engine`: `"clickhouse"` (порт 18123) или `"postgres"` (порт 5433)
    - `table`: имя таблицы (должна существовать)
    - `shuffle`: `true` — случайный порядок строк
    - `float_precision`: знаков после запятой для float
    """,
    tags=["Данные"]
)
async def fetch_data(
    request: FetchDataRequest = Body(
        ...,
        openapi_examples={
            "clickhouse": {
                "summary": "ClickHouse",
                "description": "Для ClickHouse (port 18123)",
                "value": FETCH_DATA_EXAMPLE
            },
            "postgres": {
                "summary": "PostgreSQL",
                "description": "Для PostgreSQL (port 5433). Таблица создаётся через /api/generate",
                "value": {
                    "connection": {"engine": "postgres", "host": "localhost", "port": 5433, "username": "postgres", "password": "postgres", "database": "postgres"},
                    "table": "preview_table",
                    "limit": 10,
                    "shuffle": True,
                    "float_precision": 2
                }
            }
        }
    )
) -> FetchDataResponse:
    return await FetchDataHandler.handle_fetch(request)


@app.post(
    "/api/test-connection",
    response_model=TestConnectionResponse,
    summary="Проверка подключения",
    description="Проверить подключение к ClickHouse или PostgreSQL перед генерацией. Выполняет SELECT 1.",
    tags=["Данные"]
)
async def test_connection(request: TestConnectionRequest) -> TestConnectionResponse:
    return await TestConnectionHandler.handle_test(request)


@app.post(
    "/api/clear-table",
    response_model=ClearTableResponse,
    summary="Очистка таблицы",
    description="TRUNCATE — удалить все строки из таблицы. Схема таблицы сохраняется. engine: clickhouse|postgres.",
    tags=["Данные"]
)
async def clear_table(request: ClearTableRequest) -> ClearTableResponse:
    return await ClearTableHandler.handle_clear(request)


@app.post(
    "/api/drop-table",
    response_model=DropTableResponse,
    summary="Удаление таблицы",
    description="DROP TABLE IF EXISTS — удалить таблицу (для тестов). engine: clickhouse|postgres.",
    tags=["Данные"]
)
async def drop_table(request: DropTableRequest) -> DropTableResponse:
    return await DropTableHandler.handle_drop(request)


@app.post(
    "/api/list-tables",
    response_model=ListTablesResponse,
    summary="Список таблиц",
    description="Список таблиц в выбранной базе данных. engine: clickhouse|postgres.",
    tags=["Данные"]
)
async def list_tables(request: ListTablesRequest) -> ListTablesResponse:
    return await ListTablesHandler.handle_list(request)


@app.get(
    "/api/generators",
    response_model=GeneratorsResponse,
    summary="Список генераторов",
    description="Список доступных генераторов и их параметров. Для построения UI динамически.",
    tags=["Справочники"]
)
async def get_generators() -> GeneratorsResponse:
    return await GeneratorsHandler.handle()


@app.get(
    "/api/supported-types",
    response_model=SupportedTypesResponse,
    summary="Поддерживаемые типы",
    description="Поддерживаемые типы данных: Строка (String), Число (Int32), Дата (DateTime), UUID.",
    tags=["Справочники"]
)
async def get_supported_types() -> SupportedTypesResponse:
    return await SupportedTypesHandler.handle()


@app.get(
    "/api/dictionaries",
    response_model=DictionariesResponse,
    summary="Список словарей",
    description="Именованные словари из dictionaries/index.json для enum_choice.",
    tags=["Справочники"]
)
async def get_dictionaries() -> DictionariesResponse:
    from backend.dictionaries import list_dictionaries
    items = list_dictionaries()
    return DictionariesResponse(dictionaries=[{"name": x["name"], "values_count": x["values_count"]} for x in items])


@app.post(
    "/api/describe-table",
    response_model=DescribeTableResponse,
    summary="Схема таблицы",
    description="Колонки и типы таблицы. Для добавления полей в существующую таблицу.",
    tags=["Данные"]
)
async def describe_table(request: DescribeTableRequest) -> DescribeTableResponse:
    return await DescribeTableHandler.handle(request)


@app.get("/{path:path}")
async def serve_static(path: str):
    """Статические файлы (CSS, JS). Регистрируется последним, чтобы не перехватывать /api/*"""
    if path.startswith("api/"):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not Found")
    file_path = Path("frontend") / path
    if file_path.exists() and file_path.is_file():
        return FileResponse(str(file_path))
    return FileResponse("frontend/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000, reload=True)

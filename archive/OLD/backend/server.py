"""
OLD версия Field Generator — только одно поле.
Запуск: python run_old.py  или  uvicorn OLD.backend.server:app --port 5001
"""
from pathlib import Path
from fastapi import FastAPI, Body
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from OLD.backend.layers.presentation.models import (
    GenerateRequest, GenerateResponse, FetchDataRequest, FetchDataResponse,
    TestConnectionRequest, TestConnectionResponse,
    ClearTableRequest, ClearTableResponse
)
from OLD.backend.layers.presentation.routes import (
    GenerateHandler, FetchDataHandler, TestConnectionHandler, ClearTableHandler
)

app = FastAPI(title="Field Generator (OLD)", version="0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    base = Path(__file__).resolve().parent.parent
    return FileResponse(str(base / "frontend" / "index.html"))


@app.post("/api/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest) -> GenerateResponse:
    return await GenerateHandler.handle_generate(request)


@app.post("/api/fetch-data", response_model=FetchDataResponse)
async def fetch_data(request: FetchDataRequest) -> FetchDataResponse:
    return await FetchDataHandler.handle_fetch(request)


@app.post("/api/test-connection", response_model=TestConnectionResponse)
async def test_connection(request: TestConnectionRequest) -> TestConnectionResponse:
    return await TestConnectionHandler.handle_test(request)


@app.post("/api/clear-table", response_model=ClearTableResponse)
async def clear_table(request: ClearTableRequest) -> ClearTableResponse:
    return await ClearTableHandler.handle_clear(request)


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
        {"name": "weights", "type": "array", "description": "Вероятности % (только для случайного)"},
    ]},
    {"kind": "regex", "description": "Случайная строка по regex. Выберите пресет или введите свой.", "compatible_types": ["String"], "params": [
        {"name": "preset", "type": "select", "default": "", "description": "Готовый формат", "options": ["", "ru_passport", "ru_phone", "mac_address"], "option_labels": {"": "Свой regex", "ru_passport": "Паспорт РФ", "ru_phone": "Телефон РФ (+7)", "mac_address": "MAC-адрес"}},
        {"name": "pattern", "type": "string", "default": "[a-z0-9]{8}", "description": "Регулярное выражение (если пресет не выбран)", "placeholder": "[A-Z]{3}-\\d{4}"},
    ]},
]


@app.get("/api/generators")
async def get_generators():
    return {"generators": GENERATORS}


@app.get("/api/supported-types")
async def get_supported_types():
    return {"types": [
        {"id": "String", "label": "Строка"},
        {"id": "Int32", "label": "Число"},
        {"id": "DateTime", "label": "Дата"},
        {"id": "UUID", "label": "UUID"}
    ]}


@app.get("/{path:path}")
async def serve_static(path: str):
    from fastapi import HTTPException
    if path.startswith("api/"):
        raise HTTPException(status_code=404)
    base = Path(__file__).resolve().parent.parent
    file_path = (base / "frontend" / path).resolve()
    if file_path.exists() and file_path.is_file():
        return FileResponse(str(file_path))
    return FileResponse(str(base / "frontend" / "index.html"))

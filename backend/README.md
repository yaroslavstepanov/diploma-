# Backend API

FastAPI сервер для генерации синтетических данных. Трёхслойная архитектура. Поддержка ClickHouse и PostgreSQL.

## Структура

```
backend/
├── server.py              # Точка входа, FastAPI app
└── layers/
    ├── presentation/     # API, модели запросов/ответов
    │   ├── models.py
    │   └── routes.py
    ├── business/         # Бизнес-логика генерации
    │   └── generator_service.py
    └── data/             # Репозитории
        ├── clickhouse_repository.py
        ├── postgres_repository.py
        └── type_mapping.py
```

## Запуск

```bash
# Из корня проекта
uvicorn backend.server:app --reload --port 5000
```

## API Endpoints

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/` | Веб-интерфейс (frontend/index.html) |
| POST | `/api/generate` | Генерация данных (многополевой: `fields[]` или одно поле: `generator_kind`/`generator_params`) |
| POST | `/api/fetch-data` | Выборка данных из таблицы для предпросмотра |
| POST | `/api/test-connection` | Проверка подключения к БД |
| POST | `/api/clear-table` | TRUNCATE таблицы |
| POST | `/api/drop-table` | DROP TABLE |
| POST | `/api/list-tables` | Список таблиц в БД |
| POST | `/api/describe-table` | Схема таблицы (колонки, типы) |
| GET | `/api/generators` | Справочник генераторов и параметров |
| GET | `/api/supported-types` | Поддерживаемые типы данных |
| GET | `/api/health` | Проверка работоспособности |

## Поддерживаемые СУБД

- **ClickHouse**: порт 18123 (Docker), `engine: "clickhouse"`
- **PostgreSQL**: порт 5433, `engine: "postgres"`

## Документация

- Swagger UI: http://localhost:5000/docs
- ReDoc: http://localhost:5000/redoc
- Архитектура: `ARCHITECTURE.md`

# Field Generator — генератор синтетических данных

Инструмент для генерации синтетических тестовых данных и загрузки в **ClickHouse** или **PostgreSQL**. Веб-интерфейс и CLI.

## Возможности
- **ClickHouse** и **PostgreSQL** — выбор СУБД
- Многополевой режим — таблица с несколькими колонкамиnf
- Автосоздание таблиц по схеме
- Генераторы: `random_int`, `sequence_int`, `timestamp_asc/desc`, `random_digits`, `uuid4`, `url_template`, `enum_choice`, `regex` (по regex или пресетам: паспорт РФ, телефон РФ, MAC-адрес)
- Опция вещественных чисел для `random_int` (use_float + precision)
- Вставка батчами, экспорт в CSV (CLI)

## Установка

1) Python 3.10+ (Windows: `py --version`)

2) В корне проекта:
```bash
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Структура проекта

```
Generator/
├── ch_synth/           # Модули генерации (generators, profile, client, cli)
├── backend/            # FastAPI сервер (трёхслойная архитектура)
│   ├── server.py       # Точка входа
│   └── layers/
│       ├── presentation/   # API, модели
│       ├── business/       # Логика генерации
│       └── data/           # ClickHouse, PostgreSQL репозитории
├── frontend/           # Веб-интерфейс (многополевой режим)
├── OLD/                # Старая версия (одно поле, порт 5051)
│   ├── backend/
│   ├── frontend/
│   └── ch_synth/       # Копия ch_synth для стабильной работы OLD
└── profiles/           # Примеры профилей (если есть)
```

## Быстрый старт

### Вариант 1: CLI (командная строка)

```bash
# В ClickHouse
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --batch-size 5000 --create-table

# В CSV
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --output-csv out.csv
```

### Вариант 2: Веб-интерфейс (новая версия)

1) Запустите сервер:
```bash
uvicorn backend.server:app --reload --port 5000
```

2) Откройте: **http://localhost:5000**

3) Добавьте поля в таблицу, выберите генераторы, настройте подключение к БД и сгенерируйте данные

### Вариант 3: Старая версия (одно поле)

```bash
python run_old.py
```

Откройте: **http://localhost:5051**

## Формат JSON профиля (CLI)

См. `profiles/sample_profile.json`:
- `connection` — host, port, username, password, database
- `target` — database, table, order_by, partition_by
- `fields[]` — колонки с типами и генераторами (`kind`, `params`)

## Docker (ClickHouse)

```bash
docker compose up -d
# ClickHouse: localhost:18123, user=default, password=ch_pass
```

## Тестирование

1. Запустите сервер: `python -m uvicorn backend.server:app --reload --port 5000`
2. В другом терминале: `python test_regex_api.py`

Скрипт проверяет предпросмотр regex-генератора (шаблон `[A-Z]{3}-[0-9]{4}` → например `KXM-4960`, `ITQ-2367`) и наличие `regex` в списке генераторов.

## Документация

- **API (Swagger)**: http://localhost:5000/docs
- **Backend**: `backend/README.md`, `backend/ARCHITECTURE.md`
- **Frontend**: `frontend/README.md`



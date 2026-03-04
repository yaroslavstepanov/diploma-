# Field Generator

Генератор синтетических данных для **ClickHouse** и **PostgreSQL**. Веб-интерфейс и CLI.

## Возможности

- ClickHouse и PostgreSQL
- Многополевой режим, автосоздание таблиц
- Генераторы: `random_int`, `sequence_int`, `timestamp_asc/desc`, `random_digits`, `uuid4`, `url_template`, `enum_choice`, `regex` (паспорт РФ, телефон, MAC)
- Объём: по количеству строк или по длительности × скорость

---

## Запуск приложения

### Требования

- **Python 3.10+**
- **Docker Desktop** (для ClickHouse)

### Шаг 1: Запуск ClickHouse

```bash
docker compose up -d
```

ClickHouse: `localhost:18123`, user=`default`, password=`ch_pass`, database=`default`

### Шаг 2: Установка зависимостей Python

```bash
# Windows
py -m venv .venv
.\.venv\Scripts\activate

# Mac / Linux
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

### Шаг 3: Запуск backend

```bash
uvicorn backend.server:app --reload --port 5000
```

### Шаг 4: Открыть приложение

**http://localhost:5000**

---

## Запуск через Docker (всё в контейнерах)

Если в `docker-compose.yml` есть сервис `backend`:

```bash
docker compose up -d --build
```

Приложение: **http://localhost:5000**

---

## CLI (командная строка)

```bash
# Генерация в ClickHouse
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --batch-size 5000 --create-table

# Экспорт в CSV
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --output-csv out.csv
```

---

## Стресс-тест ClickHouse (лимит памяти)

```bash
docker compose -f docker-compose.yml -f docker-compose.stress.yml up -d
python scripts/benchmark_iops.py --kill
```

---

## Бенчмарк IOPS

```bash
# Docker + ClickHouse должны быть запущены
python scripts/benchmark_iops.py --rows 10000 --batch 1000
python scripts/benchmark_iops.py --rows 50000 --threads 4 --mode both
python scripts/benchmark_iops.py --light --threads 8
```

Параметры: `--rows`, `--batch`, `--threads`, `--light`, `--catch-errors`, `--mode` (single|multi|both|max)

**Результаты бенчмарка:** `docs/BENCHMARK_REPORT.md`

---

## Структура проекта

```
├── backend/          # FastAPI API
├── frontend/         # Веб-интерфейс
├── ch_synth/         # Генерация (generators, profile, client, cli)
├── dictionaries/     # Словари для enum_choice
├── profiles/         # Примеры профилей CLI
├── scripts/          # benchmark_iops.py
├── tests/            # test_regex_api.py
└── docs/             # BENCHMARK_REPORT.md
```

---

## Дополнительно

- **Legacy-версия** (одно поле): `python scripts/run_old.py` → http://localhost:5051
- **API (Swagger):** http://localhost:5000/docs
- **Результаты бенчмарка:** `docs/BENCHMARK_REPORT.md`

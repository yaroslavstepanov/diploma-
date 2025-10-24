# Генератор синтетических данных для ClickHouse

Python инструмент для генерации синтетических тестовых данных и загрузки в ClickHouse или экспорта в CSV. Идеально подходит для наполнения витрин данных и нагрузочного тестирования.

## Возможности
- Загрузка данных в ClickHouse по профилю подключения (host/port/user/password/database)
- Автосоздание таблиц с описанием схемы (`--create-table`)
- Стратегии генерации полей:
  - `timestamp_asc` / `timestamp_desc` — возрастающие/убывающие временные метки
  - `sequence_int` — целочисленная последовательность
  - `random_int`, `random_float` — случайные числа
  - `enum_choice` — выбор из предопределенных значений
  - `random_digits` — строка случайных цифр указанной длины
  - `uuid4` — случайный UUID v4
  - `url_template` — URL с плейсхолдерами `{row}` и `{uuid}`
- Вставка батчами с прогресс-баром
- Альтернатива: экспорт в CSV

## Установка
1) Установите Python 3.10+ (Windows: `py --version`)

2) В корне проекта:
```bash
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Быстрый старт
1) Отредактируйте профиль: `profiles/sample_profile.json`

2) Сгенерировать 10 000 строк в ClickHouse с автосозданием таблицы:
```bash
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --batch-size 5000 --create-table
```

3) Экспорт в CSV вместо БД:
```bash
python -m ch_synth.cli --profile profiles/sample_profile.json --rows 10000 --output-csv out.csv
```

## Формат JSON профиля
См. `profiles/sample_profile.json`. Кратко:
- `connection` — параметры подключения к ClickHouse
- `target` — база/таблица/опции (ORDER BY, PARTITION BY)
- `fields[]` — список колонок с типами ClickHouse и генераторами

Ключевые поля:
- `generator.kind`: одна из стратегий выше
- `generator.params`: параметры стратегии (например, `start`, `step`, `min`, `max`, `values`, `length`, `pattern`)

Подсказки:
- Для `timestamp_*` используйте `start`: `"now"` или ISO-строку (`"2025-10-13T12:00:00"`), `step`: `"1s"`, `"5m"`, `"200ms"` и т.п.
- Для `url_template` используйте `pattern`, например: `"https://ex.com/item/{row}?id={uuid}"`

## Примеры
- Поле по убыванию времени: `timestamp_desc` со `start: "now"` и `step: "1s"`
- Поле по возрастанию: `timestamp_asc` со `start: "2025-01-01T00:00:00"`, `step: "1m"`
- Набор цифр: `random_digits` с `length: 10`
- Ссылки: либо `enum_choice` со списком URL, либо `url_template`

## Настройка Docker
```bash
# Запустить ClickHouse
docker compose up -d

# Проверить логи
docker compose logs -f clickhouse
```

## Лицензия
MIT

# Архитектура фронтенда

## Общая архитектура

### Backend (FastAPI)

**Единый универсальный endpoint**: `/api/generate`

Один endpoint для всех операций:
- Предпросмотр данных (preview_only=true)
- Генерация и вставка в БД (preview_only=false)

### Используемые модули

Все три модуля используются на бэкенде:

1. **`generators.py`**:
   - `build_generator()` - создание генераторов
   - Используется для генерации данных (и предпросмотр, и БД)

2. **`profile.py`**:
   - `Profile.load()` - загрузка профиля из JSON
   - Используется для генерации в БД

3. **`client.py`**:
   - `ClickHouseService` - подключение к БД, создание таблиц, вставка данных
   - Используется для генерации в БД

## Поток данных

### Предпросмотр:
```
Клиент → POST /api/generate (preview_only=true)
       → generators.py → build_generator()
       → generator.next() × 10
       → возврат данных клиенту
```

### Генерация в БД:
```
Клиент → POST /api/generate (preview_only=false)
       → generators.py → build_generator()
       → profile.py → Profile.load()
       → client.py → ClickHouseService
       → генерация данных батчами
       → client.insert_rows() → ClickHouse
```

## Преимущества единого endpoint

1. **Единая логика генерации** - один и тот же код для предпросмотра и БД
2. **Нет дублирования** - вся логика в Python модулях
3. **Простота** - один endpoint вместо нескольких
4. **Консистентность** - предпросмотр показывает те же данные, что будут в БД

## Структура запроса

```python
class GenerateRequest:
    generator_kind: str           # Тип генератора (random_int, timestamp_asc, и т.д.)
    generator_params: Dict        # Параметры генератора
    connection: Optional[Dict]    # Параметры подключения к ClickHouse
    target_table: Optional[str]   # Название таблицы
    rows: int                     # Количество строк
    batch_size: int               # Размер батча
    create_table: bool            # Создать таблицу автоматически
    preview_only: bool            # Только предпросмотр (не писать в БД)
```

## Ответ

```python
class GenerateResponse:
    success: bool                 # Успешность операции
    data: Optional[List[str]]     # Данные для предпросмотра
    rows_inserted: Optional[int]  # Количество вставленных строк
    table: Optional[str]          # Название таблицы
    message: Optional[str]        # Сообщение о результате
```

## Почему не отдельные endpoints для типов генераторов?

**Не нужно!** Один универсальный endpoint лучше, потому что:

1. **Все генераторы используют один интерфейс** - `BaseGenerator.next()`
2. **Параметры передаются динамически** - нет необходимости в отдельных endpoints
3. **Проще поддерживать** - изменения в одном месте
4. **Гибкость** - легко добавить новые типы генераторов без изменения API

Если бы были отдельные endpoints, пришлось бы:
- Дублировать код для каждого типа
- Поддерживать множество endpoints
- Усложнять клиентский код

Единый endpoint с параметром `generator_kind` - оптимальное решение.

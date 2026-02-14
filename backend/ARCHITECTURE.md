# Архитектура Backend - Трехслойная архитектура

## Обзор

Backend использует **трехслойную архитектуру (3-Tier Architecture)** для разделения ответственности:

```
┌─────────────────────────────────────┐
│   Presentation Layer (API)          │  ← Слой представления
│   - HTTP endpoints                  │
│   - Валидация запросов              │
│   - Формирование ответов            │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│   Business Logic Layer (Services)    │  ← Слой бизнес-логики
│   - Генерация данных                 │
│   - Создание профилей                │
│   - Бизнес-правила                   │
└──────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────┐
│   Data Access Layer (Repository)    │  ← Слой доступа к данным
│   - Работа с ClickHouse             │
│   - Управление подключениями         │
│   - CRUD операции                    │
└──────────────────────────────────────┘
```

## Структура проекта

```
backend/
├── server.py                    # Точка входа, FastAPI app
└── layers/
    ├── presentation/            # Presentation Layer
    │   ├── models.py           # Pydantic модели (Request/Response)
    │   └── routes.py           # Обработчики запросов
    ├── business/                # Business Logic Layer
    │   └── generator_service.py # Сервис генерации данных
    └── data/                    # Data Access Layer
        ├── clickhouse_repository.py  # Репозиторий ClickHouse
        ├── postgres_repository.py    # Репозиторий PostgreSQL
        └── type_mapping.py           # Маппинг типов ClickHouse → PostgreSQL
```

## Слои архитектуры

### 1. Presentation Layer (Слой представления)

**Расположение:** `backend/layers/presentation/`

**Ответственность:**
- Обработка HTTP запросов
- Валидация входных данных (Pydantic)
- Формирование HTTP ответов
- Обработка ошибок на уровне API

**Компоненты:**

**`models.py`** - Модели данных:
```python
class GenerateRequest(BaseModel):
    generator_kind: str
    generator_params: Dict[str, Any]
    # ... валидация через Pydantic

class GenerateResponse(BaseModel):
    success: bool
    data: Optional[List[str]]
    # ... типизированный ответ
```

**`routes.py`** - Обработчики:
```python
class GenerateHandler:
    @staticmethod
    async def handle_generate(request) -> GenerateResponse:
        # Координация между слоями
        # Вызов бизнес-логики
        # Обработка ошибок
```

**Принципы:**
- Тонкий слой - только HTTP обработка
- Не содержит бизнес-логики
- Зависит от Business Layer

### 2. Business Logic Layer (Слой бизнес-логики)

**Расположение:** `backend/layers/business/`

**Ответственность:**
- Вся бизнес-логика генерации данных
- Создание генераторов
- Создание профилей
- Правила валидации бизнес-данных

**Компоненты:**

**`generator_service.py`** - Сервис генерации:
```python
class GeneratorService:
    @staticmethod
    def create_generator(kind, params):
        # Создание генератора
    
    @staticmethod
    def generate_preview(generator, count):
        # Генерация для предпросмотра
    
    @staticmethod
    def create_profile(...):
        # Создание профиля для БД
    
    @staticmethod
    def generate_rows(profile, rows, batch_size):
        # Создание генераторов для всех полей
```

**Принципы:**
- Независим от Presentation Layer
- Не знает о HTTP
- Использует Data Access Layer для работы с БД
- Содержит всю бизнес-логику

### 3. Data Access Layer (Слой доступа к данным)

**Расположение:** `backend/layers/data/`

**Ответственность:**
- Работа с базой данных (ClickHouse)
- Управление подключениями
- Абстракция над `ch_synth.client`

**Компоненты:**

**`clickhouse_repository.py`** - Репозиторий ClickHouse  
**`postgres_repository.py`** - Репозиторий PostgreSQL  
**`type_mapping.py`** - Маппинг типов для PostgreSQL

```python
class ClickHouseRepository:
    def ensure_database(self)
    def ensure_table(self)
    def insert_rows(self, rows, column_names)
    def close(self)

class PostgresRepository:
    # Аналогичный интерфейс для PostgreSQL
```

**Принципы:**
- Абстракция над конкретной БД
- Инкапсулирует работу с ClickHouse
- Легко заменить на другую БД
- Не содержит бизнес-логики

## Поток данных

### Предпросмотр данных:

```
1. HTTP Request → Presentation Layer (routes.py)
   ↓
2. Валидация → GenerateRequest (models.py)
   ↓
3. Business Layer → GeneratorService.create_generator()
   ↓
4. Business Layer → GeneratorService.generate_preview()
   ↓
5. Presentation Layer → GenerateResponse (models.py)
   ↓
6. HTTP Response (JSON)
```

### Генерация в БД:

```
1. HTTP Request → Presentation Layer (routes.py)
   ↓
2. Валидация → GenerateRequest (models.py)
   ↓
3. Business Layer → GeneratorService.create_profile()
   ↓
4. Data Layer → ClickHouseRepository (инициализация)
   ↓
5. Data Layer → repository.ensure_database()
   ↓
6. Data Layer → repository.ensure_table()
   ↓
7. Business Layer → GeneratorService.generate_rows()
   ↓
8. Data Layer → repository.insert_rows() (в цикле)
   ↓
9. Presentation Layer → GenerateResponse (models.py)
   ↓
10. HTTP Response (JSON)
```

## Преимущества трехслойной архитектуры

### 1. **Разделение ответственности (Separation of Concerns)**
- Каждый слой отвечает за свою задачу
- Легче понимать и поддерживать код

### 2. **Независимость слоев**
- Presentation Layer не зависит от Data Layer
- Можно изменить БД без изменения бизнес-логики
- Можно изменить API без изменения бизнес-логики

### 3. **Тестируемость**
- Каждый слой можно тестировать отдельно
- Легко мокировать зависимости

### 4. **Переиспользование**
- Business Layer можно использовать из разных источников (API, CLI, задачи)
- Data Layer можно переиспользовать в разных сервисах

### 5. **Масштабируемость**
- Легко добавлять новые endpoints
- Легко добавлять новую бизнес-логику
- Легко менять источник данных

## Зависимости между слоями

```
Presentation Layer
    ↓ зависит от
Business Logic Layer
    ↓ зависит от
Data Access Layer
    ↓ использует
ch_synth/ (внешние модули)
```

**Правило:** Слои зависят только от слоев ниже, никогда наоборот!

## Примеры использования

### Добавление нового endpoint:

```python
# backend/layers/presentation/routes.py
class NewHandler:
    @staticmethod
    async def handle_new(request):
        # Используем Business Layer
        result = BusinessService.do_something()
        # Используем Data Layer если нужно
        repository.save(result)
        return Response(...)
```

### Добавление новой бизнес-логики:

```python
# backend/layers/business/new_service.py
class NewService:
    @staticmethod
    def do_something():
        # Бизнес-логика
        # Может использовать Data Layer
        pass
```

### Замена БД:

```python
# backend/layers/data/new_repository.py
class NewRepository:
    # Новая реализация репозитория
    # Business Layer не изменится!
    pass
```

## Технологии

- **FastAPI** - веб-фреймворк (Presentation Layer)
- **Pydantic** - валидация данных (Presentation Layer)
- **Python** - язык программирования
- **ch_synth** - внешние модули (используются всеми слоями)

## Итого

**Архитектура:** Трехслойная (3-Tier)

**Слои:**
1. ✅ **Presentation Layer** - HTTP, валидация, ответы
2. ✅ **Business Logic Layer** - бизнес-логика, правила
3. ✅ **Data Access Layer** - работа с БД, репозитории

**Принципы:**
- Разделение ответственности
- Независимость слоев
- Тестируемость
- Масштабируемость

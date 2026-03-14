# Манифест репозитория Field Generator

Чеклист критичных файлов. **Без них приложение не запустится или потеряет функциональность.**

## Критичные файлы (обязательны)

| Файл | Назначение |
|------|------------|
| `backend/layers/presentation/routes.py` | **Обработчики API** — без него `ModuleNotFoundError`, сервер не стартует |
| `backend/server.py` | Точка входа FastAPI |
| `backend/layers/presentation/models.py` | Модели запросов/ответов |
| `backend/layers/business/generator_service.py` | Бизнес-логика генерации |
| `backend/layers/data/clickhouse_repository.py` | Репозиторий ClickHouse |
| `backend/layers/data/postgres_repository.py` | Репозиторий PostgreSQL |
| `backend/dictionaries.py` | Загрузка словарей для enum_choice |
| `ch_synth/generators.py` | **REGEX_PRESETS** (паспорт РФ, телефон, MAC) + фабрика генераторов |
| `ch_synth/profile.py` | Профиль генерации |
| `ch_synth/client.py` | Клиент ClickHouse |
| `dictionaries/index.json` | Словари: mac_pool_4, servers, regions |

## Regex-пресеты (в `ch_synth/generators.py`)

```python
REGEX_PRESETS = {
    "ru_passport": r"[0-9]{4} [0-9]{6}",           # 1234 567890
    "ru_phone": r"\+7 \([0-9]{3}\) [0-9]{3}-[0-9]{2}-[0-9]{2}",  # +7 (999) 123-45-67
    "mac_address": r"[0-9A-Fa-f]{2}(:[0-9A-Fa-f]{2}){5}",       # AA:BB:CC:DD:EE:FF
}
```

## Публикация в GitLab

1. **Закоммитить все изменения** (в т.ч. `routes.py`):
   ```bash
   git add backend/layers/presentation/routes.py REPO_MANIFEST.md
   git commit -m "Add routes.py and REPO_MANIFEST.md"
   ```

2. **Создать новый репозиторий на GitLab** (пустой, без README).

3. **Добавить remote и запушить**:
   ```bash
   git remote add gitlab https://gitlab.com/YOUR_USERNAME/field-generator.git
   git push -u gitlab main
   ```

4. **Или заменить origin на GitLab**:
   ```bash
   git remote set-url origin https://gitlab.com/YOUR_USERNAME/field-generator.git
   git push -u origin main
   ```

## Проверка после клонирования

```bash
# Должны существовать:
ls backend/layers/presentation/routes.py
ls dictionaries/index.json
grep -l "ru_passport" ch_synth/generators.py
```

## Line endings (Mac/Linux/Windows)

`.gitattributes` задаёт `eol=lf` для `.py`, `.json`, `.md` — это помогает избежать проблем при клонировании на Mac.

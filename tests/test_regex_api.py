"""
Пример тестирования regex-генератора через API.

Запуск:
  1. Запустите сервер: python -m uvicorn backend.server:app --reload --port 5000
  2. Выполните: python tests/test_regex_api.py
"""
import requests
import json

BASE = "http://127.0.0.1:5000"

# 1. Предпросмотр (без БД) — regex-генератор
print("=== 1. Предпросмотр regex (5 значений) ===")
r = requests.post(f"{BASE}/api/generate", json={
    "connection": None,
    "target_table": None,
    "fields": [{
        "name": "code",
        "type": "String",
        "generator_kind": "regex",
        "generator_params": {"pattern": "[A-Z]{3}-[0-9]{4}"}
    }],
    "rows": 5
})
print("Status:", r.status_code)
if r.ok:
    data = r.json()
    print("Результат:", data.get("data", []))
    print("Примеры:", ", ".join(data["data"]) if data.get("data") else "-")
else:
    print("Ошибка:", r.json())

# 2. Список генераторов (regex в списке)
print("\n=== 2. Список генераторов ===")
r2 = requests.get(f"{BASE}/api/generators")
if r2.ok:
    gens = [g for g in r2.json().get("generators", []) if g["kind"] == "regex"]
    print("regex:", gens[0] if gens else "не найден")

"""
Загрузка именованных словарей из dictionaries/index.json.
"""
import json
from pathlib import Path
from typing import Dict, List, Any

_DICT_CACHE: Dict[str, List[Any]] | None = None


def _get_path() -> Path:
    """Путь к файлу словарей (относительно корня проекта)."""
    # backend/dictionaries.py -> project_root
    root = Path(__file__).resolve().parent.parent
    return root / "dictionaries" / "index.json"


def load_dictionaries() -> Dict[str, List[Any]]:
    """Загрузить все словари из JSON. Кэшируется."""
    global _DICT_CACHE
    if _DICT_CACHE is not None:
        return _DICT_CACHE
    path = _get_path()
    if not path.exists():
        _DICT_CACHE = {}
        return _DICT_CACHE
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _DICT_CACHE = {k: v if isinstance(v, list) else list(v) for k, v in data.items()}
        else:
            _DICT_CACHE = {}
    except Exception:
        _DICT_CACHE = {}
    return _DICT_CACHE


def list_dictionaries() -> List[Dict[str, Any]]:
    """Список словарей: {name, values_count}."""
    d = load_dictionaries()
    return [{"name": k, "values_count": len(v)} for k, v in d.items()]


def get_values(name: str) -> List[Any] | None:
    """Получить значения словаря по имени."""
    d = load_dictionaries()
    return d.get(name)


def resolve_enum_params(params: dict) -> dict:
    """
    Если enum_choice с dictionary — подставить values из словаря.
    Возвращает новый dict (не мутирует исходный).
    """
    params = dict(params)
    dictionary = (params.get("dictionary") or "").strip()
    if not dictionary:
        return params
    values = get_values(dictionary)
    if values is None:
        raise ValueError(f"Словарь «{dictionary}» не найден")
    if not values:
        raise ValueError(f"Словарь «{dictionary}» пуст")
    params["values"] = list(values)
    params.pop("dictionary", None)
    return params

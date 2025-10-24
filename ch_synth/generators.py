"""Генераторы значений, используемые CLI согласно определениям JSON профиля.

Каждое поле в профиле сопоставляется с типом генератора с опциональными параметрами.
Этот модуль предоставляет небольшие, композируемые генераторы и фабрику для их создания.
"""
from __future__ import annotations

import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional

from dateutil import parser as date_parser


# Утилиты

def parse_duration(duration_text: str) -> timedelta:
    """Парсить простые строки длительности типа '1s', '5m', '2h', '200ms' в timedelta."""
    duration_text = duration_text.strip().lower()
    if duration_text.endswith("ms"):
        return timedelta(milliseconds=float(duration_text[:-2]))
    unit = duration_text[-1]
    value = float(duration_text[:-1])
    if unit == "s":
        return timedelta(seconds=value)
    if unit == "m":
        return timedelta(minutes=value)
    if unit == "h":
        return timedelta(hours=value)
    if unit == "d":
        return timedelta(days=value)
    raise ValueError(f"Unsupported duration: {duration_text}")


def parse_start_ts(start_value: str | None) -> datetime:
    """Возвращать timezone-aware UTC datetime для 'now' или ISO-8601 строк."""
    if start_value is None or start_value == "now":
        return datetime.now(timezone.utc)
    parsed_dt = date_parser.isoparse(start_value)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    return parsed_dt.astimezone(timezone.utc)


# Генераторы

class BaseGenerator:
    """Общий интерфейс: генерировать next() значение для данного индекса строки."""
    def next(self, row_index: int) -> Any:
        raise NotImplementedError


@dataclass
class TimestampAscGenerator(BaseGenerator):
    """Монотонно возрастающие временные метки начиная с start с шагом step."""
    start: datetime
    step: timedelta

    def next(self, row_index: int) -> datetime:
        return self.start + self.step * row_index


@dataclass
class TimestampDescGenerator(BaseGenerator):
    """Монотонно убывающие временные метки начиная с start с шагом step."""
    start: datetime
    step: timedelta

    def next(self, row_index: int) -> datetime:
        return self.start - self.step * row_index


@dataclass
class SequenceIntGenerator(BaseGenerator):
    """Детерминированная целочисленная последовательность."""
    start: int = 0
    step: int = 1

    def next(self, row_index: int) -> int:
        return self.start + self.step * row_index


@dataclass
class RandomIntGenerator(BaseGenerator):
    """Равномерно случайное целое число в [min, max]."""
    min: int
    max: int

    def next(self, row_index: int) -> int:
        return random.randint(self.min, self.max)


@dataclass
class RandomFloatGenerator(BaseGenerator):
    """Равномерно случайное число с плавающей точкой в [min, max] округленное до precision знаков."""
    min: float
    max: float
    precision: int = 3

    def next(self, row_index: int) -> float:
        value = random.random() * (self.max - self.min) + self.min
        return float(f"{value:.{self.precision}f}")


@dataclass
class EnumChoiceGenerator(BaseGenerator):
    """Выбирать случайное значение из фиксированного списка каждый раз."""
    values: List[Any]

    def next(self, row_index: int) -> Any:
        return random.choice(self.values)


@dataclass
class RandomDigitsGenerator(BaseGenerator):
    """Генерировать строку случайных десятичных цифр фиксированной длины."""
    length: int

    def next(self, row_index: int) -> str:
        return "".join(random.choice("0123456789") for _ in range(self.length))


class UUID4Generator(BaseGenerator):
    """Генерировать RFC 4122 UUID v4 как строку для каждой строки."""
    def next(self, row_index: int) -> str:
        return str(uuid.uuid4())


@dataclass
class URLTemplateGenerator(BaseGenerator):
    """Подставлять плейсхолдеры {row} и {uuid} в заданный шаблон."""
    pattern: str

    def next(self, row_index: int) -> str:
        return self.pattern.replace("{row}", str(row_index)).replace("{uuid}", str(uuid.uuid4()))


# Фабрика

def build_generator(kind: str, params: Dict[str, Any]) -> BaseGenerator:
    """Фабрика: создать экземпляр генератора по типу и карте параметров."""
    kind_lower = kind.lower()
    if kind_lower == "timestamp_asc":
        start = parse_start_ts(params.get("start"))
        step = parse_duration(params.get("step", "1s"))
        return TimestampAscGenerator(start=start, step=step)
    if kind_lower == "timestamp_desc":
        start = parse_start_ts(params.get("start"))
        step = parse_duration(params.get("step", "1s"))
        return TimestampDescGenerator(start=start, step=step)
    if kind_lower == "sequence_int":
        return SequenceIntGenerator(start=int(params.get("start", 0)), step=int(params.get("step", 1)))
    if kind_lower == "random_int":
        return RandomIntGenerator(min=int(params["min"]), max=int(params["max"]))
    if kind_lower == "random_float":
        return RandomFloatGenerator(min=float(params["min"]), max=float(params["max"]), precision=int(params.get("precision", 3)))
    if kind_lower == "enum_choice":
        values = params.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("enum_choice requires non-empty 'values' list")
        return EnumChoiceGenerator(values=list(values))
    if kind_lower == "random_digits":
        return RandomDigitsGenerator(length=int(params.get("length", 8)))
    if kind_lower == "uuid4":
        return UUID4Generator()
    if kind_lower == "url_template":
        return URLTemplateGenerator(pattern=str(params["pattern"]))
    raise ValueError(f"Unsupported generator kind: {kind}")

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
    probability: Optional[float] = None  # Вероятность применения последовательности (0.0-1.0)

    def next(self, row_index: int) -> int:
        value = self.start + self.step * row_index
        if self.probability is not None:
            if random.random() < self.probability:
                return value
            else:
                max_value = max(self.start, value + abs(self.step) * 10)
                return random.randint(self.start, max_value)
        return value


@dataclass
class RandomIntGenerator(BaseGenerator):
    """Равномерно случайное целое число в [min, max]. При use_float — вещественные с заданной точностью."""
    min: int
    max: int
    use_float: bool = False
    precision: int = 2

    def next(self, row_index: int) -> int | float:
        if self.use_float:
            value = random.random() * (self.max - self.min) + self.min
            return float(f"{value:.{self.precision}f}")
        return random.randint(self.min, self.max)


@dataclass
class RandomFloatGenerator(BaseGenerator):
    min: float
    max: float
    precision: int = 3

    def next(self, row_index: int) -> float:
        value = random.random() * (self.max - self.min) + self.min
        return float(f"{value:.{self.precision}f}")


@dataclass
class PercentageGenerator(BaseGenerator):
    min: float = 0.0
    max: float = 100.0
    precision: int = 2

    def next(self, row_index: int) -> float:
        value = random.random() * (self.max - self.min) + self.min
        return float(f"{value:.{self.precision}f}")


@dataclass
class EnumChoiceGenerator(BaseGenerator):
    values: List[Any]
    weights: Optional[List[float]] = None

    def next(self, row_index: int) -> Any:
        if self.weights:
            return random.choices(self.values, weights=self.weights, k=1)[0]
        return random.choice(self.values)


@dataclass
class RandomDigitsGenerator(BaseGenerator):
    length: int

    def next(self, row_index: int) -> str:
        return "".join(random.choice("0123456789") for _ in range(self.length))


class UUID4Generator(BaseGenerator):
    def next(self, row_index: int) -> str:
        return str(uuid.uuid4())


@dataclass
class URLTemplateGenerator(BaseGenerator):
    pattern: str

    def next(self, row_index: int) -> str:
        return self.pattern.replace("{row}", str(row_index)).replace("{uuid}", str(uuid.uuid4()))


# Фабрика

def build_generator(kind: str, params: Dict[str, Any]) -> BaseGenerator:
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
        probability = params.get("probability")
        if probability is not None:
            probability = round(float(probability), 2)
            if probability < 0 or probability > 1:
                raise ValueError("probability must be between 0 and 1")
        return SequenceIntGenerator(
            start=int(params.get("start", 0)),
            step=int(params.get("step", 1)),
            probability=probability
        )
    if kind_lower == "random_int":
        use_float = params.get("use_float", False)
        precision = int(params.get("precision", 2))
        return RandomIntGenerator(
            min=int(params["min"]),
            max=int(params["max"]),
            use_float=use_float,
            precision=precision
        )
    if kind_lower == "random_float":
        return RandomFloatGenerator(min=float(params["min"]), max=float(params["max"]), precision=int(params.get("precision", 3)))
    if kind_lower == "percentage":
        return PercentageGenerator(
            min=float(params.get("min", 0.0)),
            max=float(params.get("max", 100.0)),
            precision=int(params.get("precision", 2))
        )
    if kind_lower == "enum_choice":
        values = params.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("enum_choice requires non-empty 'values' list")
        weights = params.get("weights")
        if weights:
            if len(weights) != len(values):
                raise ValueError("weights must have the same length as values")
            weights_sum = sum(weights)
            if weights_sum > 1.5:
                weights = [w / 100.0 for w in weights]
            normalized_weights = [w / sum(weights) for w in weights]
            return EnumChoiceGenerator(values=list(values), weights=normalized_weights)
        return EnumChoiceGenerator(values=list(values))
    if kind_lower == "random_digits":
        return RandomDigitsGenerator(length=int(params.get("length", 8)))
    if kind_lower == "uuid4":
        return UUID4Generator()
    if kind_lower == "url_template":
        return URLTemplateGenerator(pattern=str(params["pattern"]))
    raise ValueError(f"Unsupported generator kind: {kind}")

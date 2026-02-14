from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ConnectionConfig:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool = False


@dataclass
class TargetConfig:
    database: str
    table: str
    order_by: str
    partition_by: Optional[str] = None


@dataclass
class GeneratorConfig:
    kind: str
    params: Dict[str, Any]


@dataclass
class FieldConfig:
    name: str
    type: str  # Тип ClickHouse
    generator: GeneratorConfig


@dataclass
class Profile:
    connection: ConnectionConfig
    target: TargetConfig
    fields: List[FieldConfig]

    @staticmethod
    def load(path: str | Path) -> "Profile":
        data = json.loads(Path(path).read_text(encoding="utf-8"))

        connection = ConnectionConfig(
            host=data["connection"]["host"],
            port=int(data["connection"]["port"]),
            username=data["connection"].get("username", "default"),
            password=data["connection"].get("password", ""),
            database=data["connection"].get("database", "default"),
            secure=bool(data["connection"].get("secure", False)),
        )

        target = TargetConfig(
            database=data["target"]["database"],
            table=data["target"]["table"],
            order_by=data["target"].get("order_by", "tuple()"),
            partition_by=data["target"].get("partition_by"),
        )

        field_definitions: List[FieldConfig] = []
        for field_json in data["fields"]:
            field_definitions.append(
                FieldConfig(
                    name=field_json["name"],
                    type=field_json["type"],
                    generator=GeneratorConfig(
                        kind=field_json["generator"]["kind"],
                        params=field_json["generator"].get("params", {}),
                    ),
                )
            )

        return Profile(connection=connection, target=target, fields=field_definitions)

    def column_names(self) -> List[str]:
        return [field.name for field in self.fields]

    def ch_ddl_columns(self) -> str:
        return ",\n".join([f"`{field.name}` {field.type}" for field in self.fields])

    def fq_table(self) -> str:
        return f"{self.target.database}.{self.target.table}"

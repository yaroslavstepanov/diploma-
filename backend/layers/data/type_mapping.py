"""
Маппинг типов ClickHouse -> PostgreSQL
(используются единые UI-типы, совпадающие с ClickHouse)
"""
CH_TO_PG = {
    "String": "TEXT",
    "Int8": "SMALLINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "Float32": "REAL",
    "Float64": "DOUBLE PRECISION",
    "Date": "DATE",
    "DateTime": "TIMESTAMP",
    "DateTime64(3)": "TIMESTAMP",
    "Timestamp": "TIMESTAMP",
    "UUID": "UUID",
}


def to_postgres_type(ch_type: str) -> str:
    """Преобразовать тип ClickHouse в PostgreSQL."""
    return CH_TO_PG.get(ch_type, "TEXT")


# Обратный маппинг PostgreSQL -> ClickHouse (для describe_table)
PG_TO_CH = {
    "text": "String",
    "varchar": "String",
    "character varying": "String",
    "smallint": "Int16",
    "integer": "Int32",
    "bigint": "Int64",
    "real": "Float32",
    "double precision": "Float64",
    "date": "Date",
    "timestamp": "DateTime",
    "timestamp without time zone": "DateTime",
    "timestamp with time zone": "DateTime",
    "uuid": "UUID",
}


def from_postgres_type(pg_type: str) -> str:
    """Преобразовать тип PostgreSQL в ClickHouse (для describe_table)."""
    return PG_TO_CH.get(pg_type.lower(), "String")

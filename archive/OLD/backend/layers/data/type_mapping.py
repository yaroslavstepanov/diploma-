CH_TO_PG = {
    "String": "TEXT", "Int8": "SMALLINT", "Int16": "SMALLINT",
    "Int32": "INTEGER", "Int64": "BIGINT", "Float32": "REAL",
    "Float64": "DOUBLE PRECISION", "Date": "DATE", "DateTime": "TIMESTAMP",
    "DateTime64(3)": "TIMESTAMP", "Timestamp": "TIMESTAMP", "UUID": "UUID",
}

def to_postgres_type(ch_type: str) -> str:
    return CH_TO_PG.get(ch_type, "TEXT")

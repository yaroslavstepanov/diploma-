from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class GenerateRequest(BaseModel):
    generator_kind: str = Field(..., example="random_int")
    generator_params: Dict[str, Any] = Field(..., example={"min": 0, "max": 100, "field_name": "value", "field_type": "Int32"})
    connection: Optional[Dict[str, Any]] = None
    target_table: Optional[str] = None
    rows: int = Field(10, ge=1)
    batch_size: int = Field(1000, ge=1)
    create_table: bool = False
    preview_only: bool = False


class GenerateResponse(BaseModel):
    success: bool = True
    data: Optional[List[str]] = None
    rows_inserted: Optional[int] = None
    table: Optional[str] = None
    message: Optional[str] = None


class FetchDataRequest(BaseModel):
    connection: Dict[str, Any]
    table: str
    limit: int = 10
    shuffle: bool = False
    float_precision: int = 2


class FetchDataResponse(BaseModel):
    success: bool = True
    data: List[List[str]] = []
    columns: List[str] = []
    total_rows: Optional[int] = None


class TestConnectionRequest(BaseModel):
    connection: Dict[str, Any]


class TestConnectionResponse(BaseModel):
    success: bool = True
    message: str = ""
    engine: str = "clickhouse"


class ClearTableRequest(BaseModel):
    connection: Dict[str, Any]
    table: str


class ClearTableResponse(BaseModel):
    success: bool = True
    message: str = ""

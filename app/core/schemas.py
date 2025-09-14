from typing import List, Optional
from ninja import Schema
from pydantic import Field
from pydantic.config import ConfigDict  # pydantic v2


class ColumnSchema(Schema):
    name: str
    data_type: str
    is_nullable: bool
    default: Optional[str] = None

class ForeignKeySchema(Schema):
    column: str
    foreign_table: str
    foreign_column: str

class TableMetadata(Schema):
    # keep public JSON field as "schema", but avoid name clash internally
    table: str
    columns: List[ColumnSchema]
    primary_keys: List[str]
    foreign_keys: List[ForeignKeySchema]

    # ensure Ninja uses aliases when serializing (usually default, but safe)
    model_config = ConfigDict(ser_json_timedelta="iso8601", populate_by_name=True)

class ErrorResponse(Schema):
    detail: str
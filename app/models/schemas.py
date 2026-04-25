from datetime import datetime
from typing import Optional

from pydantic import BaseModel

# --- Schema config ---

class SchemaConfigResponse(BaseModel):
    id: str
    name: str
    version: str
    filename: str
    storage_path: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True  # allows building from SQLAlchemy ORM objects


class SchemaConfigListResponse(BaseModel):
    schemas: list[SchemaConfigResponse]


# --- Validation run ---

class ValidationErrorResponse(BaseModel):
    column: Optional[str]
    row: Optional[int]
    value: Optional[str]  # str to safely serialise any value type
    reason: Optional[str]

    class Config:
        from_attributes = True

class ValidationReportResponse(BaseModel):
    status: str
    schema_name: str
    schema_version: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    error_count: int
    errors: list[ValidationErrorResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class ValidationRunResponse(BaseModel):
    id: str
    schema_id: str
    filename: str
    status: str
    total_rows: int
    valid_rows: int
    error_count: int
    created_at: datetime

    class Config:
        from_attributes = True


class ValidationRunListResponse(BaseModel):
    runs: list[ValidationRunResponse]
    total: int
    limit: int
    offset: int

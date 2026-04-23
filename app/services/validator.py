from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
import pandera.pandas as pa

from app.services.schema_builder import SchemaBuilderError, build_pandera_schema
from app.services.yaml_parser import DataSchema, YAMLParseError, parse_yaml

# --- Result models ---


@dataclass
class ValidationError:
    column: str
    row: Optional[int]
    value: Any
    reason: str


@dataclass
class ValidationReport:
    status: str  # "passed" | "failed" | "error"
    schema_name: str
    schema_version: str
    total_rows: int
    valid_rows: int
    error_count: int
    errors: list[ValidationError] = field(default_factory=list)
    message: Optional[str] = None  # populated on "error" status

    @property
    def invalid_rows(self) -> int:
        return self.total_rows - self.valid_rows


# --- Main entry point ---


def validate(df: pd.DataFrame, schema_path: str) -> ValidationReport:
    """
    Full pipeline: parse YAML → build Pandera schema → validate DataFrame → report.
    Never raises — all errors are captured in the report.
    """
    # Step 1: Parse YAML
    try:
        data_schema = parse_yaml(schema_path)
    except YAMLParseError as e:
        return _error_report(str(e), total_rows=len(df))
    except Exception as e:
        return _error_report(
            f"Unexpected error parsing schema: {e}", total_rows=len(df)
        )

    # Step 2: Build Pandera schema
    try:
        pandera_schema = build_pandera_schema(data_schema)
    except SchemaBuilderError as e:
        return _error_report(str(e), total_rows=len(df))
    except Exception as e:
        return _error_report(
            f"Unexpected error building schema: {e}", total_rows=len(df)
        )

    # Step 3: Validate
    try:
        pandera_schema.validate(df, lazy=True)

        # Passed — no exceptions raised
        return ValidationReport(
            status="passed",
            schema_name=data_schema.name,
            schema_version=data_schema.version,
            total_rows=len(df),
            valid_rows=len(df),
            error_count=0,
        )

    except pa.errors.SchemaErrors as e:
        return _build_failure_report(e, data_schema, df)

    except Exception as e:
        return _error_report(
            f"Unexpected error during validation: {e}", total_rows=len(df)
        )


# --- Helpers ---


def _build_failure_report(
    exc: pa.errors.SchemaErrors,
    data_schema: DataSchema,
    df: pd.DataFrame,
) -> ValidationReport:
    """
    Parses Pandera's failure_cases DataFrame into structured ValidationErrors.
    """
    failure_cases = exc.failure_cases
    errors = []

    for _, row in failure_cases.iterrows():
        errors.append(
            ValidationError(
                column=_safe_get(row, "column"),
                row=_safe_get(row, "index"),
                value=_safe_get(row, "failure_case"),
                reason=_safe_get(row, "check"),
            )
        )

    # Deduplicate failed row indices across all columns
    failed_rows = failure_cases["index"].dropna().unique()
    valid_rows = len(df) - len(failed_rows)

    return ValidationReport(
        status="failed",
        schema_name=data_schema.name,
        schema_version=data_schema.version,
        total_rows=len(df),
        valid_rows=max(valid_rows, 0),
        error_count=len(errors),
        errors=errors,
    )


def _error_report(message: str, total_rows: int) -> ValidationReport:
    """
    Returns a report for cases where validation couldn't even run —
    e.g. bad YAML config or schema build failure.
    """
    return ValidationReport(
        status="error",
        schema_name="unknown",
        schema_version="unknown",
        total_rows=total_rows,
        valid_rows=0,
        error_count=0,
        message=message,
    )


def _safe_get(row: pd.Series, key: str) -> Any:
    """Safely retrieves a value from a Pandera failure row."""
    try:
        value = row.get(key)
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return row.get(key)

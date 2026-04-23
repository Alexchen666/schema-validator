import operator as op

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

from app.services.yaml_parser import (
    ColumnConstraints,
    ColumnRelationship,
    ColumnSchema,
    DataSchema,
)

TYPE_MAP = {
    "int": pa.Int,
    "float": pa.Float,
    "str": pa.String,
    "bool": pa.Bool,
    "datetime": pa.DateTime,
}

# Nullable-safe equivalents for types that suffer from float upcast
NULLABLE_TYPE_MAP = {
    "int": pd.Int64Dtype(),
}

OPERATOR_MAP = {
    ">": op.gt,
    ">=": op.ge,
    "<": op.lt,
    "<=": op.le,
    "==": op.eq,
    "!=": op.ne,
}

TRANSFORM_MAP = {
    "square": lambda col: col**2,
    "sqrt": lambda col: np.sqrt(col),
    "abs": lambda col: col.abs(),
    "log": lambda col: np.log(col),
    "negate": lambda col: -col,
    "pow": None,  # handled separately, needs transform_arg
}


class SchemaBuilderError(Exception):
    pass


def build_pandera_schema(data_schema: DataSchema) -> DataFrameSchema:
    columns = {col.name: _build_column(col) for col in data_schema.columns}

    # Relationship checks are DataFrame-level, not column-level
    df_checks = [
        _build_structured_check(rel) for rel in data_schema.relationships
    ]

    return DataFrameSchema(columns, checks=df_checks if df_checks else None)


def _build_column(col: ColumnSchema) -> Column:
    if col.nullable and col.type in NULLABLE_TYPE_MAP:
        pa_type = NULLABLE_TYPE_MAP[col.type]
    else:
        pa_type = TYPE_MAP.get(col.type)

    if pa_type is None:
        raise SchemaBuilderError(
            f"Cannot map type '{col.type}' to Pandera type"
        )

    checks = _build_checks(col.constraints)

    return Column(
        dtype=pa_type,
        nullable=col.nullable,
        unique=col.unique,
        checks=checks if checks else None,
        # coerce=True is intentional. When a nullable integer column contains
        # None, pandas internally upcasts it to float64. Without coercion,
        # Pandera would reject this as a dtype mismatch even though the values
        # are semantically valid integers. Setting coerce=True lets Pandera
        # cast float64 → Int64 before validation, preserving the intended
        # behaviour. Trade-off: Pandera will silently cast other type
        # mismatches too, so strict dtype enforcement is delegated to the
        # YAML schema config rather than the DataFrame's incoming dtype.
        coerce=True
    )


def _build_checks(constraints: ColumnConstraints) -> list[Check]:
    checks = []

    if constraints.min is not None and constraints.max is not None:
        checks.append(Check.in_range(constraints.min, constraints.max))
    elif constraints.min is not None:
        checks.append(Check.greater_than_or_equal_to(constraints.min))
    elif constraints.max is not None:
        checks.append(Check.less_than_or_equal_to(constraints.max))

    if constraints.regex:
        checks.append(Check.str_matches(constraints.regex))

    if constraints.allowed_values:
        checks.append(Check.isin(constraints.allowed_values))

    return checks


def _build_structured_check(rel: ColumnRelationship) -> pa.Check:
    left_col = rel.left
    right_col = rel.right.column # type: ignore
    transform = rel.right.transform # type: ignore
    transform_arg = rel.right.transform_arg # type: ignore
    comparator = OPERATOR_MAP[rel.operator]

    def check_fn(df: pd.DataFrame) -> pd.Series:
        left = df[left_col]
        right = df[right_col]

        if transform == "pow":
            if transform_arg is None:
                raise SchemaBuilderError(
                    "'pow' transform requires 'transform_arg'"
                )
            right = right**transform_arg
        elif transform:
            right = TRANSFORM_MAP[transform](right) # type: ignore

        return comparator(left, right)

    return pa.Check(
        check_fn,
        element_wise=False,
        error=rel.description,
    )

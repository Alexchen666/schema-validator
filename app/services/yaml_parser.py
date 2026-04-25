from dataclasses import dataclass, field
from typing import Any, Optional

import yaml


@dataclass
class ColumnConstraints:
    min: Optional[float] = None
    max: Optional[float] = None
    regex: Optional[str] = None
    allowed_values: list[str] = field(default_factory=list)


@dataclass
class ColumnSchema:
    name: str
    type: str
    nullable: bool = True
    unique: bool = False
    constraints: ColumnConstraints = field(default_factory=ColumnConstraints)


@dataclass
class ColumnRelationship:
    description: str
    expression: str


@dataclass
class DataSchema:
    name: str
    version: str
    columns: list[ColumnSchema]
    relationships: list[ColumnRelationship] = field(default_factory=list)


class YAMLParseError(Exception):
    pass


SUPPORTED_TYPES = {"int", "float", "str", "bool", "datetime"}


def parse_yaml(path: str) -> DataSchema:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    _validate_top_level(raw)

    columns = []
    for col_name, col_def in raw["columns"].items():
        columns.append(_parse_column(col_name, col_def))

    relationships = []
    for rel_raw in raw.get("relationships", []):
        relationships.append(_parse_relationship(rel_raw))

    return DataSchema(
        name=raw["name"],
        version=raw["version"],
        columns=columns,
        relationships=relationships,
    )


def _validate_top_level(raw: dict) -> None:
    for key in ("name", "version", "columns"):
        if key not in raw:
            raise YAMLParseError(f"Missing required top-level key: '{key}'")
    if not isinstance(raw["columns"], dict):
        raise YAMLParseError(
            "'columns' must be a mapping of column definitions"
        )


def _parse_column(name: str, col_def: dict) -> ColumnSchema:
    if "type" not in col_def:
        raise YAMLParseError(
            f"Column '{name}' is missing required field 'type'"
        )

    col_type = col_def["type"]
    if col_type not in SUPPORTED_TYPES:
        raise YAMLParseError(
            f"Column '{name}' has unsupported type '{col_type}'. "
            f"Supported types: {SUPPORTED_TYPES}"
        )

    constraints = ColumnConstraints()
    if raw_constraints := col_def.get("constraints"):
        constraints = _parse_constraints(name, col_type, raw_constraints)

    return ColumnSchema(
        name=name,
        type=col_type,
        nullable=col_def.get("nullable", True),
        unique=col_def.get("unique", False),
        constraints=constraints,
    )


def _parse_constraints(
    col_name: str, col_type: str, raw: dict[str, Any]
) -> ColumnConstraints:
    constraints = ColumnConstraints()

    if "min" in raw or "max" in raw:
        if col_type not in ("int", "float"):
            raise YAMLParseError(
                f"Column '{col_name}': min/max constraints only apply to int or float"
            )
        constraints.min = raw.get("min")
        constraints.max = raw.get("max")

    if "regex" in raw:
        if col_type != "str":
            raise YAMLParseError(
                f"Column '{col_name}': regex constraint only applies to str"
            )
        constraints.regex = raw["regex"]

    if "allowed_values" in raw:
        if col_type != "str":
            raise YAMLParseError(
                f"Column '{col_name}': allowed_values constraint only applies to str"
            )
        constraints.allowed_values = raw["allowed_values"]

    return constraints


def _parse_relationship(raw: dict) -> ColumnRelationship:
    if "expression" not in raw:
        raise YAMLParseError(
            "Relationship is missing required field 'expression'. "
            'Hint: use expression form e.g. "loan_amount <= annual_income * 5"'
        )
    if "description" not in raw:
        raise YAMLParseError(
            "Relationship is missing required field 'description'"
        )
    return ColumnRelationship(
        description=raw["description"],
        expression=raw["expression"],
    )

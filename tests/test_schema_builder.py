import pandas as pd
import pandera.pandas as pa
import pytest

from app.services.schema_builder import SchemaBuilderError, build_pandera_schema
from app.services.yaml_parser import (
    ColumnConstraints,
    ColumnRelationship,
    ColumnSchema,
    DataSchema,
    RightOperand,
)


# --- Fixtures ---
def make_schema(columns: list[ColumnSchema], relationships=None) -> DataSchema:
    return DataSchema(
        name="test",
        version="1.0",
        columns=columns,
        relationships=relationships or [],
    )


def make_column(
    name: str, type_: str, nullable=True, unique=False, **constraints
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        type=type_,
        nullable=nullable,
        unique=unique,
        constraints=ColumnConstraints(**constraints),
    )


# --- Basic schema building ---
class TestBasicSchemaBuilding:
    def test_builds_without_error(self):
        schema = make_schema([make_column("id", "int")])
        result = build_pandera_schema(schema)
        assert isinstance(result, pa.DataFrameSchema)

    def test_all_supported_types_build(self):
        columns = [
            make_column("a", "int"),
            make_column("b", "float"),
            make_column("c", "str"),
            make_column("d", "bool"),
        ]
        result = build_pandera_schema(make_schema(columns))
        assert isinstance(result, pa.DataFrameSchema)

    def test_nullable_false_rejects_null(self):
        schema = build_pandera_schema(
            make_schema([make_column("id", "int", nullable=False)])
        )
        df = pd.DataFrame({"id": [1, None, 3]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_nullable_true_accepts_null(self):
        schema = build_pandera_schema(
            make_schema([make_column("id", "int", nullable=True)])
        )
        df = pd.DataFrame({"id": [1, None, 3]})
        schema.validate(df, lazy=True)  # should not raise

    def test_unique_rejects_duplicates(self):
        schema = build_pandera_schema(
            make_schema([make_column("id", "int", unique=True)])
        )
        df = pd.DataFrame({"id": [1, 1, 3]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)


# --- Constraint checks ---
class TestConstraintChecks:
    def test_min_max_rejects_out_of_range(self):
        schema = build_pandera_schema(
            make_schema([make_column("age", "int", min=0, max=120)])
        )
        df = pd.DataFrame({"age": [25, -1, 130]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_min_max_accepts_valid_range(self):
        schema = build_pandera_schema(
            make_schema([make_column("age", "int", min=0, max=120)])
        )
        df = pd.DataFrame({"age": [0, 60, 120]})
        schema.validate(df, lazy=True)  # should not raise

    def test_min_only(self):
        schema = build_pandera_schema(
            make_schema([make_column("score", "float", min=0.0)])
        )
        df = pd.DataFrame({"score": [-1.0, 0.0, 100.0]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_max_only(self):
        schema = build_pandera_schema(
            make_schema([make_column("score", "float", max=100.0)])
        )
        df = pd.DataFrame({"score": [0.0, 50.0, 101.0]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_regex_rejects_invalid(self):
        schema = build_pandera_schema(
            make_schema(
                [make_column("email", "str", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$")]
            )
        )
        df = pd.DataFrame({"email": ["valid@example.com", "not-an-email"]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_regex_accepts_valid(self):
        schema = build_pandera_schema(
            make_schema(
                [make_column("email", "str", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$")]
            )
        )
        df = pd.DataFrame({"email": ["a@b.com", "user@domain.org"]})
        schema.validate(df, lazy=True)  # should not raise

    def test_allowed_values_rejects_invalid(self):
        schema = build_pandera_schema(
            make_schema(
                [
                    make_column(
                        "status", "str", allowed_values=["active", "inactive"]
                    )
                ]
            )
        )
        df = pd.DataFrame({"status": ["active", "pending"]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_allowed_values_accepts_valid(self):
        schema = build_pandera_schema(
            make_schema(
                [
                    make_column(
                        "status", "str", allowed_values=["active", "inactive"]
                    )
                ]
            )
        )
        df = pd.DataFrame({"status": ["active", "inactive", "active"]})
        schema.validate(df, lazy=True)  # should not raise


# --- Relationship checks ---
class TestRelationshipChecks:
    def _rel(self, left, operator, right_col, transform=None):
        return ColumnRelationship(
            description="test relationship",
            left=left,
            operator=operator,
            right=RightOperand(column=right_col, transform=transform),
        )

    def test_structured_greater_than(self):
        schema = build_pandera_schema(
            make_schema(
                columns=[
                    make_column("a", "float"),
                    make_column("b", "float"),
                ],
                relationships=[self._rel("a", ">", "b")],
            )
        )
        df = pd.DataFrame({"a": [5.0, 3.0], "b": [3.0, 5.0]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_structured_square_transform(self):
        schema = build_pandera_schema(
            make_schema(
                columns=[
                    make_column("revenue", "float"),
                    make_column("cost", "float"),
                ],
                relationships=[
                    self._rel("revenue", ">", "cost", transform="square")
                ],
            )
        )
        # revenue=5, cost=3 → 5 > 9 → False → should fail
        df = pd.DataFrame({"revenue": [5.0], "cost": [3.0]})
        with pytest.raises(pa.errors.SchemaErrors):
            schema.validate(df, lazy=True)

    def test_structured_square_transform_passes(self):
        schema = build_pandera_schema(
            make_schema(
                columns=[
                    make_column("revenue", "float"),
                    make_column("cost", "float"),
                ],
                relationships=[
                    self._rel("revenue", ">", "cost", transform="square")
                ],
            )
        )
        # revenue=100, cost=3 → 100 > 9 → True → should pass
        df = pd.DataFrame({"revenue": [100.0], "cost": [3.0]})
        schema.validate(df, lazy=True)  # should not raise

    def test_pow_transform_requires_transform_arg(self):
        rel = ColumnRelationship(
            description="test",
            left="a",
            operator=">",
            right=RightOperand(column="b", transform="pow", transform_arg=None),
        )
        schema = build_pandera_schema(
            make_schema(
                columns=[make_column("a", "float"), make_column("b", "float")],
                relationships=[rel],
            )
        )
        df = pd.DataFrame({"a": [5.0], "b": [3.0]})
        with pytest.raises((SchemaBuilderError, pa.errors.SchemaErrors)):
            schema.validate(df, lazy=True)

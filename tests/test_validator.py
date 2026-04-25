import pandas as pd
import pytest
import yaml

from app.services.validator import ValidationReport, validate


# --- Fixtures ---
@pytest.fixture
def simple_schema(tmp_path):
    data = {
        "name": "customer",
        "version": "1.0",
        "columns": {
            "id": {"type": "int", "nullable": False, "unique": True},
            "name": {"type": "str", "nullable": False},
            "age": {
                "type": "int",
                "nullable": True,
                "constraints": {"min": 0, "max": 120},
            },
            "status": {
                "type": "str",
                "nullable": False,
                "constraints": {"allowed_values": ["active", "inactive"]},
            },
        },
    }
    path = tmp_path / "schema.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


@pytest.fixture
def relationship_schema(tmp_path):
    data = {
        "name": "sales",
        "version": "1.0",
        "columns": {
            "revenue": {"type": "float"},
            "cost": {"type": "float"},
        },
        "relationships": [
            {
                "description": "revenue must exceed cost squared",
                "expression": "revenue > cost ** 2",
            }
        ],
    }
    path = tmp_path / "schema.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


@pytest.fixture
def valid_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 40, None],
            "status": ["active", "inactive", "active"],
        }
    )


@pytest.fixture
def invalid_df():
    return pd.DataFrame(
        {
            "id": [1, 1, 3],  # duplicate id
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, -5, 200],  # age out of range
            "status": ["active", "pending", "active"],  # invalid status
        }
    )


# --- Report shape ---
class TestReportShape:
    def test_returns_validation_report(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert isinstance(report, ValidationReport)

    def test_report_has_expected_fields(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert hasattr(report, "status")
        assert hasattr(report, "total_rows")
        assert hasattr(report, "valid_rows")
        assert hasattr(report, "error_count")
        assert hasattr(report, "errors")

    def test_total_rows_correct(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert report.total_rows == 3


# --- Passing cases ---
class TestPassingValidation:
    def test_valid_df_returns_passed(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert report.status == "passed"

    def test_valid_df_has_no_errors(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert report.error_count == 0
        assert report.errors == []

    def test_valid_df_valid_rows_equals_total(self, valid_df, simple_schema):
        report = validate(valid_df, simple_schema)
        assert report.valid_rows == report.total_rows


# --- Failing cases ---
class TestFailingValidation:
    def test_invalid_df_returns_failed(self, invalid_df, simple_schema):
        report = validate(invalid_df, simple_schema)
        assert report.status == "failed"

    def test_invalid_df_has_errors(self, invalid_df, simple_schema):
        report = validate(invalid_df, simple_schema)
        assert report.error_count > 0
        assert len(report.errors) > 0

    def test_error_has_expected_fields(self, invalid_df, simple_schema):
        report = validate(invalid_df, simple_schema)
        error = report.errors[0]
        assert hasattr(error, "column")
        assert hasattr(error, "row")
        assert hasattr(error, "value")
        assert hasattr(error, "reason")

    def test_valid_rows_less_than_total(self, invalid_df, simple_schema):
        report = validate(invalid_df, simple_schema)
        assert report.valid_rows < report.total_rows

    def test_invalid_rows_computed_correctly(self, invalid_df, simple_schema):
        report = validate(invalid_df, simple_schema)
        assert report.invalid_rows == report.total_rows - report.valid_rows

    def test_age_violation_detected(self, simple_schema):
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "age": [-1],
                "status": ["active"],
            }
        )
        report = validate(df, simple_schema)
        assert report.status == "failed"
        columns = [e.column for e in report.errors]
        assert "age" in columns

    def test_status_violation_detected(self, simple_schema):
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "age": [25],
                "status": ["pending"],
            }
        )
        report = validate(df, simple_schema)
        assert report.status == "failed"
        columns = [e.column for e in report.errors]
        assert "status" in columns


# --- Relationship failures ---
class TestRelationshipValidation:
    def test_relationship_violation_detected(self, relationship_schema):
        # revenue=5, cost=3 → 5 > 9 → False
        df = pd.DataFrame({"revenue": [5.0], "cost": [3.0]})
        report = validate(df, relationship_schema)
        assert report.status == "failed"

    def test_relationship_passes(self, relationship_schema):
        # revenue=100, cost=3 → 100 > 9 → True
        df = pd.DataFrame({"revenue": [100.0], "cost": [3.0]})
        report = validate(df, relationship_schema)
        assert report.status == "passed"


# --- Error status (bad config) ---
class TestErrorStatus:
    def test_bad_yaml_path_returns_error(self, valid_df):
        report = validate(valid_df, "/nonexistent/path/schema.yaml")
        assert report.status == "error"
        assert report.message is not None

    def test_error_report_never_raises(self, valid_df):
        # validate() should never raise regardless of input
        report = validate(valid_df, "/bad/path.yaml")
        assert isinstance(report, ValidationReport)

    def test_error_report_schema_name_is_unknown(self, valid_df):
        report = validate(valid_df, "/bad/path.yaml")
        assert report.schema_name == "unknown"

    def test_malformed_yaml_returns_error(self, tmp_path, valid_df):
        path = tmp_path / "bad.yaml"
        path.write_text("{ this: is: not: valid: yaml: }{{{")
        report = validate(valid_df, str(path))
        assert report.status == "error"

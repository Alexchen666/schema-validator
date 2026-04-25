import tempfile

import pytest
import yaml

from app.services.yaml_parser import (
    DataSchema,
    YAMLParseError,
    parse_yaml,
)

# --- Fixtures ---
def write_yaml(data: dict) -> str:
    """Helper: writes a dict to a temp YAML file and returns the path."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.dump(data, tmp)
    tmp.close()
    return tmp.name


@pytest.fixture
def valid_yaml(tmp_path):
    data = {
        "name": "test_schema",
        "version": "1.0",
        "columns": {
            "id": {"type": "int", "nullable": False, "unique": True},
            "name": {"type": "str", "nullable": False},
            "age": {
                "type": "int",
                "nullable": True,
                "constraints": {"min": 0, "max": 120},
            },
            "email": {
                "type": "str",
                "nullable": False,
                "constraints": {"regex": r"^[\w\.-]+@[\w\.-]+\.\w+$"},
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
def relationship_yaml(tmp_path):
    data = {
        "name": "rel_schema",
        "version": "1.0",
        "columns": {
            "revenue": {"type": "float"},
            "cost": {"type": "float"},
        },
        "relationships": [
            {
                "description": "revenue > cost squared",
                "expression": "revenue > cost ** 2",
            }
        ],
    }
    path = tmp_path / "rel_schema.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


# --- Top-level structure ---
class TestTopLevelValidation:
    def test_valid_yaml_parses_successfully(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        assert isinstance(schema, DataSchema)
        assert schema.name == "test_schema"
        assert schema.version == "1.0"

    def test_missing_name_raises(self, tmp_path):
        data = {"version": "1.0", "columns": {"id": {"type": "int"}}}
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="name"):
            parse_yaml(str(path))

    def test_missing_version_raises(self, tmp_path):
        data = {"name": "s", "columns": {"id": {"type": "int"}}}
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="version"):
            parse_yaml(str(path))

    def test_missing_columns_raises(self, tmp_path):
        data = {"name": "s", "version": "1.0"}
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="columns"):
            parse_yaml(str(path))

    def test_columns_not_a_mapping_raises(self, tmp_path):
        data = {"name": "s", "version": "1.0", "columns": ["id", "name"]}
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="mapping"):
            parse_yaml(str(path))


# --- Column parsing ---
class TestColumnParsing:
    def test_columns_parsed_correctly(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        col_names = [col.name for col in schema.columns]
        assert "id" in col_names
        assert "age" in col_names

    def test_nullable_false_parsed(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        id_col = next(c for c in schema.columns if c.name == "id")
        assert id_col.nullable is False

    def test_unique_true_parsed(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        id_col = next(c for c in schema.columns if c.name == "id")
        assert id_col.unique is True

    def test_nullable_defaults_to_true(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        name_col = next(c for c in schema.columns if c.name == "name")
        assert name_col.nullable is False  # explicitly set

    def test_missing_type_raises(self, tmp_path):
        data = {
            "name": "s",
            "version": "1.0",
            "columns": {"id": {"nullable": False}},
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="type"):
            parse_yaml(str(path))

    def test_unsupported_type_raises(self, tmp_path):
        data = {
            "name": "s",
            "version": "1.0",
            "columns": {"id": {"type": "uuid"}},
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="unsupported type"):
            parse_yaml(str(path))


# --- Constraints ---
class TestConstraintParsing:
    def test_min_max_parsed(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        age_col = next(c for c in schema.columns if c.name == "age")
        assert age_col.constraints.min == 0
        assert age_col.constraints.max == 120

    def test_regex_parsed(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        email_col = next(c for c in schema.columns if c.name == "email")
        assert email_col.constraints.regex is not None

    def test_allowed_values_parsed(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        status_col = next(c for c in schema.columns if c.name == "status")
        assert "active" in status_col.constraints.allowed_values

    def test_min_max_on_str_raises(self, tmp_path):
        data = {
            "name": "s",
            "version": "1.0",
            "columns": {"name": {"type": "str", "constraints": {"min": 0}}},
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="min/max"):
            parse_yaml(str(path))

    def test_regex_on_int_raises(self, tmp_path):
        data = {
            "name": "s",
            "version": "1.0",
            "columns": {
                "age": {"type": "int", "constraints": {"regex": r"\d+"}}
            },
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="regex"):
            parse_yaml(str(path))

    def test_allowed_values_on_int_raises(self, tmp_path):
        data = {
            "name": "s",
            "version": "1.0",
            "columns": {
                "status": {
                    "type": "int",
                    "constraints": {"allowed_values": [1, 2, 3]},
                }
            },
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="allowed_values"):
            parse_yaml(str(path))


# --- Relationships ---
class TestRelationshipParsing:
    def test_structured_relationship_parsed(self, relationship_yaml):
        schema = parse_yaml(relationship_yaml)
        assert len(schema.relationships) == 1
        rel = schema.relationships[0]
        assert rel.description == "revenue > cost squared"
        assert rel.expression == "revenue > cost ** 2"

    def test_unsupported_operator_raises(self, tmp_path):
        data = {
            "name": "s", "version": "1.0",
            "columns": {
                "a": {"type": "float"},
                "b": {"type": "float"},
            },
            "relationships": [
                {"description": "missing expression field"}
            ],
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="expression"):
            parse_yaml(str(path))

    def test_missing_description_raises(self, tmp_path):
        data = {
            "name": "s", "version": "1.0",
            "columns": {
                "a": {"type": "float"},
                "b": {"type": "float"},
            },
            "relationships": [
                {"expression": "a > b"}
            ],
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        with pytest.raises(YAMLParseError, match="description"):
            parse_yaml(str(path))

    def test_no_relationships_defaults_to_empty_list(self, valid_yaml):
        schema = parse_yaml(valid_yaml)
        assert schema.relationships == []

    def test_multiple_relationships_parsed(self, tmp_path):
        data = {
            "name": "s", "version": "1.0",
            "columns": {
                "a": {"type": "float"},
                "b": {"type": "float"},
                "c": {"type": "float"},
            },
            "relationships": [
                {"description": "a > b", "expression": "a > b"},
                {"description": "b > c", "expression": "b > c"},
            ],
        }
        path = tmp_path / "schema.yaml"
        path.write_text(yaml.dump(data))
        schema = parse_yaml(str(path))
        assert len(schema.relationships) == 2

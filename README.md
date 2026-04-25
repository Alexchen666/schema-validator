# Schema Validator

A REST API for validating CSV data against YAML-configured schemas. Define column types, constraints, and cross-column relationships in a YAML file — no Python code required.

## Features

- **YAML-configured schemas** — define column types, nullability, uniqueness, and constraints in a simple YAML file
- **Rich constraints** — min/max ranges, regex patterns, and allowed value lists
- **Cross-column relationships** — express constraints between columns using natural expressions like `loan_amount <= annual_income * 5`
- **Detailed validation reports** — per-row, per-column error breakdown with the offending value and reason
- **Schema management** — upload, update, list, and delete schemas via API
- **Validation history** — every validation run is persisted and queryable

---

## Project Structure

```
data-validator/
├── app/
│   ├── main.py              # App entry point, router registration, startup
│   ├── config.py            # Settings from environment variables
│   ├── dependencies.py      # Shared Depends() — database session
│   ├── routers/
│   │   ├── validate.py      # POST /validate/{schema_id}
│   │   ├── schemas.py       # CRUD /schemas
│   │   └── history.py       # GET /history
│   ├── services/
│   │   ├── yaml_parser.py   # YAML → internal DataSchema
│   │   ├── schema_builder.py# DataSchema → Pandera schema
│   │   └── validator.py     # Runs validation, returns report
│   ├── models/
│   │   ├── db.py            # SQLAlchemy tables + engine
│   │   └── schemas.py       # Pydantic request/response models
│   └── utils/
│       └── logging.py       # Structured JSON logging
├── tests/
│   ├── test_yaml_parser.py
│   ├── test_schema_builder.py
│   └── test_validator.py
├── example_schemas/
│   ├── employees_simple.yaml
│   ├── products.yaml
│   └── loan_applications.yaml
├── example_data/
│   ├── employees_simple_valid.csv
│   ├── products_invalid.csv
│   └── loan_applications_valid.csv
├── .env                    # Not included in the repo
├── .gitignore
├── pyproject.toml
├── uv.lock
├── .python-version
└── README.md
```

---

## Getting Started

**1. Clone and install dependencies:**

```bash
git clone <your-repo-url>
cd schema-validator
uv sync
```

**2. Set up environment variables:**

```bash
vi .env
```

There is an example below:

```
DATABASE_URL=sqlite:///./data_validator.db
SCHEMA_STORAGE_PATH=./schemas
```

**3. Run the API:**

```bash
uv run uvicorn app.main:app --reload
```

The API is available at `http://localhost:8000`.
Interactive documentation is at `http://localhost:8000/docs`.

---

## Schema Configuration

Schemas are written in YAML. Each schema defines the expected columns and their rules.

### Basic structure

```yaml
version: "1.0"
name: my_schema

columns:
  column_name:
    type: int         # int | float | str | bool | datetime
    nullable: false   # default: true
    unique: false     # default: false
    constraints:
      min: 0          # int and float only
      max: 100        # int and float only
      regex: '^\d+$'  # str only
      allowed_values: [active, inactive]  # str only
```

### Cross-column relationships

Use the `relationships` key to express constraints between columns. Expressions are evaluated in a sandboxed environment — only column names are in scope.

```yaml
relationships:
  - description: "loan_amount must not exceed 5x annual_income"
    expression: "loan_amount <= annual_income * 5"

  - description: "end_date must be after start_date"
    expression: "end_date > start_date"

  - description: "annual_income must exceed loan_amount to the power of 0.8"
    expression: "annual_income >= loan_amount ** 0.8"
```

Supported operations in expressions: arithmetic (`+`, `-`, `*`, `/`, `**`), comparisons (`>`, `>=`, `<`, `<=`, `==`, `!=`), logical (`&`, `|`, `~`), and any pandas Series method (e.g. `.fillna()`, `.abs()`).

### Full example

See [`example_schemas/loan_applications.yaml`](example_schemas/loan_applications.yaml) for a complete schema with all constraint types and relationships.

---

## API Reference

### Schemas

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/schemas` | Upload a new YAML schema |
| `GET` | `/schemas` | List all schemas |
| `GET` | `/schemas/{schema_id}` | Get a schema by ID |
| `PUT` | `/schemas/{schema_id}` | Replace a schema |
| `DELETE` | `/schemas/{schema_id}` | Delete a schema and its history |

### Validate

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/validate/{schema_id}` | Validate a CSV file against a schema |

### History

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/history` | List validation runs (supports filtering and pagination) |
| `GET` | `/history/{run_id}` | Get a specific run |
| `DELETE` | `/history/{run_id}` | Delete a run record |

### Query parameters for `GET /history`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schema_id` | string | — | Filter by schema |
| `status` | string | — | Filter by status (`passed`, `failed`, `error`) |
| `limit` | int | 50 | Max results (1–500) |
| `offset` | int | 0 | Pagination offset |

---

## Validation Report

A validation run returns one of three statuses:

- `passed` — all rows passed all constraints
- `failed` — validation ran but found violations
- `error` — validation could not run due to a bad schema config

### Example response

```json
{
  "status": "failed",
  "schema_name": "products",
  "schema_version": "1.0",
  "total_rows": 6,
  "valid_rows": 3,
  "invalid_rows": 3,
  "error_count": 5,
  "errors": [
    {
      "column": "category",
      "row": 1,
      "value": "apparel",
      "reason": "isin({'electronics', 'clothing', 'food', 'furniture'})"
    },
    {
      "column": "price",
      "row": 2,
      "value": -5.0,
      "reason": "greater_than_or_equal_to(0.0)"
    }
  ]
}
```

---

## Running Tests

```bash
uv run pytest tests/
```

---

## Limitations

- Only CSV files are supported as input
- Relationship expressions use `asteval` for sandboxed evaluation — suitable for internal/trusted use; review carefully before exposing to untrusted users
- Conditional relationship logic (e.g. "if A > X then B > Y") requires boolean expression syntax: `~(A > X) | (B > Y)`
- No support for nested or multi-file schemas

## Overview

sqlutilz is a lightweight Python package designed for generating and executing SQL queries in a headless backend, optimized for Flask applications. It provides a secure, modular framework for building dialect-specific SQL queries from JSON payloads or Pandas DataFrames and executing them with robust connection management, type coercion, auditing, and fallback support. The package is ideal for developers needing a flexible, non-ORM solution for database interactions in web applications or scripts.

The package is split into two subpackages:

- **sql_builder**: Generates SQL queries and parameters for SELECT, INSERT, UPDATE, DELETE, UPSERT, and CREATE TABLE operations.
- **sqlbbw**: Manages query execution, type coercion, auditing, and fallbacks using the SqlCon class.

**Key Use Cases**:

- Frontend-driven query generation via JSON payloads.
- DataFrame-based database operations.
- Schema creation and validation.
- Secure, audited query execution across multiple SQL dialects (SQLite, PostgreSQL, MySQL, MSSQL, Oracle).

**Note**: sqlutilz is not a full ORM. It focuses on query generation and execution without object-relational mapping, keeping it lightweight and SQL-centric.

## Package Structure
```
SqlUtils/
├── app.py                # Flask application for HTTP endpoints
├── config.py             # Database connection configuration
├── sql_builder/          # Query generation modules
│   ├── __init__.py       # Exports SQLBuilder, Condition, etc.
│   ├── adapt_sql.py      # Adjusts SQL/parameters for dialects
│   ├── conditions.py     # Parses WHERE clause conditions
│   ├── df_handler.py     # Handles DataFrame-based queries
│   ├── json_handler.py   # Processes JSON payloads
│   ├── query_builder.py  # Core SQL query generation
│   ├── table_creator.py  # Generates CREATE TABLE statements
├── sqlbbw/               # Execution and utility modules
│   ├── __init__.py       # Exports SqlCon, DataCorrector, etc.
│   ├── audit.py          # Logs query executions
│   ├── conn.py           # Database connection and execution (SqlCon)
│   ├── corrector.py      # Type coercion for data
│   ├── fallback.py       # Fallback execution with raw drivers
│   ├── mappings.py       # Dialect-specific mappings (placeholders, types)
│   ├── utils.py          # Schema utilities and helpers
```
## Installation

Install required dependencies:

```bash
pip install sqlalchemy pandas
```

Optional drivers for specific databases:

- PostgreSQL: `pip install psycopg2`
- MySQL: `pip install pymysql`
- MSSQL: `pip install pyodbc`
- Oracle: `pip install cx_Oracle`

For lightweight deployments, Pandas can be optional by checking `DB_IMPORTS_AVAILABLE` in `sqlbbw.corrector`.

## Configuration

Create a `config.py` to define database connection details:

```python
# config.py
DB_CONFIG = {
    'conn_str': 'sqlite:///:memory:',  # Example: 'postgresql://user:pass@localhost/db'
    'audit_db': 'sqlite:///audit.db'   # Optional audit database
}
```

- `conn_str`: SQLAlchemy connection string for the primary database.
- `audit_db`: Optional connection string for audit logging.

## Key Features

- **Dialect Support**: SQLite, PostgreSQL, MySQL, MSSQL, Oracle.
- **Query Generation**: Build SQL queries and parameters from JSON or DataFrames.
- **Execution**: Secure execution with SqlCon, supporting SQLAlchemy and raw driver fallbacks.
- **Security**: Sanitizes table/column names with regex and uses parameterized queries to prevent SQL injection.
- **Type Coercion**: Automatically adjusts data types to match DB schema.
- **Auditing**: Logs all executed queries to audit_db.
- **Schema Management**: Create tables with primary/foreign keys.
- **Stateless Design**: Ideal for Flask-based headless backends.

## General Usage

### Flask Integration

The primary use case is a Flask backend exposing endpoints for query generation and execution. Below is an example setup:

```python
# app.py (simplified)
from flask import Flask, request, jsonify, g
from sqlutilz.sql_builder import json_select
from sqlutilz.sqlbbw import SqlCon
from config import DB_CONFIG

app = Flask(__name__)

def get_db():
    if 'db' not in g:
        g.db = SqlCon(DB_CONFIG['conn_str'], audit_db=DB_CONFIG.get('audit_db'))
    return g.db

def validate_payload(payload, required):
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f'Missing required fields: {missing}')
    con = get_db()
    payload['dialect'] = payload.get('dialect', con.db)
    return payload

@app.route('/query/select', methods=['POST'])
def select_query():
    payload = request.get_json()
    payload = validate_payload(payload, ['table'])
    con = get_db()
    sql, params = json_select(payload, con.db)
    if not payload.get('execute', False):
        return jsonify({'sql': sql, 'params': params})
    df = con.fetch_df(sql, params)
    return jsonify({'result': df.to_dict(orient='records')})

@app.teardown_appcontext
def close_db(error):
    if 'db' in g:
        g.pop('db').engine.dispose()
```

#### JSON Payload Examples

1. **SELECT Query**:

```json
{
    "table": "users",
    "fields": ["id", "name"],
    "condition": ["age > :age"],
    "age": 25,
    "execute": true
}
```

**Response**:

```json
{
    "result": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
}
```

2. **INSERT Query**:

```json
{
    "rows": [
        {"table": "users", "insertValues": {"id": 3, "name": "Charlie", "age": 35}}
    ],
    "execute": true
}
```

**Response**:

```json
{
    "status": "success",
    "rows_affected": 1
}
```

3. **Create Table**:

```json
{
    "table": "users",
    "source": [
        {"id": 1, "name": "Alice", "age": 25}
    ],
    "pk": ["id"],
    "if_not_exists": true,
    "execute": true
}
```

**Response**:

```json
{
    "status": "success"
}
```

### Direct Usage (Scripting)

For non-Flask use (e.g., scripts, testing):

```python
from sqlutilz.sql_builder import SQLBuilder, create_table
from sqlutilz.sqlbbw import SqlCon
import pandas as pd

# Initialize
con = SqlCon('sqlite:///:memory:')
builder = SQLBuilder(con.db)  # Ensure dialect consistency

# Create table
df = pd.DataFrame([{'id': 1, 'name': 'Alice', 'age': 25}])
sql = create_table('users', df, pk=['id'], dialect=con.db)
con.execute(sql)

# Insert data
sql, params = builder.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
con.execute(sql, params)

# Query data
sql, params = builder.select('users', fields=['id', 'name'], conditions=['age > :age'])
df = con.fetch_df(sql, {'age': 25})
print(df)  # DataFrame: id, name
```

### DataFrame Operations

For bulk operations with Pandas DataFrames:

```python
from sqlutilz.sql_builder import df_sql
from sqlutilz.sqlbbw import cast_df

df = pd.DataFrame([
    {'id': 3, 'name': 'Charlie', 'age': 35},
    {'id': 4, 'name': 'Dave', 'age': 40}
])
df = cast_df(df)  # Coerce types
queries = df_sql(df, table='users', columns=['id', 'name', 'age'], dialect=con.db, ops=['insert'])
for sql, params in queries[0]:  # Process each query
    con.execute(sql, params)
```

## Security

- **Input Sanitization**: Table/column names are validated with regex.
- **Parameterized Queries**: Uses placeholders to prevent SQL injection.
- **Payload Validation**: Ensures required fields and dialect consistency in Flask endpoints.
- **Auditing**: Logs queries, parameters, and outcomes to `audit_db`.

## Performance Tips

- **Bulk Inserts**: Use `insert_bulk` or `df_sql` with `multi_row=True` for large datasets.
- **Chunking**: For DataFrames over 10,000 rows, implement chunking in `df_sql`.
- **Connection Management**: Use Flask’s `g.db` to reuse SqlCon instances.

## Error Handling

- **Client Errors**: HTTP 400 for invalid payloads or dialect mismatches.
- **Server Errors**: HTTP 500 for execution failures, logged for debugging.
- **Retries**: `sqlbbw.audit.retry` handles transient DB errors.

## Testing

Create unit tests with pytest:

```python
import pytest
from sqlutilz.sql_builder import SQLBuilder
from sqlutilz.sqlbbw import SqlCon

def test_select():
    con = SqlCon('sqlite:///:memory:')
    builder = SQLBuilder(con.db)
    sql, params = builder.select('users', conditions=['age > :age'])
    assert '?' in sql  # SQLite positional placeholders
    assert params == {'age': None}
```

## Troubleshooting

- **Dialect Mismatch**: Ensure JSON dialect matches `DB_CONFIG['conn_str']` or omit it (defaults to `con.db`).
- **Missing Tables**: Validate schemas with `SqlCon.validate_schema`.
- **Performance Issues**: Use bulk operations and monitor connection pooling.

## Limitations

- **Not an ORM**: No object-relational mapping or relationship management.
- **Pandas Dependency**: Required for DataFrame operations; optional support is possible.
- **Dialect Support**: Limited to SQLite, PostgreSQL, MySQL, MSSQL, Oracle.

## Further Information

- **Source**: E:\\PyCode\\_WAPPS\\sqlutilz
- **Support**: Contact the developer or refer to module docstrings.
```

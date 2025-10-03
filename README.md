# \## Overview

# 

# sqlutilz is a lightweight Python package designed for generating and executing SQL queries in a headless backend, optimized for Flask applications. It provides a secure, modular framework for building dialect-specific SQL queries from JSON payloads or Pandas DataFrames and executing them with robust connection management, type coercion, auditing, and fallback support. The package is ideal for developers needing a flexible, non-ORM solution for database interactions in web applications or scripts.

# 

# The package is split into two subpackages:

# 

# \-   sql\\\_builder: Generates SQL queries and parameters for SELECT, INSERT, UPDATE, DELETE, UPSERT, and CREATE TABLE operations.

# &nbsp;   

# \-   sqlbbw: Manages query execution, type coercion, auditing, and fallbacks using the SqlCon class.

# &nbsp;   

# 

# \*\*Key Use Cases\*\*:

# 

# \-   Frontend-driven query generation via JSON payloads.

# &nbsp;   

# \-   DataFrame-based database operations.

# &nbsp;   

# \-   Schema creation and validation.

# &nbsp;   

# \-   Secure, audited query execution across multiple SQL dialects (SQLite, PostgreSQL, MySQL, MSSQL, Oracle).

# &nbsp;   

# 

# \*\*Note\*\*: sqlutilz is not a full ORM. It focuses on query generation and execution without object-relational mapping, keeping it lightweight and SQL-centric.

# 

# \## Package Structure

# 

# plain

# 

# CollapseCopy

# 

# &nbsp;   sqlutilz/

# &nbsp;   ├── app.py                # Flask application for HTTP endpoints

# &nbsp;   ├── config.py             # Database connection configuration

# &nbsp;   ├── sql\_builder/          # Query generation modules

# &nbsp;   │   ├── \_\_init\_\_.py       # Exports SQLBuilder, Condition, etc.

# &nbsp;   │   ├── adapt\_sql.py      # Adjusts SQL/parameters for dialects

# &nbsp;   │   ├── conditions.py     # Parses WHERE clause conditions

# &nbsp;   │   ├── df\_handler.py     # Handles DataFrame-based queries

# &nbsp;   │   ├── json\_handler.py   # Processes JSON payloads

# &nbsp;   │   ├── query\_builder.py  # Core SQL query generation

# &nbsp;   │   ├── table\_creator.py  # Generates CREATE TABLE statements

# &nbsp;   ├── sqlbbw/               # Execution and utility modules

# &nbsp;   │   ├── \_\_init\_\_.py       # Exports SqlCon, DataCorrector, etc.

# &nbsp;   │   ├── audit.py          # Logs query executions

# &nbsp;   │   ├── conn.py           # Database connection and execution (SqlCon)

# &nbsp;   │   ├── corrector.py      # Type coercion for data

# &nbsp;   │   ├── fallback.py       # Fallback execution with raw drivers

# &nbsp;   │   ├── mappings.py       # Dialect-specific mappings (placeholders, types)

# &nbsp;   │   ├── utils.py          # Schema utilities and helpers

# 

# \## Installation

# 

# Install required dependencies:

# 

# bash

# 

# CollapseCopy

# 

# &nbsp;   pip install sqlalchemy pandas

# 

# Optional drivers for specific databases:

# 

# \-   PostgreSQL: pip install psycopg2

# &nbsp;   

# \-   MySQL: pip install pymysql

# &nbsp;   

# \-   MSSQL: pip install pyodbc

# &nbsp;   

# \-   Oracle: pip install cx\\\_Oracle

# &nbsp;   

# 

# For lightweight deployments, Pandas can be optional by checking DB\\\_IMPORTS\\\_AVAILABLE in sqlbbw.corrector.

# 

# \## Configuration

# 

# Create a config.py to define database connection details:

# 

# python

# 

# CollapseCopy

# 

# &nbsp;   # config.py

# &nbsp;   DB\_CONFIG = {

# &nbsp;       'conn\_str': 'sqlite:///:memory:',  # Example: 'postgresql://user:pass@localhost/db'

# &nbsp;       'audit\_db': 'sqlite:///audit.db'   # Optional audit database

# &nbsp;   }

# 

# \-   conn\\\_str: SQLAlchemy connection string for the primary database.

# &nbsp;   

# \-   audit\\\_db: Optional connection string for audit logging.

# &nbsp;   

# 

# \## Key Features

# 

# \-   \*\*Dialect Support\*\*: SQLite, PostgreSQL, MySQL, MSSQL, Oracle.

# &nbsp;   

# \-   \*\*Query Generation\*\*: Build SQL queries and parameters from JSON or DataFrames.

# &nbsp;   

# \-   \*\*Execution\*\*: Secure execution with SqlCon, supporting SQLAlchemy and raw driver fallbacks.

# &nbsp;   

# \-   \*\*Security\*\*: Sanitizes table/column names (re.match(r'^\\\[\\\\w\\]+$', name)) and uses parametrized queries to prevent SQL injection.

# &nbsp;   

# \-   \*\*Type Coercion\*\*: Automatically adjusts data types to match DB schema (sqlbbw.corrector).

# &nbsp;   

# \-   \*\*Auditing\*\*: Logs all executed queries to audit\\\_db (sqlbbw.audit).

# &nbsp;   

# \-   \*\*Schema Management\*\*: Create tables with primary/foreign keys (table\\\_creator.py).

# &nbsp;   

# \-   \*\*Stateless Design\*\*: Ideal for Flask-based headless backends.

# &nbsp;   

# 

# \## General Usage

# 

# \### Flask Integration

# 

# The primary use case is a Flask backend exposing endpoints for query generation and execution. Below is an example setup:

# 

# python

# 

# CollapseCopy

# 

# &nbsp;   # app.py (simplified)

# &nbsp;   from flask import Flask, request, jsonify, g

# &nbsp;   from sqlutilz.sql\_builder import json\_select

# &nbsp;   from sqlutilz.sqlbbw import SqlCon

# &nbsp;   from config import DB\_CONFIG

# &nbsp;   

# &nbsp;   app = Flask(\_\_name\_\_)

# &nbsp;   

# &nbsp;   def get\_db():

# &nbsp;       if 'db' not in g:

# &nbsp;           g.db = SqlCon(DB\_CONFIG\['conn\_str'], audit\_db=DB\_CONFIG.get('audit\_db'))

# &nbsp;       return g.db

# &nbsp;   

# &nbsp;   def validate\_payload(payload, required):

# &nbsp;       missing = \[k for k in required if k not in payload]

# &nbsp;       if missing:

# &nbsp;           raise ValueError(f'Missing required fields: {missing}')

# &nbsp;       con = get\_db()

# &nbsp;       payload\['dialect'] = payload.get('dialect', con.db)

# &nbsp;       return payload

# &nbsp;   

# &nbsp;   @app.route('/query/select', methods=\['POST'])

# &nbsp;   def select\_query():

# &nbsp;       payload = request.get\_json()

# &nbsp;       payload = validate\_payload(payload, \['table'])

# &nbsp;       con = get\_db()

# &nbsp;       sql, params = json\_select(payload, con.db)

# &nbsp;       if not payload.get('execute', False):

# &nbsp;           return jsonify({'sql': sql, 'params': params})

# &nbsp;       df = con.fetch\_df(sql, params)

# &nbsp;       return jsonify({'result': df.to\_dict(orient='records')})

# &nbsp;   

# &nbsp;   @app.teardown\_appcontext

# &nbsp;   def close\_db(error):

# &nbsp;       if 'db' in g:

# &nbsp;           g.pop('db').engine.dispose()

# 

# \#### JSON Payload Examples

# 

# 1\.  \*\*SELECT Query\*\*:

# &nbsp;   

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "table": "users",

# &nbsp;       "fields": \["id", "name"],

# &nbsp;       "condition": \["age > :age"],

# &nbsp;       "age": 25,

# &nbsp;       "execute": true

# &nbsp;   }

# 

# \*\*Response\*\*:

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "result": \[

# &nbsp;           {"id": 1, "name": "Alice"},

# &nbsp;           {"id": 2, "name": "Bob"}

# &nbsp;       ]

# &nbsp;   }

# 

# 2\.  \*\*INSERT Query\*\*:

# &nbsp;   

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "rows": \[

# &nbsp;           {"table": "users", "insertValues": {"id": 3, "name": "Charlie", "age": 35}}

# &nbsp;       ],

# &nbsp;       "execute": true

# &nbsp;   }

# 

# \*\*Response\*\*:

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "status": "success",

# &nbsp;       "rows\_affected": 1

# &nbsp;   }

# 

# 3\.  \*\*Create Table\*\*:

# &nbsp;   

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "table": "users",

# &nbsp;       "source": \[

# &nbsp;           {"id": 1, "name": "Alice", "age": 25}

# &nbsp;       ],

# &nbsp;       "pk": \["id"],

# &nbsp;       "if\_not\_exists": true,

# &nbsp;       "execute": true

# &nbsp;   }

# 

# \*\*Response\*\*:

# 

# json

# 

# CollapseCopy

# 

# &nbsp;   {

# &nbsp;       "status": "success"

# &nbsp;   }

# 

# \### Direct Usage (Scripting)

# 

# For non-Flask use (e.g., scripts, testing):

# 

# python

# 

# CollapseCopy

# 

# &nbsp;   from sqlutilz.sql\_builder import SQLBuilder, create\_table

# &nbsp;   from sqlutilz.sqlbbw import SqlCon

# &nbsp;   import pandas as pd

# &nbsp;   

# &nbsp;   # Initialize

# &nbsp;   con = SqlCon('sqlite:///:memory:')

# &nbsp;   builder = SQLBuilder(con.db)  # Ensure dialect consistency

# &nbsp;   

# &nbsp;   # Create table

# &nbsp;   df = pd.DataFrame(\[{'id': 1, 'name': 'Alice', 'age': 25}])

# &nbsp;   sql = create\_table('users', df, pk=\['id'], dialect=con.db)

# &nbsp;   con.execute(sql)

# &nbsp;   

# &nbsp;   # Insert data

# &nbsp;   sql, params = builder.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})

# &nbsp;   con.execute(sql, params)

# &nbsp;   

# &nbsp;   # Query data

# &nbsp;   sql, params = builder.select('users', fields=\['id', 'name'], conditions=\['age > :age'])

# &nbsp;   df = con.fetch\_df(sql, {'age': 25})

# &nbsp;   print(df)  # DataFrame: id, name

# 

# \### DataFrame Operations

# 

# For bulk operations with Pandas DataFrames:

# 

# python

# 

# CollapseCopy

# 

# &nbsp;   from sqlutilz.sql\_builder import df\_sql

# &nbsp;   from sqlutilz.sqlbbw import cast\_df

# &nbsp;   

# &nbsp;   df = pd.DataFrame(\[

# &nbsp;       {'id': 3, 'name': 'Charlie', 'age': 35},

# &nbsp;       {'id': 4, 'name': 'Dave', 'age': 40}

# &nbsp;   ])

# &nbsp;   df = cast\_df(df)  # Coerce types

# &nbsp;   queries = df\_sql(df, table='users', columns=\['id', 'name', 'age'], dialect=con.db, ops=\['insert'])

# &nbsp;   for sql, params in queries\[0]:  # Process each query

# &nbsp;       con.execute(sql, params)

# 

# \## Security

# 

# \-   \*\*Input Sanitization\*\*: Table/column names validated with re.match(r'^\\\[\\\\w\\]+$', name).

# &nbsp;   

# \-   \*\*Parametrized Queries\*\*: Uses placeholders (:param, ?) to prevent SQL injection.

# &nbsp;   

# \-   \*\*Payload Validation\*\*: Ensures required fields and dialect consistency in Flask endpoints.

# &nbsp;   

# \-   \*\*Auditing\*\*: Logs queries, parameters, and outcomes to audit\\\_db.

# &nbsp;   

# 

# \## Performance Tips

# 

# \-   \*\*Bulk Inserts\*\*: Use insert\\\_bulk or df\\\_sql with multi\\\_row=True for large datasets.

# &nbsp;   

# \-   \*\*Chunking\*\*: For DataFrames >10,000 rows, implement chunking in df\\\_sql.

# &nbsp;   

# \-   \*\*Connection Management\*\*: Use Flask’s g.db to reuse SqlCon instances.

# &nbsp;   

# 

# \## Error Handling

# 

# \-   \*\*Client Errors\*\*: HTTP 400 for invalid payloads or dialect mismatches.

# &nbsp;   

# \-   \*\*Server Errors\*\*: HTTP 500 for execution failures, logged for debugging.

# &nbsp;   

# \-   \*\*Retries\*\*: sqlbbw.audit.retry handles transient DB errors.

# &nbsp;   

# 

# \## Testing

# 

# Create unit tests with pytest:

# 

# python

# 

# CollapseCopy

# 

# &nbsp;   import pytest

# &nbsp;   from sqlutilz.sql\_builder import SQLBuilder

# &nbsp;   from sqlutilz.sqlbbw import SqlCon

# &nbsp;   

# &nbsp;   def test\_select():

# &nbsp;       con = SqlCon('sqlite:///:memory:')

# &nbsp;       builder = SQLBuilder(con.db)

# &nbsp;       sql, params = builder.select('users', conditions=\['age > :age'])

# &nbsp;       assert '?' in sql  # SQLite positional placeholders

# &nbsp;       assert params == {'age': None}

# 

# \## Troubleshooting

# 

# \-   \*\*Dialect Mismatch\*\*: Ensure JSON dialect matches DB\\\_CONFIG\\\['conn\\\_str'\\] or omit it (defaults to con.db).

# &nbsp;   

# \-   \*\*Missing Tables\*\*: Validate schemas with SqlCon.validate\\\_schema.

# &nbsp;   

# \-   \*\*Performance Issues\*\*: Use bulk operations and monitor connection pooling.

# &nbsp;   

# 

# \## Limitations

# 

# \-   \*\*Not an ORM\*\*: No object-relational mapping or relationship management.

# &nbsp;   

# \-   \*\*Pandas Dependency\*\*: Required for DataFrame operations; optional support possible.

# &nbsp;   

# \-   \*\*Dialect Support\*\*: Limited to SQLite, PostgreSQL, MySQL, MSSQL, Oracle.

# &nbsp;   

# 

# \## Further Information

# 

# \-   \*\*Source\*\*: E:\\\\PyCode\\\\\\\_WAPPS\\\\sqlutilz

# &nbsp;   

# \-   \*\*Support\*\*: Contact the developer or refer to module docstrings.

# 




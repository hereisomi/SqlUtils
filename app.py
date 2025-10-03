"""Flask app for sqlutilz query generation and execution."""

from flask import Flask, request, jsonify, Response, g
from sqlutilz.sql_builder import json_select, json_insert, json_update, json_delete, df_sql, create_table
from sqlutilz.sqlbbw import SqlCon, cast_df
import pandas as pd
from typing import Dict, Any, List
import logging
from config import DB_CONFIG

app = Flask(__name__)
logger = logging.getLogger(__name__)

def get_db():
    """Get or create SqlCon instance in Flask context."""
    if 'db' not in g:
        g.db = SqlCon(DB_CONFIG['conn_str'], audit_db=DB_CONFIG.get('audit_db'))
    return g.db

def validate_payload(payload: Dict[str, Any], required: List[str]) -> Dict[str, Any]:
    """Validate JSON payload and ensure dialect matches SqlCon.db."""
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f'Missing required fields: {missing}')
    con = get_db()
    if 'dialect' not in payload:
        payload['dialect'] = con.db
        logger.info(f"Set default dialect to {con.db}")
    elif payload['dialect'].lower() != con.db:
        logger.warning(f"Overriding dialect from {payload['dialect']} to {con.db}")
        payload['dialect'] = con.db
    return payload

@app.errorhandler(ValueError)
def handle_value_error(e: ValueError) -> Response:
    """Handle ValueError with 400 response."""
    return jsonify({'error': str(e)}), 400

@app.errorhandler(Exception)
def handle_general_error(e: Exception) -> Response:
    """Handle unexpected errors with 500 response."""
    logger.error(f'Server error: {e}')
    return jsonify({'error': 'Internal server error'}), 500

@app.route('/query/select', methods=['POST'])
def select_query():
    """Generate or execute SELECT query from JSON payload."""
    payload = request.get_json()
    payload = validate_payload(payload, ['table'])
    con = get_db()
    execute = payload.get('execute', False)
    sql, params = json_select(payload, con.db)
    if not execute:
        return jsonify({'sql': sql, 'params': params})
    try:
        df = con.fetch_df(sql, params)
        return jsonify({'result': df.to_dict(orient='records')})
    except Exception as e:
        raise ValueError(f'Query execution failed: {e}')

@app.route('/query/insert', methods=['POST'])
def insert_query():
    """Generate or execute INSERT query from JSON payload."""
    payload = request.get_json()
    payload = validate_payload(payload, ['rows'])
    con = get_db()
    execute = payload.get('execute', False)
    queries = json_insert(payload['rows'], con.db, multi_row=True)
    if not execute:
        return jsonify([{'sql': sql, 'params': params} for sql, params in queries])
    try:
        for sql, params in queries:
            con.execute(sql, params)
        return jsonify({'status': 'success', 'rows_affected': len(payload['rows'])})
    except Exception as e:
        raise ValueError(f'Insert execution failed: {e}')

@app.route('/query/update', methods=['POST'])
def update_query():
    """Generate or execute UPDATE query from JSON payload."""
    payload = request.get_json()
    payload = validate_payload(payload, ['table', 'updateValues'])
    con = get_db()
    execute = payload.get('execute', False)
    sql, params = json_update(payload, con.db)
    if not execute:
        return jsonify({'sql': sql, 'params': params})
    try:
        con.execute(sql, params)
        return jsonify({'status': 'success'})
    except Exception as e:
        raise ValueError(f'Update execution failed: {e}')

@app.route('/query/delete', methods=['POST'])
def delete_query():
    """Generate or execute DELETE query from JSON payload."""
    payload = request.get_json()
    payload = validate_payload(payload, ['table'])
    con = get_db()
    execute = payload.get('execute', False)
    sql, params = json_delete(payload, con.db)
    if not execute:
        return jsonify({'sql': sql, 'params': params})
    try:
        con.execute(sql, params)
        return jsonify({'status': 'success'})
    except Exception as e:
        raise ValueError(f'Delete execution failed: {e}')

@app.route('/query/dataframe', methods=['POST'])
def dataframe_query():
    """Generate or execute DataFrame-based queries."""
    payload = request.get_json()
    payload = validate_payload(payload, ['data', 'table', 'columns'])
    con = get_db()
    execute = payload.get('execute', False)
    df = pd.DataFrame(payload['data'])
    df = cast_df(df)
    queries = df_sql(
        df,
        table=payload['table'],
        columns=payload['columns'],
        expression=payload.get('expression'),
        dialect=con.db,
        pk=payload.get('pk'),
        use_upsert=payload.get('use_upsert', False),
        ops=payload.get('ops', ['select', 'update', 'insert', 'delete'])
    )
    if not execute:
        return jsonify([
            [{'sql': sql, 'params': params} for sql, params in row_queries]
            for row_queries in queries
        ])
    try:
        results = []
        for row_queries in queries:
            row_results = []
            for sql, params in row_queries:
                if 'select' in payload.get('ops', []):
                    df_result = con.fetch_df(sql, params)
                    row_results.append(df_result.to_dict(orient='records'))
                else:
                    con.execute(sql, params)
                    row_results.append({'status': 'success'})
            results.append(row_results)
        return jsonify({'results': results})
    except Exception as e:
        raise ValueError(f'DataFrame query execution failed: {e}')

@app.route('/table/create', methods=['POST'])
def create_table_endpoint():
    """Generate or execute CREATE TABLE statement."""
    payload = request.get_json()
    payload = validate_payload(payload, ['table', 'source'])
    con = get_db()
    execute = payload.get('execute', False)
    source = pd.DataFrame(payload['source']) if isinstance(payload['source'], list) else payload['source']
    sql = create_table(
        name=payload['table'],
        source=source,
        pk=payload.get('pk'),
        fk=payload.get('fk'),
        dialect=con.db,
        if_not_exists=payload.get('if_not_exists', True)
    )
    if not execute:
        return jsonify({'sql': sql})
    try:
        con.execute(sql)
        return jsonify({'status': 'success'})
    except Exception as e:
        raise ValueError(f'Table creation failed: {e}')

@app.teardown_appcontext
def close_db(error):
    """Close SqlCon instance on app context teardown."""
    if 'db' in g:
        g.pop('db').engine.dispose()

if __name__ == '__main__':
    app.run(debug=True)
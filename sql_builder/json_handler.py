"""JSON payload handling for SQL queries."""

from typing import Dict, List, Tuple, Any
from .query_builder import SQLBuilder
from .adapt_sql import adapt_sql

def json_select(payload: Dict[str, Any], dialect: str = 'default') -> Tuple[str, Any]:
    """Generate SELECT query from JSON payload."""
    builder = SQLBuilder(dialect)  # Use provided dialect (should be SqlCon.db)
    required = ['table']
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f'Missing required fields: {missing}')
    order_by = [(o['field'], o.get('direction', 'ASC')) for o in payload.get('orderby', [])]
    group_by = payload.get('groupby')
    having = payload.get('having')
    having_expr = payload.get('having_expression')
    sql, params = builder.select(
        table=payload['table'],
        columns=payload.get('fields', '*'),
        conditions=payload.get('condition'),
        expression=payload.get('expression'),
        order_by=order_by,
        limit=payload.get('limit'),
        offset=payload.get('start'),
        group_by=group_by,
        having=having,
        having_expr=having_expr
    )
    return adapt_sql(sql, params, dialect)

def json_insert(rows: List[Dict[str, Any]], dialect: str = 'default', multi_row: bool = True) -> List[Tuple[str, Any]]:
    """Generate INSERT queries from JSON payload."""
    if not rows:
        raise ValueError('No rows provided for insert')
    builder = SQLBuilder(dialect)  # Use provided dialect (should be SqlCon.db)
    same_table = len(set(r['table'] for r in rows)) == 1
    if multi_row and same_table:
        table = rows[0]['table']
        data_list = [r['insertValues'] for r in rows]
        sql, params = builder.insert_bulk(table, data_list)
        return [adapt_sql(sql, params, dialect)]
    else:
        return [adapt_sql(*builder.insert(r['table'], r['insertValues']), dialect) for r in rows]

def json_update(payload: Dict[str, Any], dialect: str = 'default') -> Tuple[str, Any]:
    """Generate UPDATE query from JSON payload."""
    required = ['table', 'updateValues']
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f'Missing required fields: {missing}')
    builder = SQLBuilder(dialect)  # Use provided dialect (should be SqlCon.db)
    if 'limit' in payload:
        raise NotImplementedError('LIMIT in UPDATE not supported')
    sql, params = builder.update(
        table=payload['table'],
        data=payload['updateValues'],
        conditions=payload.get('condition'),
        expression=payload.get('expression')
    )
    return adapt_sql(sql, params, dialect)

def json_delete(payload: Dict[str, Any], dialect: str = 'default') -> Tuple[str, Any]:
    """Generate DELETE query from JSON payload."""
    required = ['table']
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f'Missing required fields: {missing}')
    builder = SQLBuilder(dialect)  # Use provided dialect (should be SqlCon.db)
    sql, params = builder.delete(
        table=payload['table'],
        conditions=payload.get('condition'),
        expression=payload.get('expression')
    )
    return adapt_sql(sql, params, dialect)
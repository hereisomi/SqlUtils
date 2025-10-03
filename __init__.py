"""SQL utilities package for query building and execution."""

from sqlbbw import (
    SqlCon, DataCorrector, cast_df, Audit, audited, retry,
    FallbackExecutor, create_table_schema, map_alter_filter_column,
    map_alter_forced, insert_batch, upsert, dtype_map, coercers,
    patterns
)
from sql_builder import (
    SQLBuilder, Condition, df_sql, json_select, json_insert,
    json_update, json_delete, create_table, adapt_sql
)

# Re-export mappings from sqlbbw.mappings to avoid direct import
from sqlbbw.mappings import placeholders, quote_chars, valid_operators

__all__ = [
    'SqlCon', 'DataCorrector', 'cast_df', 'Audit', 'audited', 'retry',
    'FallbackExecutor', 'create_table_schema', 'map_alter_filter_column',
    'map_alter_forced', 'insert_batch', 'upsert', 'dtype_map', 'coercers',
    'patterns', 'placeholders', 'quote_chars', 'valid_operators',
    'SQLBuilder', 'Condition', 'df_sql', 'json_select', 'json_insert',
    'json_update', 'json_delete', 'create_table', 'adapt_sql'
]
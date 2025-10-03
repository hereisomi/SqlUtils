"""Database type mappings and regex patterns for type inference."""

from typing import Dict, List

# Configurable default VARCHAR length
default_varchar_length = 255

# Database-specific type mappings (Pandas dtype -> SQL type)
dtype_map = {
    'oracle': {
        'object': f'VARCHAR2({default_varchar_length})', 'string': f'VARCHAR2({default_varchar_length})',
        'category': f'VARCHAR2({default_varchar_length})', 'int64': 'NUMBER', 'Int64': 'NUMBER',
        'float32': 'BINARY_FLOAT', 'float64': 'BINARY_DOUBLE', 'bool': 'NUMBER(1,0)',
        'datetime64[ns]': 'DATE', 'timedelta[ns]': 'INTERVAL DAY TO SECOND',
        'int': 'NUMBER', 'float': 'BINARY_DOUBLE', 'str': f'VARCHAR2({default_varchar_length})',
        'datetime': 'DATE', 'timedelta': 'INTERVAL DAY TO SECOND'
    },
    'mssql': {
        'object': f'VARCHAR({default_varchar_length})', 'string': f'VARCHAR({default_varchar_length})',
        'category': f'VARCHAR({default_varchar_length})', 'int64': 'BIGINT', 'Int64': 'BIGINT',
        'float32': 'REAL', 'float64': 'FLOAT', 'bool': 'BIT', 'datetime64[ns]': 'DATETIME',
        'timedelta[ns]': 'BIGINT', 'int': 'BIGINT', 'float': 'FLOAT', 'str': f'VARCHAR({default_varchar_length})',
        'datetime': 'DATETIME', 'timedelta': 'BIGINT'
    },
    'mysql': {
        'object': f'VARCHAR({default_varchar_length})', 'string': f'VARCHAR({default_varchar_length})',
        'category': f'VARCHAR({default_varchar_length})', 'int64': 'BIGINT', 'Int64': 'BIGINT',
        'float32': 'FLOAT', 'float64': 'DOUBLE', 'bool': 'TINYINT(1)', 'datetime64[ns]': 'DATETIME',
        'timedelta[ns]': 'BIGINT', 'int': 'BIGINT', 'float': 'DOUBLE', 'str': f'VARCHAR({default_varchar_length})',
        'datetime': 'DATETIME', 'timedelta': 'BIGINT'
    },
    'postgresql': {
        'object': f'VARCHAR({default_varchar_length})', 'string': f'VARCHAR({default_varchar_length})',
        'category': f'VARCHAR({default_varchar_length})', 'int64': 'BIGINT', 'Int64': 'BIGINT',
        'float32': 'REAL', 'float64': 'DOUBLE PRECISION', 'bool': 'BOOLEAN', 'datetime64[ns]': 'TIMESTAMP',
        'timedelta[ns]': 'INTERVAL', 'int': 'BIGINT', 'float': 'DOUBLE PRECISION',
        'str': f'VARCHAR({default_varchar_length})', 'datetime': 'TIMESTAMP', 'timedelta': 'INTERVAL'
    },
    'sqlite': {
        'object': 'TEXT', 'string': 'TEXT', 'category': 'TEXT', 'int64': 'INTEGER', 'Int64': 'INTEGER',
        'float32': 'REAL', 'float64': 'REAL', 'bool': 'INTEGER', 'datetime64[ns]': 'TEXT',
        'timedelta[ns]': 'INTEGER', 'int': 'INTEGER', 'float': 'REAL', 'str': 'TEXT',
        'datetime': 'TEXT', 'timedelta': 'INTEGER'
    }
}

# DB-specific coercion functions (SQL type -> Pandas Series coercer)
coercers = {
    'oracle': {
        'VARCHAR2': lambda col, t_str: col.astype(str),
        'NUMBER': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('Int64'),
        'BINARY_FLOAT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float32'),
        'BINARY_DOUBLE': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float64'),
        'DATE': lambda col, t_str: pd.to_datetime(col, errors='coerce', dayfirst=True),
        'INTERVAL DAY TO SECOND': lambda col, t_str: pd.to_timedelta(col, errors='coerce'),
    },
    'mssql': {
        'VARCHAR': lambda col, t_str: col.astype(str),
        'BIGINT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('Int64'),
        'REAL': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float32'),
        'FLOAT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float64'),
        'BIT': lambda col, t_str: col.astype(bool, errors='ignore'),
        'DATETIME': lambda col, t_str: pd.to_datetime(col, errors='coerce'),
    },
    'mysql': {
        'VARCHAR': lambda col, t_str: col.astype(str),
        'BIGINT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('Int64'),
        'FLOAT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float32'),
        'DOUBLE': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float64'),
        'TINYINT': lambda col, t_str: col.astype(bool, errors='ignore') if '(1)' in t_str else pd.to_numeric(col, errors='coerce').astype('int8'),
        'DATETIME': lambda col, t_str: pd.to_datetime(col, errors='coerce'),
    },
    'postgresql': {
        'VARCHAR': lambda col, t_str: col.astype(str),
        'TEXT': lambda col, t_str: col.astype(str),
        'BIGINT': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('Int64'),
        'REAL': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float32'),
        'DOUBLE PRECISION': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float64'),
        'BOOLEAN': lambda col, t_str: col.astype(bool, errors='ignore'),
        'TIMESTAMP': lambda col, t_str: pd.to_datetime(col, errors='coerce'),
        'INTERVAL': lambda col, t_str: pd.to_timedelta(col, errors='coerce'),
    },
    'sqlite': {
        'TEXT': lambda col, t_str: col.astype(str),
        'INTEGER': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('Int64'),
        'REAL': lambda col, t_str: pd.to_numeric(col, errors='coerce').astype('float64'),
    }
}

# Regex patterns for type inference
patterns = {
    'epochtime': [r'^\d{10}(\.\d+)?$', r'^\d{13}$', r'^\d{16}$'],  # Added ms, us
    'date': [r'^\d{4}-\d{2}-\d{2}$', r'^\d{2}/\d{2}/\d{4}$', r'^\d{2}-\d{2}-\d{4}$', r'^\d{1,2}-\w{3}-\d{4}$', r'^\w{3}\s+\d{1,2},?\s+\d{4}$'],
    'time': [r'^\d{1,2}:\d{2}(:\d{2})?(\s*[AP]M)?$', r'^\d{2}:\d{2}:\d{2}\.\d{3}$'],
    'timestamp': [r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(\.\d+)?$', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'],
    'integer': [r'^-?\d+$'],
    'float': [r'^-?\d*\.\d+$', r'^-?\d+\.?\d*[eE][+-]?\d+$'],
    'boolean': [r'(?i)^(true|false|yes|no|y|n|0|1)$'],
    'uuid': [r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'],
    'email': [r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'],
    'url': [r'^https?://[^\s]+$'],
    'json': [r'^\{.*\}$', r'^\[.*\]$']
}
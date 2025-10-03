"""Table creation utilities."""

import pandas as pd
from typing import Dict, List, Optional, Union
from .mappings import dtype_map, quote_chars

def create_table(name: str, source: Union[pd.DataFrame, Dict[str, str]], pk: Optional[List[str]] = None,
                 fk: Optional[List[Dict[str, Optional[str]]]] = None, dialect: str = 'default',
                 if_not_exists: bool = True) -> str:
    """Generate CREATE TABLE SQL."""
    if not re.match(r'^[\w]+$', name):
        raise ValueError(f'Invalid table name: {name}')
    quote_char = quote_chars.get(dialect, '"')
    cols = []
    if isinstance(source, pd.DataFrame):
        cols = [f'{quote_char}{c}{quote_char} {dtype_map.get(dialect, {}).get(str(t).lower(), "TEXT")}' for c, t in source.dtypes.items()]
    elif isinstance(source, dict):
        cols = [f'{quote_char}{c}{quote_char} {t}' for c, t in source.items()]
    else:
        raise TypeError('Source must be DataFrame or dict')
    cons = []
    if pk:
        if not all(re.match(r'^[\w]+$', k) for k in pk):
            raise ValueError(f'Invalid primary key columns: {pk}')
        cons.append(f'PRIMARY KEY ({", ".join([f"{quote_char}{k}{quote_char}" for k in pk])})')
    if fk:
        for f in fk:
            col = f['column']
            ref_tbl = f['ref_table']
            ref_col = f.get('ref_column', 'id')
            if not all(re.match(r'^[\w]+$', x) for x in (col, ref_tbl, ref_col)):
                raise ValueError(f'Invalid foreign key: {f}')
            od, ou = f.get('on_delete', ''), f.get('on_update', '')
            fk_sql = f'FOREIGN KEY ({quote_char}{col}{quote_char}) REFERENCES {quote_char}{ref_tbl}{quote_char} ({quote_char}{ref_col}{quote_char})'
            if od:
                fk_sql += f' ON DELETE {od.upper()}'
            if ou:
                fk_sql += f' ON UPDATE {ou.upper()}'
            cons.append(fk_sql)
    ine = 'IF NOT EXISTS ' if if_not_exists and dialect != 'oracle' else ''
    return f'CREATE TABLE {ine}{quote_char}{name}{quote_char} (\n  ' + ',\n  '.join(cols + cons) + '\n)'
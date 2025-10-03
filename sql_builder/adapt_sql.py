"""Dialect-specific SQL and parameter adaptation."""

import re
from typing import Dict, Any, Union, Tuple, List

def adapt_sql(sql: str, params: Dict[str, Any], dialect: str) -> Tuple[str, Union[Dict[str, Any], List[Any]]]:
    """Adapt SQL and parameters for specific dialect."""
    d = dialect.lower()
    if d == 'oracle':
        return sql, params
    if d == 'mssql':
        sql = re.sub(r':(\w+)', r'@\1', sql)
        return sql, params
    if d in ('postgres', 'postgresql'):
        sql = re.sub(r':(\w+)', r'%(\1)s', sql)
        def _patch(m: re.Match):
            off = m.group(1)
            lim = m.group(2)
            lim_txt = f' LIMIT {lim}' if lim else ''
            off_txt = f' OFFSET {off}' if off else ''
            return lim_txt + off_txt
        sql = re.sub(r'OFFSET (\d+) ROWS(?: FETCH NEXT (\d+) ROWS ONLY)?', _patch, sql, flags=re.I)
        return sql, params
    if d in ('mysql', 'sqlite'):
        sql = re.sub(r':(\w+)', '?', sql)
        def _patch_mysql(m):
            off = m.group(1)
            lim = m.group(2)
            return f' LIMIT {lim} OFFSET {off}' if lim else f' OFFSET {off}'
        sql = re.sub(r'OFFSET (\d+) ROWS(?: FETCH NEXT (\d+) ROWS ONLY)?', _patch_mysql, sql, flags=re.I)
        return sql, list(params.values())
    raise ValueError(f'Unknown dialect: {d}')
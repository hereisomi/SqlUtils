"""SQL query builder for CRUD operations across multiple dialects."""

import re
from typing import Any, Dict, List, Optional, Tuple, Union
import logging
from .conditions import Condition
from .mappings import placeholders, quote_chars

logger = logging.getLogger(__name__)

class SQLBuilder:
    """Builds SQL queries for SELECT, INSERT, UPDATE, DELETE, UPSERT."""
    def __init__(self, dialect: str = "default"):
        """Initialize with database dialect."""
        self.dialect = dialect.lower()
        self.ph = placeholders.get(self.dialect, ':')
        self.quote_char = quote_chars.get(self.dialect, '"')
        self._wrap_dt = self._get_dt_wrapper()

    def _get_dt_wrapper(self):
        """Get function to wrap datetime parameters."""
        def wrap(ph_val, op, val):
            if op in ('=', '!=', '<', '>', '<=', '>=') and isinstance(val, str) and re.match(r'^\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?$', val):
                if self.dialect == 'oracle':
                    return f"TO_DATE({ph_val}, 'YYYY-MM-DD HH24:MI:SS')"
                if self.dialect == 'mssql':
                    return f"CAST({ph_val} AS DATETIME2)"
            return ph_val
        return wrap

    def build_where(self, conditions: List[Any], expression: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """Build WHERE clause from conditions."""
        if not conditions:
            return '', {}
        objs = [Condition.from_input(c) for c in (conditions if isinstance(conditions, list) else [conditions])]
        sql_parts = []
        params = {}
        for c in objs:
            frag, p = c.to_sql(self.dialect, self.ph, self.quote_char, self._wrap_dt)
            sql_parts.append(frag)
            params.update(p)
        if expression is None:
            where_sql = ' AND '.join(sql_parts)
        else:
            def repl(m):
                idx = int(m.group(0)) - 1
                if idx < 0 or idx >= len(sql_parts):
                    raise ValueError(f'Invalid index in expression: {m.group(0)}')
                return sql_parts[idx]
            where_sql = re.sub(r'\b\d+\b', repl, expression)
            if not re.match(r'^[\d\sANDOR()]+$', expression):
                raise ValueError(f'Invalid characters in expression: {expression}')
        return where_sql, params

    def select(self, table: str, columns: Union[str, List[str]] = '*', conditions: Optional[List[Any]] = None,
               expression: Optional[str] = None, order_by: Optional[List[Tuple[str, str]]] = None,
               limit: Optional[int] = None, offset: Optional[int] = None, group_by: Optional[List[str]] = None,
               having: Optional[List[Any]] = None, having_expr: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate SELECT query."""
        if not re.match(r'^[\w]+$', table):
            raise ValueError(f'Invalid table name: {table}')
        cols = ', '.join([f'{self.quote_char}{c}{self.quote_char}' for c in columns]) if isinstance(columns, list) else columns
        sql = f'SELECT {cols} FROM {self.quote_char}{table}{self.quote_char}'
        params = {}
        if conditions:
            w_sql, w_params = self.build_where(conditions, expression)
            if w_sql:
                sql += f' WHERE {w_sql}'
                params.update(w_params)
        if group_by:
            if not all(re.match(r'^[\w]+$', g) for g in group_by):
                raise ValueError(f'Invalid group_by columns: {group_by}')
            sql += f' GROUP BY {", ".join([f"{self.quote_char}{g}{self.quote_char}" for g in group_by])}'
            if having:
                h_sql, h_params = self.build_where(having, having_expr)
                if h_sql:
                    sql += f' HAVING {h_sql}'
                    params.update(h_params)
        if order_by:
            if not all(re.match(r'^[\w]+$', f) and d.upper() in ('ASC', 'DESC') for f, d in order_by):
                raise ValueError(f'Invalid order_by: {order_by}')
            sql += f' ORDER BY {", ".join([f"{self.quote_char}{f}{self.quote_char} {d.upper()}" for f, d in order_by])}'
        if offset is not None or limit is not None:
            if self.dialect in ('postgres', 'sqlite'):
                sql += f' OFFSET {offset or 0}'
                if limit is not None:
                    sql += f' LIMIT {limit}'
            elif self.dialect == 'oracle':
                sql += f' OFFSET {offset or 0} ROWS'
                if limit is not None:
                    sql += f' FETCH NEXT {limit} ROWS ONLY'
            elif self.dialect == 'mssql':
                sql += f' OFFSET {offset or 0} ROWS'
                if limit is not None:
                    sql += f' FETCH NEXT {limit} ROWS ONLY'
            elif self.dialect == 'mysql':
                if limit is not None:
                    sql += f' LIMIT {limit}'
                if offset:
                    sql += f' OFFSET {offset}'
        return sql, params

    def insert(self, table: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Generate INSERT query for a single row."""
        return self.insert_bulk(table, [data])

    def insert_bulk(self, table: str, rows: List[Dict[str, Any]]) -> Tuple[str, Union[Dict[str, Any], List[Any]]]:
        """Generate INSERT query for multiple rows."""
        if not rows:
            raise ValueError('No rows provided for insert')
        if not re.match(r'^[\w]+$', table):
            raise ValueError(f'Invalid table name: {table}')
        keys = list(rows[0].keys())
        if not all(re.match(r'^[\w]+$', k) for k in keys):
            raise ValueError(f'Invalid column names: {keys}')
        params = {}
        if self.dialect in ('oracle', 'mssql'):
            into_lines = []
            for idx, row in enumerate(rows):
                phs = []
                for k in keys:
                    pname = f'{k}_{idx}'
                    params[pname] = row.get(k)
                    phs.append(f'{self.ph}{pname}')
                into_lines.append(f'INTO {self.quote_char}{table}{self.quote_char} ({", ".join([f"{self.quote_char}{k}{self.quote_char}" for k in keys])}) VALUES ({", ".join(phs)})')
            sql = 'INSERT ALL\n' + ' '.join(into_lines) + '\nSELECT * FROM DUAL' if self.dialect == 'oracle' else 'INSERT INTO'
        else:
            ph_rows = []
            for idx, row in enumerate(rows):
                phs = []
                for k in keys:
                    pname = f'{k}_{idx}'
                    params[pname] = row.get(k)
                    phs.append(f'{self.ph}{pname}')
                ph_rows.append(f'({", ".join(phs)})')
            sql = f'INSERT INTO {self.quote_char}{table}{self.quote_char} ({", ".join([f"{self.quote_char}{k}{self.quote_char}" for k in keys])}) VALUES {", ".join(ph_rows)}'
        if self.dialect in ('mysql', 'sqlite'):
            return sql, list(params.values())
        return sql, params

    def update(self, table: str, data: Dict[str, Any], conditions: Optional[List[Any]] = None,
               expression: Optional[str] = None, allow_full: bool = False) -> Tuple[str, Union[Dict[str, Any], List[Any]]]:
        """Generate UPDATE query."""
        if not re.match(r'^[\w]+$', table):
            raise ValueError(f'Invalid table name: {table}')
        sets = []
        params = {}
        for k, v in data.items():
            if not re.match(r'^[\w]+$', k):
                raise ValueError(f'Invalid column name: {k}')
            pname = f'set_{k}'
            sets.append(f'{self.quote_char}{k}{self.quote_char} = {self.ph}{pname}')
            params[pname] = v
        sql = f'UPDATE {self.quote_char}{table}{self.quote_char} SET {", ".join(sets)}'
        if conditions:
            w_sql, w_params = self.build_where(conditions, expression)
            if w_sql:
                sql += f' WHERE {w_sql}'
                params.update(w_params)
        elif not allow_full:
            raise ValueError('UPDATE without WHERE refused; use allow_full=True if intended')
        if self.dialect in ('mysql', 'sqlite'):
            return sql, list(params.values())
        return sql, params

    def delete(self, table: str, conditions: Optional[List[Any]] = None,
               expression: Optional[str] = None, allow_full: bool = False) -> Tuple[str, Union[Dict[str, Any], List[Any]]]:
        """Generate DELETE query."""
        if not re.match(r'^[\w]+$', table):
            raise ValueError(f'Invalid table name: {table}')
        sql = f'DELETE FROM {self.quote_char}{table}{self.quote_char}'
        params = {}
        if conditions:
            w_sql, w_params = self.build_where(conditions, expression)
            if w_sql:
                sql += f' WHERE {w_sql}'
                params.update(w_params)
        elif not allow_full:
            raise ValueError('DELETE without WHERE refused; use allow_full=True if intended')
        if self.dialect in ('mysql', 'sqlite'):
            return sql, list(params.values())
        return sql, params

    def upsert(self, table: str, data: Dict[str, Any], pk: List[str]) -> Tuple[str, Union[Dict[str, Any], List[Any]]]:
        """Generate UPSERT query."""
        if not re.match(r'^[\w]+$', table):
            raise ValueError(f'Invalid table name: {table}')
        cols = list(data.keys())
        if not all(re.match(r'^[\w]+$', c) for c in cols + pk):
            raise ValueError(f'Invalid column names: {cols + pk}')
        logger.warning('Ensure pk columns %s are unique in table %s', pk, table)
        vals = [f'{self.ph}{c}' for c in cols]
        non_pk = [c for c in cols if c not in pk]
        params = data.copy()
        if self.dialect == 'postgres':
            updates = [f'{self.quote_char}{c}{self.quote_char}=EXCLUDED.{self.quote_char}{c}{self.quote_char}' for c in non_pk]
            sql = f'INSERT INTO {self.quote_char}{table}{self.quote_char} ({", ".join([f"{self.quote_char}{c}{self.quote_char}" for c in cols])}) VALUES ({", ".join(vals)}) ON CONFLICT ({", ".join([f"{self.quote_char}{k}{self.quote_char}" for k in pk])}) DO UPDATE SET {", ".join(updates)}'
        elif self.dialect == 'sqlite':
            updates = [f'{self.quote_char}{c}{self.quote_char}=excluded.{self.quote_char}{c}{self.quote_char}' for c in non_pk]
            sql = f'INSERT INTO {self.quote_char}{table}{self.quote_char} ({", ".join([f"{self.quote_char}{c}{self.quote_char}" for c in cols])}) VALUES ({", ".join(vals)}) ON CONFLICT ({", ".join([f"{self.quote_char}{k}{self.quote_char}" for k in pk])}) DO UPDATE SET {", ".join(updates)}'
        elif self.dialect == 'mysql':
            updates = [f'{self.quote_char}{c}{self.quote_char}=VALUES({self.quote_char}{c}{self.quote_char})' for c in non_pk]
            sql = f'INSERT INTO {self.quote_char}{table}{self.quote_char} ({", ".join([f"{self.quote_char}{c}{self.quote_char}" for c in cols])}) VALUES ({", ".join(vals)}) ON DUPLICATE KEY UPDATE {", ".join(updates)}'
        elif self.dialect in ('mssql', 'oracle'):
            join_cond = ' AND '.join(f'T.{self.quote_char}{k}{self.quote_char}=S.{self.quote_char}{k}{self.quote_char}' for k in pk)
            upd = ', '.join(f'T.{self.quote_char}{c}{self.quote_char}=S.{self.quote_char}{c}{self.quote_char}' for c in non_pk)
            vals_clause = ', '.join(f'{self.ph}{c}' for c in cols)
            sql = f'MERGE INTO {self.quote_char}{table}{self.quote_char} T USING (VALUES ({vals_clause})) S ({", ".join([f"{self.quote_char}{c}{self.quote_char}" for c in cols])}) ON ({join_cond}) WHEN MATCHED THEN UPDATE SET {upd} WHEN NOT MATCHED THEN INSERT ({", ".join([f"{self.quote_char}{c}{self.quote_char}" for c in cols])}) VALUES ({", ".join([f"S.{self.quote_char}{c}{self.quote_char}" for c in cols])})'
        else:
            raise ValueError(f'Upsert not supported for dialect: {self.dialect}')
        if self.dialect in ('mysql', 'sqlite'):
            return sql, list(params.values())
        return sql, params
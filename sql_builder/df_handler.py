"""DataFrame-based SQL query generation."""

import pandas as pd
import re
from typing import List, Tuple, Union, Dict, Any
from .query_builder import SQLBuilder
from .corrector import cast_df

def df_sql(df: pd.DataFrame, table: str, columns: List[Union[str, Tuple[str, str]]], *, expression: Optional[str] = None,
           dialect: str = 'default', pk: Optional[List[str]] = None, use_upsert: bool = False,
           ops: List[str] = ['select', 'update', 'insert', 'delete']) -> List[Tuple[Tuple[str, Union[Dict[str, Any], List[Any]]]]]:
    """Generate SQL queries from DataFrame."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Input must be a pandas DataFrame')
    if df.empty:
        return []
    df = cast_df(df)  # Coerce types using sqlbbw.py's DataCorrector
    builder = SQLBuilder(dialect)
    out = []
    proj, cond_tpls = [], []
    for c in columns:
        if isinstance(c, tuple):
            col, op = c
            if not re.match(r'^[\w]+$', col):
                raise ValueError(f'Invalid column name: {col}')
            cond_tpls.append((col, op.lower()))
            proj.append(col)
        elif '?' in str(c):
            m = re.match(r'(\w+)\s*(.*)\?', c)
            if m:
                col, op = m.groups()
                if not re.match(r'^[\w]+$', col):
                    raise ValueError(f'Invalid column name: {col}')
                cond_tpls.append((col.strip(), op.strip() or '='))
                proj.append(col.strip())
        else:
            if not re.match(r'^[\w]+$', c):
                raise ValueError(f'Invalid column name: {c}')
            proj.append(c)
    if 'insert' in ops and not use_upsert:
        data_list = df[proj].to_dict('records')
        sql, params = builder.insert_bulk(table, data_list)
        out.append([(sql, params)])
    else:
        for row in df.itertuples(index=False):
            r = dict(zip(df.columns, row))
            conds = [f'{c} {op} "{r[c]}"' if op.lower() == 'like' else f'{c} {op} {r[c]}' for c, op in cond_tpls]
            data = {k: r[k] for k in proj if k in r}
            row_ops = []
            if 'select' in ops:
                sql, params = builder.select(table, proj, conds, expression)
                row_ops.append((sql, params))
            if use_upsert and 'upsert' in ops:
                sql, params = builder.upsert(table, r, pk or proj)
                row_ops.append((sql, params))
            else:
                if 'update' in ops:
                    sql, params = builder.update(table, data, conds, expression)
                    row_ops.append((sql, params))
                if 'insert' in ops:
                    sql, params = builder.insert(table, r)
                    row_ops.append((sql, params))
            if 'delete' in ops:
                sql, params = builder.delete(table, conds, expression)
                row_ops.append((sql, params))
            out.append(tuple(row_ops))
    return out
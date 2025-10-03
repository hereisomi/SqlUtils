"""Utility functions for database operations."""

import pandas as pd
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, InterfaceError
from .corrector import cast_df
from .con import SqlCon
import logging

logger = logging.getLogger(__name__)

class DbHandler:
    """Strategy for DB-specific SQL generation."""
    def __init__(self, db: str):
        self.db = db.lower()

    def upsert_sql(self, table: str, cols: List[str], key_cols: List[str], on_conflict: str) -> str:
        """Generate upsert SQL for the database."""
        ph = ','.join(f':{c}' for c in cols)
        insert_sql = f'INSERT INTO {table} ({",".join(cols)}) VALUES ({ph})'
        if self.db == 'postgresql':
            conflict = ','.join(key_cols)
            updates = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in key_cols)
            return f"{insert_sql} ON CONFLICT ({conflict}) DO UPDATE SET {updates}" if on_conflict == 'update' else f"{insert_sql} ON CONFLICT ({conflict}) DO NOTHING"
        elif self.db == 'sqlite':
            updates = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in key_cols)
            return f"{insert_sql} ON CONFLICT ({','.join(key_cols)}) DO UPDATE SET {updates}" if on_conflict == 'update' else f"{insert_sql} ON CONFLICT ({','.join(key_cols)}) DO NOTHING"
        elif self.db == 'oracle':
            key_cond = ' AND '.join(f"t.{c} = s.{c}" for c in key_cols)
            merge_sql = f"MERGE INTO {table} t USING (SELECT {','.join(f':{c} {c}' for c in cols)} FROM dual) s ON ({key_cond})"
            if on_conflict == 'update':
                merge_sql += f" WHEN MATCHED THEN UPDATE SET {','.join(f't.{c}=s.{c}' for c in cols if c not in key_cols)}"
            merge_sql += f" WHEN NOT MATCHED THEN INSERT ({','.join(cols)}) VALUES ({','.join(f's.{c}' for c in cols)})"
            return merge_sql
        elif self.db == 'mysql':
            updates = ','.join(f"{c}=VALUES({c})" for c in cols)
            return f"{insert_sql} ON DUPLICATE KEY UPDATE {updates}" if on_conflict == 'update' else insert_sql
        elif self.db == 'mssql':
            key_cond = ' AND '.join(f"t.{c} = s.{c}" for c in key_cols)
            merge_sql = f"MERGE INTO {table} t USING (VALUES ({ph})) s ({','.join(cols)}) ON ({key_cond})"
            if on_conflict == 'update':
                merge_sql += f" WHEN MATCHED THEN UPDATE SET {','.join(f't.{c}=s.{c}' for c in cols if c not in key_cols)}"
            merge_sql += f" WHEN NOT MATCHED THEN INSERT ({','.join(cols)}) VALUES ({','.join(f's.{c}' for c in cols)})"
            return merge_sql
        raise NotImplementedError(f"Upsert not supported for {self.db}")

    def alter_column_sql(self, table: str, col: str, new_type: str, forced: bool) -> str:
        """Generate ALTER TABLE SQL for column type change."""
        if self.db == 'sqlite':
            raise NotImplementedError("Forced column type alteration not supported in SQLite")
        elif self.db == 'postgresql':
            return f"ALTER TABLE {table} ALTER COLUMN {col} TYPE {new_type} USING {col}::{new_type}" if forced else f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {new_type}"
        elif self.db == 'oracle':
            return f"ALTER TABLE {table} MODIFY {col} {new_type}" if forced else f"ALTER TABLE {table} ADD ({col} {new_type})"
        elif self.db == 'mysql':
            return f"ALTER TABLE {table} MODIFY COLUMN {col} {new_type}" if forced else f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {new_type}"
        elif self.db == 'mssql':
            return f"ALTER TABLE {table} ALTER COLUMN {col} {new_type}" if forced else f"ALTER TABLE {table} ADD {col} {new_type}"
        raise NotImplementedError(f"Alter not supported for {self.db}")

def create_table_schema(con: SqlCon, table: str, df: pd.DataFrame, execute: bool = True) -> str:
    """Create table schema from DataFrame."""
    df = cast_df(df)
    cols = [f"{col} {con.corrector.dtype_to_sql(str(dtype))}" for col, dtype in df.dtypes.items()]
    sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)})"
    if execute:
        logger.debug(f"Executing: {sql}")
        con.execute(sql)
    return sql

def map_alter_filter_column(con: SqlCon, table: str, col: str, new_type: str):
    """Add column if missing."""
    handler = DbHandler(con.db)
    sql = handler.alter_column_sql(table, col, new_type, forced=False)
    con.execute(sql)

def map_alter_forced(con: SqlCon, table: str, col: str, new_type: str):
    """Force change column type."""
    handler = DbHandler(con.db)
    sql = handler.alter_column_sql(table, col, new_type, forced=True)
    con.execute(sql)

def insert_batch(con: SqlCon, table: str, rows: List[Dict[str, Any]]):
    """Batch insert rows."""
    if not rows:
        return
    cols = list(rows[0].keys())
    ph = ','.join(f':{c}' for c in cols)
    sql = f'INSERT INTO {table} ({",".join(cols)}) VALUES ({ph})'
    con._log(sql, f'{len(rows)} rows')
    try:
        with con.engine.begin() as c:
            c.execute(text(sql), rows)
    except (OperationalError, InterfaceError) as e:
        if con.auto_fb:
            logger.warning(f"Fallback insert: {e}")
            for row in rows:
                con.execute_raw(sql, row)
        else:
            raise

def upsert(con: SqlCon, table: str, rows: List[Dict[str, Any]], key_cols: List[str], on_conflict: str = 'update'):
    """Upsert rows with conflict handling."""
    if not rows:
        return
    cols = list(rows[0].keys())
    if not all(k in cols for k in key_cols):
        raise ValueError(f"Key columns {key_cols} not in row data")
    handler = DbHandler(con.db)
    sql = handler.upsert_sql(table, cols, key_cols, on_conflict)
    con._log(sql, f'{len(rows)} rows')
    try:
        with con.engine.begin() as c:
            c.execute(text(sql), rows)
    except (OperationalError, InterfaceError) as e:
        if con.auto_fb:
            logger.warning(f"Fallback upsert: {e}")
            for row in rows:
                con.execute_raw(sql, row)
        else:
            raise
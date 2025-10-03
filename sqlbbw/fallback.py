"""Fallback execution using raw database drivers."""

import re
import sqlite3
import psycopg2
from sqlalchemy.engine.url import URL
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

try:
    import oracledb
except ImportError:
    try:
        import cx_Oracle as oracledb
    except ImportError:
        oracledb = None

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    import mysql.connector
except ImportError:
    mysql.connector = None

_rx_pg = re.compile(r':([A-Za-z_]\w*)\b')

class FallbackExecutor:
    """Executes SQL using raw drivers as a fallback."""
    def __init__(self, url: URL):
        self.url = url
        self.db = url.drivername.split('+')[0]
        if self.db == 'postgres':
            self.db = 'postgresql'

    def _pg_style(self, sql: str, params: Dict[str, Any]) -> str:
        """Convert :param to %(param)s for PostgreSQL."""
        matched = set(_rx_pg.findall(sql))
        missing = matched - set(params)
        if missing:
            raise ValueError(f"Missing parameters for PostgreSQL query: {missing}")
        return _rx_pg.sub(r'%(\1)s', sql)

    def _sqlite(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL on SQLite."""
        with sqlite3.connect(self.url.database or ':memory:') as conn:
            conn.execute('BEGIN')
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else []
                cols = [d[0] for d in cur.description] if cur.description else []
                conn.commit()
                return [dict(zip(cols, row)) for row in rows]
            except Exception as e:
                conn.rollback()
                raise e

    def _postgres(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL on PostgreSQL."""
        sql = self._pg_style(sql, params)
        with psycopg2.connect(
            dbname=self.url.database, user=self.url.username, password=self.url.password,
            host=self.url.host, port=self.url.port or 5432, sslmode='prefer'
        ) as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() if cur.description else []
                    cols = [d[0] for d in cur.description] if cur.description else []
                    conn.commit()
                    return [dict(zip(cols, row)) for row in rows]
            except Exception as e:
                conn.rollback()
                raise e

    def _oracle(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL on Oracle."""
        if not oracledb:
            raise ImportError('oracledb or cx_Oracle required')
        dsn = oracledb.makedsn(self.url.host, self.url.port or 1521, service_name=self.url.database or self.url.query.get('sid'))
        with oracledb.connect(user=self.url.username, password=self.url.password, dsn=dsn) as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() if cur.description else []
                    cols = [d[0] for d in cur.description] if cur.description else []
                    conn.commit()
                    return [dict(zip(cols, row)) for row in rows]
            except Exception as e:
                conn.rollback()
                raise e

    def _mssql(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL on MSSQL."""
        if not pyodbc:
            raise ImportError('pyodbc required')
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.url.host};DATABASE={self.url.database};UID={self.url.username};PWD={self.url.password}"
        with pyodbc.connect(conn_str) as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, list(params.values()) if params else [])
                    rows = cur.fetchall() if cur.description else []
                    cols = [d[0] for d in cur.description] if cur.description else []
                    conn.commit()
                    return [dict(zip(cols, row)) for row in rows]
            except Exception as e:
                conn.rollback()
                raise e

    def _mysql(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL on MySQL."""
        if not mysql.connector:
            raise ImportError('mysql.connector required')
        with mysql.connector.connect(
            database=self.url.database, user=self.url.username, password=self.url.password,
            host=self.url.host, port=self.url.port or 3306
        ) as conn:
            conn.autocommit = False
            try:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() if cur.description else []
                    conn.commit()
                    return rows
            except Exception as e:
                conn.rollback()
                raise e

    def execute_raw(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SQL using the appropriate driver."""
        if self.db == 'sqlite':
            return self._sqlite(sql, params)
        if self.db == 'postgresql':
            return self._postgres(sql, params)
        if self.db == 'oracle':
            return self._oracle(sql, params)
        if self.db == 'mssql':
            return self._mssql(sql, params)
        if self.db == 'mysql':
            return self._mysql(sql, params)
        raise NotImplementedError(f"Unsupported database: {self.db}")
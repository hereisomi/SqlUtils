"""Main SQL connection wrapper for ETL operations."""

import pandas as pd
import json
from typing import Dict, List, Any, Optional, Union
from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import OperationalError, InterfaceError
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import QueuePool
from .corrector import DataCorrector, cast_df
from .fallback import FallbackExecutor
from .audit import Audit, audited, retry
from .utils import create_table_schema, map_alter_filter_column, map_alter_forced, insert_batch, upsert
import logging

logger = logging.getLogger(__name__)

class SqlCon(FallbackExecutor):
    """SQL connection wrapper with data correction and auditing."""
    def __init__(
        self, conn: str, pool_size: int = 5, pool_timeout: int = 30,
        echo: bool = False, debug: bool = False, auto_fb: bool = True,
        audit_db: str = 'audit.db'
    ):
        self.url = make_url(conn)
        super().__init__(self.url)
        self.debug = debug
        self.auto_fb = auto_fb
        self.audit = True
        self.audit_obj = Audit(audit_db)
        self.engine = create_engine(
            conn, poolclass=QueuePool, pool_size=pool_size,
            pool_timeout=pool_timeout, pool_recycle=3600, echo=echo, future=True
        )
        self.db = self.db if self.db != 'postgres' else 'postgresql'
        self.corrector = DataCorrector(self.engine, self.db)
        with self.engine.connect():
            pass
        
    def _get_dialect(self) -> str:
        """Get database dialect from connection string."""
        dialect = self.engine.dialect.name.lower()
        return 'postgresql' if dialect == 'postgres' else dialect

    def _log(self, sql: str, params: Any):
        """Log SQL and params if debug enabled."""
        if self.debug:
            logger.debug(f'SQL: {sql} | Params: {params}')

    @contextmanager
    def connect(self):
        """Context-managed connection."""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    @retry()
    @audited
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query and return results as list of dicts."""
        params = params or {}
        self._log(sql, params)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(sql), params)
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        except (OperationalError, InterfaceError) as e:
            if self.auto_fb:
                logger.warning(f'Fallback execution: {e}')
                return self.execute_raw(sql, params)
            raise

    def append(self, table: str, data: Union[pd.DataFrame, List[Dict[str, Any]]], if_exists: str = 'append', repair: bool = True):
        """Append DataFrame or dicts to table."""
        if isinstance(data, pd.DataFrame):
            df = self.corrector.fix_df(table, data)
        else:
            df = pd.DataFrame(data)
            df = self.corrector.fix_df(table, df)
        tables = self.inspect_db()
        if table not in tables:
            create_table_schema(self, table, df)
        elif if_exists == 'replace':
            self.execute(f"DROP TABLE IF EXISTS {table}")
            create_table_schema(self, table, df)
        rows = df.to_dict('records')
        try:
            insert_batch(self, table, rows)
        except Exception as e:
            if repair:
                logger.warning(f"Schema mismatch: {e}. Attempting repair.")
                schema_map = self.corrector.reverse_map(table)
                for col, dtype in df.dtypes.items():
                    pd_dtype = str(dtype).lower()
                    sql_type = self.corrector.cols(table).get(col)
                    expected_sql = self.corrector.dtype_to_sql(pd_dtype)
                    if sql_type and sql_type != expected_sql:
                        if 'filter' in if_exists.lower():
                            map_alter_filter_column(self, table, col, expected_sql)
                        else:
                            map_alter_forced(self, table, col, expected_sql)
                self.append(table, df, if_exists=if_exists, repair=False)
            else:
                raise

    def upsert(self, table: str, data: Union[pd.DataFrame, List[Dict[str, Any]]], key_cols: List[str], on_conflict: str = 'update'):
        """Upsert DataFrame or dicts."""
        if isinstance(data, pd.DataFrame):
            rows = self.corrector.fix_df(table, data).to_dict('records')
        else:
            rows = self.corrector.fix_rows(table, data)
        upsert(self, table, rows, key_cols, on_conflict)

    @audited
    def audited_to_sql(self, df: pd.DataFrame, name: str, **kwargs):
        """Audited wrapper for pandas.to_sql."""
        df = self.corrector.fix_df(name, df)
        df.to_sql(name, self.engine, **kwargs)

    def fetch_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Fetch query results as DataFrame."""
        try:
            with self.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except (OperationalError, InterfaceError) as e:
            if self.auto_fb:
                logger.warning(f'Fallback fetch: {e}')
                return pd.DataFrame(self.execute_raw(sql, params or {}))
            raise

    def inspect_db(self) -> Dict[str, List[str]]:
        """Inspect database tables and columns."""
        inspector = sa_inspect(self.engine)
        return {t: [c['name'] for c in inspector.get_columns(t)] for t in inspector.get_table_names()}

    def validate_schema(self, table: str, df: pd.DataFrame, schema_map: Optional[Dict[str, str]] = None) -> Dict:
        """Validate table schema against DataFrame."""
        db_cols = {c['name']: str(c['type']) for c in sa_inspect(self.engine).get_columns(table)}
        schema_map = schema_map or {
            c: ("INTEGER" if "int" in str(t).lower() else
                "FLOAT" if "float" in str(t).lower() else
                "BOOLEAN" if "bool" in str(t).lower() else
                "TIMESTAMP" if "datetime" in str(t).lower() else "TEXT")
            for c, t in df.dtypes.items()
        }
        return {
            "missing_in_db": [c for c in schema_map if c not in db_cols],
            "extra_in_db": [c for c in db_cols if c not in schema_map],
            "mismatched_types": {
                c: {"db": db_cols[c], "df": schema_map[c]}
                for c in schema_map if c in db_cols and schema_map[c] != db_cols[c]
            }
        }

    def check_tables(self, save_path: Optional[str] = None) -> Dict:
        """Inspect and optionally save schema."""
        inspector = sa_inspect(self.engine)
        schema = {
            t: {"columns": [{"name": c["name"], "type": str(c["type"])} for c in inspector.get_columns(t)]}
            for t in inspector.get_table_names()
        }
        if save_path:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=2)
        return schema

    def load_schema(self, path: str) -> Dict:
        """Load schema JSON."""
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def compare_schema(self, saved_schema: Dict) -> Dict:
        """Compare current schema with saved schema."""
        curr = self.check_tables()
        missing = [t for t in saved_schema if t not in curr]
        extra = [t for t in curr if t not in saved_schema]
        diffs = {}
        for tbl in saved_schema:
            if tbl not in curr:
                continue
            saved_cols = {c["name"]: c["type"] for c in saved_schema[tbl]["columns"]}
            curr_cols = {c["name"]: c["type"] for c in curr[tbl]["columns"]}
            missing_cols = [c for c in saved_cols if c not in curr_cols]
            extra_cols = [c for c in curr_cols if c not in saved_cols]
            mismatched = {
                c: {"db": curr_cols[c], "saved": saved_cols[c]}
                for c in saved_cols if c in curr_cols and saved_cols[c] != curr_cols[c]
            }
            if missing_cols or extra_cols or mismatched:
                diffs[tbl] = {"missing_cols": missing_cols, "extra_cols": extra_cols, "mismatched": mismatched}
        return {"missing_tables": missing, "extra_tables": extra, "column_diffs": diffs}

    def close(self):
        """Dispose of engine resources."""
        self.engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
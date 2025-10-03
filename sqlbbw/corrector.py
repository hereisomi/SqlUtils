"""Data type correction and casting for database operations."""

import pandas as pd
import re
from typing import Dict, List, Iterable, Any
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Engine
from .mappings import dtype_map, coercers, patterns
import logging

logger = logging.getLogger(__name__)

class DataCorrector:
    """Corrects data types to match database schema."""
    def __init__(self, engine: Engine, db: str):
        self.inspector = sa_inspect(engine)
        self.db = db.lower()
        self.col_cache = {}  # Cache {table: {col: type}}

    def cols(self, table: str) -> Dict[str, str]:
        """Get column types for a table."""
        if table not in self.col_cache:
            self.col_cache[table] = {c['name']: str(c['type']).upper() for c in self.inspector.get_columns(table)}
        return self.col_cache[table]

    def fix_rows(self, table: str, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Coerce row data to match table schema."""
        meta = self.cols(table)
        out = []
        for row in rows:
            corrected = {}
            for key, value in row.items():
                if key not in meta:
                    continue
                type_str = meta[key]
                base_type = type_str.split('(')[0]
                coercer = coercers.get(self.db, {}).get(base_type)
                try:
                    corrected[key] = None if value is None else coercer(pd.Series([value]), type_str)[0] if coercer else str(value)
                except Exception as e:
                    logger.warning(f'Coerce {key}={value} ({type_str}): {e}')
                    corrected[key] = None
            out.append(corrected)
        return out

    def fix_df(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce DataFrame columns to match table schema."""
        meta = self.cols(table)
        out = df.copy()
        for col in out.columns:
            if col not in meta:
                continue
            type_str = meta[col]
            base_type = type_str.split('(')[0]
            coercer = coercers.get(self.db, {}).get(base_type)
            if coercer:
                try:
                    out[col] = coercer(out[col], type_str)
                except Exception as e:
                    logger.warning(f'Coerce column {col} ({type_str}): {e}')
                    out[col] = out[col].astype(str)
            else:
                out[col] = out[col].astype(str)
        return out

    def dtype_to_sql(self, dtype: str) -> str:
        """Map Pandas dtype to SQL type."""
        return dtype_map.get(self.db, {}).get(dtype.lower(), 'TEXT')

    def sql_to_dtype(self, sql_type: str) -> str:
        """Map SQL type to Pandas dtype."""
        base = sql_type.split('(')[0]
        for pd_dtype, _ in coercers.get(self.db, {}).items():
            if base == pd_dtype:
                return pd_dtype.lower()
        return 'object'

    def reverse_map(self, table: str) -> Dict[str, str]:
        """Get {col: pd_dtype} from table schema."""
        return {col: self.sql_to_dtype(t_str) for col, t_str in self.cols(table).items()}

def cast_df(df: pd.DataFrame) -> pd.DataFrame:
    """Infer and cast DataFrame dtypes using regex patterns."""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]) or pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        non_null = df[col].dropna().astype(str).str.strip()
        if non_null.empty:
            continue
        sample = non_null  # Use all non-null values
        matched_type = None
        for dtype, regexes in patterns.items():
            if all(any(re.match(regex, val) for regex in regexes) for val in sample):
                matched_type = dtype
                break
        if matched_type:
            try:
                if matched_type in ['date', 'timestamp', 'time']:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif matched_type == 'epochtime':
                    # Try different units for epoch times
                    s = pd.to_numeric(df[col], errors='coerce')
                    if s.max() > 1e12:  # ms
                        df[col] = pd.to_datetime(s, unit='ms', errors='coerce')
                    elif s.max() > 1e15:  # us
                        df[col] = pd.to_datetime(s, unit='us', errors='coerce')
                    else:  # s
                        df[col] = pd.to_datetime(s, unit='s', errors='coerce')
                elif matched_type == 'integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif matched_type == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                elif matched_type == 'boolean':
                    def bool_map(x):
                        if pd.isna(x):
                            return pd.NA
                        x_str = str(x).lower()
                        return True if x_str in ['true', 'yes', 'y', '1'] else False if x_str in ['false', 'no', 'n', '0'] else pd.NA
                    df[col] = df[col].apply(bool_map).astype('boolean')
                else:
                    df[col] = df[col].astype('string')
            except Exception as e:
                logger.warning(f"Error casting {col} to {matched_type}: {e}")
    return df
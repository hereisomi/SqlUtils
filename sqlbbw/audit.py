"""Audit logging and retry decorators for database operations."""

import sqlite3
import logging
import time
import functools
import inspect
from threading import Lock
from typing import Optional

logger = logging.getLogger(__name__)

class Audit:
    """Manages audit logging to an SQLite database."""
    def __init__(self, db: str = 'audit.db'):
        self.db = db
        self.lock = Lock()
        self._init()

    def _init(self):
        """Initialize audit table."""
        with self.lock, sqlite3.connect(self.db) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    fn TEXT,
                    sql TEXT,
                    params TEXT,
                    ok INTEGER,
                    err TEXT,
                    caller_module TEXT,
                    caller_path TEXT
                )
            ''')

    def log(self, fn: str, sql: str, params: str, ok: bool, err: Optional[str], caller_module: str, caller_path: str):
        """Log an operation to the audit table."""
        with self.lock, sqlite3.connect(self.db) as conn:
            conn.execute('''
                INSERT INTO audit (fn, sql, params, ok, err, caller_module, caller_path)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (fn, sql, params, int(ok), err, caller_module, caller_path))

def audited(fn):
    """Decorator to audit function calls."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        sql = args[0] if args else kwargs.get('sql', kwargs.get('q', ''))
        params = str(args[1] if len(args) > 1 else kwargs.get('p', {}))[:1000]  # Limit size
        try:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            caller_module = module.__name__ if module else '__main__'
            caller_path = frame.filename
        except Exception as e:
            caller_module = 'unknown'
            caller_path = 'unknown'
            logger.warning(f"Failed to extract caller info: {e}")
        try:
            result = fn(self, *args, **kwargs)
            if self.audit:
                self.audit_obj.log(fn.__name__, sql, params, True, None, caller_module, caller_path)
            return result
        except Exception as e:
            if self.audit:
                self.audit_obj.log(fn.__name__, sql, params, False, str(e), caller_module, caller_path)
            raise
    return wrapper

def retry(tries: int = 3, delay: float = 2.0):
    """Decorator to retry on database errors."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, tries + 1):
                try:
                    return fn(*args, **kwargs)
                except (OperationalError, InterfaceError) as e:
                    if attempt == tries:
                        raise
                    logger.warning(f'{fn.__name__} retry {attempt}/{tries} - {e}')
                    time.sleep(delay)
        return wrapper
    return decorator
"""Database configuration."""

DB_CONFIG = {
    'conn_str': 'sqlite:///:memory:',  # Replace with your DB connection string
    'audit_db': 'sqlite:///audit.db'   # Optional audit database
}
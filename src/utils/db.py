import os
from sqlalchemy import create_engine
from contextlib import contextmanager

def pg_url():
    return f"postgresql+psycopg://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"

_engine = None
def engine():
    global _engine
    if _engine is None:
        _engine = create_engine(pg_url(), pool_pre_ping=True)
    return _engine

@contextmanager
def get_conn():
    e = engine()
    with e.begin() as conn:
        yield conn

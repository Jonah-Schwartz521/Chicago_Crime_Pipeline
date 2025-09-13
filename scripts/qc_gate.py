#!/usr/bin/env python3
import os, sys, datetime as dt
from sqlalchemy import create_engine, text

PGUSER=os.getenv("PGUSER","postgres")
PGPASSWORD=os.getenv("PGPASSWORD","")
PGHOST=os.getenv("PGHOST","localhost")
PGPORT=os.getenv("PGPORT","5432")
PGDATABASE=os.getenv("PGDATABASE","postgres")

engine = create_engine(f"postgresql+psycopg://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")

def ts():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    with engine.connect() as c:
        row = c.execute(text("""
            SELECT 
              COUNT(*)::bigint AS rows,
              MAX(updated_on)  AS last_ts,
              NOW() - MAX(updated_on) AS lag
            FROM public_stg.stg_chicago_crimes
        """)).mappings().one()
    print(f"{ts()} [qc] rows={row['rows']} last_updated={row['last_ts']} lag={row['lag']}")
    # Warn if stale (>12h) or empty
    if row["rows"] == 0:
        print(f"{ts()} [qc][WARN] no rows in public_stg.stg_chicago_crimes")
        return 0  # warn but don't fail the pipeline
    if row["lag"] is not None and row["lag"].total_seconds() > 12*3600:
        print(f"{ts()} [qc][WARN] data lag exceeds 12h")
        return 0
    print(f"{ts()} [qc] freshness OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())

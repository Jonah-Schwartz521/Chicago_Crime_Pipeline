#!/usr/bin/env python3
"""
Ingest Chicago crimes (Socrata 6zsd-86xi) into raw.chicago_crimes.

- Ensures raw schema/table exist
- Fetches recent rows (last 14 days by updated_on)
- Idempotent upsert via ON CONFLICT (source_id) DO NOTHING
"""

import os
import time
import datetime as dt
import requests
from sqlalchemy import create_engine, text
from psycopg.types.json import Json  # adapts Python dict -> Postgres jsonb
from dotenv import load_dotenv

load_dotenv()

# ---- CONFIG FROM ENV ----
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "postgres")

SOCRATA_DOMAIN  = os.getenv("SOCRATA_DOMAIN", "data.cityofchicago.org")
SOCRATA_DATASET = os.getenv("SOCRATA_DATASET", "6zsd-86xi")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")

PAGE_LIMIT = int(os.getenv("SOCRATA_PAGE_LIMIT", "5000"))  # reasonable for cron

engine = create_engine(
    f"postgresql+psycopg://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
)

def log(msg: str):
    ts = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [ingest] {msg}", flush=True)

def ensure_table():
    create_raw = "CREATE SCHEMA IF NOT EXISTS raw;"
    # canonical raw table with jsonb payload + generated source_id
    create_tbl = """
    CREATE TABLE IF NOT EXISTS raw.chicago_crimes (
        id           BIGSERIAL PRIMARY KEY,
        ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        payload      JSONB NOT NULL,
        source_id    TEXT GENERATED ALWAYS AS ((payload->>'id')) STORED,
        CONSTRAINT uq_raw_chicago_crimes_source_id UNIQUE (source_id)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_raw))
        conn.execute(text(create_tbl))

def fetch_recent_rows():
    """Fetch recent records from Socrata by updated_on (last 14 days)."""
    base = f"https://{SOCRATA_DOMAIN}/resource/{SOCRATA_DATASET}.json"

    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=14)) \
        .replace(tzinfo=None).isoformat(timespec="seconds")

    params = {
        "$limit": PAGE_LIMIT,
        "$order": "updated_on DESC",
        "$where": f"updated_on > '{since}'",
    }
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    log(f"fetch since updated_on > {since} (limit={PAGE_LIMIT})")
    r = requests.get(base, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    rows = r.json()
    log(f"fetched {len(rows)} rows")
    # Fallback w/o date if empty (rare)
    if not rows:
        log("no recent rows; retrying without date filter")
        params = {"$limit": PAGE_LIMIT, "$order": "updated_on DESC"}
        r = requests.get(base, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        rows = r.json()
        log(f"fallback fetched {len(rows)} rows")
    return rows

def upsert_rows(rows):
    """Insert rows; skip duplicates via source_id unique constraint."""
    if not rows:
        log("nothing to insert")
        return 0

    insert_sql = """
    INSERT INTO raw.chicago_crimes (payload)
    VALUES (:payload)
    ON CONFLICT (source_id) DO NOTHING;
    """
    inserted = 0
    with engine.begin() as conn:
        for rec in rows:
            conn.execute(text(insert_sql), {"payload": Json(rec)})
            inserted += 1
    log(f"insert attempted={inserted} (duplicates skipped automatically)")
    return inserted

def main():
    log(f"DB → {engine.url.render_as_string(hide_password=True)}")
    ensure_table()
    rows = fetch_recent_rows()
    upsert_rows(rows)
    # quick count
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM raw.chicago_crimes")).scalar_one()
        log(f"total rows now in raw.chicago_crimes: {total}")

if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        log(f"done in {time.time() - t0:.2f}s")
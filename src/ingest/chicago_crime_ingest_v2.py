from __future__ import annotations
import os, argparse, datetime as dt, time
import pandas as pd
import requests
from src.utils.db import get_conn
from sqlalchemy.dialects.postgresql import JSONB

SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN", "data.cityofchicago.org")
DATASET = os.getenv("SOCRATA_DATASET", "6zsd-86xi")
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

def parse_since(s: str) -> str:
    now = dt.datetime.utcnow()
    if s.endswith("d"):
        return (now - dt.timedelta(days=int(s[:-1]))).isoformat()
    if s.endswith("h"):
        return (now - dt.timedelta(hours=int(s[:-1]))).isoformat()
    return dt.datetime.fromisoformat(s).isoformat()

def fetch_batch(where_after: str | None, limit: int, offset: int):
    url = f"https://{SOCRATA_DOMAIN}/resource/{DATASET}.json"
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "updated_on",
        "$select": (
            "id,case_number,date as occurrence_date,block,iucr,primary_type,"
            "description,location_description as location_desc,arrest,domestic,"
            "beat,district,ward,community_area,latitude,longitude,updated_on"
        ),
    }
    if where_after:
        params["$where"] = f"updated_on > '{where_after}'"
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def to_df(rows):
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["arrest", "domestic"]:
        if c in df:
            df[c] = df[c].astype("boolean")
    for c in ["latitude", "longitude"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["occurrence_date", "updated_on"]:
        if c in df:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    return df

def upsert(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df["raw_payload"] = df.apply(lambda r: r.to_dict(), axis=1)
    with get_conn() as c:
        # temp table
        c.exec_driver_sql("CREATE TEMP TABLE tmp_crimes (LIKE raw.crimes INCLUDING ALL) ON COMMIT DROP;")
        df.to_sql("tmp_crimes", con=c, if_exists="append", index=False, dtype={"raw_payload": JSONB})
        # merge
        c.exec_driver_sql("""
            INSERT INTO raw.crimes AS t (
                id, case_number, occurrence_date, block, iucr, primary_type, description,
                location_desc, arrest, domestic, beat, district, ward, community_area,
                latitude, longitude, updated_on, raw_payload
            )
            SELECT
                id, case_number, occurrence_date, block, iucr, primary_type, description,
                location_desc, arrest, domestic, beat, district, ward, community_area,
                latitude, longitude, updated_on, raw_payload
            FROM tmp_crimes
            ON CONFLICT (id) DO UPDATE SET
                case_number   = EXCLUDED.case_number,
                occurrence_date = EXCLUDED.occurrence_date,
                block         = EXCLUDED.block,
                iucr          = EXCLUDED.iucr,
                primary_type  = EXCLUDED.primary_type,
                description   = EXCLUDED.description,
                location_desc = EXCLUDED.location_desc,
                arrest        = EXCLUDED.arrest,
                domestic      = EXCLUDED.domestic,
                beat          = EXCLUDED.beat,
                district      = EXCLUDED.district,
                ward          = EXCLUDED.ward,
                community_area= EXCLUDED.community_area,
                latitude      = EXCLUDED.latitude,
                longitude     = EXCLUDED.longitude,
                updated_on    = EXCLUDED.updated_on,
                raw_payload   = EXCLUDED.raw_payload;
        """)
    return len(df)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="30d", help="e.g., 30d, 12h, or ISO timestamp")
    ap.add_argument("--limit", type=int, default=50000, help="page size")
    args = ap.parse_args()

    since = parse_since(args.since) if args.since else None
    offset, total = 0, 0

    while True:
        rows = fetch_batch(since, args.limit, offset)
        df = to_df(rows)
        if df.empty:
            break
        total += upsert(df)
        offset += args.limit
        time.sleep(0.2)
        if len(rows) < args.limit:
            break

    print(f"Ingest complete. Rows merged: {total}")

if __name__ == "__main__":
    main()

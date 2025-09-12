import os, datetime as dt, time, requests
from sqlalchemy import create_engine, text
from psycopg.types.json import Json  # adapts Python dict -> Postgres jsonb

# ---- CONFIG FROM ENV ----
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "postgres")
SOCRATA_DATASET = os.getenv("SOCRATA_DATASET", "6zsd-86xi")

engine = create_engine(
    f"postgresql+psycopg://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
)

def main():
    print("DB →", engine.url.render_as_string(hide_password=False))

# ---- ENSURE TABLE EXISTS ----
create_sql = """
CREATE TABLE IF NOT EXISTS crime_smoketest (
    id           BIGSERIAL PRIMARY KEY,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload      JSONB NOT NULL,
    -- derive a stable ID from the Socrata payload
    source_id    TEXT GENERATED ALWAYS AS ((payload->>'id')) STORED,
    CONSTRAINT uq_crime_smoketest_source_id UNIQUE (source_id)
);
"""
with engine.begin() as conn:
    conn.execute(text(create_sql))

# ---- FETCH FROM SOCRATA ----
base = f"https://data.cityofchicago.org/resource/{SOCRATA_DATASET}.json"

# Try last 14 days by updated_on first (captures late-arriving fixes)
since = (
    dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=14)
).replace(tzinfo=None).isoformat(timespec="seconds")

params = {
    "$limit": 500,
    "$order": "updated_on DESC",
    "$where": f"updated_on > '{since}'",
}
print("[fetch] calling Socrata (updated_on last 14 days)…")
resp = requests.get(base, params=params, timeout=30)
resp.raise_for_status()
rows = resp.json()
print(f"[fetch] got {len(rows)} rows")

# Fallback: no date filter, still order by updated_on
if not rows:
    print("[fetch] no recent rows; retrying without date filter…")
    params = {"$limit": 500, "$order": "updated_on DESC"}
    resp = requests.get(base, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    print(f"[fetch] fallback got {len(rows)} rows")

# ---- INSERT ROWS (idempotent on source_id) ----
insert_sql = """
INSERT INTO crime_smoketest (payload)
VALUES (:payload)
ON CONFLICT (source_id) DO NOTHING;
"""
with engine.begin() as conn:
    inserted = 0
    for r in rows:
        conn.execute(text(insert_sql), {"payload": Json(r)})
        inserted += 1
print(f"[insert] inserted {inserted} rows into crime_smoketest")
with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM crime_smoketest")).scalar_one()
    print(f"[check] total rows now in crime_smoketest: {total}")

if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        print(f"[done] {time.time() - t0:.2f}s")
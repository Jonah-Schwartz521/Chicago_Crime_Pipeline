.PHONY: init-db psql

psql:
	PGPASSWORD=$(PGPASSWORD) psql -h $(PGHOST) -U $(PGUSER) -d $(PGDATABASE)

init-db:
	PGPASSWORD=$(PGPASSWORD) psql -h $(PGHOST) -U $(PGUSER) -d $(PGDATABASE) -v ON_ERROR_STOP=1 \
	-c "CREATE SCHEMA IF NOT EXISTS raw;" \
	-c "CREATE SCHEMA IF NOT EXISTS stg;" \
	-c "CREATE SCHEMA IF NOT EXISTS core;" \
	-c "CREATE SCHEMA IF NOT EXISTS marts;" \
	-c "CREATE SCHEMA IF NOT EXISTS meta;" \
	-c "CREATE TABLE IF NOT EXISTS raw.crimes (id TEXT PRIMARY KEY, case_number TEXT, occurrence_date TIMESTAMPTZ, block TEXT, iucr TEXT, primary_type TEXT, description TEXT, location_desc TEXT, arrest BOOLEAN, domestic BOOLEAN, beat TEXT, district TEXT, ward TEXT, community_area TEXT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, updated_on TIMESTAMPTZ, raw_payload JSONB);" \
	-c "CREATE INDEX IF NOT EXISTS crimes_updated_on_idx ON raw.crimes(updated_on);" \
	-c "CREATE TABLE IF NOT EXISTS meta.load_state (source TEXT PRIMARY KEY, last_loaded TIMESTAMPTZ);"

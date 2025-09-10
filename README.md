# Chicago Crime Pipeline

Socrata API → Postgres → dbt → Streamlit, orchestrated with Prefect.

## Quickstart
```bash
cp .env.example .env
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make init-db
python -m src.ingest.chicago_crime_ingest --since 30d
cd dbt && dbt deps && dbt run && dbt test
streamlit run streamlit_app/Home.py
```

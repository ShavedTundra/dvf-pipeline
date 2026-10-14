# dvf-pipeline

ELT pipeline on French property-sales open data (DVF, DGFiP).
Ingestion → medallion Parquet → dbt models → Dagster orchestration → Airflow port.
Companion to a real-estate estimation project on the same data: this repo builds the
producer side (clean, modeled, tested datasets) that a consumer side consumes.

## Layout
- `data/bronze/` — raw DVF exports, unmodified (never committed)
- `data/silver/` — typed, deduped, partitioned Parquet (generated)
- `sql/` — query history, one file per pipeline step
- `.venv/` — project venv (uv)

## Tooling
DuckDB only (v1.5.6) — dbt/dagster/airflow each get their own dependency review
when they land. Storage layer designed to swap to BigQuery without touching transforms.

# dvf-pipeline

ELT pipeline on French property-sales open data (DVF, DGFiP).
Ingestion → medallion Parquet → dbt models → Dagster orchestration. Airflow port
planned as an orchestration-layer alternative.
Companion to a real-estate estimation project on the same data: this repo builds the
producer side (clean, modeled, tested datasets) that a consumer side consumes.
Scope: Loire-Atlantique (44), 2021–2025, ~111k sales — layout partitioned to absorb
more departments/years without touching transforms.

## Layout
- `data/bronze/` — raw DVF exports, unmodified (never committed)
- `data/silver/` — typed, deduped, partitioned Parquet (generated)
- `dvf/` — dbt project: staging views, marts (fact_sales + dimensions), custom data tests
- `definitions.py` — Dagster code location: bronze ingestion asset with retries,
  monthly-partitioned dbt assets, weekly build schedule, file-arrival sensor
- `sql/` — query history, one file per pipeline step
- `.venv/` — project venv (uv)

## Tooling
DuckDB (1.5.6) as engine · dbt-core 1.12 + dbt-duckdb for transforms ·
Dagster 1.13 with dagster-dbt for orchestration.
Storage layer designed to swap to BigQuery without touching transforms.

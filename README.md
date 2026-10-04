# dvf-pipeline

ELT capstone on French property-sales open data (DVF, DGFiP) — data-engineering lane of the
`find_job` mission. Consumer side of this data already mastered (STAR #1 estimation project);
this repo builds the producer side: ingestion → medallion Parquet → dbt models → Dagster
orchestration → Airflow port. Lesson track: find_job/lessons/0015–0022.

## Layout
- `data/bronze/` — raw DVF exports, unmodified (never committed)
- `data/silver/` — typed, deduped, partitioned Parquet (generated)
- `sql/` — query history, one file per exercise/lesson step
- `.venv/` — project venv (uv)

## Tooling
DuckDB only (v1.5.6) — dbt/dagster/airflow arrive at their lesson gates with their own
dependency reviews. Storage layer designed to swap to BigQuery without touching transforms.

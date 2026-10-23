# dvf-pipeline

ELT pipeline on French property-sales open data (DVF, DGFiP).
Ingestion → medallion Parquet → dbt models → orchestrated by Dagster
(`definitions.py`) and Airflow (`dags/`) — two orchestrators over the same transforms.
Companion to a real-estate estimation project on the same data: this repo builds the
producer side (clean, modeled, tested datasets) that a consumer side consumes.
Scope: West-France departments 44, 49, 53, 72, 85 (Pays de la Loire +
Maine-et-Loire), 2021–2025, ~496k mutations / ~294k modeled sales — layout
partitioned to absorb more departments/years without touching transforms.

## Layout
- `data/bronze/` — raw DVF exports, unmodified (never committed; see Data setup)
- `data/silver/` — typed, deduped, partitioned Parquet (generated; `dept=/annee=`)
- `dvf/` — dbt project: staging views, marts (fact_sales + dimensions), custom data tests
- `definitions.py` — Dagster code location: bronze ingestion asset with retries,
  monthly-partitioned dbt assets, weekly build schedule, file-arrival sensor
- `dags/` — Airflow DAG: file sensor → bronze ingest → silver build → dbt,
  weekly cron, `max_active_runs=1`
- `Dockerfile` + `requirements-airflow.txt` — custom Airflow 3.3.2 image
  (dbt + duckdb layered on ASF base)
- `.devcontainer/` — VS Code devcontainer on the same image (Pylance resolves
  Airflow imports); `.dbt/` — dbt profiles for container runs
- `sql/` — query history, one file per pipeline step
- `.venv/` — project venv (uv)

## Data setup

`data/` is gitignored (≈1 GB, regenerable). Reconstruct `data/bronze/` before
running the pipeline.

### Required files

`bronze_raw` (in `definitions.py`) globs `data/bronze/true_dvf_*.csv` —
to be ingested, each file must:

- be named `true_dvf_<dept>_<year>.csv` (e.g. `true_dvf_49_2021.csv`), one per
  department-year, 2021–2025 — any other name is ignored by ingestion
- be comma-separated, with official DVF column headers
  (`id_mutation,date_mutation,...`, 40 columns) and ISO dates
- contain rows for a single department (44, 49, 53, 72, 85)

`bronze_raw` builds silver in one pass: dedupe to one row per `id_mutation`,
rename/cast to the silver schema, compute `px_m2` and per-commune median, write
Parquet under `data/silver/sales/dept=<d>/annee=<y>/`, and refresh the
`silver_tables` table in `data_bronze.duckdb` (dbt source).

### How to fetch

Source = geolocated DVF mirror (geo-dvf, etalab). Per-year, per-department
gzipped CSVs — no filtering or conversion needed:

1. Download, for each department D in {44, 49, 53, 72, 85} and year 2021–2025:
   `https://files.data.gouv.fr/geo-dvf/<release>/csv/<year>/departements/<D>.csv.gz`
   — pin a release for reproducibility (e.g. `2025-12`, used for all current
   files); `latest` always points at the newest build
2. Gunzip and rename:
   `<D>.csv.gz` → `data/bronze/true_dvf_<D>_<year>.csv`

These exports are already comma-separated with the official DVF headers and
single-department rows, so they satisfy the required format as-is. Dataset page:
<https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres-geolocalisees/>

### Auxiliary files (not ingested)

`dvf_data_loire_atlantique.zip` (→ `1_DONNEES_LIVRAISON/dvf_plus.csv`) is the
DVF+ delivery (CEREMA), pipe-separated, all years; `dvf_44.csv` is its
44-filtered cut. Kept for reference only — the pipeline never reads them.
Note: the `bronze_arrival` sensor fires on any new `true_dvf_*.csv` directly in
`data/bronze/`; auxiliary `*.csv` files are ignored.

## Orchestration (Airflow)

`dags/dvf_pipeline.py` mirrors the Dagster pipeline: `FileSensor` on
`data/bronze/true_dvf_*.csv` (reschedule mode) → `ingest_bronze` (row-count
guard) → `build_silver` (DuckDB dedup + `px_m2`/commune medians) → `run_dbt`
(dbt build, 29 tests). Weekly cron Mon 07:00 Europe/Paris, retries=2/5min,
`max_active_runs=1`. Design decisions, all verified in-container:

- `max_active_runs=1` — DuckDB is single-writer; concurrent DAG runs fail
  fast on the file lock. Serialize at the scheduler, not by retry luck.
- Airflow 3 seeds NO default connections: FileSensor needs
  `AIRFLOW_CONN_FS_DEFAULT='fs:///?path=%2F'` injected at `docker run`.
- Image pinned `apache/airflow:3.3.2`, never `latest` — critical RCE CVEs
  below 3.3.0; dbt/duckdb layered via `Dockerfile` with `pip check` guard.
- Auth by default (FAB/JWT), unlike `dagster dev` — and CVE-2026-48726
  (JWT valid after logout) argues for staying pinned.
- Idempotent: clear-and-rerun reproduces identical silver counts
  (deterministic dedup winner rule: surface DESC NULLS LAST, numero_disposition tiebreak).

## Tooling
DuckDB (1.5.6) as engine · dbt-core 1.12 + dbt-duckdb for transforms ·
Dagster 1.13 with dagster-dbt for orchestration.
Airflow 3.3.2 (custom Docker image) as second orchestrator.
Storage layer designed to swap to BigQuery without touching transforms.

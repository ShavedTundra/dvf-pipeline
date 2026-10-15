# dvf-pipeline

ELT pipeline on French property-sales open data (DVF, DGFiP).
Ingestion → medallion Parquet → dbt models → Dagster orchestration. Airflow port
planned as an orchestration-layer alternative.
Companion to a real-estate estimation project on the same data: this repo builds the
producer side (clean, modeled, tested datasets) that a consumer side consumes.
Scope: Loire-Atlantique (44), 2021–2025, ~111k sales — layout partitioned to absorb
more departments/years without touching transforms.

## Layout
- `data/bronze/` — raw DVF exports, unmodified (never committed; see Data setup)
- `data/silver/` — typed, deduped, partitioned Parquet (generated)
- `dvf/` — dbt project: staging views, marts (fact_sales + dimensions), custom data tests
- `definitions.py` — Dagster code location: bronze ingestion asset with retries,
  monthly-partitioned dbt assets, weekly build schedule, file-arrival sensor
- `sql/` — query history, one file per pipeline step
- `.venv/` — project venv (uv)

## Data setup

`data/` is gitignored (≈1 GB, regenerable). Reconstruct `data/bronze/` before
running the pipeline.

### Required files

`bronze_raw` (in `definitions.py`) globs `data/bronze/true_dvf_44*.csv` —
to be ingested, each file must:

- be named `true_dvf_44_<year>.csv` (e.g. `true_dvf_44_2021.csv`), one per
  year, 2021–2025 — any other name is ignored by ingestion
- be comma-separated, with official DVF column headers
  (`id_mutation,date_mutation,...`, 40 columns) and ISO dates
- contain Loire-Atlantique (44) rows only

### How to fetch

Source = geolocated DVF mirror (geo-dvf, etalab). Per-year, per-department
gzipped CSVs — no filtering or conversion needed:

1. Download, for each year 2021–2025:
   `https://files.data.gouv.fr/geo-dvf/<release>/csv/<year>/departements/44.csv.gz`
   — pin a release for reproducibility (e.g. `2025-12`, used for the 2024
   file); `latest` always points at the newest build
2. Gunzip and rename:
   `44.csv.gz` → `data/bronze/true_dvf_44_<year>.csv`

These exports are already comma-separated with the official DVF headers and
44-only rows, so they satisfy the required format as-is. Dataset page:
<https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres-geolocalisees/>

### Auxiliary files (not ingested)

`dvf_data_loire_atlantique.zip` (→ `1_DONNEES_LIVRAISON/dvf_plus.csv`) is the
DVF+ delivery (CEREMA), pipe-separated, all years; `dvf_44.csv` is its
44-filtered cut. Kept for reference only — the pipeline never reads them.
Note: the `bronze_arrival` sensor fires on any new `*.csv` directly in
`data/bronze/`, but the run only reads `true_dvf_44*` files.

## Tooling
DuckDB (1.5.6) as engine · dbt-core 1.12 + dbt-duckdb for transforms ·
Dagster 1.13 with dagster-dbt for orchestration.
Storage layer designed to swap to BigQuery without touching transforms.

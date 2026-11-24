# dvf — dbt project

dbt layer of the DVF pipeline (Loire-Atlantique property sales, dep. 44). Transforms the ingested DVF data into a star schema in DuckDB.

## Architecture

```
bronze (DuckDB source table) ──> staging ──────────────> marts
                                stg_sales (view)         fact_sales (incremental)
                                                       dim_commune
                                                       dim_date
                                                       dim_type_local
                                                       median_px_m2_commune
```

- **Staging** (`models/staging/`): `stg_sales` — view over the raw source table, selecting `id_mutation`, `date_mutation`, `valeur_eur`, `type_local`, `surface_bati`, `code_insee`. Materialized as views.
- **Marts** (`models/marts/`): star schema — `fact_sales` plus dimensions `dim_commune`, `dim_date`, `dim_type_local`, and aggregate `median_px_m2_commune` (median €/m² per commune). Dimensions and aggregate materialized as tables; `fact_sales` is incremental (see below).

## Incremental strategy

`fact_sales` is materialized `incremental` with strategy `delete+insert` — an upsert, not an append. Grain is mutation×lot: `unique_key = [id_mutation, numero_disposition, id_parcelle]`, the source row identity threaded from bronze through silver to the fact.

Each run reads only rows with `date > max(date) - interval 90 days`. The lookback is calibrated on DVF's publication cadence — a November sale can land in a January file. Measured on this project: a row 77 days old was silently missed by a 14-day window and caught by a 90-day window; `fact_sales_matches_source` is the invariant that turns that silent miss into a build failure.

`on_schema_change='fail'` makes schema drift between runs loud instead of silent. Bulk correction is `dbt build --select dvf.marts.fact_sales --full-refresh` — safe because the fact is fully derived from silver — documented as a correction procedure, not a daily policy.

Source is declared in `models/staging/sources.yml` (source `bronze`, table `silver_tables`) with a freshness check on `date_mutation` (warn 100d / error 400d).

## Setup

Requires Python deps from repo root (`pip install -r requirements.txt`) and a DuckDB database populated by the bronze ingestion step (see root `README.md`, "Data setup"). A dbt profile named `dvf` must exist (e.g. in `~/.dbt/profiles.yml`) pointing at the DuckDB file.

## Usage

Run from this directory:

```bash
dbt run          # build all models
dbt test         # run schema + singular tests
dbt build        # run + test in DAG order
dbt source freshness
```

## Tests

- Schema tests (`models/marts/schema.yml`): uniqueness/not-null on dimension keys, referential integrity from `fact_sales` to all dimensions, accepted values on `dim_type_local.label` (`Appartement`, `Maison`).
- Singular tests (`tests/`):
  - `assert_median_px_m2_in_bounds.sql`
  - `assert_stg_sales_volume.sql`
  - `fact_sales_matches_source.sql`

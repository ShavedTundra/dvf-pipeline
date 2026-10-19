# dvf — dbt project

dbt layer of the DVF pipeline (Loire-Atlantique property sales, dep. 44). Transforms the ingested DVF data into a star schema in DuckDB.

## Architecture

```
bronze (DuckDB source table) ──> staging ──────────────> marts
                                stg_sales (view)         fact_sales (table)
                                                       dim_commune
                                                       dim_date
                                                       dim_type_local
                                                       median_px_m2_commune
```

- **Staging** (`models/staging/`): `stg_sales` — view over the raw source table, selecting `id_mutation`, `date_mutation`, `valeur_eur`, `type_local`, `surface_bati`, `code_insee`. Materialized as views.
- **Marts** (`models/marts/`): star schema — `fact_sales` plus dimensions `dim_commune`, `dim_date`, `dim_type_local`, and aggregate `median_px_m2_commune` (median €/m² per commune). Materialized as tables.

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

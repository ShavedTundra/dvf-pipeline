import dagster as dg
from pathlib import Path
import shutil
import duckdb
from dagster_dbt import dbt_assets, DbtCliResource

DB_PATH = "data_bronze.duckdb"
SILVER_DIR = "data/silver/sales"
GLOB = "true_dvf_*.csv"

# Silver schema consumed by dbt (stg_sales): id_mutation, date_mutation,
# valeur_eur, nature_mutation, type_local, surface_bati, code_insee,
# px_m2, median_px_m2_per_commune, mois (+ dept, annee for lineage).
SILVER_SQL = """
create or replace table silver_tables as
with raw as (
    select
        id_mutation,
        cast(date_mutation as date) as date_mutation,
        cast(valeur_fonciere as bigint) as valeur_eur,
        nature_mutation,
        type_local,
        cast(surface_reelle_bati as double) as surface_bati,
        cast(code_commune as bigint) as code_insee,
        cast(code_departement as varchar) as dept,
        cast(numero_disposition as integer) as numero_disposition,
        year(cast(date_mutation as date)) as annee
    from read_csv_auto({files}, union_by_name=true, all_varchar=true, sample_size=-1)
),
deduped as (
    select *
    from raw
    -- L16/L21 winner rule: biggest built surface wins, NULL surfaces lose,
    -- numero_disposition breaks exact ties.
    qualify row_number() over (partition by id_mutation
        order by surface_bati desc nulls last, numero_disposition asc) = 1
),
priced as (
    select
        *,
        case when surface_bati > 0 then valeur_eur / surface_bati end as px_m2,
        month(date_mutation) as mois
    from deduped
)
select
    * exclude (numero_disposition),
    median(px_m2) over (partition by code_insee) as median_px_m2_per_commune
from priced
"""


@dg.asset(retry_policy=dg.RetryPolicy(max_retries=2), key=["silver", "silver_tables"])
def bronze_raw() -> None:
    files = sorted(str(f) for f in Path("data/bronze").glob(GLOB))
    if not files:
        raise dg.Failure(description="No true_dvf_*.csv files found in data/bronze/")

    con = duckdb.connect(DB_PATH)
    try:
        file_list = ", ".join(f"'{f}'" for f in files)
        con.execute(SILVER_SQL.format(files=f"[{file_list}]"))

        # Refresh silver parquet, partitioned by dept/annee
        shutil.rmtree(SILVER_DIR, ignore_errors=True)
        con.execute(
            f"""
            copy (select * from silver_tables)
            to '{SILVER_DIR}'
            (format parquet, partition_by (dept, annee), overwrite_or_ignore true)
            """
        )
        counts = con.execute(
            "select dept, annee, count(*) from silver_tables group by 1, 2 order by 1, 2"
        ).fetchall()
    finally:
        con.close()

    print(f"Loaded {len(files)} DVF export(s): {counts}")


@dg.asset_check(asset=bronze_raw, blocking=True)
def bronze_file_volume():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        counts = con.execute(
            "select dept, annee, count(*) from silver_tables group by 1, 2 order by 1, 2"
        ).fetchall()
    finally:
        con.close()

    failed = [f"{dept}-{annee}" for dept, annee, count in counts if count <= 0]
    expected_depts = {"44", "49", "53", "72", "85"}
    missing = sorted(expected_depts - {dept for dept, _, _ in counts})
    all_valid = not failed and not missing

    return dg.AssetCheckResult(
        passed=all_valid,
        metadata={
            "status": "passed" if all_valid else "failed",
            "row_counts": str(counts),
            "failed_partitions": ", ".join(failed),
            "missing_depts": ", ".join(missing),
        },
    )


dbt_partitions = dg.MonthlyPartitionsDefinition(start_date="2020-12-31")
@dbt_assets(manifest=Path("dvf/target/manifest.json"), partitions_def=dbt_partitions)
def dvf_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

dbt_weekly = dg.ScheduleDefinition(
    job=dg.define_asset_job("dbt_weekly_build", selection=dg.AssetSelection.assets(bronze_raw)),
    cron_schedule= "0 7 * * 1",
    execution_timezone="Europe/Paris"
)


@dg.sensor(job=dg.define_asset_job("bronze_ingest", selection=dg.AssetSelection.assets(bronze_raw)))
def bronze_arrival(context: dg.AssetExecutionContext):
    files = sorted(Path("data/bronze").glob(GLOB), key= lambda p: p.stat().st_mtime)
    if not files:
        return
    newest = files[-1]
    if context.cursor and newest.stat().st_mtime <= float(context.cursor):
        return
    context.update_cursor(str(newest.stat().st_mtime))
    yield dg.RunRequest(run_key=f"bronze-{int(newest.stat().st_mtime)}")

defs = dg.Definitions(
    assets= [bronze_raw, dvf_dbt_assets],
    schedules=[dbt_weekly],
    sensors=[bronze_arrival],
    resources= {"dbt": DbtCliResource(project_dir="dvf")},
    asset_checks=[bronze_file_volume]
)

import dagster as dg
from pathlib import Path
import pandas as pd
from dagster_dbt import dbt_assets, DbtCliResource

@dg.asset(retry_policy= dg.RetryPolicy(max_retries=2))
def bronze_raw()-> None:
    files = sorted(Path("data/bronze").glob("true_dvf_44*.csv"))
    for f in files:
	    pd.read_csv(filepath_or_buffer=f)
    
    print(f"Loaded {len(files)} yearly DVF export(s)")

@dg.asset_check(asset=bronze_raw, blocking=True)
def bronze_file_volume():
    counts = {f.name: len(pd.read_csv(f)) for f in sorted(Path("data/bronze").glob("true_dvf_44*.csv"))}

    all_valid = all(count > 0 for _, count in counts.items())

    if not all_valid:
	    failed_names = [name for (name, count) in counts.items() if count <= 0]
    else:
 	    failed_names = []
    
    failed_names_str = ", ".join(failed_names)
    return dg.AssetCheckResult(
		passed=all_valid,
		metadata={
			"status": "passed" if all_valid else "failed",
			"failed_years": failed_names_str
		}
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
    files = sorted(Path("data/bronze").glob("*.csv"), key= lambda p: p.stat().st_mtime)
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


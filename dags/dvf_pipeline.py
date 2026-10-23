from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.timetables.trigger import CronTriggerTimetable
from datetime import datetime, timedelta

REPO = "/opt/airflow/repo"
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
BRONZE_GLOB = f"true_dvf_*.csv"
DB_PATH = "data_bronze.duckdb"

@dag(schedule=CronTriggerTimetable("0 7 * * 1", timezone="Europe/Paris"), 
        start_date=datetime(2025, 1, 1), catchup=False, max_active_runs=1, 
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}, tags=["dvf"])                                              
def dvf_pipeline():                                                                                                                               
    bronze_arrived = FileSensor(
        task_id="bronze_arrived",
        filepath=f"{REPO}/data/bronze/{BRONZE_GLOB}",
        mode="reschedule",
        poke_interval=300
    )                                                                                                                                                    
    @task                                                                                                                                         
    def ingest_bronze() -> int: # → data/bronze                                                                             
        import duckdb
        from pathlib import Path
        files = sorted(str(f) for f in Path(f"{REPO}/data/bronze").glob(BRONZE_GLOB))
        assert files, "no true_dvf_*.csv in bronze - fail loudly"
        file_list = ", ".join(f"'{f}'" for f in files)
        n = duckdb.sql(
            f"SELECT count(*) FROM read_csv_auto([{file_list}], union_by_name=true, "
            f"all_varchar=true, sample_size=-1)"
        ).fetchone()[0]     
        assert n > 0, "bronze CSV empty - fail loudly"
        print(f"bronze rows: {n} across {len(files)} files")
        return n
    
    @task                                                                                                                                         
    def build_silver(bronze_rows: int) -> None:     # wraps spark_silver.py                                                                     
        import duckdb
        from pathlib import Path
        files = sorted(str(f) for f in Path(f"{REPO}/data/bronze").glob(BRONZE_GLOB))
        assert files, "no true_dvf_*.csv in bronze - fail loudly"
        file_list = ", ".join(f"'{f}'" for f in files)

        con = duckdb.connect(f"{REPO}/{DB_PATH}")
        try:
            con.execute(SILVER_SQL.format(files=f"[{file_list}]"))
            counts = con.execute(
                "select dept, annee, count(*) from silver_tables group by 1, 2 order by 1, 2"
            ).fetchall()
        finally:
            con.close()
        silver_rows = sum(c[2] for c in counts)
        print(f"silver rows: {silver_rows} from bronze {bronze_rows}; by dept/annee: {counts}")


    @task                                                                                                                                         
    def run_dbt() -> None:                         # dbt run / dbt build                                                                       
        import subprocess
        from pathlib import Path
        dbt = str(Path.home() / ".local" / "bin" / "dbt")
        subprocess.run([dbt, "build", "--project-dir", f"{REPO}/dvf", "--profiles-dir", f"{REPO}/.dbt"], 
                       check=True, cwd=REPO)                                                                                                                                                
                                                                                                                                                    
    bronze_arrived >> (bronze := ingest_bronze())                                                                              
    bronze >> build_silver(bronze) >> run_dbt()                                                                                                   
                                                                                                                                                     
dvf_pipeline()   # ← required: Airflow finds the DAG by calling the function     
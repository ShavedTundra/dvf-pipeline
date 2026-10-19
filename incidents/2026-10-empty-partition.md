# 2026-10: Empty Partition Incident

## Signal
#### What, when, at which field
Blocking from Dagster run at bronze field
Asset check bronze_file_volume on bronze_raw returned passed=false during materialization. It blocked downstream dbt assets, metadata named true_dvf_44_2022.csv
## Detection Path
#### Which check caught and which layers passed first
All 21 dbt tests passed, they aim at correctness, not volume. Freshness dbt tests would have also missed this incidentd
## Root cause
#### Mechanism
Planted zero-row dept-year source file -> upstream so no downstream check can see it until manual re-landing (silver needs to be rebuilt)
## Fix
#### Smallest change to correctness
Replace true_dvf_44_2022.csv by the correct filled csv source file for the year-dept then retry run to check signal. Needs a manual partitioned (month) scoped rerun
## Prevention
#### New/changed check
All dbt downstreams checks were green, what was needed was an upstream blocking asset volume check, 2 singular tests more downstream to confirm and freshness tests written in sources
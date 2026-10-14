with bounds as (
    select min(date_mutation)::DATE as dmin, max(date_mutation)::DATE as dmax
    from {{ ref('stg_sales') }}
),
days as (
    select unnest(generate_series(dmin, dmax, interval 1 day))::DATE as d
    from bounds
)
select
d as date,
year(d) as annee,
month(d) as mois
from days

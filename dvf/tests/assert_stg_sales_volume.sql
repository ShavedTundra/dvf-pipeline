-- Fails when a dept-year extract lands empty
with expected as (
    select distinct annee from {{ ref('dim_date')}}),
    actual as (
        select year(date_mutation) as yr, count(*) as n from {{ ref('stg_sales') }} group by 1)
select expected.annee
from expected
left join actual on expected.annee = actual.yr
where coalesce(n,0) < 5000

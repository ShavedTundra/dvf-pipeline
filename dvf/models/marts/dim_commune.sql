select
  row_number() over (order by code_insee) as commune_key,
  code_insee as code_insee
from (SELECT DISTINCT code_insee FROM {{ ref('stg_sales') }})
order by code_insee

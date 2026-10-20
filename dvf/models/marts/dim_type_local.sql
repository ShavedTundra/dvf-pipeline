select
  row_number() over (order by type_local) as local_key,
  type_local as label
from {{ ref('stg_sales') }}
where type_local is not null
group by type_local

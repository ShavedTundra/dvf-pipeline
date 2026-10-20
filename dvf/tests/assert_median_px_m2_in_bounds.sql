select *
from {{ ref('median_px_m2_commune') }}
WHERE  (median_px_m2_commune < 300 OR median_px_m2_commune > 20000)
  AND nb_sales >= 5
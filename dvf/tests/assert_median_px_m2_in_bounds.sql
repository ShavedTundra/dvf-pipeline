select *
from {{ ref('median_px_m2_commune') }}
WHERE  median_px_m2_commune < 500 OR median_px_m2_commune > 20000
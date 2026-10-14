select c.code_insee, round(median(f.valeur_eur / nullif(f.surface_bati, 0)),1) as median_px_m2_commune, count(*) as nb_sales
from {{ ref('fact_sales')}} f
join {{ ref('dim_commune')}} c on c.commune_key = f.commune_key
group by c.code_insee
order by c.code_insee

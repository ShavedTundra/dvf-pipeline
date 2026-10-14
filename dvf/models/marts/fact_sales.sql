select
d.date,
c.commune_key,
t.local_key,
s.valeur_eur,
s.surface_bati,
s.id_mutation

from {{ ref('stg_sales') }} s
join {{ ref('dim_type_local') }} t on t.label = s.type_local
join {{ ref('dim_commune') }} c on c.code_insee = s.code_insee
join {{ ref('dim_date') }} d on d.date = s.date_mutation

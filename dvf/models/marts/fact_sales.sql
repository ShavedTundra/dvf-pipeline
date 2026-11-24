{{ config(
    materialized='incremental',
    unique_key=['id_mutation', 'numero_disposition', 'id_parcelle'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

select
d.date,
c.commune_key,
t.local_key,
s.valeur_eur,
s.surface_bati,
s.id_mutation,
s.numero_disposition,
s.id_parcelle

from {{ ref('stg_sales') }} s
join {{ ref('dim_type_local') }} t on t.label = s.type_local
join {{ ref('dim_commune') }} c on c.code_insee = s.code_insee
join {{ ref('dim_date') }} d on d.date = s.date_mutation

{% if is_incremental() %}
where date > (select coalesce(max(date), '1900-01-01') from {{ this}})
		- interval 90 days

{% endif %}

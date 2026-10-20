select id_mutation, date_mutation, valeur_eur, type_local, surface_bati, code_insee
from {{ source('silver', 'silver_tables') }}

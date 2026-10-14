select id_mutation, date_mutation, valeur_eur, type_local, surface_bati, code_insee
from {{ source('bronze', 'silver_tables') }}

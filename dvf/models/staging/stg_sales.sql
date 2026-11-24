select id_mutation, date_mutation, valeur_eur, type_local, surface_bati, code_insee, numero_disposition, id_parcelle
from {{ source('silver', 'silver_tables') }}

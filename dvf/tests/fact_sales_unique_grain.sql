-- Guard the mutation×lot grain of fact_sales: no two rows may share
-- (id_mutation, numero_disposition, id_parcelle). Even with the real identity
-- columns, a disposition selling several lots of the SAME parcel stays
-- ambiguous at the source; silver's ROW_NUMBER already arbitrated it, this
-- test keeps that residual risk visible instead of silent. Group-by form so
-- NULL keys are caught too (NULL = NULL is not true in a join).
select
    id_mutation,
    numero_disposition,
    id_parcelle,
    count(*) as duplicates
from {{ ref('fact_sales') }}
group by id_mutation, numero_disposition, id_parcelle
having count(*) > 1

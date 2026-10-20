-- tests/fact_sales_matches_source.sql                                                                                                                                                                          
   with counts as (
     select
       (select count(*) from {{ ref('fact_sales') }})      as fact_n,
       (select count(*) from {{ source('silver', 'silver_tables') }} where type_local is not null) as silver_n
   )                                                                                                                                                                                                               
   select * from counts                                                                                                                                                                                            
   where fact_n <> silver_n   

select
d::DATE as date,      
year(d) as annee,
month(d) as mois
from range(DATE '2025-01-01', DATE '2026-01-01', INTERVAL 1 DAY) t(d)    

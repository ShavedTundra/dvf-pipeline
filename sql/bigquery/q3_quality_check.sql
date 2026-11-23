SELECT 
  COUNT(*) AS lignes,
  COUNT(DISTINCT id_mutation) AS mutations_uniques,
  COUNTIF(valeur_eur IS NULL OR valeur_eur <= 0) AS valeurs_invalides,
  COUNTIF(surface_bati IS NULL OR surface_bati <= 0) AS surfaces_invalides,
  COUNTIF(px_m2 > 20000) AS px_m2_hors_bornes
FROM `dvf-pipeline-2311261818.dvf.silver_sales`
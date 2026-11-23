SELECT 
  type_local,
  COUNT(*) AS n_ventes,
  ROUND(APPROX_QUANTILES(px_m2, 2)[SAFE_OFFSET(1)], 1) AS median_px_m2
FROM `dvf-pipeline-2311261818.dvf.silver_sales` 
WHERE type_local IS NOT NULL
GROUP BY type_local
ORDER BY n_ventes DESC;
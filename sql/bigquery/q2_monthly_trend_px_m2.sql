WITH med AS (
  SELECT
    code_insee,
    DATE_TRUNC(date_mutation, MONTH) AS mois,
    APPROX_QUANTILES(px_m2, 2)[SAFE_OFFSET(1)] AS median_px_m2
  FROM `dvf-pipeline-2311261818.dvf.silver_sales`
  WHERE px_m2 IS NOT NULL AND px_m2 < 20000
  GROUP BY code_insee, mois
),
w AS (
  SELECT 
    med.*,
    LAG(med.median_px_m2) OVER (PARTITION BY med.code_insee ORDER BY med.mois) AS median_mois_prec
  FROM med
)

SELECT 
  code_insee, mois, median_px_m2, median_mois_prec,
  ROUND(100* SAFE_DIVIDE(median_px_m2 - median_mois_prec, median_mois_prec),1) AS var_pct
FROM w
WHERE median_mois_prec IS NOT NULL
ORDER BY ABS(var_pct) DESC;
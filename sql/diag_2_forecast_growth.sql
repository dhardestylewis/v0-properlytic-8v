-- =============================================================================
-- DIAGNOSTIC QUERY 2/3: Forecast growth percentiles by horizon (run separately)
-- =============================================================================

WITH forecast_growth AS (
    SELECT
        f.horizon_m,
        CASE WHEN coalesce(h.p50, h.value) > 0
             THEN 100.0 * (coalesce(f.p50, f.value) - coalesce(h.p50, h.value))
                        / coalesce(h.p50, h.value)
             ELSE NULL
        END AS growth_pct
    FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast f
    JOIN forecast_20260220_7f31c6e4.metrics_parcel_forecast h
      ON h.acct = f.acct
     AND h.origin_year = f.origin_year
     AND h.horizon_m = 12
     AND h.series_kind = f.series_kind
     AND h.variant_id = f.variant_id
    WHERE f.series_kind = 'forecast'
      AND f.variant_id = '__forecast__'
      AND f.horizon_m != 12
      AND coalesce(h.p50, h.value) > 0
)
SELECT
    horizon_m,
    count(*) AS n,
    round(min(growth_pct)::numeric, 1) AS min_g,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS median,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1) AS p99,
    round(max(growth_pct)::numeric, 1) AS max_g
FROM forecast_growth
WHERE growth_pct IS NOT NULL
GROUP BY horizon_m
ORDER BY horizon_m;

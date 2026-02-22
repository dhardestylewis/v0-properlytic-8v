-- =============================================================================
-- DIAGNOSTIC QUERY 3/3: Absolute value distribution (run separately)
-- =============================================================================

SELECT
    horizon_m,
    count(*) AS n,
    round(min(coalesce(p50, value))::numeric, 0) AS min_val,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0) AS p01_val,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0) AS p05_val,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0) AS median_val,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0) AS p95_val,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0) AS p99_val,
    round(max(coalesce(p50, value))::numeric, 0) AS max_val
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast
WHERE series_kind = 'forecast'
  AND variant_id = '__forecast__'
GROUP BY horizon_m
ORDER BY horizon_m;

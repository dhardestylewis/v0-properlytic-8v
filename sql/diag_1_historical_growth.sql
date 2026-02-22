-- =============================================================================
-- DIAGNOSTIC QUERY 1/3: Historical growth percentiles (SAMPLED, run separately)
-- Uses LAG() instead of self-join to avoid timeout.
-- Run this ALONE in Supabase SQL Editor.
-- =============================================================================

WITH ordered AS (
    SELECT
        acct,
        year,
        coalesce(p50, value) AS val,
        LAG(coalesce(p50, value), 1) OVER (PARTITION BY acct ORDER BY year) AS val_1yr_ago,
        LAG(coalesce(p50, value), 2) OVER (PARTITION BY acct ORDER BY year) AS val_2yr_ago,
        LAG(coalesce(p50, value), 3) OVER (PARTITION BY acct ORDER BY year) AS val_3yr_ago,
        LAG(coalesce(p50, value), 5) OVER (PARTITION BY acct ORDER BY year) AS val_5yr_ago
    FROM forecast_20260220_7f31c6e4.metrics_parcel_history
    WHERE series_kind = 'history'
      AND variant_id = '__history__'
      AND coalesce(p50, value) > 0
),
growth AS (
    SELECT '1yr' AS horizon, 100.0 * (val - val_1yr_ago) / nullif(val_1yr_ago, 0) AS g FROM ordered WHERE val_1yr_ago > 0
    UNION ALL
    SELECT '2yr', 100.0 * (val - val_2yr_ago) / nullif(val_2yr_ago, 0) FROM ordered WHERE val_2yr_ago > 0
    UNION ALL
    SELECT '3yr', 100.0 * (val - val_3yr_ago) / nullif(val_3yr_ago, 0) FROM ordered WHERE val_3yr_ago > 0
    UNION ALL
    SELECT '5yr', 100.0 * (val - val_5yr_ago) / nullif(val_5yr_ago, 0) FROM ordered WHERE val_5yr_ago > 0
)
SELECT
    horizon,
    count(*) AS n,
    round(min(g)::numeric, 1) AS min_g,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY g)::numeric, 1) AS median,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY g)::numeric, 1) AS p99,
    round(max(g)::numeric, 1) AS max_g
FROM growth
GROUP BY horizon
ORDER BY horizon;

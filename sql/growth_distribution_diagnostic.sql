-- =============================================================================
-- DIAGNOSTIC: Historical growth rate distribution by horizon
-- Purpose: Find the actual min/max/percentile growth rates from HISTORY data
-- to set empirical, data-driven outlier cutoffs for forecast predictions.
-- Run this in the Supabase SQL Editor.
-- =============================================================================

-- 1) HISTORICAL GROWTH RATES (year-over-year) at parcel level
-- Shows p1, p5, p25, p50, p75, p95, p99, min, max for each horizon-equivalent
WITH yoy AS (
    SELECT
        h1.acct,
        h1.year AS from_year,
        h2.year AS to_year,
        (h2.year - h1.year) AS years_apart,
        coalesce(h1.p50, h1.value) AS val_from,
        coalesce(h2.p50, h2.value) AS val_to,
        CASE WHEN coalesce(h1.p50, h1.value) > 0
             THEN 100.0 * (coalesce(h2.p50, h2.value) - coalesce(h1.p50, h1.value))
                        / coalesce(h1.p50, h1.value)
             ELSE NULL
        END AS growth_pct
    FROM forecast_20260220_7f31c6e4.metrics_parcel_history h1
    JOIN forecast_20260220_7f31c6e4.metrics_parcel_history h2
      ON h2.acct = h1.acct
     AND h2.year > h1.year
     AND h2.year - h1.year IN (1, 2, 3, 5)   -- approximate horizon buckets
    WHERE h1.series_kind = 'history'
      AND h1.variant_id = '__history__'
      AND h2.series_kind = 'history'
      AND h2.variant_id = '__history__'
      AND coalesce(h1.p50, h1.value) > 0
      AND coalesce(h2.p50, h2.value) > 0
)
SELECT
    years_apart,
    count(*) AS n_pairs,
    round(min(growth_pct)::numeric, 1)                        AS min_growth,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS median,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p99,
    round(max(growth_pct)::numeric, 1)                        AS max_growth
FROM yoy
GROUP BY years_apart
ORDER BY years_apart;

-- 2) FORECAST prediction distribution (current run)
-- Shows the same percentiles for FORECAST growth_pct so you can compare
WITH forecast_growth AS (
    SELECT
        f.acct,
        f.horizon_m,
        coalesce(f.p50, f.value) AS forecast_val,
        coalesce(h.p50, h.value) AS anchor_val,
        CASE WHEN coalesce(h.p50, h.value) > 0
             THEN 100.0 * (coalesce(f.p50, f.value) - coalesce(h.p50, h.value))
                        / coalesce(h.p50, h.value)
             ELSE NULL
        END AS growth_pct
    FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast f
    -- Join to the "now" baseline (horizon_m=12, same origin)
    LEFT JOIN forecast_20260220_7f31c6e4.metrics_parcel_forecast h
      ON h.acct = f.acct
     AND h.origin_year = f.origin_year
     AND h.horizon_m = 12
     AND h.series_kind = f.series_kind
     AND h.variant_id = f.variant_id
    WHERE f.series_kind = 'forecast'
      AND f.variant_id = '__forecast__'
      AND coalesce(h.p50, h.value) > 0
)
SELECT
    horizon_m,
    count(*) AS n,
    round(min(growth_pct)::numeric, 1)                        AS min_growth,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS median,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY growth_pct)::numeric, 1)  AS p99,
    round(max(growth_pct)::numeric, 1)                        AS max_growth
FROM forecast_growth
GROUP BY horizon_m
ORDER BY horizon_m;

-- 3) ABSOLUTE VALUE distribution (forecast predictions)
-- To see if there are absurd values ($0 lots, $100M lots, etc.)
SELECT
    horizon_m,
    count(*) AS n,
    round(min(coalesce(p50, value))::numeric, 0)                        AS min_val,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0)  AS p01_val,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0)  AS p05_val,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0)  AS median_val,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0)  AS p95_val,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY coalesce(p50, value))::numeric, 0)  AS p99_val,
    round(max(coalesce(p50, value))::numeric, 0)                        AS max_val
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast
WHERE series_kind = 'forecast'
  AND variant_id = '__forecast__'
GROUP BY horizon_m
ORDER BY horizon_m;

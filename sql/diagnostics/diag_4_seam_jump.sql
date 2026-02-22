-- Diagnostic: find parcels with extreme "seam" jumps between
-- last historical value and first forecast prediction.
--
-- A well-calibrated model should produce a first-year forecast
-- close to the last known value. Large jumps (>50% change in 1yr)
-- indicate calibration problems worth excluding.

SET statement_timeout = '120s';

-- Get last historical value vs first forecast value per parcel
WITH hist AS (
    SELECT acct, value AS hist_value
    FROM forecast_20260220_7f31c6e4.metrics_parcel_history
    WHERE origin_year = 2025  -- latest historical snapshot year
      AND series_kind = 'history'
),
fcast AS (
    SELECT acct, p50 AS fcast_value
    FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast
    WHERE horizon_m = 12  -- first forecast year (1yr out)
      AND series_kind = 'forecast'
      AND variant_id = '__forecast__'
      AND coalesce(is_outlier, false) = false
),
seam AS (
    SELECT
        h.acct,
        h.hist_value,
        f.fcast_value,
        CASE WHEN h.hist_value > 0
             THEN ((f.fcast_value - h.hist_value) / h.hist_value * 100)
             ELSE NULL
        END AS pct_change_1yr
    FROM hist h
    JOIN fcast f USING (acct)
    WHERE h.hist_value > 0  -- exclude zero-value parcels
)
SELECT
    count(*) AS total_parcels,
    count(*) FILTER (WHERE abs(pct_change_1yr) > 50)  AS jump_gt_50pct,
    count(*) FILTER (WHERE abs(pct_change_1yr) > 75)  AS jump_gt_75pct,
    count(*) FILTER (WHERE abs(pct_change_1yr) > 100) AS jump_gt_100pct,
    count(*) FILTER (WHERE pct_change_1yr < -50)       AS drop_gt_50pct,
    count(*) FILTER (WHERE pct_change_1yr > 100)       AS spike_gt_100pct,
    round(avg(pct_change_1yr)::numeric, 2)             AS avg_pct_change,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pct_change_1yr)::numeric, 2) AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pct_change_1yr)::numeric, 2) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pct_change_1yr)::numeric, 2) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pct_change_1yr)::numeric, 2) AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pct_change_1yr)::numeric, 2) AS p95
FROM seam;

-- Also show the worst offenders (top 20 biggest jumps)
-- Run this separately if needed:
-- SELECT acct, hist_value, fcast_value, round(pct_change_1yr::numeric, 1) AS pct_jump
-- FROM seam
-- WHERE abs(pct_change_1yr) > 50
-- ORDER BY abs(pct_change_1yr) DESC
-- LIMIT 20;

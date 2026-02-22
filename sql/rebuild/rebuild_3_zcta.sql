-- =============================================================================
-- ZCTA aggregate rebuild — optimized for Supabase SQL Editor (<60s/query)
-- Run DELETE first, then each INSERT block separately.
-- =============================================================================

-- STEP 1: DELETE
DELETE FROM forecast_20260220_7f31c6e4.metrics_zcta_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

-- STEP 2: One INSERT per horizon (run each alone)

-- ── horizon 12 ──
INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(zcta5, origin_year, horizon_m, forecast_year,
 value, p10, p25, p50, p75, p90, n,
 run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
 is_backtest, series_kind, inserted_at, updated_at)
SELECT pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id), '__forecast__',
    MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
    false, 'forecast', now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl USING (acct)
WHERE mp.series_kind = 'forecast' AND mp.variant_id = '__forecast__'
  AND mp.horizon_m = 12 AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year;

-- ── horizon 24 ──
INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(zcta5, origin_year, horizon_m, forecast_year,
 value, p10, p25, p50, p75, p90, n,
 run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
 is_backtest, series_kind, inserted_at, updated_at)
SELECT pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id), '__forecast__',
    MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
    false, 'forecast', now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl USING (acct)
WHERE mp.series_kind = 'forecast' AND mp.variant_id = '__forecast__'
  AND mp.horizon_m = 24 AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year;

-- ── horizon 36 ──
INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(zcta5, origin_year, horizon_m, forecast_year,
 value, p10, p25, p50, p75, p90, n,
 run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
 is_backtest, series_kind, inserted_at, updated_at)
SELECT pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id), '__forecast__',
    MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
    false, 'forecast', now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl USING (acct)
WHERE mp.series_kind = 'forecast' AND mp.variant_id = '__forecast__'
  AND mp.horizon_m = 36 AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year;

-- ── horizon 48 ──
INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(zcta5, origin_year, horizon_m, forecast_year,
 value, p10, p25, p50, p75, p90, n,
 run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
 is_backtest, series_kind, inserted_at, updated_at)
SELECT pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id), '__forecast__',
    MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
    false, 'forecast', now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl USING (acct)
WHERE mp.series_kind = 'forecast' AND mp.variant_id = '__forecast__'
  AND mp.horizon_m = 48 AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year;

-- ── horizon 60 ──
INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(zcta5, origin_year, horizon_m, forecast_year,
 value, p10, p25, p50, p75, p90, n,
 run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
 is_backtest, series_kind, inserted_at, updated_at)
SELECT pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id), '__forecast__',
    MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
    false, 'forecast', now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl USING (acct)
WHERE mp.series_kind = 'forecast' AND mp.variant_id = '__forecast__'
  AND mp.horizon_m = 60 AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year;

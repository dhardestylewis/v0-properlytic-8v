-- =============================================================================
-- LAYER 2 STEP 3: Rebuild aggregates excluding outliers
-- Run AFTER layer2_flag_outliers.sql has been executed.
-- Run each level separately if timeout occurs.
-- =============================================================================

-- ─── TABBLOCK aggregates ─────────────────────────────────────────────────────
DELETE FROM forecast_20260220_7f31c6e4.metrics_tabblock_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

INSERT INTO forecast_20260220_7f31c6e4.metrics_tabblock_forecast
(
    tabblock_geoid20,
    origin_year, horizon_m, forecast_year,
    value, p10, p25, p50, p75, p90, n,
    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
    is_backtest, series_kind,
    inserted_at, updated_at
)
SELECT
    pl.tabblock_geoid20,
    mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8,
    AVG(mp.p10)::float8,
    AVG(mp.p25)::float8,
    AVG(mp.p50)::float8,
    AVG(mp.p75)::float8,
    AVG(mp.p90)::float8,
    COUNT(*)::int AS n,
    mp.run_id,
    mp.backtest_id,
    mp.variant_id,
    mp.model_version,
    mp.as_of_date,
    MAX(mp.n_scenarios)::int,
    mp.is_backtest,
    mp.series_kind,
    now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
WHERE mp.series_kind = 'forecast'
  AND mp.variant_id = '__forecast__'
  AND coalesce(mp.is_outlier, false) = false
  AND pl.tabblock_geoid20 IS NOT NULL
GROUP BY pl.tabblock_geoid20, mp.origin_year, mp.horizon_m, mp.forecast_year,
         mp.run_id, mp.backtest_id, mp.variant_id, mp.model_version,
         mp.as_of_date, mp.is_backtest, mp.series_kind;


-- ─── TRACT aggregates ────────────────────────────────────────────────────────
DELETE FROM forecast_20260220_7f31c6e4.metrics_tract_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

INSERT INTO forecast_20260220_7f31c6e4.metrics_tract_forecast
(
    tract_geoid20,
    origin_year, horizon_m, forecast_year,
    value, p10, p25, p50, p75, p90, n,
    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
    is_backtest, series_kind,
    inserted_at, updated_at
)
SELECT
    pl.tract_geoid20,
    mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8,
    AVG(mp.p10)::float8,
    AVG(mp.p25)::float8,
    AVG(mp.p50)::float8,
    AVG(mp.p75)::float8,
    AVG(mp.p90)::float8,
    COUNT(*)::int AS n,
    mp.run_id,
    mp.backtest_id,
    mp.variant_id,
    mp.model_version,
    mp.as_of_date,
    MAX(mp.n_scenarios)::int,
    mp.is_backtest,
    mp.series_kind,
    now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
WHERE mp.series_kind = 'forecast'
  AND mp.variant_id = '__forecast__'
  AND coalesce(mp.is_outlier, false) = false
  AND pl.tract_geoid20 IS NOT NULL
GROUP BY pl.tract_geoid20, mp.origin_year, mp.horizon_m, mp.forecast_year,
         mp.run_id, mp.backtest_id, mp.variant_id, mp.model_version,
         mp.as_of_date, mp.is_backtest, mp.series_kind;


-- ─── ZCTA aggregates ─────────────────────────────────────────────────────────
DELETE FROM forecast_20260220_7f31c6e4.metrics_zcta_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_forecast
(
    zcta5,
    origin_year, horizon_m, forecast_year,
    value, p10, p25, p50, p75, p90, n,
    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
    is_backtest, series_kind,
    inserted_at, updated_at
)
SELECT
    pl.zcta5,
    mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8,
    AVG(mp.p10)::float8,
    AVG(mp.p25)::float8,
    AVG(mp.p50)::float8,
    AVG(mp.p75)::float8,
    AVG(mp.p90)::float8,
    COUNT(*)::int AS n,
    mp.run_id,
    mp.backtest_id,
    mp.variant_id,
    mp.model_version,
    mp.as_of_date,
    MAX(mp.n_scenarios)::int,
    mp.is_backtest,
    mp.series_kind,
    now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
WHERE mp.series_kind = 'forecast'
  AND mp.variant_id = '__forecast__'
  AND coalesce(mp.is_outlier, false) = false
  AND pl.zcta5 IS NOT NULL
GROUP BY pl.zcta5, mp.origin_year, mp.horizon_m, mp.forecast_year,
         mp.run_id, mp.backtest_id, mp.variant_id, mp.model_version,
         mp.as_of_date, mp.is_backtest, mp.series_kind;


-- ─── UNSD aggregates ─────────────────────────────────────────────────────────
DELETE FROM forecast_20260220_7f31c6e4.metrics_unsd_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

INSERT INTO forecast_20260220_7f31c6e4.metrics_unsd_forecast
(
    unsd_geoid,
    origin_year, horizon_m, forecast_year,
    value, p10, p25, p50, p75, p90, n,
    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
    is_backtest, series_kind,
    inserted_at, updated_at
)
SELECT
    pl.unsd_geoid,
    mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8,
    AVG(mp.p10)::float8,
    AVG(mp.p25)::float8,
    AVG(mp.p50)::float8,
    AVG(mp.p75)::float8,
    AVG(mp.p90)::float8,
    COUNT(*)::int AS n,
    mp.run_id,
    mp.backtest_id,
    mp.variant_id,
    mp.model_version,
    mp.as_of_date,
    MAX(mp.n_scenarios)::int,
    mp.is_backtest,
    mp.series_kind,
    now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
WHERE mp.series_kind = 'forecast'
  AND mp.variant_id = '__forecast__'
  AND coalesce(mp.is_outlier, false) = false
  AND pl.unsd_geoid IS NOT NULL
GROUP BY pl.unsd_geoid, mp.origin_year, mp.horizon_m, mp.forecast_year,
         mp.run_id, mp.backtest_id, mp.variant_id, mp.model_version,
         mp.as_of_date, mp.is_backtest, mp.series_kind;


-- ─── NEIGHBORHOOD aggregates ─────────────────────────────────────────────────
DELETE FROM forecast_20260220_7f31c6e4.metrics_neighborhood_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__';

INSERT INTO forecast_20260220_7f31c6e4.metrics_neighborhood_forecast
(
    neighborhood_id,
    origin_year, horizon_m, forecast_year,
    value, p10, p25, p50, p75, p90, n,
    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
    is_backtest, series_kind,
    inserted_at, updated_at
)
SELECT
    pl.neighborhood_id,
    mp.origin_year, mp.horizon_m, mp.forecast_year,
    AVG(mp.value)::float8,
    AVG(mp.p10)::float8,
    AVG(mp.p25)::float8,
    AVG(mp.p50)::float8,
    AVG(mp.p75)::float8,
    AVG(mp.p90)::float8,
    COUNT(*)::int AS n,
    mp.run_id,
    mp.backtest_id,
    mp.variant_id,
    mp.model_version,
    mp.as_of_date,
    MAX(mp.n_scenarios)::int,
    mp.is_backtest,
    mp.series_kind,
    now(), now()
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast mp
JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
WHERE mp.series_kind = 'forecast'
  AND mp.variant_id = '__forecast__'
  AND coalesce(mp.is_outlier, false) = false
  AND pl.neighborhood_id IS NOT NULL
GROUP BY pl.neighborhood_id, mp.origin_year, mp.horizon_m, mp.forecast_year,
         mp.run_id, mp.backtest_id, mp.variant_id, mp.model_version,
         mp.as_of_date, mp.is_backtest, mp.series_kind;


-- ─── VERIFICATION ────────────────────────────────────────────────────────────
SELECT 'tabblock' AS level, count(*) FROM forecast_20260220_7f31c6e4.metrics_tabblock_forecast WHERE series_kind='forecast'
UNION ALL
SELECT 'tract', count(*) FROM forecast_20260220_7f31c6e4.metrics_tract_forecast WHERE series_kind='forecast'
UNION ALL
SELECT 'zcta', count(*) FROM forecast_20260220_7f31c6e4.metrics_zcta_forecast WHERE series_kind='forecast'
UNION ALL
SELECT 'unsd', count(*) FROM forecast_20260220_7f31c6e4.metrics_unsd_forecast WHERE series_kind='forecast'
UNION ALL
SELECT 'neighborhood', count(*) FROM forecast_20260220_7f31c6e4.metrics_neighborhood_forecast WHERE series_kind='forecast';

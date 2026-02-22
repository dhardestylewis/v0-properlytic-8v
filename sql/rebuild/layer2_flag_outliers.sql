-- =============================================================================
-- LAYER 2: Flag outlier parcels + rebuild aggregates excluding them
-- Run in Supabase SQL Editor (each section separately if timeout occurs)
-- =============================================================================

-- ─── STEP 1: Add is_outlier column ───────────────────────────────────────────
ALTER TABLE forecast_20260220_7f31c6e4.metrics_parcel_forecast
  ADD COLUMN IF NOT EXISTS is_outlier boolean DEFAULT false;

-- ─── STEP 2: Flag outlier parcels ────────────────────────────────────────────
-- Criteria (data-driven from diagnostic results):
--   • p50 < 0         → negative prediction (physically impossible)
--   • p50 < 1000      → sub-$1K prediction (below historical p01 ≈ $131–$206)
--   • p50 > 5000000   → above $5M (well past p99 ≈ $2.5–$4M)
-- These thresholds capture the broken predictions without touching the
-- reasonable range ($13K p05 to $928K p95).
UPDATE forecast_20260220_7f31c6e4.metrics_parcel_forecast
SET is_outlier = true
WHERE series_kind = 'forecast'
  AND variant_id = '__forecast__'
  AND (
    coalesce(p50, value) <= 0
    OR coalesce(p50, value) < 1000
    OR coalesce(p50, value) > 5000000
  );

-- Check how many got flagged:
SELECT
  is_outlier,
  count(*) AS n,
  round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast
WHERE series_kind = 'forecast' AND variant_id = '__forecast__'
GROUP BY is_outlier;

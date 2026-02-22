#!/usr/bin/env python3
"""
Flag seam-jump outliers and rebuild aggregates.

A "seam jump" is a large discontinuity between the last historical value
and the first forecast prediction. Uses a fixed 50% threshold.

Then rebuilds aggregates using the SAME logic as rebuild_aggregates.py
(parcel_ladder_v1 for geo joins).

Usage:
  set SUPABASE_DB_URL=postgres://...
  python scripts/flag_seam_outliers.py
"""
import os, sys, time
import psycopg2

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    print("ERROR: Set SUPABASE_DB_URL env var")
    sys.exit(1)

SCHEMA = "forecast_20260220_7f31c6e4"
THRESHOLD = 50.0  # absolute seam-jump % threshold
HORIZONS = [12, 24, 36, 48, 60]

AGG_LEVELS = [
    ("tabblock",      "tabblock_geoid20", "metrics_tabblock_forecast"),
    ("tract",         "tract_geoid20",    "metrics_tract_forecast"),
    ("zcta",          "zcta5",            "metrics_zcta_forecast"),
    ("unsd",          "unsd_geoid",       "metrics_unsd_forecast"),
    ("neighborhood",  "neighborhood_id",  "metrics_neighborhood_forecast"),
]

def run():
    conn = psycopg2.connect(DB_URL)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # ── Step 1: Compute distribution ──────────────────────────────
    print("=" * 60)
    print("Step 1: Seam-jump distribution")
    print("=" * 60)

    cur.execute(f"""
    WITH last_hist AS (
        SELECT DISTINCT ON (acct) acct, value AS hist_value
        FROM {SCHEMA}.metrics_parcel_history
        WHERE series_kind = 'history' AND value > 0
        ORDER BY acct, year DESC
    ),
    first_fcast AS (
        SELECT acct, p50 AS fcast_value
        FROM {SCHEMA}.metrics_parcel_forecast
        WHERE horizon_m = 12 AND p50 > 0
    ),
    seam AS (
        SELECT h.acct,
            ((f.fcast_value - h.hist_value) / h.hist_value * 100) AS seam_pct
        FROM last_hist h JOIN first_fcast f ON h.acct = f.acct
    )
    SELECT
        count(*) AS total,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(seam_pct))::numeric, 1) AS abs_p95,
        round(percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(seam_pct))::numeric, 1) AS abs_p99,
        count(*) FILTER (WHERE abs(seam_pct) > {THRESHOLD}) AS gt_threshold
    FROM seam
    """)
    row = cur.fetchone()
    total, abs_p95, abs_p99, gt_threshold = row
    print(f"  Total parcels: {total:,}")
    print(f"  |seam| p95: {abs_p95}%")
    print(f"  |seam| p99: {abs_p99}%")
    print(f"  Parcels with |seam| > {THRESHOLD}%: {gt_threshold:,}")

    # ── Step 2: Flag seam-jump parcels ────────────────────────────
    print(f"\n{'='*60}")
    print(f"Step 2: Flagging parcels with |seam| > {THRESHOLD}%")
    print(f"{'='*60}")

    cur.execute(f"""
    WITH last_hist AS (
        SELECT DISTINCT ON (acct) acct, value AS hist_value
        FROM {SCHEMA}.metrics_parcel_history
        WHERE series_kind = 'history' AND value > 0
        ORDER BY acct, year DESC
    ),
    first_fcast AS (
        SELECT acct, p50 AS fcast_value
        FROM {SCHEMA}.metrics_parcel_forecast
        WHERE horizon_m = 12 AND p50 > 0
    ),
    seam_outliers AS (
        SELECT h.acct
        FROM last_hist h JOIN first_fcast f ON h.acct = f.acct
        WHERE abs((f.fcast_value - h.hist_value) / h.hist_value * 100) > {THRESHOLD}
    )
    UPDATE {SCHEMA}.metrics_parcel_forecast f
    SET is_outlier = true
    FROM seam_outliers s
    WHERE f.acct = s.acct
      AND coalesce(f.is_outlier, false) = false
    """)
    forecast_flagged = cur.rowcount
    print(f"  Flagged {forecast_flagged:,} forecast rows")

    # Total outlier count
    cur.execute(f"""
    SELECT count(DISTINCT acct)
    FROM {SCHEMA}.metrics_parcel_forecast
    WHERE is_outlier = true
    """)
    total_outlier = cur.fetchone()[0]
    print(f"  Total outlier parcels (all reasons): {total_outlier:,}")

    # ── Step 3: Rebuild aggregates (same logic as rebuild_aggregates.py) ──
    print(f"\n{'='*60}")
    print("Step 3: Rebuilding aggregates (excluding outliers)")
    print(f"{'='*60}")

    for level_name, geoid_col, table_name in AGG_LEVELS:
        print(f"\n  {level_name.upper()} — {table_name}")

        # Delete forecast rows
        cur.execute(f"""
            DELETE FROM {SCHEMA}.{table_name}
            WHERE series_kind = 'forecast' AND variant_id = '__forecast__'
        """)
        print(f"    DELETE: {cur.rowcount} rows removed")

        for h in HORIZONS:
            t0 = time.time()
            cur.execute(f"""
                INSERT INTO {SCHEMA}.{table_name}
                ({geoid_col}, origin_year, horizon_m, forecast_year,
                 value, p10, p25, p50, p75, p90, n,
                 run_id, backtest_id, variant_id, model_version,
                 as_of_date, n_scenarios, is_backtest, series_kind,
                 inserted_at, updated_at)
                SELECT
                    pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year,
                    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
                    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
                    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id),
                    '__forecast__', MAX(mp.model_version), MAX(mp.as_of_date),
                    MAX(mp.n_scenarios)::int, false, 'forecast', now(), now()
                FROM {SCHEMA}.metrics_parcel_forecast mp
                JOIN public.parcel_ladder_v1 pl USING (acct)
                WHERE mp.series_kind = 'forecast'
                  AND mp.variant_id = '__forecast__'
                  AND mp.horizon_m = {h}
                  AND coalesce(mp.is_outlier, false) = false
                  AND pl.{geoid_col} IS NOT NULL
                GROUP BY pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
            """)
            elapsed = time.time() - t0
            print(f"    horizon {h:>2}m → {cur.rowcount:>6} rows  ({elapsed:.1f}s)")

    # ── Step 4: Verify ────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  VERIFICATION")
    print(f"{'='*60}")
    for level_name, _, table_name in AGG_LEVELS:
        cur.execute(f"SELECT count(*) FROM {SCHEMA}.{table_name} WHERE series_kind = 'forecast'")
        cnt = cur.fetchone()[0]
        print(f"  {level_name:<15} {cnt:>8} rows")

    cur.close()
    conn.close()
    print("\n✅ Done! Seam-jump outliers flagged and aggregates rebuilt.")

if __name__ == "__main__":
    run()

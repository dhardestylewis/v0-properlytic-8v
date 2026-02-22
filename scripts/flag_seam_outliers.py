#!/usr/bin/env python3
"""
Flag seam-jump outliers and rebuild aggregates.

A "seam jump" is a large discontinuity between the last historical value
and the first forecast prediction. We use the historical year-over-year
p99 absolute change as the threshold — any parcel with a seam jump
exceeding this is flagged as is_outlier.

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

def run():
    conn = psycopg2.connect(DB_URL)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # ── Step 1: Compute threshold from seam-jump distribution ──────────
    print("=" * 60)
    print("Step 1: Computing seam-jump threshold")
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
        count(*) FILTER (WHERE abs(seam_pct) > 50) AS gt50
    FROM seam
    """)
    row = cur.fetchone()
    total, abs_p95, abs_p99, gt50 = row
    print(f"  Total parcels with seam data: {total:,}")
    print(f"  |seam_pct| p95: {abs_p95}%")
    print(f"  |seam_pct| p99: {abs_p99}%")
    print(f"  Parcels with |seam| > 50%: {gt50:,}")

    # Use p99 as threshold, but cap at 50% minimum
    threshold = max(float(abs_p99), 50.0)
    print(f"\n  ➤ Using threshold: ±{threshold}%")

    # ── Step 2: Flag seam-jump parcels as is_outlier ───────────────────
    print("\n" + "=" * 60)
    print(f"Step 2: Flagging parcels with |seam_pct| > {threshold}%")
    print("=" * 60)

    # Flag in metrics_parcel_forecast
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
        WHERE abs((f.fcast_value - h.hist_value) / h.hist_value * 100) > {threshold}
    )
    UPDATE {SCHEMA}.metrics_parcel_forecast f
    SET is_outlier = true
    FROM seam_outliers s
    WHERE f.acct = s.acct
      AND f.is_outlier IS NOT TRUE
    """)
    forecast_flagged = cur.rowcount
    print(f"  Flagged {forecast_flagged:,} forecast rows")

    # Flag in metrics_parcel_history too (add column if missing)
    try:
        cur.execute(f"""
        ALTER TABLE {SCHEMA}.metrics_parcel_history
        ADD COLUMN IF NOT EXISTS is_outlier boolean DEFAULT false
        """)
        print("  Added is_outlier column to metrics_parcel_history")
    except Exception as e:
        print(f"  is_outlier column exists or cannot add: {e}")

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
        WHERE abs((f.fcast_value - h.hist_value) / h.hist_value * 100) > {threshold}
    )
    UPDATE {SCHEMA}.metrics_parcel_history h
    SET is_outlier = true
    FROM seam_outliers s
    WHERE h.acct = s.acct
      AND h.is_outlier IS NOT TRUE
    """)
    history_flagged = cur.rowcount
    print(f"  Flagged {history_flagged:,} history rows")

    # Summary
    cur.execute(f"""
    SELECT count(DISTINCT acct)
    FROM {SCHEMA}.metrics_parcel_forecast
    WHERE is_outlier = true
    """)
    total_outlier = cur.fetchone()[0]
    print(f"\n  Total outlier parcels (all reasons): {total_outlier:,}")

    # ── Step 3: Rebuild aggregates ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 3: Rebuilding aggregates (excluding outliers)")
    print("=" * 60)

    levels = [
        ("tabblock", "tabblock20", "metrics_tabblock_forecast"),
        ("tract", "tract",        "metrics_tract_forecast"),
        ("zcta",  "zcta5",        "metrics_zcta_forecast"),
        ("unsd",  "unsd_geoid",   "metrics_unsd_forecast"),
    ]

    for level_name, geo_col, target_table in levels:
        print(f"\n  Rebuilding {level_name}...")
        t0 = time.time()

        # Delete existing
        cur.execute(f"DELETE FROM {SCHEMA}.{target_table}")
        deleted = cur.rowcount
        print(f"    Deleted {deleted:,} old rows")

        # Rebuild from non-outlier parcels
        cur.execute(f"""
        INSERT INTO {SCHEMA}.{target_table}
        SELECT
            g.{geo_col},
            f.origin_year,
            f.horizon_m,
            f.origin_year + (f.horizon_m / 12) AS forecast_year,
            round(avg(f.value)::numeric, 0) AS value,
            round(percentile_cont(0.10) WITHIN GROUP (ORDER BY f.p50)::numeric, 0) AS p10,
            round(percentile_cont(0.25) WITHIN GROUP (ORDER BY f.p50)::numeric, 0) AS p25,
            round(percentile_cont(0.50) WITHIN GROUP (ORDER BY f.p50)::numeric, 0) AS p50,
            round(percentile_cont(0.75) WITHIN GROUP (ORDER BY f.p50)::numeric, 0) AS p75,
            round(percentile_cont(0.90) WITHIN GROUP (ORDER BY f.p50)::numeric, 0) AS p90,
            count(*) AS n_parcels,
            round(avg(CASE WHEN f.p50 > 0 AND f.value > 0
                  THEN (f.p50 - f.value) / f.value * 100
                  ELSE NULL END)::numeric, 2) AS growth_pct,
            false AS is_outlier
        FROM {SCHEMA}.metrics_parcel_forecast f
        JOIN {SCHEMA}.geo_parcel g ON g.acct = f.acct
        WHERE f.is_outlier IS NOT TRUE
        GROUP BY g.{geo_col}, f.origin_year, f.horizon_m
        """)
        inserted = cur.rowcount
        elapsed = time.time() - t0
        print(f"    Inserted {inserted:,} rows ({elapsed:.1f}s)")

    # ── Step 4: Rebuild history aggregates too ─────────────────────────
    print("\n  Rebuilding history aggregates...")
    history_levels = [
        ("tabblock", "tabblock20", "metrics_tabblock_history"),
        ("tract", "tract",        "metrics_tract_history"),
        ("zcta",  "zcta5",        "metrics_zcta_history"),
    ]

    for level_name, geo_col, target_table in history_levels:
        print(f"\n  Rebuilding {level_name} history...")
        t0 = time.time()

        cur.execute(f"DELETE FROM {SCHEMA}.{target_table}")
        deleted = cur.rowcount
        print(f"    Deleted {deleted:,} old rows")

        cur.execute(f"""
        INSERT INTO {SCHEMA}.{target_table}
        SELECT
            g.{geo_col},
            h.year,
            h.series_kind,
            round(avg(h.value)::numeric, 0) AS value,
            count(*) AS n_parcels,
            false AS is_outlier
        FROM {SCHEMA}.metrics_parcel_history h
        JOIN {SCHEMA}.geo_parcel g ON g.acct = h.acct
        WHERE h.is_outlier IS NOT TRUE
        GROUP BY g.{geo_col}, h.year, h.series_kind
        """)
        inserted = cur.rowcount
        elapsed = time.time() - t0
        print(f"    Inserted {inserted:,} rows ({elapsed:.1f}s)")

    cur.close()
    conn.close()
    print("\n✅ Done! Seam-jump outliers flagged and aggregates rebuilt.")

if __name__ == "__main__":
    run()

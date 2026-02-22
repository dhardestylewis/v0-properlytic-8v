"""
Rebuild forecast aggregates excluding outliers.

Connects directly to Postgres via SUPABASE_DB_URL (bypasses Supabase SQL Editor
gateway timeout). Runs each geography × horizon combination as a separate
transaction.

Usage:
    # Set env var first (same one used by inference_pipeline.py)
    export SUPABASE_DB_URL="postgresql://postgres.xxx:password@host:port/postgres"
    python scripts/rebuild_aggregates.py

    # Or pass inline:
    SUPABASE_DB_URL="postgresql://..." python scripts/rebuild_aggregates.py
"""

import os
import sys
import time
import psycopg2

SCHEMA = "forecast_20260220_7f31c6e4"
HORIZONS = [12, 24, 36, 48, 60]

AGG_LEVELS = [
    ("tabblock",      "tabblock_geoid20", "metrics_tabblock_forecast"),
    ("tract",         "tract_geoid20",    "metrics_tract_forecast"),
    ("zcta",          "zcta5",            "metrics_zcta_forecast"),
    ("unsd",          "unsd_geoid",       "metrics_unsd_forecast"),
    ("neighborhood",  "neighborhood_id",  "metrics_neighborhood_forecast"),
]


def run():
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if not db_url:
        print("ERROR: Set SUPABASE_DB_URL env var first.")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    # Check if is_outlier column exists
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
          AND table_name = 'metrics_parcel_forecast'
          AND column_name = 'is_outlier'
    """)
    has_outlier_col = cur.fetchone() is not None
    outlier_filter = "AND coalesce(mp.is_outlier, false) = false" if has_outlier_col else ""

    if has_outlier_col:
        print("✓ is_outlier column found — filtering outliers")
    else:
        print("⚠ is_outlier column not found — rebuilding WITHOUT outlier filter")
        print("  Run layer2_flag_outliers.sql first if you want outlier exclusion.")

    for level_name, geoid_col, table_name in AGG_LEVELS:
        # DELETE all forecast rows for this level
        print(f"\n{'='*60}")
        print(f"  {level_name.upper()} — {table_name}")
        print(f"{'='*60}")

        del_sql = f"""
            DELETE FROM {SCHEMA}.{table_name}
            WHERE series_kind = 'forecast' AND variant_id = '__forecast__'
        """
        cur.execute(del_sql)
        print(f"  DELETE complete ({cur.rowcount} rows removed)")

        # INSERT one horizon at a time
        for h in HORIZONS:
            t0 = time.time()
            ins_sql = f"""
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
                  {outlier_filter}
                  AND pl.{geoid_col} IS NOT NULL
                GROUP BY pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
            """
            cur.execute(ins_sql)
            elapsed = time.time() - t0
            print(f"  horizon {h:>2}m → {cur.rowcount:>6} rows  ({elapsed:.1f}s)")

    # Verification
    print(f"\n{'='*60}")
    print("  VERIFICATION")
    print(f"{'='*60}")
    for level_name, _, table_name in AGG_LEVELS:
        cur.execute(f"SELECT count(*) FROM {SCHEMA}.{table_name} WHERE series_kind = 'forecast'")
        cnt = cur.fetchone()[0]
        print(f"  {level_name:<15} {cnt:>8} rows")

    cur.close()
    conn.close()
    print("\n✓ Done!")


if __name__ == "__main__":
    run()

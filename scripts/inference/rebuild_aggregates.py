"""Rebuild aggregated history + fix duplicate conflict + verify schema for multi-jurisdiction."""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = False
    cur = conn.cursor()

    # ── 1. Rebuild ZCTA history ────────────────────────────────────
    print("=== Rebuilding metrics_zcta_history ===")
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zcta_history (zcta5, year, value, p50, series_kind, variant_id)
        SELECT
            l.zcta5,
            ph.year,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS p50,
            ph.series_kind,
            ph.variant_id
        FROM {SCHEMA}.metrics_parcel_history ph
        JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
        WHERE l.zcta5 IS NOT NULL
        GROUP BY l.zcta5, ph.year, ph.series_kind, ph.variant_id
        ON CONFLICT (zcta5, year, series_kind, variant_id)
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50
    """)
    print(f"  Inserted/updated: {cur.rowcount:,} rows")
    conn.commit()

    # ── 2. Rebuild tract history ───────────────────────────────────
    print("\n=== Rebuilding metrics_tract_history ===")
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_tract_history (tract_geoid20, year, value, p50, series_kind, variant_id)
        SELECT
            l.tract_geoid20,
            ph.year,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS p50,
            ph.series_kind,
            ph.variant_id
        FROM {SCHEMA}.metrics_parcel_history ph
        JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
        WHERE l.tract_geoid20 IS NOT NULL
        GROUP BY l.tract_geoid20, ph.year, ph.series_kind, ph.variant_id
        ON CONFLICT (tract_geoid20, year, series_kind, variant_id)
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50
    """)
    print(f"  Inserted/updated: {cur.rowcount:,} rows")
    conn.commit()

    # ── 3. Rebuild tabblock history ────────────────────────────────
    print("\n=== Rebuilding metrics_tabblock_history ===")
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_tabblock_history (tabblock_geoid20, year, value, p50, series_kind, variant_id)
        SELECT
            l.tabblock_geoid20,
            ph.year,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value) AS p50,
            ph.series_kind,
            ph.variant_id
        FROM {SCHEMA}.metrics_parcel_history ph
        JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
        WHERE l.tabblock_geoid20 IS NOT NULL
        GROUP BY l.tabblock_geoid20, ph.year, ph.series_kind, ph.variant_id
        ON CONFLICT (tabblock_geoid20, year, series_kind, variant_id)
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50
    """)
    print(f"  Inserted/updated: {cur.rowcount:,} rows")
    conn.commit()

    # ── 4. Verify counts ──────────────────────────────────────────
    print("\n=== Verification ===")
    for tbl in ['metrics_zcta_history', 'metrics_tract_history', 'metrics_tabblock_history']:
        cur.execute(f'SELECT COUNT(*) FROM {SCHEMA}.{tbl}')
        print(f"  {tbl}: {cur.fetchone()[0]:,} rows")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()

"""Diagnostic: check history & forecast table schemas, then compare
historical YoY changes vs forecast seam jumps."""

import os, sys, psycopg2

db_url = os.environ.get("SUPABASE_DB_URL", "")
if not db_url:
    print("ERROR: Set SUPABASE_DB_URL"); sys.exit(1)

conn = psycopg2.connect(db_url)
cur = conn.cursor()

# 1) Schema of history table
print("=== metrics_parcel_history columns ===")
cur.execute("""SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='forecast_20260220_7f31c6e4' AND table_name='metrics_parcel_history'
ORDER BY ordinal_position""")
for r in cur.fetchall():
    print(f"  {r[0]:>25}: {r[1]}")

# 2) Schema of forecast table
print("\n=== metrics_parcel_forecast columns ===")
cur.execute("""SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='forecast_20260220_7f31c6e4' AND table_name='metrics_parcel_forecast'
ORDER BY ordinal_position""")
for r in cur.fetchall():
    print(f"  {r[0]:>25}: {r[1]}")

# 3) Sample from history
print("\n=== Sample history rows ===")
cur.execute("""SELECT * FROM forecast_20260220_7f31c6e4.metrics_parcel_history LIMIT 3""")
cols = [d[0] for d in cur.description]
print("  Columns:", cols)
for row in cur.fetchall():
    print("  ", dict(zip(cols, row)))

# 4) Historical YoY distribution (data-driven threshold)
print("\n=== Historical YoY % changes (all parcels, all year pairs) ===")
cur.execute("""
WITH yoy AS (
    SELECT acct, year,
        value AS val,
        LAG(value) OVER (PARTITION BY acct ORDER BY year) AS prev_val
    FROM forecast_20260220_7f31c6e4.metrics_parcel_history
    WHERE series_kind = 'history'
),
changes AS (
    SELECT acct, year,
        CASE WHEN prev_val > 0 THEN ((val - prev_val) / prev_val * 100) ELSE NULL END AS yoy_pct
    FROM yoy
    WHERE prev_val IS NOT NULL AND prev_val > 0
)
SELECT
    count(*) AS n,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p99
FROM changes
""")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
for c, v in zip(cols, row):
    print(f"  {c:>6}: {v}")

# 5) Forecast seam distribution (last history -> first forecast)
print("\n=== Seam jump: last history vs first forecast ===")
cur.execute("""
WITH last_hist AS (
    SELECT DISTINCT ON (acct) acct, value AS hist_value
    FROM forecast_20260220_7f31c6e4.metrics_parcel_history
    WHERE series_kind = 'history' AND value > 0
    ORDER BY acct, year DESC
),
first_fcast AS (
    SELECT acct, p50 AS fcast_value
    FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast
    WHERE horizon_m = 12
      AND series_kind = 'forecast'
      AND variant_id = '__forecast__'
      AND coalesce(is_outlier, false) = false
),
seam AS (
    SELECT h.acct, h.hist_value, f.fcast_value,
        CASE WHEN h.hist_value > 0
             THEN ((f.fcast_value - h.hist_value) / h.hist_value * 100)
             ELSE NULL END AS pct_jump
    FROM last_hist h JOIN first_fcast f USING (acct)
    WHERE h.hist_value > 0
)
SELECT
    count(*) AS total,
    count(*) FILTER (WHERE abs(pct_jump) > 50) AS jump_gt50,
    count(*) FILTER (WHERE abs(pct_jump) > 75) AS jump_gt75,
    count(*) FILTER (WHERE abs(pct_jump) > 100) AS jump_gt100,
    round(percentile_cont(0.01) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p01,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p05,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p75,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pct_jump)::numeric, 1) AS p99
FROM seam
""")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
for c, v in zip(cols, row):
    print(f"  {c:>10}: {v}")

cur.close()
conn.close()
print("\nDone!")

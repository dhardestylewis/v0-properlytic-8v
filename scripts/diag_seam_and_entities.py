#!/usr/bin/env python3
"""
Diagnostic: Seam-jump outliers + high-activity buying/selling entities.

Part 1: Compare historical year-over-year % changes with forecast "seam jumps"
         (last historical value → first forecast prediction). Use historical
         distribution to set data-driven thresholds.

Part 2: Identify high-activity entities (owners) from year-over-year ownership
         changes in the HCAD master panel on Google Drive.

Usage:
  set SUPABASE_DB_URL=postgres://...
  python scripts/diag_seam_and_entities.py
"""
import os, sys

# ── Part 1: Seam-jump outliers (Supabase) ──────────────────────────────────
def seam_jump_analysis():
    import psycopg2
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("⚠ SUPABASE_DB_URL not set — skipping seam-jump analysis")
        return
    conn = psycopg2.connect(db_url)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    print("=" * 60)
    print("PART 1: Historical YoY vs Forecast Seam Jumps")
    print("=" * 60)

    # Historical YoY % change distribution
    print("\n── Historical year-over-year % changes ──")
    cur.execute("""
    WITH yoy AS (
        SELECT acct, year, value AS val,
            LAG(value) OVER (PARTITION BY acct ORDER BY year) AS prev_val
        FROM forecast_20260220_7f31c6e4.metrics_parcel_history
        WHERE series_kind = 'history'
    ),
    changes AS (
        SELECT acct, year, 
            CASE WHEN prev_val > 0 THEN ((val - prev_val) / prev_val * 100) ELSE NULL END AS yoy_pct
        FROM yoy WHERE prev_val IS NOT NULL AND prev_val > 0
    )
    SELECT
        count(*) AS n,
        round(percentile_cont(0.01) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p01,
        round(percentile_cont(0.05) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p05,
        round(percentile_cont(0.10) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p10,
        round(percentile_cont(0.25) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p25,
        round(percentile_cont(0.50) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p50,
        round(percentile_cont(0.75) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p75,
        round(percentile_cont(0.90) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p90,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p95,
        round(percentile_cont(0.99) WITHIN GROUP (ORDER BY yoy_pct)::numeric, 1) AS p99,
        round(min(yoy_pct)::numeric, 1) AS min_val,
        round(max(yoy_pct)::numeric, 1) AS max_val
    FROM changes
    """)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    for col, val in zip(cols, row):
        print(f"  {col:>12}: {val}")

    # Seam jump distribution
    print("\n── Forecast seam jumps (last hist → first forecast) ──")
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
        WHERE horizon_m = 12 AND p50 > 0
    ),
    seam AS (
        SELECT h.acct, 
            ((f.fcast_value - h.hist_value) / h.hist_value * 100) AS seam_pct
        FROM last_hist h JOIN first_fcast f ON h.acct = f.acct
    )
    SELECT
        count(*) AS total,
        count(*) FILTER (WHERE abs(seam_pct) > 30)  AS jump_gt30,
        count(*) FILTER (WHERE abs(seam_pct) > 50)  AS jump_gt50,
        count(*) FILTER (WHERE abs(seam_pct) > 75)  AS jump_gt75,
        count(*) FILTER (WHERE abs(seam_pct) > 100) AS jump_gt100,
        round(percentile_cont(0.01) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p01,
        round(percentile_cont(0.05) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p05,
        round(percentile_cont(0.10) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p10,
        round(percentile_cont(0.25) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p25,
        round(percentile_cont(0.50) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p50,
        round(percentile_cont(0.75) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p75,
        round(percentile_cont(0.90) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p90,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p95,
        round(percentile_cont(0.99) WITHIN GROUP (ORDER BY seam_pct)::numeric, 1) AS p99,
        round(min(seam_pct)::numeric, 1) AS min_val,
        round(max(seam_pct)::numeric, 1) AS max_val
    FROM seam
    """)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    for col, val in zip(cols, row):
        print(f"  {col:>12}: {val}")

    # Suggested threshold
    hist_p99 = float(row[cols.index('p99')]) if 'p99' in cols else 50.0
    print(f"\n  ➤ Suggested seam-jump threshold: ±{abs(hist_p99):.0f}% (based on seam p99)")

    cur.close()
    conn.close()

# ── Part 2: High-activity entities (Google Drive panel) ────────────────────
def entity_analysis():
    try:
        import polars as pl
    except ImportError:
        print("\n⚠ polars not installed — install with: pip install polars")
        print("  Skipping entity analysis")
        return

    panel_path = r"G:\My Drive\HCAD_Archive_Aggregates\hcad_master_panel"
    if not os.path.exists(panel_path):
        print(f"\n⚠ Panel data not found at: {panel_path}")
        return

    print("\n" + "=" * 60)
    print("PART 2: High-Activity Buying/Selling Entities")
    print("=" * 60)

    # Read partitioned parquet
    print("\nReading panel data...")
    df = pl.read_parquet(panel_path)
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {df.columns}")

    # Check for owner column
    owner_cols = [c for c in df.columns if 'owner' in c.lower() or 'ownr' in c.lower()]
    print(f"  Owner columns found: {owner_cols}")

    if not owner_cols:
        print("  ⚠ No owner columns found — checking all columns...")
        for c in df.columns:
            print(f"    {c}: {df[c].dtype}")
        return

    owner_col = owner_cols[0]
    print(f"\n  Using owner column: {owner_col}")

    # Detect ownership changes by year
    df_sorted = df.sort(["acct", "year"])
    df_with_prev = df_sorted.with_columns(
        pl.col(owner_col).shift(1).over("acct").alias("prev_owner"),
        pl.col("year").shift(1).over("acct").alias("prev_year"),
    )
    # A "transaction" = owner changed from one year to the next
    transactions = df_with_prev.filter(
        (pl.col(owner_col) != pl.col("prev_owner"))
        & pl.col("prev_owner").is_not_null()
        & (pl.col("year") - pl.col("prev_year") == 1)  # consecutive years only
    )
    print(f"  Total ownership changes detected: {len(transactions):,}")

    # Top sellers (prev_owner lost the property)
    print("\n── Top 20 Sellers (entities that sold the most properties) ──")
    top_sellers = (
        transactions
        .group_by("prev_owner")
        .agg(pl.len().alias("sales_count"))
        .sort("sales_count", descending=True)
        .head(20)
    )
    for row in top_sellers.iter_rows(named=True):
        print(f"  {row['sales_count']:>5}  {row['prev_owner']}")

    # Top buyers (new owner gained the property)
    print("\n── Top 20 Buyers (entities that bought the most properties) ──")
    top_buyers = (
        transactions
        .group_by(owner_col)
        .agg(pl.len().alias("buys_count"))
        .sort("buys_count", descending=True)
        .head(20)
    )
    for row in top_buyers.iter_rows(named=True):
        print(f"  {row['buys_count']:>5}  {row[owner_col]}")

    # Net activity (buys - sells)
    print("\n── Top 20 Net Accumulators (bought more than sold) ──")
    buys = transactions.group_by(owner_col).agg(pl.len().alias("buys"))
    sells = transactions.group_by("prev_owner").agg(pl.len().alias("sells"))
    net = (
        buys.rename({owner_col: "entity"})
        .join(sells.rename({"prev_owner": "entity"}), on="entity", how="outer")
        .with_columns(
            pl.col("buys").fill_null(0),
            pl.col("sells").fill_null(0),
        )
        .with_columns((pl.col("buys") - pl.col("sells")).alias("net"))
        .sort("net", descending=True)
    )
    for row in net.head(20).iter_rows(named=True):
        print(f"  net {row['net']:>+5} (bought {row['buys']}, sold {row['sells']})  {row['entity']}")

    print("\n── Top 20 Net Liquidators (sold more than bought) ──")
    for row in net.sort("net").head(20).iter_rows(named=True):
        print(f"  net {row['net']:>+5} (bought {row['buys']}, sold {row['sells']})  {row['entity']}")


if __name__ == "__main__":
    seam_jump_analysis()
    entity_analysis()
    print("\nDone!")

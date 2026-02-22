"""
🏠 High-Activity Entity Analysis — Colab Cell (v2)
Paste into a Colab cell. Requires HCAD master panel on Google Drive.

Fixes:
- Reads Hive-partitioned parquet with `hive_partitioning=True`
- Auto-detects the actual owner name column (not aggregated columns)
"""

# !pip install polars pyarrow -q  # Uncomment if needed

import polars as pl
import os

# ── Config ────────────────────────────────────────────────────────
PANEL_PATH = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel"

# ── Load ──────────────────────────────────────────────────────────
print("Loading panel data (Hive-partitioned)...")
if os.path.isdir(PANEL_PATH):
    df = pl.read_parquet(PANEL_PATH, hive_partitioning=True)
else:
    df = pl.read_parquet(PANEL_PATH)
print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
print(f"  Column list: {sorted(df.columns)}")

# ── Check for year column ─────────────────────────────────────────
if "year" not in df.columns:
    print("\n⚠ 'year' column not found. Available columns:")
    for c in sorted(df.columns):
        print(f"    {c}: {df[c].dtype}")
    # Try to find a year-like column
    year_candidates = [c for c in df.columns if 'year' in c.lower() or 'yr' in c.lower()]
    if year_candidates:
        print(f"\n  Found year candidate: {year_candidates[0]}")
        df = df.rename({year_candidates[0]: "year"})
    else:
        raise SystemExit("Cannot find year column")

# ── Find owner NAME column (not aggregated columns) ──────────────
# Skip columns that are clearly aggregated/computed
skip_keywords = ['count', 'nunique', 'any_entity', 'num_', 'n_']
all_owner_cols = [c for c in df.columns if 'owner' in c.lower() or 'ownr' in c.lower()]
name_cols = [c for c in all_owner_cols
             if not any(kw in c.lower() for kw in skip_keywords)]
print(f"\n  All owner-related columns: {all_owner_cols}")
print(f"  Likely name columns: {name_cols}")

if not name_cols:
    # Fallback: look for string-type columns with 'owner' in name
    name_cols = [c for c in all_owner_cols if df[c].dtype == pl.Utf8 or df[c].dtype == pl.String]
    print(f"  String owner columns: {name_cols}")

if not name_cols:
    print("\n⚠ No owner name column found. Showing sample of all owner-related:")
    for c in all_owner_cols:
        print(f"    {c}: {df[c].dtype} → sample: {df[c].head(3).to_list()}")
    # Try any string column with relevant name patterns
    for c in df.columns:
        if df[c].dtype in (pl.Utf8, pl.String):
            sample = df[c].head(3).to_list()
            if any(isinstance(s, str) and len(s) > 5 for s in sample if s):
                print(f"\n  Possible name column: {c} → {sample}")
    raise SystemExit("Cannot identify owner name column. Check output above.")

owner_col = name_cols[0]
print(f"  Using owner column: {owner_col}")
print(f"  Sample values: {df[owner_col].head(5).to_list()}")

# ── Detect Transactions ──────────────────────────────────────────
print("\nDetecting ownership changes...")
df_sorted = df.sort(["acct", "year"])
df_with_prev = df_sorted.with_columns(
    pl.col(owner_col).shift(1).over("acct").alias("prev_owner"),
    pl.col("year").shift(1).over("acct").alias("prev_year"),
)
transactions = df_with_prev.filter(
    (pl.col(owner_col) != pl.col("prev_owner"))
    & pl.col("prev_owner").is_not_null()
    & (pl.col("year") - pl.col("prev_year") == 1)
)
print(f"  Total ownership changes: {len(transactions):,}")
print(f"  Unique parcels involved: {transactions['acct'].n_unique():,}")

# ── Top Sellers ───────────────────────────────────────────────────
print("\n" + "="*60)
print("🔴 TOP 30 SELLERS")
print("="*60)
top_sellers = (
    transactions
    .group_by("prev_owner")
    .agg(pl.len().alias("sales"))
    .sort("sales", descending=True)
    .head(30)
)
for i, row in enumerate(top_sellers.iter_rows(named=True), 1):
    print(f"  {i:>2}. {row['sales']:>5} sales  {row['prev_owner']}")

# ── Top Buyers ────────────────────────────────────────────────────
print("\n" + "="*60)
print("🟢 TOP 30 BUYERS")
print("="*60)
top_buyers = (
    transactions
    .group_by(owner_col)
    .agg(pl.len().alias("buys"))
    .sort("buys", descending=True)
    .head(30)
)
for i, row in enumerate(top_buyers.iter_rows(named=True), 1):
    print(f"  {i:>2}. {row['buys']:>5} buys   {row[owner_col]}")

# ── Net Activity ──────────────────────────────────────────────────
buys = transactions.group_by(owner_col).agg(pl.len().alias("buys"))
sells = transactions.group_by("prev_owner").agg(pl.len().alias("sells"))
net = (
    buys.rename({owner_col: "entity"})
    .join(sells.rename({"prev_owner": "entity"}), on="entity", how="full")
    .with_columns(
        pl.col("buys").fill_null(0),
        pl.col("sells").fill_null(0),
    )
    .with_columns((pl.col("buys") - pl.col("sells")).alias("net"))
)

print("\n" + "="*60)
print("📈 TOP 20 NET ACCUMULATORS")
print("="*60)
for i, row in enumerate(net.sort("net", descending=True).head(20).iter_rows(named=True), 1):
    print(f"  {i:>2}. net {row['net']:>+5} (bought {row['buys']}, sold {row['sells']})  {row['entity']}")

print("\n" + "="*60)
print("📉 TOP 20 NET LIQUIDATORS")
print("="*60)
for i, row in enumerate(net.sort("net").head(20).iter_rows(named=True), 1):
    print(f"  {i:>2}. net {row['net']:>+5} (bought {row['buys']}, sold {row['sells']})  {row['entity']}")

# ── Activity by Year ──────────────────────────────────────────────
print("\n" + "="*60)
print("📅 TRANSACTIONS PER YEAR")
print("="*60)
yearly = transactions.group_by("year").agg(pl.len().alias("count")).sort("year")
for row in yearly.iter_rows(named=True):
    bar = "█" * (row['count'] // 200)
    print(f"  {row['year']}  {row['count']:>6}  {bar}")

print("\n✅ Done!")

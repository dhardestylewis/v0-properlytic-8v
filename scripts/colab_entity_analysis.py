"""
🏢 Named Entity Portfolio Analysis — Colab Cell
Paste into a Colab cell. Loads raw HCAD Real_acct_owner.zip files
to identify actual entity names, portfolio sizes, and buying/selling activity.

Requires: Google Drive mounted at /content/drive/
Data:  /content/drive/MyDrive/HCAD_Archive/property/{YEAR}/Real_acct_owner.zip
       Years available: 2005-2025
       2025 also has Real_acct_ownership_history.zip
"""

# !pip install polars pyarrow -q  # Uncomment if needed

import polars as pl
import os, zipfile, io
from pathlib import Path

# ── Config — exact paths from GDrive inspection ──────────────────
HCAD_BASE = Path("/content/drive/MyDrive/HCAD_Archive/property")
YEARS = list(range(2015, 2026))  # adjust range as needed

# ── Load owner files from zips ────────────────────────────────────
print("� Loading Real_acct_owner.zip for each year...")
owner_frames = []

for yr in YEARS:
    zip_path = HCAD_BASE / str(yr) / "Real_acct_owner.zip"
    if not zip_path.exists():
        print(f"  {yr}: ❌ NOT FOUND at {zip_path}")
        continue

    try:
        with zipfile.ZipFile(str(zip_path), 'r') as zf:
            # List files in the zip to find the owner file
            names = zf.namelist()
            # Find the owner txt/csv file
            owner_file = None
            for n in names:
                if 'owner' in n.lower() and n.endswith('.txt'):
                    owner_file = n
                    break
            if not owner_file:
                # Try any .txt file
                txt_files = [n for n in names if n.endswith('.txt')]
                owner_file = txt_files[0] if txt_files else None

            if not owner_file:
                print(f"  {yr}: ⚠ No .txt file found in zip. Files: {names[:5]}")
                continue

            with zf.open(owner_file) as f:
                raw = f.read()
                # HCAD files use Latin-1 encoding, not UTF-8
                raw_utf8 = raw.decode('latin-1').encode('utf-8')
                df = pl.read_csv(io.BytesIO(raw_utf8), separator="\t",
                               infer_schema_length=10000,
                               ignore_errors=True,
                               truncate_ragged_lines=True,
                               quote_char=None)  # HCAD has unescaped quotes

            # Normalize column names
            df = df.rename({c: c.strip().lower().replace(' ', '_') for c in df.columns})
            df = df.with_columns(pl.lit(yr).alias("year"))

            owner_frames.append(df)
            print(f"  {yr}: ✅ {owner_file} → {len(df):,} rows, cols={df.columns[:5]}...")

    except Exception as e:
        print(f"  {yr}: ⚠ Error: {e}")

if not owner_frames:
    raise SystemExit("No owner files loaded")

# Show schema from first frame
sample = owner_frames[0]
print(f"\n📋 Schema from {YEARS[0]}:")
for col in sample.columns:
    print(f"    {col}: {sample[col].dtype}")

# ── Auto-detect columns ──────────────────────────────────────────
# Owner name column: look for columns containing 'owner' or 'name'
name_candidates = [c for c in sample.columns
                   if any(kw in c for kw in ['owner_nm', 'owner_name', 'ownr_nm'])]
if not name_candidates:
    name_candidates = [c for c in sample.columns
                       if 'name' in c and sample[c].dtype == pl.Utf8]
if not name_candidates:
    name_candidates = [c for c in sample.columns
                       if 'owner' in c and sample[c].dtype == pl.Utf8]

# Account column
acct_candidates = [c for c in sample.columns
                   if any(kw in c for kw in ['acct', 'account', 'prop_id'])]

print(f"\n  Name candidates: {name_candidates}")
print(f"  Acct candidates: {acct_candidates}")

if not name_candidates:
    # Show all string columns as fallback
    str_cols = [c for c in sample.columns if sample[c].dtype == pl.Utf8]
    print(f"\n  All string columns: {str_cols}")
    for c in str_cols[:10]:
        print(f"    {c}: {sample[c].head(3).to_list()}")
    raise SystemExit("Cannot auto-detect name column. Set name_col manually.")

name_col = name_candidates[0]
acct_col = acct_candidates[0] if acct_candidates else sample.columns[0]
print(f"\n  ✓ Using: name_col='{name_col}', acct_col='{acct_col}'")

# ── Combine all years ────────────────────────────────────────────
print(f"\nCombining {len(owner_frames)} years...")
all_owners = pl.concat([
    df.select([acct_col, name_col, "year"]).cast({acct_col: pl.Utf8, name_col: pl.Utf8})
    for df in owner_frames
], how="vertical_relaxed")
print(f"  Total rows: {len(all_owners):,}")

# ── Entity detection ─────────────────────────────────────────────
print("\n🏢 Filtering to entity names...")
entity_keywords = ['LLC', 'INC', 'CORP', 'TRUST', 'LP', 'LTD', 'HOLDINGS',
                   'PROPERTIES', 'INVESTMENTS', 'CAPITAL', 'PARTNERS',
                   'VENTURES', 'GROUP', 'FUND', 'REIT', 'HOMES',
                   'OPENDOOR', 'INVITATION', 'ZILLOW', 'OFFERPAD',
                   'REDFIN', 'CERBERUS', 'BLACKSTONE', 'STARWOOD']

entity_filter = pl.col(name_col).str.to_uppercase().str.contains(
    "|".join(entity_keywords)
)
entities = all_owners.filter(entity_filter)
non_entities = all_owners.filter(~entity_filter)
print(f"  Entity rows: {len(entities):,} ({100*len(entities)/len(all_owners):.1f}%)")
print(f"  Individual rows: {len(non_entities):,}")

# ── 1. Top portfolios (latest year) ──────────────────────────────
print("\n" + "="*60)
print("🏢 TOP 30 ENTITIES BY PORTFOLIO SIZE (latest year)")
print("="*60)
latest_yr = all_owners["year"].max()
latest_entities = entities.filter(pl.col("year") == latest_yr)
portfolios = (
    latest_entities
    .group_by(name_col)
    .agg(pl.col(acct_col).n_unique().alias("parcels"))
    .sort("parcels", descending=True)
    .head(30)
)
for i, row in enumerate(portfolios.iter_rows(named=True), 1):
    bar = "█" * min(row['parcels'] // 10, 80)
    print(f"  {i:>2}. {row['parcels']:>5} parcels  {row[name_col]}  {bar}")

# ── 2. Ownership changes (buy/sell) ──────────────────────────────
print("\n" + "="*60)
print("🔄 TRANSACTIONS — Ownership changes per parcel across years")
print("="*60)
sorted_owners = all_owners.sort([acct_col, "year"])
with_prev = sorted_owners.with_columns(
    pl.col(name_col).shift(1).over(acct_col).alias("prev_owner"),
    pl.col("year").shift(1).over(acct_col).alias("prev_year"),
)
transactions = with_prev.filter(
    pl.col("prev_owner").is_not_null()
    & (pl.col(name_col) != pl.col("prev_owner"))
    & (pl.col("year") - pl.col("prev_year") <= 2)
)
print(f"  Total transactions detected: {len(transactions):,}")

# Transactions by year
txn_by_yr = transactions.group_by("year").agg(pl.len().alias("txns")).sort("year")
for row in txn_by_yr.iter_rows(named=True):
    bar = "█" * (row['txns'] // 500)
    print(f"    {row['year']}  {row['txns']:>6} transactions  {bar}")

# ── 3. Top sellers ────────────────────────────────────────────────
print("\n  🔴 TOP 20 ENTITY SELLERS (all time):")
sellers = (
    transactions
    .filter(pl.col("prev_owner").str.to_uppercase().str.contains("|".join(entity_keywords)))
    .group_by("prev_owner")
    .agg(pl.len().alias("sales"))
    .sort("sales", descending=True)
    .head(20)
)
for i, row in enumerate(sellers.iter_rows(named=True), 1):
    print(f"    {i:>2}. {row['sales']:>5} sales  {row['prev_owner']}")

# ── 4. Top buyers ─────────────────────────────────────────────────
print("\n  🟢 TOP 20 ENTITY BUYERS (all time):")
buyers = (
    transactions
    .filter(entity_filter)
    .group_by(name_col)
    .agg(pl.len().alias("buys"))
    .sort("buys", descending=True)
    .head(20)
)
for i, row in enumerate(buyers.iter_rows(named=True), 1):
    print(f"    {i:>2}. {row['buys']:>5} buys   {row[name_col]}")

# ── 5. Net accumulators/liquidators ───────────────────────────────
print("\n" + "="*60)
print("📊 NET ENTITY ACTIVITY (buys - sells)")
print("="*60)
all_buys = (
    transactions.filter(entity_filter)
    .group_by(name_col).agg(pl.len().alias("buys"))
    .rename({name_col: "entity"})
)
all_sells = (
    transactions
    .filter(pl.col("prev_owner").str.to_uppercase().str.contains("|".join(entity_keywords)))
    .group_by("prev_owner").agg(pl.len().alias("sells"))
    .rename({"prev_owner": "entity"})
)
net = (
    all_buys.join(all_sells, on="entity", how="full")
    .with_columns(pl.col("buys").fill_null(0), pl.col("sells").fill_null(0))
    .with_columns((pl.col("buys") - pl.col("sells")).alias("net"))
)
print("\n  📈 TOP 15 NET ACCUMULATORS:")
for i, row in enumerate(net.sort("net", descending=True).head(15).iter_rows(named=True), 1):
    print(f"    {i:>2}. net {row['net']:>+5} (buy={row['buys']}, sell={row['sells']})  {row['entity']}")

print("\n  📉 TOP 15 NET LIQUIDATORS:")
for i, row in enumerate(net.sort("net").head(15).iter_rows(named=True), 1):
    print(f"    {i:>2}. net {row['net']:>+5} (buy={row['buys']}, sell={row['sells']})  {row['entity']}")

# ── 6. iBuyer / Institutional investor detection ─────────────────
print("\n" + "="*60)
print("🤖 iBUYER & INSTITUTIONAL INVESTOR DETECTION")
print("="*60)
ibuyer_keywords = ['OPENDOOR', 'OFFERPAD', 'ZILLOW', 'REDFIN',
                   'INVITATION HOMES', 'AMERICAN HOMES', 'PROGRESS RESIDENTIAL',
                   'TRICON', 'PRETIUM', 'AMHERST', 'CERBERUS', 'BLACKSTONE',
                   'STARWOOD', 'COLONY', 'FRONT YARD']
for kw in ibuyer_keywords:
    matches = entities.filter(pl.col(name_col).str.to_uppercase().str.contains(kw))
    if len(matches) > 0:
        unique_parcels = matches.select(pl.col(acct_col).n_unique()).item()
        years_active = sorted(matches["year"].unique().to_list())
        print(f"  ✓ {kw}: {unique_parcels} unique parcels, active {years_active[0]}-{years_active[-1]}")
    else:
        print(f"  ✗ {kw}: not found")

# ── 7. Portfolio growth over time ─────────────────────────────────
print("\n" + "="*60)
print("📈 TOP 5 FASTEST-GROWING ENTITY PORTFOLIOS")
print("="*60)
entity_portfolio_by_year = (
    entities
    .group_by([name_col, "year"])
    .agg(pl.col(acct_col).n_unique().alias("parcels"))
    .sort([name_col, "year"])
)
# Get entities with data in both earliest and latest years
min_yr = all_owners["year"].min()
growth = (
    entity_portfolio_by_year
    .filter(pl.col("year").is_in([min_yr, latest_yr]))
    .group_by(name_col)
    .agg([
        pl.col("parcels").filter(pl.col("year") == min_yr).first().alias("start"),
        pl.col("parcels").filter(pl.col("year") == latest_yr).first().alias("end"),
    ])
    .filter(pl.col("start").is_not_null() & pl.col("end").is_not_null())
    .with_columns((pl.col("end") - pl.col("start")).alias("growth"))
    .filter(pl.col("start") >= 10)  # min 10 parcels at start
    .sort("growth", descending=True)
    .head(5)
)
for i, row in enumerate(growth.iter_rows(named=True), 1):
    print(f"  {i}. {row[name_col]}: {row['start']} → {row['end']} parcels (+{row['growth']})")

print("\n✅ Done!")

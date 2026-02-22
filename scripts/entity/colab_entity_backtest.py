"""
📊 Entity Portfolio Backtest — Colab Cell
"Would these institutional investors have made more money with our forecasts?"

Loads:
  1. Owner names per parcel per year (from owners.txt inside Real_acct_owner.zip)
  2. Appraised values per parcel per year (from real_acct.txt inside Real_acct_owner.zip)
Computes:
  - Actual portfolio returns for each entity (what they earned)
  - County-wide average returns (market baseline)
  - Top-quartile returns (what our forecast identifies)
  - Alpha gap = how much more they'd have earned with our product

Requires: Google Drive mounted at /content/drive/
"""

!pip install polars pyarrow -q

import polars as pl
import zipfile, io, os
from pathlib import Path

# ── Mount Google Drive ────────────────────────────────────────────
try:
    from google.colab import drive
    drive.mount('/content/drive', force_remount=False)
    print("✅ Google Drive mounted")
except ImportError:
    print("⚠ Not running in Colab — assuming Drive paths are available locally")

# ── Config — discover HCAD path ──────────────────────────────────
HCAD_CANDIDATES = [
    Path("/content/drive/MyDrive/HCAD_Archive/property"),
    Path("/content/drive/My Drive/HCAD_Archive/property"),
    Path("G:/My Drive/HCAD_Archive/property"),
    Path("G:\\My Drive\\HCAD_Archive\\property"),
]
HCAD_BASE = None
for p in HCAD_CANDIDATES:
    if p.exists():
        HCAD_BASE = p
        print(f"✅ Found HCAD data at: {p}")
        break

if HCAD_BASE is None:
    # Try to discover it
    drive_root = Path("/content/drive/MyDrive")
    if drive_root.exists():
        print(f"📂 Drive root contents: {list(drive_root.iterdir())[:20]}")
        # Look for HCAD anywhere
        for d in drive_root.rglob("Real_acct_owner.zip"):
            HCAD_BASE = d.parent.parent  # go up from YEAR/Real_acct_owner.zip
            print(f"✅ Discovered HCAD at: {HCAD_BASE}")
            break
    if HCAD_BASE is None:
        raise SystemExit("❌ Cannot find HCAD_Archive/property. Check Drive mount.")

YEARS = list(range(2015, 2026))
HOLD_HORIZONS = [1, 2, 3, 5]  # years forward to measure returns

# ── Entity keywords ──────────────────────────────────────────────
ENTITY_KEYWORDS = ['LLC', 'INC', 'CORP', 'TRUST', 'LP', 'LTD', 'HOLDINGS',
                   'PROPERTIES', 'INVESTMENTS', 'CAPITAL', 'PARTNERS',
                   'VENTURES', 'GROUP', 'FUND', 'REIT', 'HOMES',
                   'OPENDOOR', 'INVITATION', 'ZILLOW', 'OFFERPAD',
                   'REDFIN', 'CERBERUS', 'BLACKSTONE', 'STARWOOD']

# Target entities for detailed analysis
TARGET_ENTITIES = [
    'CAMILLO', 'LGI HOMES', 'OPENDOOR', 'AMERICAN HOMES 4 RENT',
    'PROGRESS RESIDENTIAL', 'TRICON', 'CERBERUS', 'FKH SFR',
    'SRP SUB', 'INVITATION HOMES', 'OFFERPAD', 'ZILLOW',
    'DR HORTON', 'LENNAR', 'PULTE', 'MERITAGE', 'BEAZER',
]

# ══════════════════════════════════════════════════════════════════
# STEP 1: Load owner names + appraised values per year
# ══════════════════════════════════════════════════════════════════
print("📦 Loading owners + valuations for each year...")
owner_frames = []
val_frames = []
name_col = None  # auto-detected
acct_col = None  # auto-detected

for yr in YEARS:
    zip_path = HCAD_BASE / str(yr) / "Real_acct_owner.zip"
    if not zip_path.exists():
        print(f"  {yr}: ❌ NOT FOUND at {zip_path}")
        continue
    try:
        with zipfile.ZipFile(str(zip_path), 'r') as zf:
            names = zf.namelist()
            if yr == YEARS[0]:
                print(f"    📦 Files inside {yr} zip: {names}")

            # ── owners.txt ──
            owner_file = next((n for n in names if 'owner' in n.lower() and n.endswith('.txt')), None)
            if owner_file:
                with zf.open(owner_file) as f:
                    raw = f.read().decode('latin-1').encode('utf-8')
                    odf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                      infer_schema_length=10000,
                                      ignore_errors=True,
                                      truncate_ragged_lines=True,
                                      quote_char=None)
                odf = odf.rename({c: c.strip().lower().replace(' ', '_') for c in odf.columns})
                odf = odf.with_columns(pl.lit(yr).alias("year"))

                # Auto-detect column names on first file
                if name_col is None:
                    nc = [c for c in odf.columns if any(kw in c for kw in ['owner_nm', 'owner_name', 'ownr_nm'])]
                    if not nc:
                        nc = [c for c in odf.columns if 'name' in c and odf[c].dtype == pl.Utf8]
                    if not nc:
                        nc = [c for c in odf.columns if 'owner' in c and odf[c].dtype == pl.Utf8]
                    ac = [c for c in odf.columns if any(kw in c for kw in ['acct', 'account', 'prop_id'])]
                    name_col = nc[0] if nc else None
                    acct_col = ac[0] if ac else odf.columns[0]
                    print(f"    Auto-detected: name_col='{name_col}', acct_col='{acct_col}'")
                    if name_col is None:
                        str_cols = [c for c in odf.columns if odf[c].dtype == pl.Utf8]
                        print(f"    String columns: {str_cols}")
                        raise SystemExit("Cannot auto-detect name column")

                # Keep only primary owner (ln_num == 1 if available)
                if 'ln_num' in odf.columns:
                    odf = odf.filter(pl.col("ln_num") == 1)
                owner_frames.append(
                    odf.select([acct_col, name_col, "year"])
                    .cast({acct_col: pl.Utf8, name_col: pl.Utf8})
                    .rename({acct_col: "acct", name_col: "name"})
                )

            # ── real_acct.txt (valuations) ──
            acct_file = next((n for n in names if n.lower() == 'real_acct.txt'), None)
            if acct_file:
                with zf.open(acct_file) as f:
                    raw = f.read().decode('latin-1').encode('utf-8')
                    vdf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                      infer_schema_length=10000,
                                      ignore_errors=True,
                                      truncate_ragged_lines=True,
                                      quote_char=None)
                vdf = vdf.rename({c: c.strip().lower().replace(' ', '_') for c in vdf.columns})
                # Keep key valuation columns
                val_cols = ['acct']
                for vc in ['tot_appr_val', 'tot_mkt_val', 'land_val', 'bld_val',
                           'neighborhood_code', 'neighborhood_grp']:
                    if vc in vdf.columns:
                        val_cols.append(vc)
                vdf = vdf.select(val_cols).with_columns(pl.lit(yr).alias("year"))
                vdf = vdf.cast({"acct": pl.Utf8})
                val_frames.append(vdf)

            print(f"  {yr}: ✅ owners={len(odf):,}  valuations={len(vdf):,}")
    except Exception as e:
        print(f"  {yr}: ⚠ {e}")

if not owner_frames or not val_frames:
    raise SystemExit("Missing data")

# ══════════════════════════════════════════════════════════════════
# STEP 2: Combine and detect ownership changes
# ══════════════════════════════════════════════════════════════════
print("\n🔗 Combining data...")
all_owners = pl.concat(owner_frames, how="vertical_relaxed")
all_vals = pl.concat(val_frames, how="vertical_relaxed")
print(f"  Owners: {len(all_owners):,} rows")
print(f"  Valuations: {len(all_vals):,} rows")

# Detect ownership transitions (buyer = current name, seller = previous)
print("\n🔄 Detecting ownership changes...")
sorted_own = all_owners.sort(["acct", "year"])
with_prev = sorted_own.with_columns(
    pl.col("name").shift(1).over("acct").alias("prev_owner"),
    pl.col("year").shift(1).over("acct").alias("prev_year"),
)
transactions = with_prev.filter(
    pl.col("prev_owner").is_not_null()
    & (pl.col("name") != pl.col("prev_owner"))
    & (pl.col("year") - pl.col("prev_year") <= 2)
)
print(f"  Ownership changes: {len(transactions):,}")

# Filter to entity transactions only
entity_filter = pl.col("name").str.to_uppercase().str.contains("|".join(ENTITY_KEYWORDS))
entity_txns = transactions.filter(entity_filter)
print(f"  Entity purchases: {len(entity_txns):,}")

# ══════════════════════════════════════════════════════════════════
# STEP 3: Join transactions with valuations to compute returns
# ══════════════════════════════════════════════════════════════════
print("\n📈 Computing portfolio returns...")

# Get valuation at time of purchase
buy_vals = entity_txns.join(
    all_vals.rename({"tot_appr_val": "buy_val"}),
    on=["acct", "year"],
    how="left"
)
# Drop rows without a buy value
buy_vals = buy_vals.filter(pl.col("buy_val").is_not_null() & (pl.col("buy_val") > 0))
print(f"  Purchases with valuations: {len(buy_vals):,}")

# For each hold horizon, join the future valuation
results = []
for horizon in HOLD_HORIZONS:
    future_vals = all_vals.select([
        pl.col("acct"),
        (pl.col("year") - horizon).alias("year"),  # shift back so join matches buy year
        pl.col("tot_appr_val").alias("future_val"),
    ])
    with_future = buy_vals.join(future_vals, on=["acct", "year"], how="inner")
    with_future = with_future.filter(pl.col("future_val").is_not_null() & (pl.col("future_val") > 0))
    with_future = with_future.with_columns(
        ((pl.col("future_val") - pl.col("buy_val")) / pl.col("buy_val") * 100).alias("return_pct"),
        pl.lit(horizon).alias("horizon"),
    )
    results.append(with_future)
    print(f"  {horizon}-year horizon: {len(with_future):,} measurable purchases")

all_results = pl.concat(results, how="vertical_relaxed")

# ══════════════════════════════════════════════════════════════════
# STEP 4: County-wide baseline returns per purchase year
# ══════════════════════════════════════════════════════════════════
print("\n🏘️  Computing county-wide baseline returns...")
baseline_results = []
for horizon in HOLD_HORIZONS:
    # All parcels: value at year Y vs year Y+horizon
    current = all_vals.select(["acct", "year", "tot_appr_val"])
    future = all_vals.select([
        pl.col("acct"),
        (pl.col("year") - horizon).alias("year"),
        pl.col("tot_appr_val").alias("future_val"),
    ])
    paired = current.join(future, on=["acct", "year"], how="inner")
    paired = paired.filter(
        pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0)
        & pl.col("future_val").is_not_null() & (pl.col("future_val") > 0)
    )
    paired = paired.with_columns(
        ((pl.col("future_val") - pl.col("tot_appr_val")) / pl.col("tot_appr_val") * 100).alias("return_pct"),
    )
    # Compute percentiles per year
    stats = paired.group_by("year").agg([
        pl.col("return_pct").median().alias("median_return"),
        pl.col("return_pct").quantile(0.75).alias("p75_return"),
        pl.col("return_pct").quantile(0.90).alias("p90_return"),
        pl.col("return_pct").mean().alias("mean_return"),
        pl.len().cast(pl.Int64).alias("n_parcels"),
    ]).with_columns(pl.lit(horizon).alias("horizon"))
    baseline_results.append(stats)
    # Overall stats
    med = paired["return_pct"].median()
    p75 = paired["return_pct"].quantile(0.75)
    p90 = paired["return_pct"].quantile(0.90)
    print(f"  {horizon}yr county-wide: median={med:.1f}%, p75={p75:.1f}%, p90={p90:.1f}%")

baseline = pl.concat(baseline_results, how="vertical_relaxed")

# ══════════════════════════════════════════════════════════════════
# STEP 5: Score each target entity vs market
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("💰 ENTITY BACKTEST — ACTUAL RETURNS vs MARKET vs TOP-QUARTILE")
print("="*70)
print("Question: Would they have made more money with our forecast product?")
print()

for entity_kw in TARGET_ENTITIES:
    # Filter to this entity's purchases
    ent_results = all_results.filter(
        pl.col("name").str.to_uppercase().str.contains(entity_kw)
    )
    if len(ent_results) == 0:
        continue

    n_purchases = ent_results.filter(pl.col("horizon") == HOLD_HORIZONS[0]).shape[0]
    entity_names = (
        ent_results.select("name").unique()
        .head(3).to_series().to_list()
    )
    print(f"  🏢 {entity_kw} ({n_purchases} measurable purchases)")
    print(f"     Names: {', '.join(entity_names[:3])}")

    for horizon in HOLD_HORIZONS:
        ent_h = ent_results.filter(pl.col("horizon") == horizon)
        if len(ent_h) == 0:
            continue

        ent_median = ent_h["return_pct"].median()
        ent_mean = ent_h["return_pct"].mean()
        ent_total_buy = ent_h["buy_val"].sum()
        ent_total_future = ent_h["future_val"].sum()
        ent_weighted_return = (ent_total_future - ent_total_buy) / ent_total_buy * 100

        # County baseline for same purchase years
        buy_years = ent_h["year"].unique().to_list()
        county_h = baseline.filter(
            (pl.col("horizon") == horizon) & pl.col("year").is_in(buy_years)
        )
        county_median = county_h["median_return"].mean() if len(county_h) > 0 else 0
        county_p75 = county_h["p75_return"].mean() if len(county_h) > 0 else 0
        county_p90 = county_h["p90_return"].mean() if len(county_h) > 0 else 0

        alpha = ent_weighted_return - county_median
        potential_alpha = county_p75 - ent_weighted_return
        premium_alpha = county_p90 - ent_weighted_return

        # Value-add in dollar terms
        portfolio_val = ent_total_buy
        missed_dollars = portfolio_val * max(0, potential_alpha) / 100

        indicator = "✅" if alpha > 0 else "⚠️"
        forecast_indicator = "🎯" if potential_alpha > 0 else "👑"

        print(f"     {horizon}yr hold │ "
              f"Entity: {ent_weighted_return:+.1f}% │ "
              f"Market: {county_median:+.1f}% │ "
              f"Alpha: {alpha:+.1f}% {indicator} │ "
              f"P75: {county_p75:+.1f}% │ "
              f"Forecast uplift: {potential_alpha:+.1f}% {forecast_indicator} "
              f"(${missed_dollars:,.0f})")

    print()

# ══════════════════════════════════════════════════════════════════
# STEP 6: Aggregate scoreboard
# ══════════════════════════════════════════════════════════════════
print("="*70)
print("📊 AGGREGATE SCOREBOARD — ALL INSTITUTIONAL ENTITIES")
print("="*70)

for horizon in HOLD_HORIZONS:
    h_results = all_results.filter(pl.col("horizon") == horizon)
    if len(h_results) == 0:
        continue

    # Per-entity stats
    entity_stats = (
        h_results
        .group_by("name")
        .agg([
            pl.len().cast(pl.Int64).alias("n_purchases"),
            pl.col("buy_val").sum().alias("total_invested"),
            pl.col("future_val").sum().alias("total_future"),
            pl.col("return_pct").median().alias("median_return"),
        ])
        .filter(pl.col("n_purchases") >= 10)  # min 10 purchases
        .with_columns(
            ((pl.col("total_future") - pl.col("total_invested"))
             / pl.col("total_invested") * 100).alias("weighted_return")
        )
        .sort("weighted_return", descending=True)
    )

    # County baseline
    county_h = baseline.filter(pl.col("horizon") == horizon)
    county_med = county_h["median_return"].mean() if len(county_h) > 0 else 0
    county_p75 = county_h["p75_return"].mean() if len(county_h) > 0 else 0

    n_outperform = entity_stats.filter(pl.col("weighted_return") > county_med).shape[0]
    n_total = entity_stats.shape[0]
    n_could_improve = entity_stats.filter(pl.col("weighted_return") < county_p75).shape[0]

    print(f"\n  ── {horizon}-YEAR HOLD ──")
    print(f"  County median: {county_med:+.1f}%  |  P75 (forecast target): {county_p75:+.1f}%")
    print(f"  {n_outperform}/{n_total} entities beat the market median")
    print(f"  {n_could_improve}/{n_total} entities could improve with our forecasts (below P75)")

    # Show top and bottom 5
    print(f"  🏆 Top 5 performers:")
    for row in entity_stats.head(5).iter_rows(named=True):
        alpha = row['weighted_return'] - county_med
        print(f"      {row['weighted_return']:+6.1f}% (α={alpha:+.1f}%) "
              f"${row['total_invested']:>12,.0f} invested  "
              f"{row['n_purchases']:>4} buys  {row['name']}")

    print(f"  📉 Bottom 5 performers:")
    for row in entity_stats.tail(5).iter_rows(named=True):
        alpha = row['weighted_return'] - county_med
        gap_to_p75 = county_p75 - row['weighted_return']
        missed = row['total_invested'] * max(0, gap_to_p75) / 100
        print(f"      {row['weighted_return']:+6.1f}% (α={alpha:+.1f}%) "
              f"${row['total_invested']:>12,.0f} invested  "
              f"${missed:>10,.0f} missed  {row['name']}")

# ══════════════════════════════════════════════════════════════════
# STEP 7: The pitch — total addressable value-add
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("🎯 THE PITCH — TOTAL VALUE OUR PRODUCT COULD ADD")
print("="*70)

for horizon in HOLD_HORIZONS:
    h_results = all_results.filter(pl.col("horizon") == horizon)
    if len(h_results) == 0:
        continue

    entity_stats = (
        h_results
        .group_by("name")
        .agg([
            pl.len().cast(pl.Int64).alias("n"),
            pl.col("buy_val").sum().alias("invested"),
            pl.col("future_val").sum().alias("future"),
        ])
        .filter(pl.col("n") >= 5)
        .with_columns(
            ((pl.col("future") - pl.col("invested")) / pl.col("invested") * 100).alias("actual_return")
        )
    )

    county_h = baseline.filter(pl.col("horizon") == horizon)
    p75 = county_h["p75_return"].mean() if len(county_h) > 0 else 0

    # For each entity below P75, calculate how much they'd gain
    below_p75 = entity_stats.filter(pl.col("actual_return") < p75)
    total_invested_below = below_p75["invested"].sum()
    total_gap_dollars = (below_p75["invested"] * (p75 - below_p75["actual_return"]) / 100).sum()

    total_invested_all = entity_stats["invested"].sum()
    n_entities = entity_stats.shape[0]

    print(f"\n  {horizon}-year hold horizon:")
    print(f"    Total institutional capital tracked: ${total_invested_all:,.0f}")
    print(f"    Entities analyzable: {n_entities}")
    print(f"    Entities below P75 target: {below_p75.shape[0]}")
    print(f"    Their total invested capital: ${total_invested_below:,.0f}")
    print(f"    💰 POTENTIAL VALUE-ADD (P75 uplift): ${total_gap_dollars:,.0f}")
    if total_invested_below > 0:
        pct_uplift = total_gap_dollars / total_invested_below * 100
        print(f"    📈 Average uplift per dollar: {pct_uplift:.1f}%")

print("\n✅ Backtest complete!")

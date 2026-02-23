# ══════════════════════════════════════════════════════════════════
# TAM ANALYSIS: How many entities exist at each portfolio size?
# Run AFTER colab_model_backtest.py — uses in-memory variables
# ══════════════════════════════════════════════════════════════════

from collections import defaultdict
import numpy as np

print("=" * 110)
print("📊 ADDRESSABLE MARKET: Entity counts by portfolio size × model performance")
print("   Portfolio size = total purchases across all origins in sample")
print("=" * 110)

# Aggregate all entities
ent_all = defaultdict(lambda: {"n_total": 0, "segment": "", "diffs": [], "ent_meds": [],
                                "origins": set(), "origin_diffs": defaultdict(list)})
for ep in entity_perf_rows:
    name = ep["entity"]
    ea = ent_all[name]
    ea["n_total"] += ep["n_buys"]
    ea["segment"] = ep["segment"]
    ea["diffs"].append(ep["diff"])
    ea["ent_meds"].append(ep["ent_med"])
    ea["origins"].add(ep["origin"])
    ea["origin_diffs"][ep["origin"]].append(ep["diff"])

# Price tiers for a proptech analytics subscription
TIERS = [
    (5,    "$99/mo",   "Solo investor / side hustle"),
    (10,   "$99/mo",   "Small landlord"),
    (20,   "$199/mo",  "Active investor"),
    (50,   "$499/mo",  "Boutique fund"),
    (100,  "$999/mo",  "Regional operator"),
    (200,  "$2K/mo",   "Mid-market fund"),
    (500,  "$5K/mo",   "Institutional"),
    (1000, "$10K/mo",  "Enterprise"),
]

print(f"\n  {'Min Buys':>9}  {'Price Pt':>10}  {'Customer Type':<25}  "
      f"{'All':>5}  {'ICP':>5}  {'M>E':>5}  {'M>E+':>5}  {'Con3+':>5}  {'Con4':>5}")
print(f"  {'─'*9}  {'─'*10}  {'─'*25}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}")

for min_buys, price, desc in TIERS:
    all_count = 0     # total entities at this size
    icp_count = 0     # ICP-only
    model_wins = 0    # model outperforms on average
    model_wins_pos = 0 # model outperforms AND entity has positive returns
    consistent_3 = 0  # model wins in ≥3 origins
    consistent_4 = 0  # model wins in all 4 origins
    
    for name, ea in ent_all.items():
        if ea["n_total"] < min_buys:
            continue
        all_count += 1
        
        if not is_icp(name):
            continue
        icp_count += 1
        
        avg_diff = np.mean(ea["diffs"])
        avg_ent = np.mean(ea["ent_meds"])
        
        if avg_diff > 0:
            model_wins += 1
            if avg_ent > 0:
                model_wins_pos += 1
        
        # Per-origin consistency
        n_origins = len(ea["origins"])
        if n_origins >= 3:
            origin_wins = sum(1 for o, ds in ea["origin_diffs"].items() if np.mean(ds) > 0)
            if origin_wins >= 3:
                consistent_3 += 1
            if origin_wins >= 4 and n_origins >= 4:
                consistent_4 += 1
    
    print(f"  {min_buys:>9}  {price:>10}  {desc:<25}  "
          f"{all_count:>5}  {icp_count:>5}  {model_wins:>5}  {model_wins_pos:>5}  {consistent_3:>5}  {consistent_4:>5}")

print(f"\n  Column legend:")
print(f"    All     = total entities with ≥N buys")
print(f"    ICP     = after excluding HOAs, govt, industrial")
print(f"    M>E     = model outperforms entity (avg diff > 0)")
print(f"    M>E+    = model outperforms AND entity has positive returns (not just 'they lost money')")
print(f"    Con3+   = model wins in ≥3 of 4 origins (consistency)")
print(f"    Con4    = model wins in all 4 origins (strongest signal)")

# Also show: what's the TOTAL addressable market (all ICP entities, regardless of model perf)?
print(f"\n{'─'*110}")
print(f"📊 TOTAL ADDRESSABLE MARKET (all ICP entities, any model result):")
print(f"\n  If you sell market intelligence / screening (NOT 'we beat you'), your TAM is the ICP column.")
print(f"  If you sell 'we outperform your picks', your TAM is the M>E+ column.")
print(f"  If you need ironclad proof, your TAM is the Con3+ column.")

# Revenue estimates
print(f"\n{'─'*110}")
print(f"💰 REVENUE SCENARIOS (ICP column × price point):")
for min_buys, price, desc in TIERS:
    icp_count = sum(1 for name, ea in ent_all.items()
                    if ea["n_total"] >= min_buys and is_icp(name))
    # Parse price to number
    price_num = float(price.replace("$","").replace("/mo","").replace("K","000"))
    arr = icp_count * price_num * 12
    if arr >= 1_000_000:
        arr_str = f"${arr/1_000_000:.1f}M"
    else:
        arr_str = f"${arr/1_000:,.0f}K"
    capture_5 = icp_count * price_num * 12 * 0.05
    if capture_5 >= 1_000_000:
        cap_str = f"${capture_5/1_000_000:.1f}M"
    else:
        cap_str = f"${capture_5/1_000:,.0f}K"
    print(f"    ≥{min_buys:>4} buys × {price:>8} × {icp_count:>4} entities = {arr_str:>8} TAM  ({cap_str} at 5% capture)")

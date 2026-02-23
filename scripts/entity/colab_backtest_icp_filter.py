# ══════════════════════════════════════════════════════════════════
# FOLLOW-ON CELL: Filter to ICP leads where model outperforms CONSISTENTLY
# Run this AFTER colab_model_backtest.py — uses in-memory variables
# ══════════════════════════════════════════════════════════════════

MIN_BUYS = 20          # minimum total purchases across all origins
MIN_ENT_RETURN = 0.0   # entity must have positive avg actual return

# Require model outperforms in ALL origins the entity appears in?
# If True: model must win on average in every single origin (strictest)
# If False: model must win on average overall and in ≥75% of origins
STRICT_ALL_ORIGINS = False

# Minimum number of origins the entity must appear in
MIN_ORIGINS = 3        # out of 4 possible (2021-2024)

print("=" * 110)
print("🔍 ICP CONSISTENCY FILTER: Model outperforms across ALL years")
print("=" * 110)

from collections import defaultdict
import numpy as np

# Aggregate per entity AND per origin
ent_by_origin = defaultdict(lambda: defaultdict(lambda: {"diffs": [], "ent_meds": [], "mod_meds": [], "n_buys": 0, "segment": ""}))

for ep in entity_perf_rows:
    name = ep["entity"]
    origin = ep["origin"]
    eo = ent_by_origin[name][origin]
    eo["diffs"].append(ep["diff"])
    eo["ent_meds"].append(ep["ent_med"])
    eo["mod_meds"].append(ep["model_med"])
    eo["n_buys"] += ep["n_buys"]
    eo["segment"] = ep["segment"]

# Evaluate consistency
leads = []
for name, origins_data in ent_by_origin.items():
    if not is_icp(name):
        continue
    
    n_origins = len(origins_data)
    if n_origins < MIN_ORIGINS:
        continue
    
    total_buys = sum(od["n_buys"] for od in origins_data.values())
    if total_buys < MIN_BUYS:
        continue
    
    # Per-origin avg diffs
    origin_results = {}
    for origin, od in sorted(origins_data.items()):
        avg_diff = np.mean(od["diffs"])
        avg_ent = np.mean(od["ent_meds"])
        avg_mod = np.mean(od["mod_meds"])
        origin_results[origin] = {
            "diff": avg_diff,
            "ent_return": avg_ent,
            "mod_return": avg_mod,
            "n_buys": od["n_buys"],
            "horizons": len(od["diffs"]),
        }
    
    # Overall averages
    all_diffs = [r["diff"] for r in origin_results.values()]
    avg_diff_overall = np.mean(all_diffs)
    avg_ent_overall = np.mean([r["ent_return"] for r in origin_results.values()])
    avg_mod_overall = np.mean([r["mod_return"] for r in origin_results.values()])
    
    # Must have positive entity return
    if avg_ent_overall <= MIN_ENT_RETURN:
        continue
    
    # Must outperform overall
    if avg_diff_overall <= 0:
        continue
    
    # Consistency check
    wins_by_origin = sum(1 for d in all_diffs if d > 0)
    
    if STRICT_ALL_ORIGINS:
        # Every single origin must be positive
        if wins_by_origin < n_origins:
            continue
    else:
        # At least 75% of origins must be positive
        if wins_by_origin / n_origins < 0.75:
            continue
    
    segment = list(origins_data.values())[0]["segment"]
    
    leads.append({
        "name": name,
        "segment": segment,
        "buys": total_buys,
        "n_origins": n_origins,
        "wins_by_origin": wins_by_origin,
        "avg_diff": avg_diff_overall,
        "avg_ent_return": avg_ent_overall,
        "avg_mod_return": avg_mod_overall,
        "origin_results": origin_results,
    })

# Sort by consistency (wins/origins) then by portfolio size
leads.sort(key=lambda x: (-x["wins_by_origin"] / x["n_origins"], -x["buys"]))

mode = "ALL origins" if STRICT_ALL_ORIGINS else f"≥75% origins"
print(f"\n  Filters: ≥{MIN_BUYS} buys, ≥{MIN_ORIGINS} origins, entity return > {MIN_ENT_RETURN}%,")
print(f"           model outperforms in {mode}, ICP only")
print(f"  Result: {len(leads)} qualifying entities\n")

if leads:
    # Detailed per-origin breakdown
    for i, ld in enumerate(leads, 1):
        consistency = f"{ld['wins_by_origin']}/{ld['n_origins']} origins"
        print(f"  {'─'*106}")
        print(f"  #{i:3d}  {ld['name']}")
        print(f"       {ld['segment']} | {ld['buys']} buys | {consistency} | "
              f"Avg: Ent {ld['avg_ent_return']:+.1f}%  Model {ld['avg_mod_return']:+.1f}%  Diff {ld['avg_diff']:+.1f}pp")
        
        # Per-origin line
        print(f"       ", end="")
        for origin, r in sorted(ld["origin_results"].items()):
            icon = "✅" if r["diff"] > 0 else "❌"
            print(f"  {origin}: {icon} {r['diff']:+.1f}pp ({r['n_buys']} buys)", end="")
        print()
    
    # Summary
    print(f"\n  {'═'*106}")
    all_origins_consistent = [ld for ld in leads if ld["wins_by_origin"] == ld["n_origins"]]
    mostly_consistent = [ld for ld in leads if ld["wins_by_origin"] < ld["n_origins"]]
    
    print(f"\n  📊 Consistency breakdown:")
    print(f"    ✅ Win in ALL origins: {len(all_origins_consistent)} entities")
    print(f"    🟡 Win in most origins: {len(mostly_consistent)} entities")
    
    if all_origins_consistent:
        print(f"\n  🏆 STRONGEST LEADS (model outperforms in every single origin):")
        for ld in all_origins_consistent:
            origins_str = ", ".join(f"{o}:{r['diff']:+.1f}pp" for o, r in sorted(ld["origin_results"].items()))
            print(f"    → {ld['name']} ({ld['segment']}, {ld['buys']} buys)")
            print(f"      {origins_str}")
    
    # Tier by size among consistent leads
    tier1 = [ld for ld in leads if ld["buys"] >= 100]
    tier2 = [ld for ld in leads if 50 <= ld["buys"] < 100]
    tier3 = [ld for ld in leads if 20 <= ld["buys"] < 50]
    
    print(f"\n  📊 Size tiers (among consistent leads):")
    print(f"    Tier 1 (≥100 buys): {len(tier1)} entities")
    print(f"    Tier 2 (50-99 buys): {len(tier2)} entities")
    print(f"    Tier 3 (20-49 buys): {len(tier3)} entities")

else:
    print("  ⚠️  No entities pass all filters.")
    
    # Diagnostic: how many pass with relaxed origin requirements
    for min_o in range(1, 5):
        count = 0
        for name, origins_data in ent_by_origin.items():
            if not is_icp(name) or len(origins_data) < min_o:
                continue
            total_buys = sum(od["n_buys"] for od in origins_data.values())
            if total_buys < MIN_BUYS:
                continue
            all_diffs = [np.mean(od["diffs"]) for od in origins_data.values()]
            avg_ent = np.mean([np.mean(od["ent_meds"]) for od in origins_data.values()])
            if avg_ent <= 0 or np.mean(all_diffs) <= 0:
                continue
            wins = sum(1 for d in all_diffs if d > 0)
            if wins == len(origins_data):
                count += 1
        print(f"    With ≥{min_o} origins, model wins ALL: {count} entities")
    
    print(f"\n  💡 Try relaxing: set MIN_ORIGINS = 2 or STRICT_ALL_ORIGINS = False")

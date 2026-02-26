"""
Cell 2c: Calibration Leakage Test — Leave-One-Origin-Out
=========================================================
Fully self-contained — works after runtime restart.
Loads variant_raw_results from Drive pickle + HCAD actuals from zips if needed.

Tests whether post-hoc fan calibration constitutes future leakage by:
  1. Learning a scaler adjustment from origins 2021-2023 ONLY
  2. Testing whether it improves origin 2024's PIT (held out)
  3. If it does → calibration is generalizing, not leaking
  4. If it doesn't → the bias is origin-specific and correction would overfit

Also quantifies: how much does PIT improve with the correction?
Is the correction even necessary, or is the uncorrected fan "good enough"?
"""

import os
import io
import zipfile
import pickle
import numpy as np
import polars as pl
from typing import Dict
from scipy.stats import kstest, spearmanr, ttest_1samp
from scipy.optimize import minimize_scalar

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
ALL_VARIANTS  = ["baseline", "SF200K", "SF500K", "SFstrat200K"]
CALIB_ORIGINS = [2021, 2022, 2023]  # learn calibration from these
TEST_ORIGINS  = [2024]              # hold out for testing
ALL_ORIGINS   = CALIB_ORIGINS + TEST_ORIGINS

VALUE_BRACKETS = [
    ("<200K",     0,        200_000),
    ("200K-500K", 200_000,  500_000),
    ("500K-1M",   500_000,  1_000_000),
    ("1M+",       1_000_000, 1e18),
    ("ALL",       0,        1e18),
]

_out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")

# ═══════════════════════════════════════════════════════════════════
# LOAD DATA — self-contained (works after runtime restart)
# ═══════════════════════════════════════════════════════════════════

# ── 1. Load raw inference results ──
_raw = globals().get("variant_raw_results")
if _raw is None:
    print("📦 variant_raw_results not in memory — loading from Drive pickle...")
    _results_dir = os.path.join(_out_dir, "hypothesis_results")
    _pkls = sorted([f for f in os.listdir(_results_dir) if f.startswith("variant_raw_") and f.endswith(".pkl")])
    if not _pkls:
        raise RuntimeError(f"No raw result pickles found in {_results_dir}")
    _pkl_path = os.path.join(_results_dir, _pkls[-1])
    print(f"   Loading: {_pkl_path}")
    with open(_pkl_path, "rb") as f:
        _raw = pickle.load(f)
    globals()["variant_raw_results"] = _raw
    print(f"   ✅ Loaded {len(_raw)} variant×origin combos")
else:
    print("✅ variant_raw_results already in memory")

# ── 2. Load HCAD actuals ──
_actuals = globals().get("actual_vals")
if _actuals is None:
    print("📊 actual_vals not in memory — loading from HCAD zips...")
    HCAD_BASE_CANDIDATES = [
        "/content/drive/MyDrive/HCAD_Archive/property",
        "G:/My Drive/HCAD_Archive/property",
        os.path.join(os.path.expanduser("~"), "HCAD_Archive", "property"),
    ]
    hcad_base = None
    for cand in HCAD_BASE_CANDIDATES:
        if os.path.isdir(cand):
            hcad_base = cand
            break
    assert hcad_base, f"❌ Cannot find HCAD data in: {HCAD_BASE_CANDIDATES}"

    _actuals: Dict[int, Dict[str, float]] = {}
    for yr in range(2015, 2026):
        zp = os.path.join(hcad_base, str(yr), "Real_acct_owner.zip")
        if not os.path.exists(zp):
            continue
        try:
            with zipfile.ZipFile(zp) as zf:
                names = zf.namelist()
                af = next((n for n in names if n.lower() == "real_acct.txt"), None)
                if af:
                    with zf.open(af) as f:
                        raw = f.read().decode("latin-1").encode("utf-8")
                        vdf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                          infer_schema_length=10000, ignore_errors=True,
                                          truncate_ragged_lines=True, quote_char=None)
                        vdf = vdf.rename({c: c.strip().lower().replace(" ","_") for c in vdf.columns})
                        ac = next((c for c in vdf.columns if c in ("acct","account","prop_id")), vdf.columns[0])
                        vc = next((c for c in vdf.columns if "tot_appr" in c or "total_appraised" in c), None)
                        if vc is None: vc = next((c for c in vdf.columns if "appr" in c), vdf.columns[1])
                        vdf = vdf.select([ac,vc]).cast({ac:pl.Utf8, vc:pl.Float64})
                        vdf = vdf.with_columns(pl.col(ac).str.strip_chars())
                        vdf = vdf.filter(pl.col(vc)>0)
                        _actuals[yr] = dict(zip(vdf[ac].to_list(), vdf[vc].to_list()))
            print(f"   {yr}: {len(_actuals.get(yr,{})):,} properties")
        except Exception as e:
            print(f"   {yr}: ⚠️ {e}")
    globals()["actual_vals"] = _actuals
    print(f"   ✅ Loaded actuals for {len(_actuals)} years")
else:
    print("✅ actual_vals already in memory")

print(f"\n🔬 Calibration Leakage Test — ALL VARIANTS")
print(f"   Variants: {ALL_VARIANTS}")
print(f"   Calibrate on: {CALIB_ORIGINS}")
print(f"   Test on: {TEST_ORIGINS}")


# ═══════════════════════════════════════════════════════════════════
# HELPER: Collect PIT values for a set of origins
# ═══════════════════════════════════════════════════════════════════
def collect_pits(variant, origins, actuals, raw_data, bracket_lo=0, bracket_hi=1e18,
                 scale_factor=1.0, horizons=range(1, 6)):
    """
    Compute PIT values for a variant across given origins.
    
    scale_factor: multiply the fan width by this factor (>1 = wider fans).
      Applied in log-space: adjusted_fan = y_anchor + (fan - y_anchor) * scale_factor
      This simulates widening y_scaler.scale_ by scale_factor.
    
    Returns: list of (pit_value, base_val, horizon) tuples
    """
    results = []
    
    for origin in origins:
        key = (variant, origin)
        if key not in raw_data:
            continue
        
        r = raw_data[key]
        accts = r["accts"]
        y_anchor = r["y_anchor"]
        y_levels = r["y_levels"]  # [N, S, H]
        base_v = actuals.get(origin, {})
        
        for hold in horizons:
            eyr = origin + hold
            if eyr not in actuals:
                continue
            future_v = actuals[eyr]
            
            for i in range(len(accts)):
                acct = str(accts[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                
                if bv <= 0 or av is None:
                    continue
                if not (bracket_lo <= bv < bracket_hi):
                    continue
                
                # Get the fan for this parcel at this horizon
                fan_log = y_levels[i, :, hold - 1]  # [S] log-space
                
                # Apply scale factor in log-space (widen around anchor)
                if scale_factor != 1.0:
                    anchor = y_anchor[i]
                    fan_log = anchor + (fan_log - anchor) * scale_factor
                
                fan_prices = np.exp(fan_log)
                
                # PIT: what fraction of scenarios fall at or below actual?
                pit = np.mean(fan_prices <= av)
                results.append((pit, bv, hold))
    
    return results


def pit_ks_stat(pits):
    """KS statistic against uniform — lower is better calibrated."""
    if len(pits) < 10:
        return 1.0
    return kstest(pits, 'uniform').statistic


def pit_summary(pits, label=""):
    """Print a one-line PIT summary."""
    pits = np.array(pits)
    med = np.median(pits)
    lo = np.mean(pits < 0.1) * 100
    hi = np.mean(pits > 0.9) * 100
    ks = pit_ks_stat(pits)
    return med, lo, hi, ks


# ═══════════════════════════════════════════════════════════════════
# 1. UNCORRECTED PIT — How bad is it without calibration?
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("📊 STEP 1: Uncorrected PIT by origin (no calibration applied) — ALL VARIANTS")
print("   Shows whether the bias is consistent across origins or origin-specific")
print("=" * 100)

for variant in ALL_VARIANTS:
    print(f"\n  ── {variant} ──")
    print(f"  {'Origin':<8} {'Bracket':<12} {'n':>7} {'med_PIT':>8} {'lo%':>5} {'hi%':>5} {'KS':>7} {'Assessment'}")
    print(f"  {'─'*8} {'─'*12} {'─'*7} {'─'*8} {'─'*5} {'─'*5} {'─'*7} {'─'*20}")
    
    for origin in ALL_ORIGINS:
        for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
            pit_data = collect_pits(variant, [origin], _actuals, _raw,
                                    bracket_lo=bkt_lo, bracket_hi=bkt_hi)
            if len(pit_data) < 50:
                continue
            pits = np.array([p[0] for p in pit_data])
            med, lo, hi, ks = pit_summary(pits)
            
            assess = ""
            if med < 0.35:
                assess = "OVERPREDICTS"
            elif med > 0.65:
                assess = "UNDERPREDICTS"
            else:
                assess = "✅ ok"
            
            split = "CAL" if origin in CALIB_ORIGINS else "TEST"
            print(f"  {origin} ({split}) {bkt_label:<12} {len(pits):>7,} {med:>7.3f}  {lo:>4.0f}% {hi:>4.0f}% {ks:>6.3f}  {assess}")
        print()


# ═══════════════════════════════════════════════════════════════════
# 2. PER-HORIZON PIT — Does bias compound across horizons?
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("📊 STEP 2: PIT by horizon ($1M+ bracket) — Does bias compound? — ALL VARIANTS")
print("   If h=1 bias causes cascade, h=2+ should be progressively worse")
print("=" * 100)

for variant in ALL_VARIANTS:
    print(f"\n  ── {variant} ($1M+ bracket) ──")
    print(f"  {'Horizon':>7} {'n':>7} {'med_PIT':>8} {'lo%':>5} {'hi%':>5} {'KS':>7} {'Assessment'}")
    print(f"  {'─'*7} {'─'*7} {'─'*8} {'─'*5} {'─'*5} {'─'*7} {'─'*20}")
    
    h1_pits = []
    for h in range(1, 6):
        pit_data = collect_pits(variant, ALL_ORIGINS, _actuals, _raw,
                                bracket_lo=1_000_000, bracket_hi=1e18,
                                horizons=[h])
        if len(pit_data) < 20:
            print(f"  h={h:>5} {'too few observations':>30}")
            continue
        pits = np.array([p[0] for p in pit_data])
        if h == 1:
            h1_pits = pits.copy()
        med, lo, hi, ks = pit_summary(pits)
        
        trend = ""
        if h > 1 and len(h1_pits) > 0:
            h1_med = np.median(h1_pits)
            if abs(med - 0.5) > abs(h1_med - 0.5) + 0.05:
                trend = "← worse than h=1 (cascade)"
            elif abs(med - 0.5) < abs(h1_med - 0.5) - 0.05:
                trend = "← better than h=1"
            else:
                trend = "← similar to h=1"
        
        print(f"  h={h:>5} {len(pits):>7,} {med:>7.3f}  {lo:>4.0f}% {hi:>4.0f}% {ks:>6.3f}  {trend}")


# ═══════════════════════════════════════════════════════════════════
# 3. FIND OPTIMAL SCALE FACTOR — Calibrate on training origins only
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("📊 STEP 3: Optimal scaler widening factor (learned from calibration origins ONLY) — ALL VARIANTS")
print(f"   Grid search over scale factors, minimizing KS stat on origins {CALIB_ORIGINS}")
print("=" * 100)

# Summary dict for cross-variant table at end
_step3_summary = {}  # variant -> {bracket -> (best_k, status)}

for variant in ALL_VARIANTS:
    print(f"\n  ── {variant} ──")
    _step3_summary[variant] = {}
    
    for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
        calib_pit_data = collect_pits(variant, CALIB_ORIGINS, _actuals, _raw,
                                      bracket_lo=bkt_lo, bracket_hi=bkt_hi)
        if len(calib_pit_data) < 100:
            continue
        
        # Grid search for optimal scale factor
        best_k = 1.0
        best_ks = 1.0
        for k in np.arange(0.5, 3.01, 0.05):
            scaled_pits = collect_pits(variant, CALIB_ORIGINS, _actuals, _raw,
                                        bracket_lo=bkt_lo, bracket_hi=bkt_hi,
                                        scale_factor=k)
            pits = np.array([p[0] for p in scaled_pits])
            ks = pit_ks_stat(pits)
            if ks < best_ks:
                best_ks = ks
                best_k = k
        
        # Test on held-out origin
        test_uncorrected = collect_pits(variant, TEST_ORIGINS, _actuals, _raw,
                                         bracket_lo=bkt_lo, bracket_hi=bkt_hi,
                                         scale_factor=1.0)
        test_corrected = collect_pits(variant, TEST_ORIGINS, _actuals, _raw,
                                       bracket_lo=bkt_lo, bracket_hi=bkt_hi,
                                       scale_factor=best_k)
        if len(test_uncorrected) < 20:
            continue
        
        pits_unc = np.array([p[0] for p in test_uncorrected])
        pits_cor = np.array([p[0] for p in test_corrected])
        med_unc = np.median(pits_unc)
        med_cor = np.median(pits_cor)
        ks_unc = pit_ks_stat(pits_unc)
        ks_cor = pit_ks_stat(pits_cor)
        
        improved = ks_cor < ks_unc
        leakage = ks_cor > ks_unc + 0.02
        status = "✅ GENERALIZES" if improved else ("⚠️  LEAKAGE" if leakage else "➖ NO EFFECT")
        _step3_summary[variant][bkt_label] = (best_k, status, med_unc, ks_unc, med_cor, ks_cor)
        
        print(f"    {bkt_label:<12}  k={best_k:.2f} (cal KS={best_ks:.3f})  "
              f"test unc: med={med_unc:.3f} KS={ks_unc:.4f}  →  "
              f"corr: med={med_cor:.3f} KS={ks_cor:.4f}  {status}")


# ═══════════════════════════════════════════════════════════════════
# 4. CROSS-VARIANT SUMMARY: TEST origin calibration head-to-head
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print(f"📊 STEP 4: Cross-variant calibration summary — TEST origin {TEST_ORIGINS} ONLY (no data leakage)")
print("   This is the cleanest comparison: how well does each variant's fan chart fit held-out data?")
print("=" * 100)

print(f"\n  {'Variant':<14} {'Bracket':<12} {'n':>7} {'med_PIT':>8} {'lo%':>5} {'hi%':>5} {'KS':>7} {'Assessment'} {'vs ideal 0.5'}")
print(f"  {'─'*14} {'─'*12} {'─'*7} {'─'*8} {'─'*5} {'─'*5} {'─'*7} {'─'*20} {'─'*12}")

for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
    for variant in ALL_VARIANTS:
        pit_data = collect_pits(variant, TEST_ORIGINS, _actuals, _raw,
                                bracket_lo=bkt_lo, bracket_hi=bkt_hi)
        if len(pit_data) < 20:
            continue
        pits = np.array([p[0] for p in pit_data])
        med, lo, hi, ks = pit_summary(pits)
        
        assess = ""
        if med < 0.35:
            assess = "OVERPREDICTS"
        elif med > 0.65:
            assess = "UNDERPREDICTS"
        else:
            assess = "✅ ok"
        
        bias = med - 0.5
        bias_str = f"{bias:+.3f}"
        
        print(f"  {variant:<14} {bkt_label:<12} {len(pits):>7,} {med:>7.3f}  {lo:>4.0f}% {hi:>4.0f}% {ks:>6.3f}  {assess:<20} {bias_str}")
    print()

# ρ invariance check — just for 1M+
print(f"\n  Does scaler widening affect ρ? (it shouldn't — ρ is rank-based)")

for bkt_label, bkt_lo, bkt_hi in [("1M+", 1_000_000, 1e18)]:
    for k_test in [1.0, 1.5, 2.0]:
        all_pred = []
        all_actual = []
        
        for origin in ALL_ORIGINS:
            key = ("SFstrat200K", origin)
            if key not in _raw:
                continue
            r = _raw[key]
            accts = r["accts"]
            base_v = _actuals.get(origin, {})
            y_anchor = r["y_anchor"]
            y_levels = r["y_levels"]
            
            for hold in range(1, 6):
                eyr = origin + hold
                if eyr not in _actuals:
                    continue
                future_v = _actuals[eyr]
                
                for i in range(len(accts)):
                    acct = str(accts[i]).strip()
                    bv = base_v.get(acct, 0)
                    av = future_v.get(acct)
                    if bv < bkt_lo or bv >= bkt_hi:
                        continue
                    if bv <= 0 or av is None:
                        continue
                    
                    fan_log = y_levels[i, :, hold - 1]
                    if k_test != 1.0:
                        fan_log = y_anchor[i] + (fan_log - y_anchor[i]) * k_test
                    
                    pred_med = np.expm1(np.nanmedian(fan_log) - y_anchor[i]) * 100
                    actual_g = (av - bv) / bv * 100
                    all_pred.append(pred_med)
                    all_actual.append(actual_g)
        
        if len(all_pred) > 20:
            rho, p = spearmanr(all_pred, all_actual)
            print(f"    SFstrat200K $1M+  k={k_test:.1f}  ρ={rho:+.3f} (p={p:.4f})  n={len(all_pred)}")


# ═══════════════════════════════════════════════════════════════════
# 5. BOTTOM LINE
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("📋 BOTTOM LINE")
print("=" * 80)
print("""
  Three possible deployment strategies:

  A) DEPLOY UNCORRECTED SFstrat200K
     - Fans are slightly narrow / conservative for lower brackets
     - $1M+ fans undershoot but MdAE is still 14%
     - No leakage risk at all
     - Simplest path

  B) DEPLOY WITH SCALER WIDENING (k factor from above)
     - Apply k learned from origins 2021-2023
     - If Step 3 shows "GENERALIZES" for $1M+, this is safe
     - ρ is unaffected (rank-based, invariant to monotone transforms)
     - Easy to implement: multiply y_scaler.scale_ *= k

  C) DEPLOY WITH PER-HORIZON ISOTONIC RECALIBRATION
     - Most accurate but most complex
     - Higher leakage risk if calibration set is small
     - Overkill unless you need precise quantile coverage for regulatory use
""")

print("✅ Calibration leakage analysis complete!")

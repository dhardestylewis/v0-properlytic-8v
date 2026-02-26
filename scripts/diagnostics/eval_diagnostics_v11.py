"""
v11 Post-Training Evaluation Diagnostics
========================================
Computes all 5 metric groups and logs them to W&B with full dimensional breakdown:
  - Per horizon (h=1..5)
  - Per origin (2021..2024)
  - Per value bracket (<200K, 200K-500K, 500K-1M, 1M+, ALL)
  - Per checkpoint (via step parameter)

Usage on Colab:
    from eval_diagnostics_v11 import run_eval_diagnostics
    run_eval_diagnostics(variant_raw_results, actual_vals,
                         variant="v11", checkpoint_name="v11_epoch_30")

Metrics logged to W&B (homecastr project):
  1. Fan width per horizon × origin
  2. Fan width std (heterogeneity) per horizon × origin
  3. Spearman ρ per horizon × origin × bracket
  4. PIT / KS per horizon × origin × bracket
  5. Cross-parcel correlation per origin (spatial coherence)
"""

import os
import pickle
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy.stats import kstest, spearmanr
from scipy.spatial.distance import pdist, squareform

try:
    import wandb
except ImportError:
    wandb = None

# ── Config ──
DEFAULT_ORIGINS = [2021, 2022, 2023, 2024]
MAX_HORIZON = 5

VALUE_BRACKETS = [
    ("<200K",     0,        200_000),
    ("200K-500K", 200_000,  500_000),
    ("500K-1M",   500_000,  1_000_000),
    ("1M+",       1_000_000, 1e18),
    ("ALL",       0,        1e18),
]


# ═══════════════════════════════════════════════════════════════════
# W&B HELPERS
# ═══════════════════════════════════════════════════════════════════
def _wb_init(run_name: str, tags: list = None, config: dict = None):
    if wandb is None:
        print("⚠️  wandb not installed, metrics will only be printed")
        return None
    try:
        run = wandb.init(
            project="homecastr",
            entity="dhardestylewis-columbia-university",
            name=run_name,
            tags=tags or ["eval", "v11"],
            config=config or {},
            job_type="evaluation",
            reinit=True,
        )
        print(f"✅ W&B eval run: {run.url}")
        return run
    except Exception as e:
        print(f"⚠️  W&B init failed: {e}")
        return None


def _wb_log(data: dict, run=None):
    if run is not None:
        run.log(data)


def _wb_summary(data: dict, run=None):
    if run is not None:
        for k, v in data.items():
            run.summary[k] = v


# ═══════════════════════════════════════════════════════════════════
# METRIC FUNCTIONS — all return granular per-unit results
# ═══════════════════════════════════════════════════════════════════

def _fan_widths_for_origin_horizon(raw_data, variant, origin, h_idx):
    """Return list of fan width percentages for one origin × horizon."""
    key = (variant, origin)
    if key not in raw_data:
        return []
    r = raw_data[key]
    y_levels = r["y_levels"]  # [N, S, H]
    widths = []
    for i in range(y_levels.shape[0]):
        fan = y_levels[i, :, h_idx]
        p10 = np.expm1(np.percentile(fan, 10))
        p50 = np.expm1(np.percentile(fan, 50))
        p90 = np.expm1(np.percentile(fan, 90))
        if p50 > 0:
            widths.append((p90 - p10) / p50 * 100)
    return widths


def _pit_rho_for_origin_horizon_bracket(raw_data, actuals, variant, origin, horizon,
                                         bracket_lo, bracket_hi):
    """
    Compute per-parcel PIT, predicted growth, actual growth for one origin × horizon × bracket.
    Returns: list of (pit, pred_growth, actual_growth) tuples.
    """
    key = (variant, origin)
    if key not in raw_data:
        return []
    r = raw_data[key]
    accts = r["accts"]
    y_anchor = r["y_anchor"]
    y_levels = r["y_levels"]
    base_v = actuals.get(origin, {})
    eyr = origin + horizon
    if eyr not in actuals:
        return []
    future_v = actuals[eyr]

    results = []
    for i in range(len(accts)):
        acct = str(accts[i]).strip()
        bv = base_v.get(acct, 0)
        av = future_v.get(acct)
        if bv <= 0 or av is None:
            continue
        if not (bracket_lo <= bv < bracket_hi):
            continue

        fan_prices = np.exp(y_levels[i, :, horizon - 1])
        pit = float(np.mean(fan_prices <= av))
        pred_med = float(np.expm1(np.nanmedian(y_levels[i, :, horizon - 1]) - y_anchor[i]) * 100)
        actual_g = float((av - bv) / bv * 100)
        results.append((pit, pred_med, actual_g))

    return results


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def run_eval_diagnostics(
    raw_data: dict,
    actuals: dict,
    variant: str = "v11",
    origins: list = None,
    checkpoint_name: str = "v11",
    log_to_wandb: bool = True,
):
    """
    Run full evaluation with per-horizon × per-origin × per-bracket breakdown.
    Each metric logged individually to W&B for max slice-and-dice in the dashboard.

    Args:
        raw_data: {(variant, origin): {accts, y_anchor, y_levels, ...}}
        actuals: {year: {acct: value}}
        variant: which variant to evaluate
        origins: list of origin years
        checkpoint_name: used as W&B run name and to tag metrics
        log_to_wandb: whether to log to W&B
    """
    if origins is None:
        origins = DEFAULT_ORIGINS

    wb_run = _wb_init(
        run_name=checkpoint_name,
        tags=["eval", "v11", variant, checkpoint_name],
        config={"variant": variant, "origins": origins, "checkpoint": checkpoint_name},
    ) if log_to_wandb else None

    summary = {}

    print(f"\n{'='*80}")
    print(f"📊 v11 EVALUATION — variant={variant} checkpoint={checkpoint_name}")
    print(f"   origins={origins}  brackets={len(VALUE_BRACKETS)}  horizons=1..{MAX_HORIZON}")
    print(f"{'='*80}")

    # ══════════════════════════════════════════════════════════════
    # 1 & 2. FAN WIDTH + HETEROGENEITY — per origin × per horizon
    # ══════════════════════════════════════════════════════════════
    print(f"\n── 1/2. Fan Width + Heterogeneity (per origin × horizon) ──")
    print(f"  {'Origin':>6} {'h':>3} {'Avg%':>7} {'Std%':>7} {'CV%':>7} {'n':>7}")

    for origin in origins:
        for h in range(1, MAX_HORIZON + 1):
            widths = _fan_widths_for_origin_horizon(raw_data, variant, origin, h - 1)
            if not widths:
                continue
            avg = float(np.mean(widths))
            std = float(np.std(widths))
            cv = std / max(avg, 1e-6) * 100

            print(f"  {origin:>6} {h:>3} {avg:>7.1f} {std:>7.1f} {cv:>6.1f}% {len(widths):>7,}")

            _wb_log({
                f"eval/fan_width/o{origin}_h{h}": avg,
                f"eval/fan_std/o{origin}_h{h}": std,
                f"eval/fan_cv/o{origin}_h{h}": cv,
            }, wb_run)
            summary[f"fan_width_o{origin}_h{h}"] = avg
            summary[f"fan_std_o{origin}_h{h}"] = std

    # Aggregated compounding check
    all_h1 = []
    all_h5 = []
    for origin in origins:
        all_h1.extend(_fan_widths_for_origin_horizon(raw_data, variant, origin, 0))
        all_h5.extend(_fan_widths_for_origin_horizon(raw_data, variant, origin, 4))
    avg_h1 = float(np.mean(all_h1)) if all_h1 else 0
    avg_h5 = float(np.mean(all_h5)) if all_h5 else 0
    compounds = avg_h5 > avg_h1 * 1.3 if avg_h1 > 0 else False
    print(f"\n  Aggregate: h=1 {avg_h1:.1f}% → h=5 {avg_h5:.1f}%  "
          f"ratio={avg_h5/max(avg_h1,0.01):.2f}x  {'✅ compounds' if compounds else '❌ flat'}")
    summary["compounds"] = compounds
    _wb_log({"eval/fan_width_h1_agg": avg_h1, "eval/fan_width_h5_agg": avg_h5,
             "eval/compound_ratio": avg_h5 / max(avg_h1, 0.01)}, wb_run)

    # ══════════════════════════════════════════════════════════════
    # 3 & 4. ρ + PIT/KS — per origin × per horizon × per bracket
    # ══════════════════════════════════════════════════════════════
    print(f"\n── 3/4. ρ + Calibration (per origin × horizon × bracket) ──")
    print(f"  {'Origin':>6} {'h':>3} {'Bracket':<12} {'n':>6} {'ρ':>7} {'med_PIT':>8} {'KS':>7}")

    for origin in origins:
        for h in range(1, MAX_HORIZON + 1):
            for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
                triples = _pit_rho_for_origin_horizon_bracket(
                    raw_data, actuals, variant, origin, h, bkt_lo, bkt_hi
                )
                if len(triples) < 20:
                    continue
                pits = np.array([t[0] for t in triples])
                preds = np.array([t[1] for t in triples])
                acts = np.array([t[2] for t in triples])

                med_pit = float(np.median(pits))
                ks = float(kstest(pits, 'uniform').statistic)
                rho, _ = spearmanr(preds, acts)
                rho = float(rho)

                tag = f"o{origin}_h{h}_{bkt_label}"
                print(f"  {origin:>6} {h:>3} {bkt_label:<12} {len(triples):>6,} {rho:>+6.3f} {med_pit:>7.3f} {ks:>6.3f}")

                _wb_log({
                    f"eval/rho/{tag}": rho,
                    f"eval/pit_med/{tag}": med_pit,
                    f"eval/pit_ks/{tag}": ks,
                }, wb_run)
                summary[f"rho_{tag}"] = rho
                summary[f"ks_{tag}"] = ks

    # ══════════════════════════════════════════════════════════════
    # 5. CROSS-PARCEL CORRELATION (spatial coherence)
    # ══════════════════════════════════════════════════════════════
    print(f"\n── 5. Spatial Coherence (cross-parcel scenario correlation) ──")
    for origin in origins:
        key = (variant, origin)
        if key not in raw_data:
            continue
        r = raw_data[key]
        y_levels = r["y_levels"]
        N = y_levels.shape[0]

        for h in [1, 3, 5]:
            if h > y_levels.shape[2]:
                continue
            # Subsample for speed
            max_p = min(500, N)
            idx = np.random.choice(N, max_p, replace=False) if N > max_p else np.arange(N)
            y_h = y_levels[idx, :, h - 1]  # [N_sub, S]

            corr_mat = np.corrcoef(y_h)
            triu = np.triu_indices(len(idx), k=1)
            corrs = corr_mat[triu]
            corrs = corrs[np.isfinite(corrs)]

            if len(corrs) > 0:
                mc = float(np.mean(corrs))
                sc = float(np.std(corrs))
                print(f"  origin={origin} h={h}  mean_corr={mc:.3f}  std={sc:.3f}  pairs={len(corrs):,}")
                _wb_log({
                    f"eval/parcel_corr/o{origin}_h{h}": mc,
                    f"eval/parcel_corr_std/o{origin}_h{h}": sc,
                }, wb_run)
                summary[f"corr_o{origin}_h{h}"] = mc

    # ══════════════════════════════════════════════════════════════
    # SUMMARY TABLE
    # ══════════════════════════════════════════════════════════════
    _wb_summary(summary, wb_run)
    if wb_run is not None:
        wb_run.finish()
        print(f"\n✅ All metrics logged to W&B: {len(summary)} total data points")

    print(f"\n{'='*80}")
    print(f"📋 CHECKPOINT SUMMARY: {checkpoint_name}")
    print(f"{'='*80}")
    print(f"  Fan compounding:  h1={avg_h1:.1f}% → h5={avg_h5:.1f}%  {'✅' if compounds else '❌ flat'}")
    std_h1 = float(np.std(all_h1)) if all_h1 else 0
    print(f"  Heterogeneity:    std={std_h1:.1f}%  {'✅' if std_h1 > 5 else '❌ too uniform'}")
    print(f"  Total metrics:    {len(summary)} logged to W&B")

    return summary


# ═══════════════════════════════════════════════════════════════════
# CLI / Colab entry point
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__" or globals().get("__colab__"):
    _raw = globals().get("variant_raw_results")
    _actuals = globals().get("actual_vals")

    if _raw is None:
        _out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")
        _results_dir = os.path.join(_out_dir, "hypothesis_results")
        if os.path.isdir(_results_dir):
            _pkls = sorted([f for f in os.listdir(_results_dir) if f.startswith("variant_raw_") and f.endswith(".pkl")])
            if _pkls:
                with open(os.path.join(_results_dir, _pkls[-1]), "rb") as f:
                    _raw = pickle.load(f)
                print(f"Loaded {len(_raw)} variant×origin combos")

    if _raw is not None and _actuals is not None:
        run_eval_diagnostics(_raw, _actuals)
    else:
        print("⚠️  Load variant_raw_results and actual_vals into globals first, then call:")
        print("    run_eval_diagnostics(variant_raw_results, actual_vals, checkpoint_name='v11_epoch_30')")

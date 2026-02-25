"""
Cell 1.5: Sample-Size Sweep — Retrain checkpoints (SF-only, scaled)
==========================================================================
Run AFTER Cell 1 (worldmodel.py) and BEFORE Cell 2 (backtest).
Tests the hypothesis that $1M+ properties are OOD due to undersampling.

Key design decisions:
  - Filters panel to SINGLE FAMILY homes only (auto-detects state_class column)
  - Uses large sample sizes (200K+) to reach the stable sampling regime
  - Epochs scale proportionally with sample size

Reuses all Cell 1 globals:
  lf, train_accts, num_use, cat_use, NUM_DIM, N_CAT, work_dirs, cfg,
  build_master_training_shards_v102_local, derive_origin_shards_from_master,
  train_diffusion_from_shards_v102_local, create_denoiser_v102,
  GlobalProjection, GeoProjection, copy_small_artifacts_to_drive

Produces checkpoints named:  ckpt_origin_{ORIGIN}_{VARIANT}.pt
  e.g. ckpt_origin_2021_SF200K.pt, ckpt_origin_2021_SF500K.pt
"""
import os, time, hashlib
import numpy as np
import polars as pl

# ═══════════════════════════════════════════════════════════════════
# SWEEP CONFIG
# ═══════════════════════════════════════════════════════════════════

# Origins are auto-computed from BACKTEST_MIN_ORIGIN up to FORECAST_ORIGIN_YEAR
# (inclusive).  2025 is the latest viable anchor given HCAD data availability;
# 2026 HCAD appraisals do not yet exist.  Set SWEEP_ORIGINS_OVERRIDE to a
# specific list to override (e.g. [2025] to only train the production year).
BACKTEST_MIN_ORIGIN    = int(os.environ.get("BACKTEST_MIN_ORIGIN",    "2021"))
FORECAST_ORIGIN_YEAR   = int(os.environ.get("FORECAST_ORIGIN_YEAR",   "2025"))
SWEEP_ORIGINS_OVERRIDE = globals().get("SWEEP_ORIGINS_OVERRIDE", None)   # list or None

if SWEEP_ORIGINS_OVERRIDE is not None:
    SWEEP_ORIGINS = sorted(int(y) for y in SWEEP_ORIGINS_OVERRIDE)
else:
    SWEEP_ORIGINS = list(range(BACKTEST_MIN_ORIGIN, FORECAST_ORIGIN_YEAR + 1))

# Each variant: (tag, sample_size, stratify_above_dollar, stratify_target_pct)
#   stratify_above_dollar = None  → pure random sampling
#   stratify_above_dollar = 5e5  → oversample parcels appraised above $500K
SWEEP_VARIANTS = [
    ("SF200K",      200_000,  None,  None),
    ("SF500K",      500_000,  None,  None),
    ("SFstrat200K", 200_000,  5e5,   0.20),   # 20% of sample forced to $500K+ SF parcels
]

# Base epochs for a ~200K-row dataset.  Larger datasets get proportionally more.
# At 60 base epochs, N×rows gives ~same per-example gradient exposure.
SWEEP_EPOCHS_BASE = int(os.environ.get("SWEEP_EPOCHS", "60"))

# ─── Validate Cell 1 globals ───
_required = [
    "lf", "train_accts", "num_use", "cat_use", "NUM_DIM", "N_CAT",
    "work_dirs", "cfg", "OUT_DIR", "EVAL_ORIGINS", "FULL_HORIZON_ONLY",
    "build_master_training_shards_v102_local",
    "derive_origin_shards_from_master",
    "train_diffusion_from_shards_v102_local",
    "create_denoiser_v102", "GlobalProjection", "GeoProjection",
    "copy_small_artifacts_to_drive",
]
_missing = [r for r in _required if r not in dir() and r not in globals()]
if _missing:
    print(f"⚠️  Missing Cell 1 globals: {_missing}")
    print("   Make sure Cell 1 (worldmodel.py) has been executed first")
    raise SystemExit

import torch
import torch.nn as nn
_device = "cuda" if torch.cuda.is_available() else "cpu"
_out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")

print(f"🔬 SF-Only Sample-Size Sweep")
print(f"   Origins: {SWEEP_ORIGINS}")
print(f"   Variants: {[v[0] for v in SWEEP_VARIANTS]}")
print(f"   Base epochs: {SWEEP_EPOCHS_BASE} (scales with dataset size)")
print(f"   Device: {_device}")
print(f"   Output: {_out_dir}")


# ═══════════════════════════════════════════════════════════════════
# SAMPLING HELPERS
# ═══════════════════════════════════════════════════════════════════

def _sample_random(accts, n, seed=42):
    """Pure random subsample of account list."""
    rng = np.random.default_rng(seed)
    if len(accts) <= n:
        return list(accts)
    idx = rng.choice(len(accts), size=n, replace=False)
    return [accts[i] for i in idx]


def _sample_stratified(accts, n, lf_panel, origin_year, above_dollar, target_pct, seed=42):
    """
    Stratified sampling: force target_pct of the sample to be parcels
    valued above `above_dollar` at `origin_year`.

    Steps:
      1) Query panel for parcels with tot_appr_val > above_dollar at origin_year
      2) Intersect with available accts
      3) Take up to target_pct * n of those (the "high-value" slice)
      4) Fill the rest from remaining accts
    """
    rng = np.random.default_rng(seed)
    accts_set = set(accts)

    # Find high-value parcels at origin year
    hv_df = (
        lf_panel
        .filter(pl.col("yr") == int(origin_year))
        .filter(pl.col("tot_appr_val") > float(above_dollar))
        .select(pl.col("acct").cast(pl.Utf8))
        .collect()
    )
    hv_accts = [a for a in hv_df["acct"].to_list() if a in accts_set]

    n_hv_target = int(n * target_pct)
    n_hv = min(n_hv_target, len(hv_accts))

    if n_hv == 0:
        print(f"    ⚠️  No high-value parcels found above ${above_dollar/1e6:.0f}M at {origin_year}")
        return _sample_random(accts, n, seed)

    # Sample high-value parcels
    if len(hv_accts) > n_hv:
        hv_idx = rng.choice(len(hv_accts), size=n_hv, replace=False)
        hv_selected = [hv_accts[i] for i in hv_idx]
    else:
        hv_selected = list(hv_accts)

    hv_set = set(hv_selected)
    remaining = [a for a in accts if a not in hv_set]
    n_fill = n - len(hv_selected)

    if len(remaining) > n_fill:
        fill_idx = rng.choice(len(remaining), size=n_fill, replace=False)
        fill_selected = [remaining[i] for i in fill_idx]
    else:
        fill_selected = remaining

    result = hv_selected + fill_selected
    pct = len(hv_selected) / len(result) * 100 if result else 0
    print(f"    Stratified: {len(hv_selected):,} high-value ({pct:.1f}%) + "
          f"{len(fill_selected):,} random = {len(result):,} total")
    print(f"    (County has {len(hv_accts):,} parcels above ${above_dollar/1e6:.0f}M)")

    return result


# ═══════════════════════════════════════════════════════════════════
# MAIN SWEEP LOOP
# ═══════════════════════════════════════════════════════════════════
_lf = globals().get("lf")

# ── Auto-detect SF filter column ──
# HCAD panels typically have a state_class column where A1 = single-family.
# We search for common column names and apply the filter.
_panel_cols = set(_lf.collect_schema().names())
_SF_FILTER_COL = None
_SF_FILTER_VALUES = None

for _cand_col, _cand_vals in [
    ("state_class", ["A1"]),                       # HCAD standard
    ("property_type", ["SF", "SFR", "SINGLE"]),     # alternative naming
    ("prop_type_cd", ["A1"]),
    ("impr_tp_cd", ["1001", "1002", "1003"]),       # improvement type codes for SF
]:
    if _cand_col in _panel_cols:
        _SF_FILTER_COL = _cand_col
        _SF_FILTER_VALUES = _cand_vals
        break

if _SF_FILTER_COL:
    # Show what values exist so we can verify the filter is correct
    _unique_vals = (
        _lf.select(pl.col(_SF_FILTER_COL).cast(pl.Utf8))
        .unique()
        .collect()[_SF_FILTER_COL]
        .to_list()
    )
    print(f"\n🏠 SF filter: column '{_SF_FILTER_COL}' found")
    print(f"   Unique values ({len(_unique_vals)}): {sorted(_unique_vals)[:20]}")
    print(f"   Filtering to: {_SF_FILTER_VALUES}")

    _sf_lf = _lf.filter(
        pl.col(_SF_FILTER_COL).cast(pl.Utf8).str.strip_chars().is_in(_SF_FILTER_VALUES)
    )
    _all_accts_from_panel = (
        _sf_lf.select(pl.col("acct").cast(pl.Utf8).str.strip_chars())
        .unique()
        .collect()["acct"]
        .to_list()
    )
    # Also filter the panel used for shard building
    _lf_for_training = _sf_lf
else:
    print(f"\n⚠️  No SF filter column found in panel columns: {sorted(_panel_cols)[:25]}")
    print(f"   Proceeding WITHOUT SF filter — results may be noisy from mixed asset classes")
    print(f"   Consider adding a state_class or property_type column to the panel")
    _all_accts_from_panel = (
        _lf.select(pl.col("acct").cast(pl.Utf8).str.strip_chars())
        .unique()
        .collect()["acct"]
        .to_list()
    )
    _lf_for_training = _lf

_total_panel = (
    _lf.select(pl.col("acct").cast(pl.Utf8).str.strip_chars())
    .unique()
    .collect()
    .height
)
print(f"\n📋 Full panel: {_total_panel:,} unique accounts")
print(f"   SF-filtered: {len(_all_accts_from_panel):,} unique accounts")
print(f"   (train_accts had {len(globals().get('train_accts', []))})")

_all_accts = _all_accts_from_panel
_num_use = globals().get("num_use", [])
_cat_use = globals().get("cat_use", [])
_num_dim = globals().get("NUM_DIM", len(_num_use))
_n_cat = globals().get("N_CAT", len(_cat_use))
_full_horizon_only = globals().get("FULL_HORIZON_ONLY", True)
_work_dirs = globals().get("work_dirs", {})
_cfg = globals().get("cfg", {})

sweep_results = []

# ═══════════════════════════════════════════════════════════════════
# STARTUP: checkpoint existence matrix
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("📋 CHECKPOINT AUDIT — what exists vs what will be trained")
print("=" * 70)
print(f"  Origins : {SWEEP_ORIGINS}")
print(f"  Variants: {[v[0] for v in SWEEP_VARIANTS]}")
print(f"  CKPT_DIR: {_out_dir}")
print()
_to_train = []
for _vtag, _, _, _ in SWEEP_VARIANTS:
    _row = f"  {_vtag:<14}"
    for _orig in SWEEP_ORIGINS:
        _cp = os.path.join(_out_dir, f"ckpt_origin_{_orig}_{_vtag}.pt")
        if os.path.exists(_cp):
            _sz = os.path.getsize(_cp) / 1e6
            _row += f"  {_orig}:✅({_sz:.0f}MB)"
        else:
            _row += f"  {_orig}:❌MISSING"
            _to_train.append((_vtag, _orig))
    print(_row)
print()
if _to_train:
    print(f"  Will train {len(_to_train)} missing checkpoint(s):")
    for _vt, _oy in _to_train:
        print(f"    → {_vt} / origin {_oy}")
else:
    print("  ✅ All checkpoints already exist — nothing to train.")
print("=" * 70 + "\n")

for variant_tag, sample_size, strat_above, strat_pct in SWEEP_VARIANTS:
    print(f"\n{'█' * 70}")
    print(f"█  VARIANT = {variant_tag} (n={sample_size:,}"
          f"{f', stratify>{strat_above/1e6:.0f}M@{strat_pct:.0%}' if strat_above else ', random'})")
    print(f"{'█' * 70}")

    for origin in SWEEP_ORIGINS:
        # ── Check if checkpoint already exists ──
        ckpt_name = f"ckpt_origin_{origin}_{variant_tag}.pt"
        ckpt_path = os.path.join(_out_dir, ckpt_name)

        if os.path.exists(ckpt_path):
            sz = os.path.getsize(ckpt_path) / 1e6
            print(f"\n  ✅ {ckpt_name} already exists ({sz:.0f}MB), skipping")
            sweep_results.append({
                "variant": variant_tag, "origin": origin,
                "status": "skipped", "ckpt_path": ckpt_path,
            })
            continue

        print(f"\n  ── Origin {origin} ──")
        t0 = time.time()

        # ── Sample accounts ──
        seed = 42 + origin * 100  # reproducible but different per origin
        if strat_above is not None:
            sweep_accts = _sample_stratified(
                _all_accts, sample_size, _lf_for_training, origin, strat_above, strat_pct, seed)
        else:
            sweep_accts = _sample_random(_all_accts, sample_size, seed)

        print(f"  Sampled {len(sweep_accts):,} accounts")

        # ── Build master shards for this variant ──
        # Use a variant-specific scratch dir to avoid collisions
        import copy
        variant_work_dirs = copy.deepcopy(_work_dirs)
        variant_scratch = os.path.join(
            variant_work_dirs.get("SCRATCH_ROOT", "/content/wm_scratch"),
            f"sweep_{variant_tag}"
        )
        os.makedirs(variant_scratch, exist_ok=True)
        variant_work_dirs["RAW_SHARD_ROOT"] = os.path.join(variant_scratch, "raw")
        variant_work_dirs["SCALED_SHARD_ROOT"] = os.path.join(variant_scratch, "scaled")
        os.makedirs(variant_work_dirs["RAW_SHARD_ROOT"], exist_ok=True)
        os.makedirs(variant_work_dirs["SCALED_SHARD_ROOT"], exist_ok=True)

        max_origin = max(SWEEP_ORIGINS)
        master_result = build_master_training_shards_v102_local(
            lf=_lf_for_training,
            accts=sweep_accts,
            num_use_local=_num_use,
            cat_use_local=_cat_use,
            max_origin=max_origin,
            full_horizon_only=_full_horizon_only,
            work_dirs=variant_work_dirs,
        )
        _global_medians = master_result["global_medians"]
        master_shards = master_result["shards"]

        # ── Derive origin-specific shards ──
        origin_result = derive_origin_shards_from_master(
            master_shard_paths=master_shards,
            origin=origin,
            full_horizon_only=_full_horizon_only,
            work_dirs=variant_work_dirs,
        )
        origin_shards = origin_result["shards"]
        n_train = origin_result["n_train"]
        print(f"  Origin {origin}: {n_train:,} training rows across {len(origin_shards)} shards")

        if n_train == 0:
            print(f"  ⚠️  No training data for origin {origin}, skipping")
            sweep_results.append({
                "variant": variant_tag, "origin": origin,
                "status": "no_data", "ckpt_path": None,
            })
            continue

        # ── Create fresh model (no warmstart — clean hypothesis test) ──
        _hist_len = int(_cfg.get("FULL_HIST_LEN", 21))
        model = create_denoiser_v102(
            target_dim=int(_cfg.get("H", 5)),
            hist_len=_hist_len,
            num_dim=_num_dim,
            n_cat=_n_cat,
        ).to(_device)

        proj_g = GlobalProjection(int(_cfg.get("GLOBAL_RANK", 8))).to(_device)
        proj_geo = GeoProjection(int(_cfg.get("GEO_RANK", 4))).to(_device)
        # ── Compute scaled epochs ──
        # Base: 60 epochs for ~200K training rows.  Scale proportionally.
        _ref_rows = 200_000
        _epochs = max(SWEEP_EPOCHS_BASE, int(SWEEP_EPOCHS_BASE * n_train / _ref_rows))
        # Cap at 300 to avoid absurdly long runs
        _epochs = min(_epochs, 300)
        print(f"  🧪 Training from scratch ({_epochs} epochs, scaled for {n_train:,} rows)")

        # ── Train ──
        y_scaler, n_scaler, t_scaler, losses, _ = train_diffusion_from_shards_v102_local(
            shard_paths=origin_shards,
            origin=origin,
            epochs=_epochs,
            model=model,
            proj_g=proj_g,
            proj_geo=proj_geo,
            device=_device,
            num_dim=_num_dim,
            n_cat=_n_cat,
            work_dirs=variant_work_dirs,
        )

        # ── Save checkpoint ──
        ckpt_data = {
            "model_state_dict": model.state_dict(),
            "proj_g_state_dict": proj_g.state_dict(),
            "proj_geo_state_dict": proj_geo.state_dict(),
            "y_scaler_mean": y_scaler.mean_.tolist(),
            "y_scaler_scale": y_scaler.scale_.tolist(),
            "n_scaler_mean": n_scaler.mean_.tolist(),
            "n_scaler_scale": n_scaler.scale_.tolist(),
            "t_scaler_mean": t_scaler.mean_.tolist(),
            "t_scaler_scale": t_scaler.scale_.tolist(),
            "global_medians": _global_medians,
            "cfg": _cfg,
            "sweep": {
                "variant": variant_tag,
                "sample_size": sample_size,
                "stratify_above": strat_above,
                "stratify_target_pct": strat_pct,
                "n_train": n_train,
                "epochs": _epochs,
                "final_loss": losses[-1] if losses else None,
            },
        }

        # Save locally first, then copy to Drive
        local_ckpt = os.path.join(variant_scratch, ckpt_name)
        torch.save(ckpt_data, local_ckpt)
        final_path = copy_small_artifacts_to_drive(local_ckpt, _out_dir)
        dt = time.time() - t0
        print(f"  💾 Saved: {final_path} ({os.path.getsize(final_path)/1e6:.0f}MB)")
        print(f"  ⏱  Total time for {variant_tag}/{origin}: {dt:.0f}s")

        sweep_results.append({
            "variant": variant_tag, "origin": origin,
            "status": "trained", "ckpt_path": final_path,
            "n_train": n_train, "final_loss": losses[-1] if losses else None,
            "time_s": dt,
        })

        # Free GPU memory
        del model, proj_g, proj_geo
        if _device == "cuda":
            torch.cuda.empty_cache()

# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("🔬 SWEEP SUMMARY")
print("=" * 80)
print(f"\n  {'Variant':<12} {'Origin':>6}  {'Status':<10} {'n_train':>8} {'Loss':>8} {'Time':>8}")
print(f"  {'─'*12} {'─'*6}  {'─'*10} {'─'*8} {'─'*8} {'─'*8}")
for r in sweep_results:
    loss_s = f"{r.get('final_loss', 0):.5f}" if r.get('final_loss') else "  —"
    time_s = f"{r.get('time_s', 0):.0f}s" if r.get('time_s') else "  —"
    n_s = f"{r.get('n_train', 0):,}" if r.get('n_train') else "  —"
    print(f"  {r['variant']:<12} {r['origin']:>6}  {r['status']:<10} {n_s:>8} {loss_s:>8} {time_s:>8}")

trained = [r for r in sweep_results if r["status"] == "trained"]
skipped = [r for r in sweep_results if r["status"] == "skipped"]
print(f"\n  Trained: {len(trained)}  Skipped: {len(skipped)}")
print("\n✅ Sample-size sweep complete!")
print("   Run Cell 2 (backtest_entity_screening.py) to compare across variants.")

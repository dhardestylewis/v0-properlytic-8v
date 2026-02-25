# Single Colab cell: HCAD World Model v10.2-FULLPANEL (STABLE SAMPLING PATCHED)
# Fully consolidated, ASCII-only
#
# Full-panel capability:
# - Account-chunked materialization to on-disk NPZ shards (no giant in-RAM collect)
# - Shard-based diffusion training (batches copied to GPU per-step)
# - Chunked inference context builder (optional cap)
#
# Preserves v10.2 fixes:
# - Year-aligned labels (no row-shift year gaps)
# - Inference context builder (no label requirement)
# - Sampling-time shared initial noise distribution (matches training coherence)
# - Vectorized region_id and categorical hashing in Polars (no Python loops for hashing)
# - ONE model, ONE objective: predict H-step log1p deltas of tot_appr_val
#
# Numerical stability fixes (this rewrite):
# - Sampler runs in float32 (autocast disabled) until proven stable
# - Inline finite guards and clipping inside DDIM loop
# - Hard floor on scaler scales (prevents huge z-scores)
# - Optional z-score clipping at sampling-time only (conditioning)
# - Default scaled shard dtype float32 (float16 only after stability is proven)
# - Missing lag fill uses per-lag median (not zeros) in shard build and inference

import os, sys, time, math, json, warnings, hashlib, subprocess, contextlib, inspect, shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

warnings.filterwarnings("ignore")

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

# -----------------------------
# 0) Install deps (Colab-safe)
# -----------------------------
def _pip_install(pkgs: List[str]) -> None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + pkgs)

def ensure_import(mod: str, pip_name: Optional[str] = None) -> None:
    try:
        __import__(mod)
    except Exception:
        _pip_install([pip_name or mod])

ensure_import("polars", "polars")
ensure_import("pyarrow", "pyarrow")
ensure_import("wandb", "wandb")
import wandb

try:
    import torch
except ImportError:
    _pip_install(["torch", "--index-url", "https://download.pytorch.org/whl/cu121"])

import numpy as np
import polars as pl

import torch
import torch.nn as nn
import torch.nn.functional as F

torch.backends.cudnn.benchmark = True
try:
    torch.set_float32_matmul_precision("high")
except Exception:
    pass

# -----------------------------
# 0b) W&B experiment tracking
# -----------------------------
_WB_RUN = None

def init_wandb(
    project: str = "properlytic",
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    extra_config: Optional[Dict[str, Any]] = None,
    mode: str = "online",
) -> None:
    """Initialize W&B run.  Call once before training loop."""
    global _WB_RUN
    wb_cfg = dict(extra_config or {})
    try:
        _WB_RUN = wandb.init(
            project=project,
            name=name,
            tags=tags or [],
            config=wb_cfg,
            mode=mode,
            reinit=True,
        )
        print(f"[{ts()}] W&B run initialized: {_WB_RUN.url}")
    except Exception as e:
        print(f"[{ts()}] W&B init failed (continuing without): {e}")
        _WB_RUN = None

def wb_log(data: Dict[str, Any], **kw) -> None:
    """Log to W&B if active, otherwise silently skip."""
    if _WB_RUN is not None:
        try:
            wandb.log(data, **kw)
        except Exception:
            pass

def wb_config_update(data: Dict[str, Any]) -> None:
    """Update W&B run config if active."""
    if _WB_RUN is not None:
        try:
            wandb.config.update(data, allow_val_change=True)
        except Exception:
            pass

def wb_log_artifact(path: str, name: str, artifact_type: str = "model") -> None:
    """Log a file as W&B artifact."""
    if _WB_RUN is not None:
        try:
            art = wandb.Artifact(name, type=artifact_type)
            art.add_file(path)
            _WB_RUN.log_artifact(art)
        except Exception as e:
            print(f"[{ts()}] W&B artifact log failed: {e}")

# -----------------------------
# 1) Config - v10.2 FULLPANEL
# -----------------------------
PANEL_PATH_DRIVE = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
PANEL_PATH_LOCAL = "/content/local_panel.parquet"
PANEL_PATH = PANEL_PATH_LOCAL if os.path.exists(PANEL_PATH_LOCAL) else PANEL_PATH_DRIVE

OUT_DIR = "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel"
os.makedirs(OUT_DIR, exist_ok=True)

SEED = 42
rng = np.random.default_rng(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

# Time contract
MIN_YEAR = 2005
MAX_YEAR = 2025
SEAM_YEAR = 2025
H = 5
FULL_HIST_LEN = MAX_YEAR - MIN_YEAR + 1  # 21

EVAL_ORIGINS = [2021, 2022, 2023, 2024]
FULL_HORIZON_ONLY = True

# Mode toggles
FULL_PANEL_MODE = False
TRAIN_MAX_ACCTS = None
INFER_MAX_PARCELS = 500

# Sampling mode controls (used only if FULL_PANEL_MODE=False)
# Override via env var WM_SAMPLE_FRACTION for scaling sweeps
ACCT_SAMPLE_FRACTION = float(os.environ.get("WM_SAMPLE_FRACTION", "0.02"))
MAX_ACCTS_DIFFUSION = int(os.environ.get("WM_MAX_ACCTS", "50000"))

# Full-panel materialization controls
ACCT_CHUNK_SIZE_TRAIN = 300_000
ACCT_CHUNK_SIZE_INFER = 200_000
MAX_ROWS_PER_SHARD = 8_000_000

# Hash bucket sizes
HASH_BUCKET_SIZE = 32768
GEO_BUCKETS = 4096
GEO_CELL_DEG = 0.01
REGION_EMB_DIM = 32

# Hierarchical coherence shares
RHO_GLOBAL = 0.20
RHO_GEO = 0.20

# Latent path dimensions and AR(1)
GLOBAL_RANK = 8
GEO_RANK = 4
PHI_GLOBAL = 0.85
PHI_GEO = 0.70

# Training config
DIFF_BATCH = 524288
DIFF_LR = 4e-4
DIFF_EPOCHS = 60
DIFF_EPOCHS_WARMSTART = 20
DIFF_STEPS_TRAIN = 128
DIFF_STEPS_SAMPLE = 20

# Denoiser
DENOISER_HIDDEN = 256
DENOISER_LAYERS = 4
CONV_KERNEL_SIZE = 3

# Inference
INFERENCE_BATCH_SIZE = 16384
S_SCENARIOS = 256

# Precision / compilation
USE_BF16 = True
USE_TORCH_COMPILE = True
COMPILE_MODE = "max-autotune"

# Scaler floors (critical)
SCALE_FLOOR_Y = 1e-2
SCALE_FLOOR_NUM = 1e-2
SCALE_FLOOR_TGT = 1e-2

# Sampling stability controls
SAMPLER_DISABLE_AUTOCAST = False   # BF16 autocast on A100 — 2× matmul throughput; safe with nan_to_num guards
SAMPLER_Z_CLIP = 20.0        # conditioning z-score clip (sampling only)
SAMPLER_NOISE_CLIP = 10.0    # clamp noise_hat each step
SAMPLER_X0_CLIP = 50.0       # clamp x0_pred each step
SAMPLER_X_CLIP = 50.0        # clamp x each step
SAMPLER_REPORT_BAD_STEP = False    # suppresses per-step GPU→CPU syncs (.item() calls)

# Scaled shard dtype
SCALED_SHARDS_FLOAT16 = False  # keep float32 until stability is proven

print(f"[{ts()}] HCAD World Model v10.2 FULLPANEL (STABLE SAMPLING PATCHED)")
print(f"[{ts()}] FULL_PANEL_MODE={FULL_PANEL_MODE} FULL_HORIZON_ONLY={FULL_HORIZON_ONLY}")
print(f"[{ts()}] DIFF_BATCH={DIFF_BATCH} DIFF_LR={DIFF_LR} DIFF_EPOCHS={DIFF_EPOCHS} DIFF_EPOCHS_WARMSTART={DIFF_EPOCHS_WARMSTART}")
print(f"[{ts()}] RHO_GLOBAL={RHO_GLOBAL} RHO_GEO={RHO_GEO} PHI_GLOBAL={PHI_GLOBAL} PHI_GEO={PHI_GEO}")
print(f"[{ts()}] SCALE_FLOOR_Y={SCALE_FLOOR_Y} SCALE_FLOOR_NUM={SCALE_FLOOR_NUM} SCALE_FLOOR_TGT={SCALE_FLOOR_TGT}")
print(f"[{ts()}] SAMPLER_DISABLE_AUTOCAST={SAMPLER_DISABLE_AUTOCAST} SAMPLER_Z_CLIP={SAMPLER_Z_CLIP}")

cfg = {
    "version": "v10.2_fullpanel_stable_sampling",
    "FULL_PANEL_MODE": bool(FULL_PANEL_MODE),
    "FULL_HORIZON_ONLY": bool(FULL_HORIZON_ONLY),
    "MIN_YEAR": int(MIN_YEAR),
    "MAX_YEAR": int(MAX_YEAR),
    "SEAM_YEAR": int(SEAM_YEAR),
    "H": int(H),
    "EVAL_ORIGINS": list(EVAL_ORIGINS),
    "ACCT_SAMPLE_FRACTION": float(ACCT_SAMPLE_FRACTION),
    "MAX_ACCTS_DIFFUSION": int(MAX_ACCTS_DIFFUSION),
    "ACCT_CHUNK_SIZE_TRAIN": int(ACCT_CHUNK_SIZE_TRAIN),
    "ACCT_CHUNK_SIZE_INFER": int(ACCT_CHUNK_SIZE_INFER),
    "MAX_ROWS_PER_SHARD": int(MAX_ROWS_PER_SHARD),
    "HASH_BUCKET_SIZE": int(HASH_BUCKET_SIZE),
    "GEO_BUCKETS": int(GEO_BUCKETS),
    "GEO_CELL_DEG": float(GEO_CELL_DEG),
    "REGION_EMB_DIM": int(REGION_EMB_DIM),
    "RHO_GLOBAL": float(RHO_GLOBAL),
    "RHO_GEO": float(RHO_GEO),
    "GLOBAL_RANK": int(GLOBAL_RANK),
    "GEO_RANK": int(GEO_RANK),
    "PHI_GLOBAL": float(PHI_GLOBAL),
    "PHI_GEO": float(PHI_GEO),
    "DIFF_BATCH": int(DIFF_BATCH),
    "DIFF_LR": float(DIFF_LR),
    "DIFF_EPOCHS": int(DIFF_EPOCHS),
    "DIFF_EPOCHS_WARMSTART": int(DIFF_EPOCHS_WARMSTART),
    "DIFF_STEPS_TRAIN": int(DIFF_STEPS_TRAIN),
    "DIFF_STEPS_SAMPLE": int(DIFF_STEPS_SAMPLE),
    "INFERENCE_BATCH_SIZE": int(INFERENCE_BATCH_SIZE),
    "S_SCENARIOS": int(S_SCENARIOS),
    "USE_BF16": bool(USE_BF16),
    "USE_TORCH_COMPILE": bool(USE_TORCH_COMPILE),
    "COMPILE_MODE": str(COMPILE_MODE),
    "SCALE_FLOOR_Y": float(SCALE_FLOOR_Y),
    "SCALE_FLOOR_NUM": float(SCALE_FLOOR_NUM),
    "SCALE_FLOOR_TGT": float(SCALE_FLOOR_TGT),
    "SAMPLER_Z_CLIP": float(SAMPLER_Z_CLIP),
    "SAMPLER_NOISE_CLIP": float(SAMPLER_NOISE_CLIP),
    "SAMPLER_X0_CLIP": float(SAMPLER_X0_CLIP),
    "SAMPLER_X_CLIP": float(SAMPLER_X_CLIP),
    "SCALED_SHARDS_FLOAT16": bool(SCALED_SHARDS_FLOAT16),
}
with open(os.path.join(OUT_DIR, "run_config.json"), "w") as f:
    json.dump(cfg, f, indent=2)

# -----------------------------
# 2) Mount Drive + load panel
# -----------------------------
try:
    from google.colab import drive
    if not os.path.exists("/content/drive"):
        drive.mount("/content/drive", force_remount=False)
except Exception as e:
    print(f"[{ts()}] Drive mount skipped: {e}")

if not os.path.exists(PANEL_PATH):
    if os.path.exists(PANEL_PATH_DRIVE):
        PANEL_PATH = PANEL_PATH_DRIVE
    else:
        raise FileNotFoundError("Panel not found")

print(f"[{ts()}] Loading panel: {PANEL_PATH}")
lf = pl.scan_parquet(PANEL_PATH)
schema = lf.collect_schema()
cols = schema.names()
cols_set = set(cols)

# Contract checks
assert all(c in cols_set for c in ["acct", "yr", "tot_appr_val"]), "Missing required columns"
forbidden = ["tot_mkt_val", "assessed_val", "land_val", "bld_val"]
assert not any(c in cols_set for c in forbidden), "Forbidden columns present"
print(f"[{ts()}] Panel acceptance: PASS")

# Feature selection
cat_cols = [c for c in cols if c not in ["acct", "yr", "tot_appr_val"] and "str" in str(schema.get(c)).lower()]
num_cols = [c for c in cols if c not in ["acct", "yr", "tot_appr_val"] and c not in cat_cols]
num_use = num_cols[:30]
cat_use = cat_cols[:10]
NUM_DIM = int(len(num_use))
N_CAT = int(len(cat_use))
print(f"[{ts()}] Features: {NUM_DIM} numeric, {N_CAT} categorical")
print(f"[{ts()}] FEATURE AUDIT — numeric ({NUM_DIM}): {num_use}")
print(f"[{ts()}] FEATURE AUDIT — categorical ({N_CAT}): {cat_use}")
if len(num_cols) > 30:
    print(f"[{ts()}] FEATURE AUDIT — UNUSED numeric ({len(num_cols)-30}): {num_cols[30:]}")
if len(cat_cols) > 10:
    print(f"[{ts()}] FEATURE AUDIT — UNUSED categorical ({len(cat_cols)-10}): {cat_cols[10:]}")
wb_config_update({
    "features/num_use": num_use,
    "features/cat_use": cat_use,
    "features/num_available": num_cols,
    "features/cat_available": cat_cols,
    "features/num_unused": num_cols[30:] if len(num_cols) > 30 else [],
    "features/cat_unused": cat_cols[10:] if len(cat_cols) > 10 else [],
})

# Geo column discovery (GIS-aware)
GEO_COL = None
for c in ["gis_zip", "h3_8", "h3_7", "h3_9", "geoid", "tract", "zip", "zipcode"]:
    if c in cols_set:
        GEO_COL = c
        break
HAS_LATLON = ("gis_lat" in cols_set) and ("gis_lon" in cols_set)
print(f"[{ts()}] GEO_COL={GEO_COL} HAS_LATLON={HAS_LATLON}")

# -----------------------------
# PATCH A) Hist lag fill that cannot create "all-missing -> 0.0" columns
# Also computes a safe fallback log1p(level) for rows that are entirely missing (rare).
# -----------------------------
def compute_year_median_log1p_level(lf: pl.LazyFrame, min_year: int, max_year: int) -> Dict[int, float]:
    dfm = (
        lf
        .filter(pl.col("yr").is_between(int(min_year), int(max_year)))
        .filter(pl.col("tot_appr_val") > 0)
        .select([
            pl.col("yr").cast(pl.Int32).alias("yr"),
            pl.col("tot_appr_val").log1p().alias("y_log"),
        ])
        .group_by("yr")
        .agg(pl.col("y_log").median().alias("y_med"))
        .collect()
    )
    out = {}
    for r in dfm.iter_rows(named=True):
        out[int(r["yr"])] = float(r["y_med"])
    return out

Y_MED_BY_YEAR = compute_year_median_log1p_level(lf, MIN_YEAR, MAX_YEAR)
Y_FALLBACK_LOG1P = float(Y_MED_BY_YEAR.get(int(MIN_YEAR), 0.0))
print(f"[{ts()}] Y_FALLBACK_LOG1P (year {MIN_YEAR} median) = {Y_FALLBACK_LOG1P:.6f}")

def fill_hist_lags_no_zeros(hist_mat: np.ndarray, fallback_value: float) -> np.ndarray:
    """
    hist_mat: [N, FULL_HIST_LEN], oldest->newest (lag_{L-1} ... lag_0), may contain NaN for missing years.
    Goal:
      - Fill partially-missing columns using column medians.
      - Fill fully-missing columns (e.g., calendar underflow) using per-row earliest observed value (not a constant).
      - Never emit 0.0 due to NaN medians; only use fallback for rows that are entirely missing.
    """
    if hist_mat.size == 0:
        return hist_mat.astype(np.float32)

    X = hist_mat.astype(np.float32, copy=False)

    # Step 1) Fill columns that have at least one finite value with that column median.
    col_med = np.nanmedian(X, axis=0)  # NaN for fully-missing columns
    finite_col = np.isfinite(col_med)
    if finite_col.any():
        Xm = X[:, finite_col]
        Xm = np.where(np.isfinite(Xm), Xm, col_med[finite_col][None, :]).astype(np.float32)
        X[:, finite_col] = Xm

    # Step 2) Remaining NaNs are from fully-missing columns (calendar underflow) or rare full-row gaps.
    # Fill per-row with the earliest finite value in that row (after Step 1).
    finite_mask = np.isfinite(X)
    any_finite_row = finite_mask.any(axis=1)

    # Find first finite per row scanning left->right (oldest->newest).
    idx_first = np.argmax(finite_mask, axis=1)  # 0 if none finite; guarded below
    row_first = X[np.arange(X.shape[0]), idx_first]
    row_first = np.where(any_finite_row, row_first, float(fallback_value)).astype(np.float32)

    X = np.where(np.isfinite(X), X, row_first[:, None]).astype(np.float32)

    # Step 3) Final guard
    X = np.nan_to_num(X, nan=float(fallback_value), posinf=float(fallback_value), neginf=float(fallback_value)).astype(np.float32)
    return X

# -----------------------------
# 3) Polars stable hashing helpers
# -----------------------------
_HASH_HAS_SEED = False
try:
    _HASH_HAS_SEED = "seed" in inspect.signature(pl.Expr.hash).parameters
except Exception:
    _HASH_HAS_SEED = False

def stable_hash_expr(e: pl.Expr) -> pl.Expr:
    if _HASH_HAS_SEED:
        return e.hash(seed=SEED)
    return e.hash()

def bucket_hash_expr_str(col_expr: pl.Expr, n_buckets: int) -> pl.Expr:
    return (
        pl.when(col_expr.is_null())
        .then(pl.lit(0))
        .otherwise(
            (stable_hash_expr(col_expr.cast(pl.Utf8)) % (n_buckets - 1) + 1).cast(pl.Int64)
        )
        .cast(pl.Int64)
    )

def build_region_id_expr() -> pl.Expr:
    if GEO_COL and GEO_COL in cols_set:
        return bucket_hash_expr_str(pl.col(GEO_COL), GEO_BUCKETS).alias("region_id")
    if HAS_LATLON:
        lat_col = "gis_lat"
        lon_col = "gis_lon"
        lat_bin = (pl.col(lat_col).fill_null(0.0) / GEO_CELL_DEG).floor().cast(pl.Int64)
        lon_bin = (pl.col(lon_col).fill_null(0.0) / GEO_CELL_DEG).floor().cast(pl.Int64)
        key = (lat_bin * 1_000_000 + lon_bin).cast(pl.Utf8)
        return bucket_hash_expr_str(key, GEO_BUCKETS).alias("region_id")
    return bucket_hash_expr_str(pl.col("acct").cast(pl.Utf8), GEO_BUCKETS).alias("region_id")

def build_cat_hash_exprs(cat_cols_local: List[str]) -> List[pl.Expr]:
    exprs = []
    for c in cat_cols_local:
        if c in cols_set:
            exprs.append(bucket_hash_expr_str(pl.col(c), HASH_BUCKET_SIZE).alias(f"cat_{c}"))
        else:
            exprs.append(pl.lit(0).cast(pl.Int64).alias(f"cat_{c}"))
    return exprs

# -----------------------------
# 4) Account discovery and sampling
# -----------------------------
print(f"[{ts()}] Discovering accounts...")
accts_df = lf.select(pl.col("acct").cast(pl.Utf8).unique()).collect()
all_accts = accts_df["acct"].to_list()
print(f"[{ts()}] all_accts={len(all_accts):,}")

if TRAIN_MAX_ACCTS is not None:
    all_accts = all_accts[:int(TRAIN_MAX_ACCTS)]
    print(f"[{ts()}] TRAIN_MAX_ACCTS applied: {len(all_accts):,}")

if not FULL_PANEL_MODE:
    sampled_accts = [
        a for a in all_accts
        if int(hashlib.md5(a.encode()).hexdigest(), 16) % 10000 < int(ACCT_SAMPLE_FRACTION * 10000)
    ][:MAX_ACCTS_DIFFUSION]
    train_accts = sampled_accts
    infer_accts = sampled_accts
    print(f"[{ts()}] Sampled accounts: {len(sampled_accts):,}")
else:
    train_accts = all_accts
    infer_accts = all_accts
    print(f"[{ts()}] Full-panel accounts: {len(train_accts):,}")

# -----------------------------
# 5) Local scratch dir resolver
# -----------------------------
def resolve_local_work_dirs(out_dir_drive: str, scratch_root: str = "/content/wm_scratch") -> Dict[str, str]:
    os.makedirs(scratch_root, exist_ok=True)
    raw_shard_root = os.path.join(scratch_root, "train_shards_raw")
    scaled_shard_root = os.path.join(scratch_root, "train_shards_scaled")
    os.makedirs(raw_shard_root, exist_ok=True)
    os.makedirs(scaled_shard_root, exist_ok=True)
    return {
        "OUT_DIR_DRIVE": out_dir_drive,
        "SCRATCH_ROOT": scratch_root,
        "RAW_SHARD_ROOT": raw_shard_root,
        "SCALED_SHARD_ROOT": scaled_shard_root,
    }

def copy_small_artifacts_to_drive(src_path: str, dst_dir_drive: str) -> str:
    os.makedirs(dst_dir_drive, exist_ok=True)
    base = os.path.basename(src_path)
    dst_path = os.path.join(dst_dir_drive, base)
    try:
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception as e:
        print(f"[{ts()}] WARNING copy_to_drive failed: {e}")
        return src_path

work_dirs = resolve_local_work_dirs(OUT_DIR)
print(f"[{ts()}] work_dirs={work_dirs}")

# -----------------------------
# 6) Shard I/O (mmap-friendly)
# -----------------------------
def _np_savez_shard(
    path: str,
    hist_y: np.ndarray,
    cur_num: np.ndarray,
    cur_cat: np.ndarray,
    region_id: np.ndarray,
    target: np.ndarray,      # [N,H]
    mask: np.ndarray,        # [N,H] float32
    yr_label: np.ndarray,    # [N,H] int32
    y_anchor: np.ndarray,
    anchor_year: np.ndarray,
    acct: np.ndarray,
) -> None:
    np.savez(
        path,
        hist_y=hist_y.astype(np.float32, copy=False),
        cur_num=cur_num.astype(np.float32, copy=False),
        cur_cat=cur_cat.astype(np.int64, copy=False),
        region_id=region_id.astype(np.int64, copy=False),
        target=target.astype(np.float32, copy=False),
        mask=mask.astype(np.float32, copy=False),
        yr_label=yr_label.astype(np.int32, copy=False),
        y_anchor=y_anchor.astype(np.float32, copy=False),
        anchor_year=anchor_year.astype(np.int32, copy=False),
        acct=acct.astype(object, copy=False),
    )

def _iter_shards_npz(shard_paths: List[str]):
    for p in shard_paths:
        z = np.load(p, allow_pickle=True)
        yield p, z

# -----------------------------
# 7) Master shards at max_origin, then derive per-origin shards
# -----------------------------
def _nan_fill_with_median_per_col(X: np.ndarray) -> np.ndarray:
    # X: [N,D]
    if X.size == 0:
        return X.astype(np.float32)
    X = X.astype(np.float32, copy=False)
    med = np.nanmedian(X, axis=0)
    med = np.where(np.isfinite(med), med, 0.0).astype(np.float32)
    X2 = np.where(np.isfinite(X), X, med[None, :]).astype(np.float32)
    return X2

# -----------------------------
# PATCH B) Master shard builder: use fill_hist_lags_no_zeros so early-lag columns never collapse
# -----------------------------
def build_master_training_shards_v102_local(
    lf: pl.LazyFrame,
    accts: List[str],
    num_use_local: List[str],
    cat_use_local: List[str],
    max_origin: int,
    full_horizon_only: bool,
    work_dirs: Dict[str, str],
    acct_chunk_size: int = ACCT_CHUNK_SIZE_TRAIN,
    max_rows_per_shard: int = MAX_ROWS_PER_SHARD,
) -> Dict[str, Any]:
    origin = int(max_origin)
    train_max_year = int(origin - 1)

    if full_horizon_only:
        anchor_cutoff = int(origin - 1 - H)
    else:
        anchor_cutoff = int(origin - 2)

    out_root = work_dirs["RAW_SHARD_ROOT"]
    shard_dir = os.path.join(out_root, f"master_origin_{origin}")
    os.makedirs(shard_dir, exist_ok=True)

    print(f"[{ts()}] Building MASTER shards at max_origin={origin}")
    print(f"[{ts()}] train_max_year={train_max_year} anchor_cutoff={anchor_cutoff} full_horizon_only={full_horizon_only}")
    print(f"[{ts()}] shard_dir={shard_dir}")

    region_expr = build_region_id_expr()
    cat_hash_exprs = build_cat_hash_exprs(cat_use_local)

    shift_exprs: List[pl.Expr] = []
    for k in range(0, H + 1):
        shift_exprs.append(pl.col("y_log").shift(-k).over("acct").alias(f"y_shift_{k}"))
        shift_exprs.append(pl.col("yr").shift(-k).over("acct").alias(f"yr_shift_{k}"))

    hist_exprs = [pl.col("y_log").shift(i).over("acct").alias(f"lag_{i}") for i in range(FULL_HIST_LEN)]
    global_medians_accum: Dict[str, List[float]] = {c: [] for c in num_use_local}

    shard_paths: List[str] = []
    shard_id = 0
    n_train_total = 0
    max_anchor_year_seen: Optional[int] = None
    max_label_year_used = 0

    buf_hist: List[np.ndarray] = []
    buf_num: List[np.ndarray] = []
    buf_cat: List[np.ndarray] = []
    buf_rid: List[np.ndarray] = []
    buf_tgt: List[np.ndarray] = []
    buf_msk: List[np.ndarray] = []
    buf_yr: List[np.ndarray] = []
    buf_yanc: List[np.ndarray] = []
    buf_ay: List[np.ndarray] = []
    buf_acct: List[np.ndarray] = []
    buf_rows = 0

    def flush() -> None:
        nonlocal shard_id, buf_rows
        if buf_rows <= 0:
            return
        hist_y = np.concatenate(buf_hist, axis=0)
        cur_num = np.concatenate(buf_num, axis=0) if buf_num else np.zeros((hist_y.shape[0], 0), np.float32)
        cur_cat = np.concatenate(buf_cat, axis=0) if buf_cat else np.zeros((hist_y.shape[0], 0), np.int64)
        region_id = np.concatenate(buf_rid, axis=0)
        target = np.concatenate(buf_tgt, axis=0)
        mask = np.concatenate(buf_msk, axis=0)
        yr_label = np.concatenate(buf_yr, axis=0)
        y_anchor = np.concatenate(buf_yanc, axis=0)
        anchor_year = np.concatenate(buf_ay, axis=0)
        acct_arr = np.concatenate(buf_acct, axis=0).astype(object)

        shard_path = os.path.join(shard_dir, f"shard_{shard_id:05d}.npz")
        _np_savez_shard(
            shard_path,
            hist_y=hist_y,
            cur_num=cur_num,
            cur_cat=cur_cat,
            region_id=region_id,
            target=target,
            mask=mask,
            yr_label=yr_label,
            y_anchor=y_anchor,
            anchor_year=anchor_year,
            acct=acct_arr,
        )
        shard_paths.append(shard_path)
        print(f"[{ts()}] Wrote MASTER {os.path.basename(shard_path)} rows={hist_y.shape[0]:,}")

        shard_id += 1
        buf_hist.clear()
        buf_num.clear()
        buf_cat.clear()
        buf_rid.clear()
        buf_tgt.clear()
        buf_msk.clear()
        buf_yr.clear()
        buf_yanc.clear()
        buf_ay.clear()
        buf_acct.clear()
        buf_rows = 0

    t0_all = time.time()

    for s in range(0, len(accts), int(acct_chunk_size)):
        acct_chunk = accts[s:s + int(acct_chunk_size)]
        if not acct_chunk:
            continue

        t0 = time.time()
        base_q = (
            lf
            .filter(pl.col("acct").cast(pl.Utf8).is_in(acct_chunk))
            .filter(pl.col("yr").is_between(MIN_YEAR, MAX_YEAR))
            .filter(pl.col("tot_appr_val") > 0)
            .with_columns([
                pl.col("acct").cast(pl.Utf8).alias("acct"),
                pl.col("yr").cast(pl.Int32).alias("yr"),
                pl.col("tot_appr_val").log1p().alias("y_log"),
            ])
            .sort(["acct", "yr"])
        )

        q = (
            base_q
            .with_columns(shift_exprs + hist_exprs + [region_expr] + cat_hash_exprs)
            .filter(pl.col("yr") <= int(anchor_cutoff))
        )

        df = q.collect()
        dt = time.time() - t0
        print(f"[{ts()}] MASTER acct_chunk={s}:{s+len(acct_chunk)} collect_rows={len(df):,} time={dt:.1f}s")
        if len(df) == 0:
            continue

        anchor_years = df["yr"].to_numpy().astype(np.int32)
        if anchor_years.size > 0:
            ay_max = int(anchor_years.max())
            if (max_anchor_year_seen is None) or (ay_max > max_anchor_year_seen):
                max_anchor_year_seen = ay_max

        n = int(len(df))
        target = np.zeros((n, H), dtype=np.float32)
        mask = np.zeros((n, H), dtype=np.float32)
        yr_label = np.zeros((n, H), dtype=np.int32)

        for k in range(1, H + 1):
            y_curr = df[f"y_shift_{k}"].to_numpy().astype(np.float32)
            y_prev = df[f"y_shift_{k-1}"].to_numpy().astype(np.float32)
            yr_curr = df[f"yr_shift_{k}"].to_numpy()
            yr_prev = df[f"yr_shift_{k-1}"].to_numpy()

            expected_curr = anchor_years + k
            expected_prev = anchor_years + (k - 1)

            year_aligned = (yr_curr == expected_curr) & (yr_prev == expected_prev)
            valid = np.isfinite(y_curr) & np.isfinite(y_prev) & year_aligned & (yr_curr <= int(train_max_year))

            target[:, k - 1] = np.where(valid, y_curr - y_prev, 0.0).astype(np.float32)
            mask[:, k - 1] = valid.astype(np.float32)
            yr_label[:, k - 1] = expected_curr.astype(np.int32)

            if valid.any():
                max_label_year_used = max(max_label_year_used, int(np.max(yr_curr[valid])))

        if full_horizon_only:
            keep = (mask.sum(axis=1) == float(H))
        else:
            keep = (mask.sum(axis=1) >= 1.0)

        if not keep.any():
            continue

        idx = np.where(keep)[0]
        n_keep = int(idx.size)
        n_train_total += n_keep

        # Build hist_y (oldest->newest) and fill safely (never 0.0 from NaN medians)
        hist_cols = [f"lag_{i}" for i in range(FULL_HIST_LEN - 1, -1, -1)]
        hist_mat = np.column_stack([df[c].to_numpy().astype(np.float32) for c in hist_cols]).astype(np.float32)
        hist_mat = fill_hist_lags_no_zeros(hist_mat, fallback_value=float(Y_FALLBACK_LOG1P))
        hist_y = hist_mat[idx].astype(np.float32)

        y_anchor = df["y_log"].to_numpy().astype(np.float32)[idx]
        region_id = df["region_id"].to_numpy().astype(np.int64)[idx]
        acct_arr = df["acct"].to_numpy().astype(object)[idx]
        anchor_year_keep = anchor_years[idx]

        cur_num_list = []
        for c in num_use_local:
            if c in df.columns:
                vals = df[c].to_numpy().astype(np.float32)
                med = float(np.nanmedian(vals)) if np.isfinite(np.nanmedian(vals)) else 0.0
                global_medians_accum[c].append(med)
                cur_num_list.append(np.nan_to_num(vals, nan=med).astype(np.float32)[idx])
            else:
                global_medians_accum[c].append(0.0)
                cur_num_list.append(np.zeros(n_keep, dtype=np.float32))
        cur_num = np.column_stack(cur_num_list).astype(np.float32) if cur_num_list else np.zeros((n_keep, 0), np.float32)

        cur_cat_list = []
        for c in cat_use_local:
            hc = f"cat_{c}"
            if hc in df.columns:
                cur_cat_list.append(df[hc].to_numpy().astype(np.int64)[idx])
            else:
                cur_cat_list.append(np.zeros(n_keep, dtype=np.int64))
        cur_cat = np.column_stack(cur_cat_list).astype(np.int64) if cur_cat_list else np.zeros((n_keep, 0), np.int64)

        buf_hist.append(hist_y)
        buf_num.append(cur_num)
        buf_cat.append(cur_cat)
        buf_rid.append(region_id)
        buf_tgt.append(target[idx].astype(np.float32))
        buf_msk.append(mask[idx].astype(np.float32))
        buf_yr.append(yr_label[idx].astype(np.int32))
        buf_yanc.append(y_anchor.astype(np.float32))
        buf_ay.append(anchor_year_keep.astype(np.int32))
        buf_acct.append(acct_arr.astype(object))
        buf_rows += n_keep

        if buf_rows >= int(max_rows_per_shard):
            flush()

    flush()

    global_medians: Dict[str, float] = {}
    for c in num_use_local:
        vals = global_medians_accum.get(c, [])
        global_medians[c] = float(np.median(np.asarray(vals, dtype=np.float32))) if vals else 0.0

    if max_anchor_year_seen is not None:
        assert int(max_anchor_year_seen) <= int(origin - 1), f"Anchor leakage (master): {max_anchor_year_seen} > {origin-1}"
    assert int(max_label_year_used) <= int(origin - 1), f"Label leakage (master): {max_label_year_used} > {origin-1}"

    dt_all = time.time() - t0_all
    print(f"[{ts()}] MASTER build done shards={len(shard_paths)} n_train={n_train_total:,} time={dt_all:.1f}s")
    print(f"[{ts()}] MASTER max_anchor_year={max_anchor_year_seen} max_label_year_used={max_label_year_used}")

    return {
        "max_origin": int(origin),
        "shards": shard_paths,
        "n_train": int(n_train_total),
        "global_medians": global_medians,
        "max_anchor_year": max_anchor_year_seen,
        "max_label_year_used": int(max_label_year_used),
        "master_dir": shard_dir,
    }

def derive_origin_shards_from_master(
    master_shard_paths: List[str],
    origin: int,
    full_horizon_only: bool,
    work_dirs: Dict[str, str],
    max_rows_per_shard: int = MAX_ROWS_PER_SHARD,
) -> Dict[str, Any]:
    origin = int(origin)
    out_root = work_dirs["RAW_SHARD_ROOT"]
    shard_dir = os.path.join(out_root, f"origin_{origin}")
    os.makedirs(shard_dir, exist_ok=True)

    print(f"[{ts()}] Deriving origin shards from master origin={origin} dir={shard_dir}")

    shard_paths: List[str] = []
    shard_id = 0
    n_train_total = 0
    max_anchor_year_seen: Optional[int] = None
    max_label_year_used = 0

    buf_hist: List[np.ndarray] = []
    buf_num: List[np.ndarray] = []
    buf_cat: List[np.ndarray] = []
    buf_rid: List[np.ndarray] = []
    buf_tgt: List[np.ndarray] = []
    buf_msk: List[np.ndarray] = []
    buf_yr: List[np.ndarray] = []
    buf_yanc: List[np.ndarray] = []
    buf_ay: List[np.ndarray] = []
    buf_acct: List[np.ndarray] = []
    buf_rows = 0

    def flush() -> None:
        nonlocal shard_id, buf_rows
        if buf_rows <= 0:
            return
        hist_y = np.concatenate(buf_hist, axis=0)
        cur_num = np.concatenate(buf_num, axis=0) if buf_num else np.zeros((hist_y.shape[0], 0), np.float32)
        cur_cat = np.concatenate(buf_cat, axis=0) if buf_cat else np.zeros((hist_y.shape[0], 0), np.int64)
        region_id = np.concatenate(buf_rid, axis=0)
        target = np.concatenate(buf_tgt, axis=0)
        mask = np.concatenate(buf_msk, axis=0)
        yr_label = np.concatenate(buf_yr, axis=0)
        y_anchor = np.concatenate(buf_yanc, axis=0)
        anchor_year = np.concatenate(buf_ay, axis=0)
        acct_arr = np.concatenate(buf_acct, axis=0).astype(object)

        shard_path = os.path.join(shard_dir, f"shard_{shard_id:05d}.npz")
        _np_savez_shard(
            shard_path,
            hist_y=hist_y,
            cur_num=cur_num,
            cur_cat=cur_cat,
            region_id=region_id,
            target=target,
            mask=mask,
            yr_label=yr_label,
            y_anchor=y_anchor,
            anchor_year=anchor_year,
            acct=acct_arr,
        )
        shard_paths.append(shard_path)
        print(f"[{ts()}] Wrote ORIGIN {os.path.basename(shard_path)} rows={hist_y.shape[0]:,}")

        shard_id += 1
        buf_hist.clear()
        buf_num.clear()
        buf_cat.clear()
        buf_rid.clear()
        buf_tgt.clear()
        buf_msk.clear()
        buf_yr.clear()
        buf_yanc.clear()
        buf_ay.clear()
        buf_acct.clear()
        buf_rows = 0

    t0 = time.time()
    required_max_label = int(origin - 1)

    for _, z in _iter_shards_npz(master_shard_paths):
        hist_y = z["hist_y"].astype(np.float32, copy=False)
        cur_num = z["cur_num"].astype(np.float32, copy=False)
        cur_cat = z["cur_cat"].astype(np.int64, copy=False)
        region_id = z["region_id"].astype(np.int64, copy=False)
        target = z["target"].astype(np.float32, copy=False)
        mask = z["mask"].astype(np.float32, copy=False)
        yr_label = z["yr_label"].astype(np.int32, copy=False)
        y_anchor = z["y_anchor"].astype(np.float32, copy=False)
        anchor_year = z["anchor_year"].astype(np.int32, copy=False)
        acct = z["acct"].astype(object, copy=False)

        if hist_y.shape[0] == 0:
            continue

        allowed = (yr_label <= required_max_label).astype(np.float32)
        mask_new = mask * allowed

        if full_horizon_only:
            keep = (mask_new.sum(axis=1) == float(H))
        else:
            keep = (mask_new.sum(axis=1) >= 1.0)

        if not keep.any():
            continue

        idx = np.where(keep)[0]
        n_keep = int(idx.size)
        n_train_total += n_keep

        ay_max = int(np.max(anchor_year[idx])) if n_keep > 0 else None
        if ay_max is not None:
            if (max_anchor_year_seen is None) or (ay_max > max_anchor_year_seen):
                max_anchor_year_seen = ay_max

        yr_used = yr_label[idx][mask_new[idx] > 0.0]
        if yr_used.size > 0:
            max_label_year_used = max(max_label_year_used, int(np.max(yr_used)))

        buf_hist.append(hist_y[idx])
        buf_num.append(cur_num[idx] if cur_num.shape[1] > 0 else np.zeros((n_keep, 0), np.float32))
        buf_cat.append(cur_cat[idx] if cur_cat.shape[1] > 0 else np.zeros((n_keep, 0), np.int64))
        buf_rid.append(region_id[idx])
        buf_tgt.append(target[idx])
        buf_msk.append(mask_new[idx])
        buf_yr.append(yr_label[idx])
        buf_yanc.append(y_anchor[idx])
        buf_ay.append(anchor_year[idx])
        buf_acct.append(acct[idx])
        buf_rows += n_keep

        if buf_rows >= int(max_rows_per_shard):
            flush()

    flush()

    if max_anchor_year_seen is not None:
        assert int(max_anchor_year_seen) <= int(origin - 1), f"Anchor leakage (origin): {max_anchor_year_seen} > {origin-1}"
    assert int(max_label_year_used) <= int(origin - 1), f"Label leakage (origin): {max_label_year_used} > {origin-1}"

    dt = time.time() - t0
    print(f"[{ts()}] Derived origin shards done origin={origin} shards={len(shard_paths)} n_train={n_train_total:,} time={dt:.1f}s")
    return {
        "origin": int(origin),
        "shards": shard_paths,
        "n_train": int(n_train_total),
        "max_anchor_year": max_anchor_year_seen,
        "max_label_year_used": int(max_label_year_used),
        "required_max_label": int(origin - 1),
        "leakage_free": bool(int(max_label_year_used) <= int(origin - 1)),
    }

# -----------------------------
# 8) Streaming scalers with hard scale floors
# -----------------------------
class RunningMeanVar:
    def __init__(self, dim: int):
        self.dim = int(dim)
        self.n = 0
        self.mean = np.zeros((self.dim,), dtype=np.float64)
        self.M2 = np.zeros((self.dim,), dtype=np.float64)

    def update(self, X: np.ndarray) -> None:
        if X.size == 0:
            return
        X = X.astype(np.float64, copy=False)
        n_b = int(X.shape[0])
        mean_b = X.mean(axis=0)
        var_b = X.var(axis=0)
        if self.n == 0:
            self.n = n_b
            self.mean = mean_b
            self.M2 = var_b * n_b
            return
        n_a = int(self.n)
        mean_a = self.mean
        M2_a = self.M2
        n = n_a + n_b
        delta = mean_b - mean_a
        mean = mean_a + delta * (float(n_b) / float(n))
        M2 = M2_a + var_b * n_b + (delta * delta) * (float(n_a) * float(n_b) / float(n))
        self.n = n
        self.mean = mean
        self.M2 = M2

    def finalize(self, scale_floor: float) -> Tuple[np.ndarray, np.ndarray]:
        if self.n <= 1:
            mu = self.mean.astype(np.float32)
            sc = np.full((self.dim,), float(scale_floor), dtype=np.float32)
            return mu, sc
        var = self.M2 / float(max(1, int(self.n)))
        std = np.sqrt(np.maximum(var, 0.0))
        std = np.maximum(std, float(scale_floor))
        return self.mean.astype(np.float32), std.astype(np.float32)

class SimpleScaler:
    def __init__(self, mean: np.ndarray, scale: np.ndarray):
        self.mean_ = mean.astype(np.float32)
        self.scale_ = scale.astype(np.float32)

    def transform(self, X: np.ndarray) -> np.ndarray:
        if X.size == 0:
            return X.astype(np.float32)
        return ((X.astype(np.float32) - self.mean_) / self.scale_).astype(np.float32)

    def inverse_transform(self, X: np.ndarray) -> np.ndarray:
        if X.size == 0:
            return X.astype(np.float32)
        return (X.astype(np.float32) * self.scale_ + self.mean_).astype(np.float32)

# -----------------------------
# PATCH D) Robust y_scaler + fail-fast saturation gate
# -----------------------------
def _robust_loc_scale(X: np.ndarray, scale_floor: float) -> Tuple[np.ndarray, np.ndarray]:
    X = X.astype(np.float32, copy=False)
    med = np.nanmedian(X, axis=0).astype(np.float32)
    q25 = np.nanpercentile(X, 25, axis=0).astype(np.float32)
    q75 = np.nanpercentile(X, 75, axis=0).astype(np.float32)
    sc = (q75 - q25) / 1.349
    sc = np.where(np.isfinite(sc), sc, float(scale_floor)).astype(np.float32)
    sc = np.maximum(sc, float(scale_floor)).astype(np.float32)
    med = np.where(np.isfinite(med), med, 0.0).astype(np.float32)
    return med, sc

def fit_scalers_from_shards_v102_robust_y(
    shard_paths: List[str],
    num_dim: int,
    scale_floor_y: float,
    scale_floor_num: float,
    scale_floor_tgt: float,
    max_y_rows: int = 500_000,
) -> Tuple[SimpleScaler, SimpleScaler, SimpleScaler]:
    """
    - y_scaler: robust median/IQR computed on a subsample of hist_y rows (fast, stable).
    - num_scaler + tgt_scaler: mean/std streaming (unchanged).
    """
    # Streaming stats for num and target
    t_stat = RunningMeanVar(H)
    n_stat = RunningMeanVar(int(num_dim)) if int(num_dim) > 0 else None

    # Subsample for robust y
    y_samples = []
    y_rows = 0

    for _, z in _iter_shards_npz(shard_paths):
        hy = z["hist_y"].astype(np.float32, copy=False)
        tg = z["target"].astype(np.float32, copy=False)
        t_stat.update(tg)
        if n_stat is not None:
            n_stat.update(z["cur_num"].astype(np.float32, copy=False))

        if y_rows < int(max_y_rows) and hy.shape[0] > 0:
            take = min(int(hy.shape[0]), int(max_y_rows - y_rows))
            if take > 0:
                y_samples.append(hy[:take].astype(np.float32, copy=False))
                y_rows += int(take)

        if y_rows >= int(max_y_rows):
            break

    if y_rows <= 0:
        # This should not happen if shards exist, but keep safe.
        y_mu = np.zeros((FULL_HIST_LEN,), dtype=np.float32)
        y_sc = np.full((FULL_HIST_LEN,), float(scale_floor_y), dtype=np.float32)
    else:
        Y = np.concatenate(y_samples, axis=0).astype(np.float32, copy=False)
        y_mu, y_sc = _robust_loc_scale(Y, scale_floor=float(scale_floor_y))

    t_mu, t_sc = t_stat.finalize(scale_floor=float(scale_floor_tgt))
    if n_stat is not None:
        n_mu, n_sc = n_stat.finalize(scale_floor=float(scale_floor_num))
    else:
        n_mu = np.zeros((0,), dtype=np.float32)
        n_sc = np.ones((0,), dtype=np.float32)

    return SimpleScaler(y_mu, y_sc), SimpleScaler(n_mu, n_sc), SimpleScaler(t_mu, t_sc)

def assert_y_scaler_contract(
    y_scaler: SimpleScaler,
    shard_paths: List[str],
    z_clip: float = 20.0,
    max_check_rows: int = 200_000,
    max_sat_frac: float = 0.01,
) -> None:
    """
    Fail-fast if the standardized hist_y saturates the sampler regime again.
    This is the exact failure you saw (|hy_z|>20 on a large fraction).
    """
    checked = 0
    xs = []
    for _, z in _iter_shards_npz(shard_paths):
        hy = z["hist_y"].astype(np.float32, copy=False)
        if hy.shape[0] == 0:
            continue
        take = min(int(hy.shape[0]), int(max_check_rows - checked))
        if take <= 0:
            break
        xs.append(hy[:take])
        checked += int(take)
        if checked >= int(max_check_rows):
            break

    if checked <= 0:
        raise RuntimeError("assert_y_scaler_contract: no hist_y rows available")

    X = np.concatenate(xs, axis=0).astype(np.float32, copy=False)
    Z = y_scaler.transform(X).astype(np.float32, copy=False)

    absz = np.abs(Z)
    sat = float(np.mean(absz > float(z_clip)))
    p95 = float(np.percentile(absz, 95))
    p99 = float(np.percentile(absz, 99))
    p999 = float(np.percentile(absz, 99.9))

    print(f"[{ts()}] y_scaler_contract checked_rows={checked:,} sat_frac(|z|>{z_clip})={sat:.6f} absz_p95={p95:.3f} absz_p99={p99:.3f} absz_p99_9={p999:.3f}")

    if not np.isfinite(sat) or sat > float(max_sat_frac):
        raise RuntimeError(f"y_scaler_contract FAIL: saturation frac {sat:.6f} > {max_sat_frac}")

# -----------------------------
# 9) One-time scaled shard materialization
# -----------------------------
def write_scaled_shards_v102(
    shard_paths_raw: List[str],
    out_dir_scaled: str,
    y_scaler: SimpleScaler,
    n_scaler: SimpleScaler,
    t_scaler: SimpleScaler,
    num_dim: int,
    keep_acct: bool = False,
    use_float16: bool = False,
) -> List[str]:
    os.makedirs(out_dir_scaled, exist_ok=True)
    out_paths: List[str] = []

    for p, z in _iter_shards_npz(shard_paths_raw):
        hist_y = z["hist_y"].astype(np.float32, copy=False)
        cur_num = z["cur_num"].astype(np.float32, copy=False)
        cur_cat = z["cur_cat"].astype(np.int64, copy=False)
        region_id = z["region_id"].astype(np.int64, copy=False)
        target = z["target"].astype(np.float32, copy=False)
        mask = z["mask"].astype(np.float32, copy=False)
        y_anchor = z["y_anchor"].astype(np.float32, copy=False)
        anchor_year = z["anchor_year"].astype(np.int32, copy=False)

        hist_y_s = y_scaler.transform(hist_y)
        if int(num_dim) > 0:
            cur_num_s = n_scaler.transform(cur_num)
        else:
            cur_num_s = np.zeros((hist_y_s.shape[0], 0), dtype=np.float32)
        x0_s = t_scaler.transform(target)

        if use_float16:
            hist_y_s = hist_y_s.astype(np.float16, copy=False)
            cur_num_s = cur_num_s.astype(np.float16, copy=False)
            x0_s = x0_s.astype(np.float16, copy=False)
            mask_s = mask.astype(np.float16, copy=False)
        else:
            hist_y_s = hist_y_s.astype(np.float32, copy=False)
            cur_num_s = cur_num_s.astype(np.float32, copy=False)
            x0_s = x0_s.astype(np.float32, copy=False)
            mask_s = mask.astype(np.float32, copy=False)

        cur_cat_i = cur_cat.astype(np.int32, copy=False)
        region_id_i = region_id.astype(np.int32, copy=False)

        base = os.path.basename(p)
        out_p = os.path.join(out_dir_scaled, base.replace(".npz", "_scaled.npz"))

        if keep_acct:
            acct = z["acct"].astype(object, copy=False)
            np.savez(
                out_p,
                hist_y_s=hist_y_s,
                cur_num_s=cur_num_s,
                x0_s=x0_s,
                mask=mask_s,
                cur_cat=cur_cat_i,
                region_id=region_id_i,
                anchor_year=anchor_year,
                y_anchor=y_anchor,
                acct=acct,
            )
        else:
            np.savez(
                out_p,
                hist_y_s=hist_y_s,
                cur_num_s=cur_num_s,
                x0_s=x0_s,
                mask=mask_s,
                cur_cat=cur_cat_i,
                region_id=region_id_i,
                anchor_year=anchor_year,
                y_anchor=y_anchor,
            )

        out_paths.append(out_p)

    return out_paths

# -----------------------------
# 10) Latent samplers and hierarchical mixing
# -----------------------------
def sample_ar1_path(n_steps: int, rank: int, phi: float, batch_size: int, device: str) -> torch.Tensor:
    Z = torch.zeros((batch_size, n_steps, rank), device=device)
    Z[:, 0, :] = torch.randn((batch_size, rank), device=device)
    innovation_std = math.sqrt(max(0.0, 1.0 - phi ** 2))
    for k in range(1, n_steps):
        eta = torch.randn((batch_size, rank), device=device)
        Z[:, k, :] = phi * Z[:, k - 1, :] + innovation_std * eta
    return Z

class GlobalProjection(nn.Module):
    def __init__(self, rank: int):
        super().__init__()
        self.proj = nn.Linear(rank, 1)
    def forward(self, Zg: torch.Tensor) -> torch.Tensor:
        return self.proj(Zg).squeeze(-1)

class GeoProjection(nn.Module):
    def __init__(self, rank: int):
        super().__init__()
        self.proj = nn.Linear(rank, 1)
    def forward(self, Zgeo: torch.Tensor) -> torch.Tensor:
        return self.proj(Zgeo).squeeze(-1)

def mix_hierarchical_noise(
    Zg: torch.Tensor,
    Zgeo: torch.Tensor,
    eps: torch.Tensor,
    proj_g: GlobalProjection,
    proj_geo: GeoProjection,
    rho_g: float,
    rho_geo: float,
) -> torch.Tensor:
    a_g = math.sqrt(float(rho_g))
    a_geo = math.sqrt(float(rho_geo))
    a_idio = math.sqrt(max(0.0, 1.0 - float(rho_g) - float(rho_geo)))
    return a_g * proj_g(Zg) + a_geo * proj_geo(Zgeo) + a_idio * eps

# -----------------------------
# 11) Denoiser with region + macro conditioning
# -----------------------------
class SinTime(nn.Module):
    def __init__(self, dim: int):
        super().__init__()
        self.dim = int(dim)
    def forward(self, t: torch.Tensor) -> torch.Tensor:
        half = self.dim // 2
        if half <= 1:
            return torch.zeros((t.shape[0], self.dim), device=t.device, dtype=t.dtype)
        freqs = torch.exp(torch.arange(half, device=t.device, dtype=t.dtype) * (-math.log(10000.0) / float(half - 1)))
        ang = t.unsqueeze(1) * freqs.unsqueeze(0)
        return torch.cat([torch.sin(ang), torch.cos(ang)], dim=1)

class FiLMLayer(nn.Module):
    def __init__(self, cond_dim: int, channels: int):
        super().__init__()
        self.scale = nn.Linear(cond_dim, channels)
        self.shift = nn.Linear(cond_dim, channels)
    def forward(self, x: torch.Tensor, cond: torch.Tensor) -> torch.Tensor:
        return x * (1 + self.scale(cond).unsqueeze(-1)) + self.shift(cond).unsqueeze(-1)

class MacroEncoder(nn.Module):
    def __init__(self, horizon: int, rank: int, hidden: int):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(horizon * rank, hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
        )
    def forward(self, Zg: torch.Tensor) -> torch.Tensor:
        return self.net(Zg.flatten(1))

class GeoEncoder(nn.Module):
    def __init__(self, horizon: int, rank: int, hidden: int):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(horizon * rank, hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
        )
    def forward(self, Zgeo: torch.Tensor) -> torch.Tensor:
        return self.net(Zgeo.flatten(1))

class Conv1dDenoiserV102(nn.Module):
    def __init__(self, target_dim: int, hist_len: int, num_dim: int, n_cat: int, hidden: int, n_layers: int, kernel_size: int):
        super().__init__()
        self.target_dim = int(target_dim)
        self.cat_emb_dim = 16

        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim) for _ in range(int(n_cat))])
        cat_dim = self.cat_emb_dim * int(n_cat)

        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        self.region_enc = nn.Sequential(nn.Linear(REGION_EMB_DIM, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        self.macro_enc = MacroEncoder(self.target_dim, GLOBAL_RANK, hidden)
        self.geo_enc = GeoEncoder(self.target_dim, GEO_RANK, hidden)

        self.hist_enc = nn.Sequential(nn.Linear(int(hist_len), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.num_enc = nn.Sequential(nn.Linear(max(1, int(num_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.cat_enc = nn.Sequential(nn.Linear(max(1, int(cat_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))

        self.t_dim = 128
        self.t_emb = SinTime(self.t_dim)
        self.t_enc = nn.Sequential(nn.Linear(self.t_dim, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        self.input_proj = nn.Conv1d(1, hidden, kernel_size=1)
        self.conv_blocks = nn.ModuleList()
        self.film_layers = nn.ModuleList()
        for _ in range(int(n_layers)):
            self.conv_blocks.append(nn.Sequential(
                nn.Conv1d(hidden, hidden, kernel_size, padding=kernel_size // 2),
                nn.GELU(),
                nn.Conv1d(hidden, hidden, kernel_size, padding=kernel_size // 2),
            ))
            self.film_layers.append(FiLMLayer(hidden, hidden))
        self.output_proj = nn.Conv1d(hidden, 1, kernel_size=1)

        self._y_scaler = None
        self._n_scaler = None
        self._t_scaler = None

    def forward(self, x_t, t, hist_y, cur_num, cur_cat, region_id, Zg, Zgeo):
        h_hist = self.hist_enc(hist_y)
        if cur_num.shape[1] > 0:
            h_num = self.num_enc(cur_num)
        else:
            h_num = self.num_enc(torch.zeros((x_t.shape[0], 1), device=x_t.device, dtype=x_t.dtype))
        h_t = self.t_enc(self.t_emb(t))

        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = []
            for j, emb in enumerate(self.cat_embs):
                v = cur_cat[:, j].clamp(0, HASH_BUCKET_SIZE - 1).long()
                cat_vecs.append(emb(v))
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((x_t.shape[0], 1), device=x_t.device, dtype=x_t.dtype)

        h_cat = self.cat_enc(cat_vec)

        region_vec = self.region_emb(region_id.clamp(0, GEO_BUCKETS - 1).long())
        h_region = self.region_enc(region_vec)

        h_macro = self.macro_enc(Zg)
        h_geo = self.geo_enc(Zgeo)

        h_cond = h_hist + h_num + h_cat + h_region + h_macro + h_geo + h_t

        x = self.input_proj(x_t.unsqueeze(1))
        for conv, film in zip(self.conv_blocks, self.film_layers):
            x = film(conv(x) + x, h_cond)
        return self.output_proj(x).squeeze(1)

def create_denoiser_v102(target_dim: int, hist_len: int, num_dim: int, n_cat: int) -> nn.Module:
    return Conv1dDenoiserV102(target_dim, hist_len, num_dim, n_cat, DENOISER_HIDDEN, DENOISER_LAYERS, CONV_KERNEL_SIZE)

# -----------------------------
# 12) Diffusion scheduler
# -----------------------------
class Scheduler:
    def __init__(self, steps: int, device: str):
        betas = torch.linspace(1e-4, 0.02, int(steps), device=device)
        alphas = 1.0 - betas
        self.abar = torch.cumprod(alphas, dim=0)
        self.sqrt_abar = torch.sqrt(self.abar)
        self.sqrt_om = torch.sqrt(1.0 - self.abar)
        self.steps = int(steps)

    def q(self, x0, t_idx, noise):
        return self.sqrt_abar[t_idx].view(-1, 1) * x0 + self.sqrt_om[t_idx].view(-1, 1) * noise

# -----------------------------
# 13) Autocast helpers
# -----------------------------
def get_autocast_ctx(device: str):
    if USE_BF16 and device == "cuda" and torch.cuda.is_available():
        return torch.autocast(device_type="cuda", dtype=torch.bfloat16)
    return contextlib.nullcontext()

# -----------------------------
# 14) Shard-based diffusion training loop (local scaled shards)
# -----------------------------
# -----------------------------
# PATCH E) Training: robust y_scaler + contract gate before scaled shard write
# -----------------------------
def train_diffusion_from_shards_v102_local(
    shard_paths: List[str],
    origin: int,
    epochs: int,
    model: nn.Module,
    proj_g: GlobalProjection,
    proj_geo: GeoProjection,
    device: str,
    num_dim: int,
    n_cat: int,
    work_dirs: Dict[str, str],
) -> Tuple[SimpleScaler, SimpleScaler, SimpleScaler, List[float], List[str]]:
    if not shard_paths:
        raise ValueError(f"No shards for origin {origin}")

    print(f"[{ts()}] train_diffusion origin={origin} shards={len(shard_paths)} epochs={epochs} diff_batch={DIFF_BATCH}")

    # IMPORTANT: raise the history scale floor to prevent tiny scales from producing huge z-scores
    # This floor only binds for degenerate columns; it should not affect normal columns.
    y_floor = max(float(SCALE_FLOOR_Y), 0.25)

    y_scaler, n_scaler, t_scaler = fit_scalers_from_shards_v102_robust_y(
        shard_paths=shard_paths,
        num_dim=int(num_dim),
        scale_floor_y=float(y_floor),
        scale_floor_num=float(SCALE_FLOOR_NUM),
        scale_floor_tgt=float(SCALE_FLOOR_TGT),
        max_y_rows=500_000,
    )

    # Fail-fast if we are back in the saturation regime.
    assert_y_scaler_contract(
        y_scaler=y_scaler,
        shard_paths=shard_paths,
        z_clip=float(SAMPLER_Z_CLIP) if SAMPLER_Z_CLIP is not None else 20.0,
        max_check_rows=200_000,
        max_sat_frac=0.01,
    )

    model._y_scaler = y_scaler
    model._n_scaler = n_scaler
    model._t_scaler = t_scaler

    scaled_dir = os.path.join(work_dirs["SCALED_SHARD_ROOT"], f"origin_{int(origin)}")
    scaled_paths = write_scaled_shards_v102(
        shard_paths_raw=shard_paths,
        out_dir_scaled=scaled_dir,
        y_scaler=y_scaler,
        n_scaler=n_scaler,
        t_scaler=t_scaler,
        num_dim=int(num_dim),
        keep_acct=False,
        use_float16=bool(SCALED_SHARDS_FLOAT16),
    )
    print(f"[{ts()}] Scaled shards ready: {len(scaled_paths)} dir={scaled_dir} float16={SCALED_SHARDS_FLOAT16}")

    sched = Scheduler(DIFF_STEPS_TRAIN, device=device)
    params = list(model.parameters()) + list(proj_g.parameters()) + list(proj_geo.parameters())
    try:
        opt = torch.optim.AdamW(params, lr=DIFF_LR, weight_decay=1e-4, fused=True)
    except TypeError:
        opt = torch.optim.AdamW(params, lr=DIFF_LR, weight_decay=1e-4)

    lr_sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=int(epochs), eta_min=DIFF_LR * 0.1)

    model.train()
    proj_g.train()
    proj_geo.train()

    autocast_ctx = get_autocast_ctx(device)
    losses: List[float] = []

    ZG_BANK = 128
    ZG_bank = sample_ar1_path(H, GLOBAL_RANK, PHI_GLOBAL, batch_size=ZG_BANK, device=device)

    scaled_paths_local = list(scaled_paths)

    for ep in range(int(epochs)):
        t_ep0 = time.time()
        rng.shuffle(scaled_paths_local)
        ep_losses: List[float] = []

        if (ep > 0) and ((ep % 5) == 0):
            ZG_bank = sample_ar1_path(H, GLOBAL_RANK, PHI_GLOBAL, batch_size=ZG_BANK, device=device)

        for _, z in _iter_shards_npz(scaled_paths_local):
            hist_y_s = z["hist_y_s"]
            cur_num_s = z["cur_num_s"]
            x0_s = z["x0_s"]
            mask = z["mask"]
            cur_cat = z["cur_cat"].astype(np.int64, copy=False)
            region_id = z["region_id"].astype(np.int64, copy=False)

            n = int(x0_s.shape[0])
            if n == 0:
                continue

            perm = rng.permutation(n)

            u_r_cpu, inv_cpu = np.unique(region_id, return_inverse=True)
            U = int(u_r_cpu.shape[0])
            Zgeo_u = sample_ar1_path(H, GEO_RANK, PHI_GEO, batch_size=U, device=device)
            inv = torch.from_numpy(inv_cpu.astype(np.int64, copy=False)).to(device)

            for start in range(0, n, int(DIFF_BATCH)):
                b = perm[start:start + int(DIFF_BATCH)]
                B = int(b.size)
                if B == 0:
                    continue

                hy = torch.from_numpy(hist_y_s[b]).to(device, non_blocking=True)
                if int(num_dim) > 0:
                    xn = torch.from_numpy(cur_num_s[b]).to(device, non_blocking=True)
                else:
                    xn = torch.zeros((B, 0), device=device, dtype=torch.float32)

                x0 = torch.from_numpy(x0_s[b]).to(device, non_blocking=True).float()
                m = torch.from_numpy(mask[b]).to(device, non_blocking=True).float()

                xc = torch.from_numpy(cur_cat[b]).to(device, non_blocking=True)
                rid = torch.from_numpy(region_id[b]).to(device, non_blocking=True)

                zgi = int(rng.integers(0, ZG_BANK))
                Zg = ZG_bank[zgi:zgi+1].expand(B, -1, -1)

                b_t = torch.from_numpy(b.astype(np.int64, copy=False)).to(device)
                rid_dense = inv.index_select(0, b_t)
                Zgeo = Zgeo_u.index_select(0, rid_dense)

                eps = torch.randn_like(x0)

                with autocast_ctx:
                    noise = mix_hierarchical_noise(Zg, Zgeo, eps, proj_g, proj_geo, RHO_GLOBAL, RHO_GEO)
                    t_idx = torch.randint(0, int(DIFF_STEPS_TRAIN), (B,), device=device)
                    xt = sched.q(x0, t_idx, noise)
                    noise_hat = model(xt, t_idx.float(), hy.float(), xn.float(), xc, rid, Zg, Zgeo)
                    loss = ((noise_hat - noise) ** 2 * m).sum() / (m.sum() + 1e-8)

                opt.zero_grad(set_to_none=True)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(params, 1.0)
                opt.step()

                ep_losses.append(float(loss.item()))

        lr_sched.step()
        mean_loss = float(np.mean(ep_losses)) if ep_losses else float("nan")
        losses.append(mean_loss)

        dt_ep = time.time() - t_ep0
        if (ep == 0) or ((ep + 1) % max(1, int(epochs) // 5) == 0):
            print(f"[{ts()}] origin={origin} ep={ep+1}/{epochs} loss={mean_loss:.6f} time={dt_ep:.1f}s")

        # W&B per-epoch logging
        wb_log({
            f"train/loss_origin_{origin}": mean_loss,
            "train/loss": mean_loss,
            "train/epoch": ep + 1,
            "train/origin": origin,
            "train/lr": float(lr_sched.get_last_lr()[0]),
        })

    return y_scaler, n_scaler, t_scaler, losses, scaled_paths

# -----------------------------
# 15) Chunked inference context builder (no future labels)
# -----------------------------
# -----------------------------
# PATCH C) Inference context builder: safe hist lag fill (no zeros, no underflow distortion)
# -----------------------------
def build_inference_context_chunked_v102(
    lf: pl.LazyFrame,
    accts: List[str],
    num_use_local: List[str],
    cat_use_local: List[str],
    global_medians: Dict[str, float],
    anchor_year: int,
    acct_chunk_size: int = ACCT_CHUNK_SIZE_INFER,
    max_parcels: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if global_medians is None:
        global_medians = {}

    print(f"[{ts()}] Building inference context anchor_year={anchor_year} max_parcels={max_parcels}")

    region_expr = build_region_id_expr()
    cat_hash_exprs = build_cat_hash_exprs(cat_use_local)
    hist_exprs = [pl.col("y_log").shift(i).over("acct").alias(f"lag_{i}") for i in range(FULL_HIST_LEN)]

    hist_buf: List[np.ndarray] = []
    num_buf: List[np.ndarray] = []
    cat_buf: List[np.ndarray] = []
    rid_buf: List[np.ndarray] = []
    yanc_buf: List[np.ndarray] = []
    acct_buf: List[np.ndarray] = []
    total = 0

    for s in range(0, len(accts), int(acct_chunk_size)):
        acct_chunk = accts[s:s + int(acct_chunk_size)]
        if not acct_chunk:
            continue

        base_q = (
            lf
            .filter(pl.col("acct").cast(pl.Utf8).is_in(acct_chunk))
            .filter(pl.col("yr").is_between(MIN_YEAR, int(anchor_year)))
            .filter(pl.col("tot_appr_val") > 0)
            .with_columns([
                pl.col("acct").cast(pl.Utf8).alias("acct"),
                pl.col("yr").cast(pl.Int32).alias("yr"),
                pl.col("tot_appr_val").log1p().alias("y_log"),
            ])
            .sort(["acct", "yr"])
            .with_columns(hist_exprs + [region_expr] + cat_hash_exprs)
        )

        df = base_q.filter(pl.col("yr") == int(anchor_year)).collect()
        if len(df) == 0:
            continue

        hist_cols = [f"lag_{i}" for i in range(FULL_HIST_LEN - 1, -1, -1)]
        hist_mat = np.column_stack([df[c].to_numpy().astype(np.float32) for c in hist_cols]).astype(np.float32)
        hist_mat = fill_hist_lags_no_zeros(hist_mat, fallback_value=float(Y_FALLBACK_LOG1P))
        hist_y = hist_mat.astype(np.float32)

        y_anchor = df["y_log"].to_numpy().astype(np.float32)
        region_id = df["region_id"].to_numpy().astype(np.int64)
        acct_arr = df["acct"].to_numpy().astype(object)

        cur_num_list = []
        for c in num_use_local:
            if c in df.columns:
                vals = df[c].to_numpy().astype(np.float32)
                med = float(global_medians.get(c, 0.0))
                cur_num_list.append(np.nan_to_num(vals, nan=med).astype(np.float32))
            else:
                cur_num_list.append(np.full(len(df), float(global_medians.get(c, 0.0)), dtype=np.float32))
        cur_num = np.column_stack(cur_num_list).astype(np.float32) if cur_num_list else np.zeros((len(df), 0), np.float32)

        cur_cat_list = []
        for c in cat_use_local:
            hc = f"cat_{c}"
            if hc in df.columns:
                cur_cat_list.append(df[hc].to_numpy().astype(np.int64))
            else:
                cur_cat_list.append(np.zeros(len(df), dtype=np.int64))
        cur_cat = np.column_stack(cur_cat_list).astype(np.int64) if cur_cat_list else np.zeros((len(df), 0), np.int64)

        if max_parcels is not None:
            need = int(max_parcels) - int(total)
            if need <= 0:
                break
            take = min(int(len(df)), int(need))
            hist_y = hist_y[:take]
            cur_num = cur_num[:take]
            cur_cat = cur_cat[:take]
            region_id = region_id[:take]
            y_anchor = y_anchor[:take]
            acct_arr = acct_arr[:take]

        hist_buf.append(hist_y)
        num_buf.append(cur_num)
        cat_buf.append(cur_cat)
        rid_buf.append(region_id)
        yanc_buf.append(y_anchor)
        acct_buf.append(acct_arr)
        total += int(hist_y.shape[0])

        if (max_parcels is not None) and (int(total) >= int(max_parcels)):
            break

    if total == 0:
        print(f"[{ts()}] No inference anchors found at anchor_year={anchor_year}")
        return None

    hist_y_out = np.concatenate(hist_buf, axis=0)
    cur_num_out = np.concatenate(num_buf, axis=0) if num_buf else np.zeros((total, 0), np.float32)
    cur_cat_out = np.concatenate(cat_buf, axis=0) if cat_buf else np.zeros((total, 0), np.int64)
    rid_out = np.concatenate(rid_buf, axis=0)
    y_anchor_out = np.concatenate(yanc_buf, axis=0)
    acct_out = np.concatenate(acct_buf, axis=0).astype(object)

    print(f"[{ts()}] Inference context built n={total:,}")
    return {
        "hist_y": hist_y_out.astype(np.float32),
        "cur_num": cur_num_out.astype(np.float32),
        "cur_cat": cur_cat_out.astype(np.int64),
        "region_id": rid_out.astype(np.int64),
        "y_anchor": y_anchor_out.astype(np.float32),
        "acct": acct_out.astype(object),
        "anchor_year": int(anchor_year),
        "n_parcels": int(total),
    }

# -----------------------------
# 16) Stable coherent DDIM sampler (float32 + guards + clipping)
# -----------------------------
@torch.no_grad()
def sample_ddim_v102_coherent_stable(
    model: nn.Module,
    proj_g: GlobalProjection,
    proj_geo: GeoProjection,
    sched: Scheduler,
    hist_y_b: np.ndarray,
    cur_num_b: np.ndarray,
    cur_cat_b: np.ndarray,
    region_id_b: np.ndarray,
    Zg_scenarios: torch.Tensor,              # [S,H,Rg]
    Zgeo_scenarios: Dict[int, torch.Tensor], # region_id int to [S,H,Rgeo]
    device: str,
) -> np.ndarray:
    model.eval()
    proj_g.eval()
    proj_geo.eval()

    N = int(hist_y_b.shape[0])
    S = int(Zg_scenarios.shape[0])
    if N == 0:
        return np.zeros((0, S, H), dtype=np.float32)

    # Conditioning scaling
    hy_np = model._y_scaler.transform(hist_y_b).astype(np.float32)
    if SAMPLER_Z_CLIP is not None:
        hy_np = np.clip(hy_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP)).astype(np.float32)

    if cur_num_b.shape[1] > 0:
        xn_np = model._n_scaler.transform(cur_num_b).astype(np.float32)
        if SAMPLER_Z_CLIP is not None:
            xn_np = np.clip(xn_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP)).astype(np.float32)
    else:
        xn_np = np.zeros((N, 0), dtype=np.float32)

    hy_absmax = float(np.max(np.abs(hy_np))) if hy_np.size > 0 else 0.0
    xn_absmax = float(np.max(np.abs(xn_np))) if xn_np.size > 0 else 0.0
    print(f"[{ts()}] SAMPLER conditioning absmax hy={hy_absmax:.3f} xn={xn_absmax:.3f} N={N} S={S}")

    # pin_memory + non_blocking: overlap CPU→GPU transfer with compute
    hy = torch.from_numpy(hy_np).pin_memory().to(device=device, dtype=torch.float32, non_blocking=True)
    xn = torch.from_numpy(xn_np).pin_memory().to(device=device, dtype=torch.float32, non_blocking=True) if xn_np.shape[1] > 0 else torch.zeros((N, 0), device=device, dtype=torch.float32)
    xc = torch.from_numpy(cur_cat_b.astype(np.int64)).pin_memory().to(device=device, non_blocking=True)
    rid = torch.from_numpy(region_id_b.astype(np.int64)).pin_memory().to(device=device, non_blocking=True)

    # Expand across scenarios
    hy_exp = hy.repeat_interleave(S, dim=0)
    xn_exp = xn.repeat_interleave(S, dim=0)
    xc_exp = xc.repeat_interleave(S, dim=0)
    rid_exp = rid.repeat_interleave(S, dim=0)

    if Zg_scenarios.device != torch.device(device):
        Zg_scenarios = Zg_scenarios.to(device)
    Zg_exp = Zg_scenarios.repeat(N, 1, 1).contiguous()  # [N*S,H,Rg]

    # Geo gather (unique regions)
    u_rid, inv = torch.unique(rid, return_inverse=True)
    U = int(u_rid.shape[0])

    Zgeo_u_list: List[torch.Tensor] = []
    for j in range(U):
        r_int = int(u_rid[j].item())
        z = Zgeo_scenarios.get(r_int, None)
        if z is None:
            z = sample_ar1_path(H, GEO_RANK, PHI_GEO, batch_size=S, device=device)
        else:
            if z.device != torch.device(device):
                z = z.to(device)
        Zgeo_u_list.append(z)

    Zgeo_u = torch.stack(Zgeo_u_list, dim=0).contiguous()      # [U,S,H,Rgeo]
    Zgeo_ns = Zgeo_u.index_select(0, inv).contiguous()         # [N,S,H,Rgeo]
    Zgeo_exp = Zgeo_ns.view(N * S, H, GEO_RANK)                # [N*S,H,Rgeo]

    # Initial x (hierarchical noise distribution)
    a_g = math.sqrt(RHO_GLOBAL)
    a_geo = math.sqrt(RHO_GEO)
    a_idio = math.sqrt(max(0.0, 1.0 - RHO_GLOBAL - RHO_GEO))

    # Safe: never keep a CUDAGraph output alive across another compiled invocation
    x = (a_g * proj_g(Zg_exp).float())                             # materialize immediately

    x = x + (a_geo * proj_geo(Zgeo_exp).float())                   # next compiled call is now safe

    idio_noise = torch.randn((N * S, H), device=device, dtype=torch.float32)
    x = x + (a_idio * idio_noise)
    x = x.float()

    # DDIM schedule indices
    T = int(sched.steps)
    idx = np.linspace(0, T - 1, int(DIFF_STEPS_SAMPLE)).round().astype(int)
    idx = np.unique(idx)[::-1].copy()

    # Sampling in float32 (autocast disabled by default)
    if SAMPLER_DISABLE_AUTOCAST:
        autocast_ctx = contextlib.nullcontext()
    else:
        autocast_ctx = get_autocast_ctx(device)

    first_bad_step = None

    for i_step, t_idx in enumerate(idx):
        t = torch.full((N * S,), float(t_idx), device=device, dtype=torch.float32)

        with autocast_ctx:
            noise_hat = model(
                x,
                t,
                hy_exp,
                xn_exp,
                xc_exp,
                rid_exp,
                Zg_exp,
                Zgeo_exp,
            ).to(dtype=torch.float32)

        # Finite guards and clipping
        noise_hat = torch.nan_to_num(noise_hat, nan=0.0, posinf=0.0, neginf=0.0)
        noise_hat = noise_hat.clamp(-float(SAMPLER_NOISE_CLIP), float(SAMPLER_NOISE_CLIP))

        abar = sched.abar[int(t_idx)].to(dtype=torch.float32)
        if (i_step + 1) < len(idx):
            abar_prev = sched.abar[int(idx[i_step + 1])].to(dtype=torch.float32)
        else:
            abar_prev = torch.tensor(1.0, device=device, dtype=torch.float32)

        sqrt_abar = torch.sqrt(abar).clamp(min=1e-6)
        sqrt_om = torch.sqrt(1.0 - abar).clamp(min=1e-6)

        x0_pred = (x - sqrt_om * noise_hat) / sqrt_abar
        x0_pred = torch.nan_to_num(x0_pred, nan=0.0, posinf=0.0, neginf=0.0)
        x0_pred = x0_pred.clamp(-float(SAMPLER_X0_CLIP), float(SAMPLER_X0_CLIP))

        if (i_step + 1) < len(idx):
            x = torch.sqrt(abar_prev).clamp(min=0.0) * x0_pred + torch.sqrt(1.0 - abar_prev).clamp(min=0.0) * noise_hat
        else:
            x = x0_pred

        x = torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
        x = x.clamp(-float(SAMPLER_X_CLIP), float(SAMPLER_X_CLIP))

        if SAMPLER_REPORT_BAD_STEP:
            bad = (~torch.isfinite(x)).any(dim=1)
            bad_frac = float(bad.float().mean().item())
            if (first_bad_step is None) and (bad_frac > 0.0):
                first_bad_step = int(i_step)
            if (i_step == 0) or (i_step == len(idx) - 1) or ((i_step % 5) == 0):
                x_absmax = float(x.abs().max().item())
                print(f"[{ts()}] SAMPLER step={i_step}/{len(idx)} t_idx={int(t_idx)} bad_frac={bad_frac:.4f} x_absmax={x_absmax:.3f}")

    if first_bad_step is not None:
        print(f"[{ts()}] SAMPLER first_bad_step={first_bad_step}")

    x_np = model._t_scaler.inverse_transform(x.detach().cpu().numpy().astype(np.float32))
    return x_np.reshape(N, S, H).astype(np.float32)

# -----------------------------
# 17) Acceptance tests
# -----------------------------
def acceptance_test_increment_consistency(deltas: np.ndarray, y_anchor: np.ndarray) -> Dict[str, Any]:
    cumsum_d = np.cumsum(deltas, axis=2)
    y_levels = y_anchor[:, None, None] + cumsum_d
    max_err = 0.0
    for k in range(1, deltas.shape[2]):
        diff = y_levels[:, :, k] - y_levels[:, :, k - 1]
        err = float(np.max(np.abs(diff - deltas[:, :, k])))
        if err > max_err:
            max_err = err
    return {"max_reconstruction_error": float(max_err), "status": "PASS" if max_err < 1e-5 else "FAIL"}

def acceptance_test_finite(deltas: np.ndarray) -> Dict[str, Any]:
    finite = np.isfinite(deltas)
    good_rows = int(np.all(finite, axis=(1, 2)).sum())
    n = int(deltas.shape[0])
    return {"n": n, "finite_rows": good_rows, "finite_frac": float(good_rows / max(1, n)), "status": "PASS" if good_rows == n else "FAIL"}


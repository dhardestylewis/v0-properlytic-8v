"""
Modal training wrapper for Properlytic world model v11.
Runs on Modal's serverless A100 GPUs, pulls panel from GCS.

Usage:
    modal run scripts/train_modal.py --jurisdiction sf_ca

Cost: ~$3-4/hr on A100-40GB, ~1-2 hrs for 500K parcels @ 60 epochs = ~$4-6 total.
"""
import modal
import os
import sys

# ─── Parse args at module load for descriptive Modal app name ───
_jur = "unknown"
_ori = "unknown"
for i, arg in enumerate(sys.argv):
    if arg == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]
    if arg == "--origin" and i + 1 < len(sys.argv):
        _ori = sys.argv[i + 1]

app = modal.App(f"train-{_jur}-o{_ori}")

# Container image with all dependencies
training_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=0.20",
        "pyarrow>=14.0",
        "numpy>=1.24",
        "torch>=2.1",
        "wandb>=0.16",
        "google-cloud-storage>=2.10",
        "scikit-learn>=1.3",
    )
)

# GCS credentials as Modal secret (set via: modal secret create gcs-creds ...)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])


@app.function(
    image=training_image,
    gpu="A100",
    timeout=7200,  # 2 hours max
    secrets=[gcs_secret, wandb_secret],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)},
)
def train_worldmodel(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    epochs: int = 60,
    sample_size: int = 500_000,
    origin: int = 2019,
):
    """Download panel from GCS, adapt schema, train v11 model."""
    import json, time, tempfile, shutil
    import numpy as np
    import polars as pl

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── Set up GCS credentials ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        creds_path = "/tmp/gcs_creds.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

    # ─── Download panel from GCS ───
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    if jurisdiction == "all":
        # Grand panel: download and concatenate all jurisdiction partitions
        panel_blobs = [b for b in bucket.list_blobs(prefix="panel/jurisdiction=") if b.name.endswith("/part.parquet")]
        if not panel_blobs:
            raise FileNotFoundError("No panel partitions found in gs://{bucket_name}/panel/")
        frames = []
        for blob in panel_blobs:
            jur = blob.name.split("jurisdiction=")[1].split("/")[0]
            local = f"/tmp/panel_{jur}.parquet"
            blob.download_to_filename(local)
            df_j = pl.read_parquet(local)
            
            # Ensure global uniqueness of parcel_id
            if "jurisdiction" not in df_j.columns:
                df_j = df_j.with_columns(pl.lit(jur).alias("jurisdiction"))
            
            if "parcel_id" in df_j.columns:
                df_j = df_j.with_columns(
                    (pl.col("jurisdiction") + "_" + pl.col("parcel_id").cast(pl.Utf8)).alias("parcel_id")
                )
            
            print(f"[{ts()}] Loaded {jur}: {len(df_j):,} rows")
            frames.append(df_j)
        # Harmonize types across jurisdictions before concat
        NUMERIC_COLS = {"property_value", "sale_price", "assessed_value", "land_value",
                        "improvement_value", "sqft", "land_area", "year_built",
                        "bedrooms", "bathrooms", "stories", "lat", "lon",
                        "lehd_total_jobs", "lehd_retail_jobs", "lehd_finance_jobs",
                        "fema_disaster_count", "year"}
        STRING_COLS = {"parcel_id", "jurisdiction", "dwelling_type", "address", "sale_date"}
        harmonized = []
        for f in frames:
            casts = {}
            for c in f.columns:
                if c in NUMERIC_COLS:
                    casts[c] = pl.Float64
                elif c in STRING_COLS:
                    casts[c] = pl.Utf8
            harmonized.append(f.cast(casts))
        df = pl.concat(harmonized, how="diagonal")
        panel_local = "/tmp/panel_all.parquet"
        df.write_parquet(panel_local)
        size_mb = os.path.getsize(panel_local) / 1e6
        print(f"[{ts()}] Grand panel: {len(df):,} rows, {size_mb:.1f} MB from {len(panel_blobs)} jurisdictions")
        # Persist grand panel to GCS
        grand_blob = bucket.blob("panel/grand_panel/part.parquet")
        grand_blob.upload_from_filename(panel_local)
        print(f"[{ts()}] Uploaded grand panel to gs://{bucket_name}/panel/grand_panel/part.parquet")

        # Fall through to adaptation and training using the merged dataframe `df`
        jurisdiction_suffix = "grand_panel"
    else:
        blob_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
        blob = bucket.blob(blob_path)
        if not blob.exists():
            available = [b.name for b in bucket.list_blobs(prefix="panel/")]
            raise FileNotFoundError(
                f"Panel not found at gs://{bucket_name}/{blob_path}\n"
                f"Available: {available}"
            )
        panel_local = f"/tmp/panel_{jurisdiction}.parquet"
        blob.download_to_filename(panel_local)
        size_mb = os.path.getsize(panel_local) / 1e6
        print(f"[{ts()}] Downloaded panel: {size_mb:.1f} MB")
        df = pl.read_parquet(panel_local)
        jurisdiction_suffix = jurisdiction

    # ─── Adapt canonical → worldmodel schema ───
    print(f"[{ts()}] Raw panel: {len(df):,} rows, columns: {df.columns}")

    rename_map = {
        "parcel_id": "acct",
        "year": "yr",
        # "property_value": "tot_appr_val", # Handled via hierarchy coalesce below 
        "sqft": "living_area",
        "land_area": "land_ar",
        "year_built": "yr_blt",
        "bedrooms": "bed_cnt",
        "bathrooms": "full_bath",
        "stories": "nbr_story",
        "lat": "gis_lat",
        "lon": "gis_lon",
    }
    actual_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    
    # Drop any existing columns that clashing with our target names
    drop_targets = [v for k, v in actual_renames.items() if v in df.columns]
    if drop_targets:
        df = df.drop(drop_targets)

    df = df.rename(actual_renames)

    # SECURE TARGET LEAKAGE 
    # Determine the strongest valuation signal available per row
    available_val_cols = [c for c in ["sale_price", "property_value", "assessed_value"] if c in df.columns]
    if available_val_cols and "tot_appr_val" not in df.columns:
        df = df.with_columns(
            pl.coalesce([pl.col(c) for c in available_val_cols]).alias("tot_appr_val")
        )
    elif "tot_appr_val" not in df.columns:
        raise ValueError("Panel contains no sale_price, property_value, assessed_value, or tot_appr_val to form a target.")

    # Explicitly DROP all canonical valuation components to strictly prevent model leakage into the features
    leaky_cols = ["sale_price", "property_value", "assessed_value", "land_value", "improvement_value"]
    drop_leaks = [c for c in leaky_cols if c in df.columns]
    if drop_leaks:
        print(f"[{ts()}] Dropping LEAKY valuation columns from feature set: {drop_leaks}")
        df = df.drop(drop_leaks)

    df = df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))

    # Map canonical dwelling_type values → codes the worldmodel SF filter expects
    # retrain_sample_sweep.py looks for property_type in ["SF", "SFR", "SINGLE"]
    if "property_type" in df.columns:
        DWELLING_MAP = {
            "single_family": "SF",
            "condo": "CONDO",
            "multi_family": "MF",
            "townhouse": "TH",
            "cooperative": "COOP",
            "mobile_home": "MH",
        }
        df = df.with_columns(
            pl.col("property_type")
            .fill_null("UNKNOWN")
            .map_elements(lambda v: DWELLING_MAP.get(v, v), return_dtype=pl.Utf8)
            .alias("property_type")
        )

    # Fill null string columns — worldmodel.py sorts/compares these
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).fill_null("UNKNOWN"))

    df = df.with_columns([
        pl.col("acct").cast(pl.Utf8),
        pl.col("yr").cast(pl.Int32),
        pl.col("tot_appr_val").cast(pl.Float64),
    ])

    # Drop columns that are entirely null — they inflate NUM_DIM but have no
    # data, causing shape mismatches between model creation and shard data
    null_counts = df.null_count()
    n_rows = len(df)
    all_null_cols = [c for c in df.columns if null_counts[c][0] == n_rows and c not in ("acct", "yr", "tot_appr_val")]
    if all_null_cols:
        print(f"[{ts()}] Dropping {len(all_null_cols)} all-null columns: {all_null_cols}")
        df = df.drop(all_null_cols)

    adapted_path = f"/tmp/panel_{jurisdiction_suffix}_adapted.parquet"
    df.write_parquet(adapted_path)

    yr_min = int(df["yr"].min())
    yr_max = int(df["yr"].max())
    n_accts = df["acct"].n_unique()
    print(f"[{ts()}] Adapted: {len(df):,} rows, {n_accts:,} parcels, years {yr_min}-{yr_max}")

    # ─── Patch worldmodel.py config for this panel ───
    # Override path/config before exec'ing worldmodel.py
    os.environ["WM_MAX_ACCTS"] = str(sample_size)
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SWEEP_EPOCHS"] = str(epochs)
    os.environ["BACKTEST_MIN_ORIGIN"] = str(origin)
    os.environ["FORECAST_ORIGIN_YEAR"] = str(origin)

    # Create output directory
    out_dir = f"/output/{jurisdiction_suffix}_v11"
    os.makedirs(out_dir, exist_ok=True)

    # Patch the globals that worldmodel.py sets
    import builtins
    _original_open = builtins.open

    # We'll exec worldmodel.py with patched constants
    # Read the worldmodel.py source
    import urllib.request
    # Since we can't easily import from the repo, we'll download from GCS
    # For now, set up the essential training pipeline inline

    print(f"[{ts()}] Setting up v11 training pipeline...")

    # Load panel with Polars lazy
    lf = pl.scan_parquet(adapted_path)
    schema = lf.collect_schema()
    cols = schema.names()
    cols_set = set(cols)

    # Verify required columns
    assert all(c in cols_set for c in ["acct", "yr", "tot_appr_val"]), \
        f"Missing required columns. Have: {cols}"

    print(f"[{ts()}] Panel loaded. Columns: {cols}")
    print(f"[{ts()}] Training config: origin={origin}, epochs={epochs}, sample={sample_size:,}")
    print(f"[{ts()}] GPU: {os.popen('nvidia-smi --query-gpu=name,memory.total --format=csv,noheader').read().strip()}")

    # ─── Upload worldmodel.py + retrain to GCS for exec ───
    # For the full training, we need the complete worldmodel.py
    # Check if it exists in GCS
    wm_blob = bucket.blob("code/worldmodel.py")
    retrain_blob = bucket.blob("code/retrain_sample_sweep.py")

    if not wm_blob.exists():
        print(f"[{ts()}] worldmodel.py not found in GCS. Please upload it first:")
        print(f"  gsutil cp scripts/inference/worldmodel.py gs://{bucket_name}/code/worldmodel.py")
        print(f"  gsutil cp scripts/retrain_sample_sweep.py gs://{bucket_name}/code/retrain_sample_sweep.py")
        raise FileNotFoundError("Upload worldmodel.py to GCS first")

    wm_local = "/tmp/worldmodel.py"
    retrain_local = "/tmp/retrain_sample_sweep.py"
    wm_blob.download_to_filename(wm_local)
    retrain_blob.download_to_filename(retrain_local)

    # Patch worldmodel.py constants before exec
    with open(wm_local, "r") as f:
        wm_source = f.read()

    # Replace hardcoded paths and config
    wm_source = wm_source.replace(
        'PANEL_PATH_DRIVE = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"',
        f'PANEL_PATH_DRIVE = "{adapted_path}"'
    )
    wm_source = wm_source.replace(
        'PANEL_PATH_LOCAL = "/content/local_panel.parquet"',
        f'PANEL_PATH_LOCAL = "{adapted_path}"'
    )
    wm_source = wm_source.replace(
        'OUT_DIR = "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel"',
        f'OUT_DIR = "{out_dir}"'
    )
    wm_source = wm_source.replace(
        f'MIN_YEAR = 2005', f'MIN_YEAR = {yr_min}'
    )
    wm_source = wm_source.replace(
        f'MAX_YEAR = 2025', f'MAX_YEAR = {yr_max}'
    )
    wm_source = wm_source.replace(
        f'SEAM_YEAR = 2025', f'SEAM_YEAR = {yr_max}'
    )

    # Remove Drive mount attempt
    wm_source = wm_source.replace(
        'from google.colab import drive',
        '# from google.colab import drive  # patched out for Modal'
    )

    print(f"[{ts()}] Executing worldmodel.py (Cell 1)...")
    exec(wm_source, globals())

    # ─── Diagnostic: verify globals after exec ───
    _g = globals()
    print(f"[{ts()}] POST-EXEC DIAGNOSTICS:")
    print(f"  MIN_YEAR={_g.get('MIN_YEAR')} MAX_YEAR={_g.get('MAX_YEAR')} H={_g.get('H')} SEAM_YEAR={_g.get('SEAM_YEAR')}")
    print(f"  FULL_HIST_LEN={_g.get('FULL_HIST_LEN')} FULL_HORIZON_ONLY={_g.get('FULL_HORIZON_ONLY')}")
    print(f"  train_accts count={len(_g.get('train_accts', []))}")
    print(f"  num_use count={len(_g.get('num_use', []))}")
    print(f"  cat_use count={len(_g.get('cat_use', []))}")
    print(f"  PANEL_PATH={_g.get('PANEL_PATH')}")
    _lf_check = _g.get('lf')
    if _lf_check is not None:
        _schema = _lf_check.collect_schema()
        print(f"  lf columns ({len(_schema.names())}): {_schema.names()[:15]}...")
    else:
        print(f"  lf=None (PROBLEM!)")

    # ─── Fix EVAL_ORIGINS for our year range ───
    # Default EVAL_ORIGINS=[2021,2022,2023,2024] is wrong for origin=2019
    globals()['EVAL_ORIGINS'] = [origin - 3, origin - 2, origin - 1, origin]
    print(f"  Patched EVAL_ORIGINS={globals()['EVAL_ORIGINS']}")

    # ─── Force NUM_DIM/N_CAT to match actual feature lists ───
    # worldmodel.py feature discovery can diverge from actual num_use/cat_use
    _nu = globals().get('num_use', [])
    _cu = globals().get('cat_use', [])
    globals()['NUM_DIM'] = len(_nu)
    globals()['N_CAT'] = len(_cu)
    print(f"  Forced NUM_DIM={len(_nu)} N_CAT={len(_cu)} to match feature lists")

    # ─── Inject FULL_HIST_LEN and H into cfg dict ───
    # retrain_sample_sweep.py reads _hist_len from cfg.get("FULL_HIST_LEN", 21)
    # cfg doesn't have these keys → falls back to default 21 (HCAD year range)
    # but our panel has FULL_HIST_LEN=18 (2024-2007+1), causing shape mismatch
    _cfg = globals().get('cfg', {})
    _cfg['FULL_HIST_LEN'] = globals().get('FULL_HIST_LEN', 21)
    _cfg['H'] = globals().get('H', 5)
    _cfg['MIN_YEAR'] = globals().get('MIN_YEAR', 2005)
    _cfg['MAX_YEAR'] = globals().get('MAX_YEAR', 2025)
    globals()['cfg'] = _cfg
    print(f"  Injected into cfg: FULL_HIST_LEN={_cfg['FULL_HIST_LEN']} H={_cfg['H']}")

    print(f"[{ts()}] Executing retrain_sample_sweep.py (Cell 1.5)...")
    with open(retrain_local, "r") as f:
        retrain_source = f.read()

    # PATCH: Force _num_dim to match actual num_use list length
    # The globals lookup can diverge from the actual feature list
    retrain_source = retrain_source.replace(
        '_num_dim = globals().get("NUM_DIM", len(_num_use))',
        '_num_dim = len(_num_use); print(f"  PATCHED _num_dim={_num_dim} from len(_num_use)={len(_num_use)}")'
    )
    retrain_source = retrain_source.replace(
        '_n_cat = globals().get("N_CAT", len(_cat_use))',
        '_n_cat = len(_cat_use); print(f"  PATCHED _n_cat={_n_cat} from len(_cat_use)={len(_cat_use)}")'
    )
    # PATCH: Include jurisdiction in W&B run names so HCAD isn't mislabeled as SF
    retrain_source = retrain_source.replace(
        'name=f"v11-{variant_tag}-o{origin}"',
        f'name=f"v11-{jurisdiction}-{{variant_tag}}-o{{origin}}"'
    )
    retrain_source = retrain_source.replace(
        'tags=["v11", variant_tag, f"origin_{origin}", "retrain"]',
        f'tags=["v11", variant_tag, f"origin_{{origin}}", "retrain", "{jurisdiction}"]'
    )

    exec(retrain_source, globals())

    # ─── Upload checkpoint to GCS ───
    print(f"[{ts()}] Uploading checkpoints to GCS...")
    for fname in os.listdir(out_dir):
        if fname.endswith(".pt") or fname.endswith(".json"):
            local_path = os.path.join(out_dir, fname)
            gcs_path = f"checkpoints/{jurisdiction}/{fname}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            size_mb = os.path.getsize(local_path) / 1e6
            print(f"  Uploaded {fname} ({size_mb:.1f} MB) → gs://{bucket_name}/{gcs_path}")

    # Also persist to Modal volume
    print(f"[{ts()}] Checkpoints saved to Modal volume at /output/{jurisdiction}_v11/")

    return {
        "jurisdiction": jurisdiction,
        "origin": origin,
        "epochs": epochs,
        "output_dir": out_dir,
        "files": os.listdir(out_dir),
    }


@app.local_entrypoint()
def main(
    jurisdiction: str = "sf_ca",
    epochs: int = 60,
    sample_size: int = 500_000,
    origin: int = 2019,
):
    """Entry point: modal run scripts/train_modal.py --jurisdiction sf_ca"""
    print(f"🚀 Launching v11 training on Modal A100")
    print(f"   Jurisdiction: {jurisdiction}")
    print(f"   Epochs: {epochs}")
    print(f"   Sample: {sample_size:,}")
    print(f"   Origin: {origin}")

    result = train_worldmodel.remote(
        jurisdiction=jurisdiction,
        epochs=epochs,
        sample_size=sample_size,
        origin=origin,
    )

    print(f"\n✅ Training complete!")
    print(f"   Output: {result['output_dir']}")
    print(f"   Files: {result['files']}")

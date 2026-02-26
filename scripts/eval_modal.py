"""
Modal evaluation wrapper for Properlytic world model v11.
Runs inference across all checkpoints and computes backtest metrics logged to W&B.

Usage:
    modal run scripts/eval_modal.py --jurisdiction sf_ca
"""
import modal
import os

app = modal.App("properlytic-eval-v11")

inference_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=0.20",
        "pyarrow>=14.0",
        "numpy>=1.24",
        "scipy>=1.11",
        "torch>=2.1",
        "wandb>=0.16",
        "google-cloud-storage>=2.10",
        "scikit-learn>=1.3",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])


@app.function(
    image=inference_image,
    gpu="A10G",  # Inference can use smaller GPUs
    timeout=7200,
    secrets=[gcs_secret, wandb_secret],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints")},
)
def evaluate_checkpoints(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    origin: int = 2019,
    sample_size: int = 20_000,
    scenarios: int = 128,
):
    import json, time, tempfile, glob, pickle
    import numpy as np
    import polars as pl
    import torch
    from scipy.stats import kstest, spearmanr
    import wandb
    
    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── Auth ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ─── 1. Download WorldModel Code ───
    print(f"[{ts()}] Downloading worldmodel logic...")
    wm_blob = bucket.blob("code/worldmodel.py")
    if not wm_blob.exists():
        raise FileNotFoundError("code/worldmodel.py not found in GCS. Run training first to upload.")
    
    wm_blob.download_to_filename("/tmp/worldmodel.py")
    
    # ─── 2. Inject Runtime Global Overrides ───
    # We patch constants to point to local Modal paths
    patch_code = f"""
import os
os.environ["JURISDICTION"] = "{jurisdiction}"
os.environ["WM_MAX_ACCTS"] = "{sample_size}"
os.environ["WM_PANEL_OVERRIDE_PATH"] = "/tmp/panel_actuals.parquet"
# Do *not* run build shards or train loops upon import
os.environ["SKIP_WM_MAIN"] = "1"
os.environ["INFERENCE_ONLY"] = "1"
"""
    with open("/tmp/worldmodel.py", "r") as f:
        wm_code = f.read()
        
    modified_wm_code = patch_code + "\n" + wm_code.replace("if __name__ == '__main__' or globals().get('__colab__'):", "if False:")
    modified_wm_code = patch_code + "\n" + wm_code.replace("if __name__ == '__main__' or globals().get('__colab__'):", "if False:")
    # Force the local path override to bypass os.path.exists checks at import time
    modified_wm_code = modified_wm_code.replace('PANEL_PATH = PANEL_PATH_LOCAL if os.path.exists(PANEL_PATH_LOCAL) else PANEL_PATH_DRIVE', 'PANEL_PATH = "/tmp/panel_actuals.parquet"')
    
    # Force bypassing the explicit check on line 299-301 in worldmodel
    modified_wm_code = modified_wm_code.replace('if not os.path.exists(PANEL_PATH):\n    raise FileNotFoundError("Panel not found")', 'pass')
    modified_wm_code = modified_wm_code.replace('if not os.path.exists(PANEL_PATH):', 'if False:')
    
    # ─── 2.5 Load and Format Panel (Must happen BEFORE worldmodel exec) ───
    print(f"[{ts()}] Loading {jurisdiction} ground truth from panel...")
    panel_blob_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    if jurisdiction == "all":
        panel_blob_path = "panel/grand_panel/part.parquet"
        
    local_panel_path = "/tmp/panel_actuals.parquet"
    bucket.blob(panel_blob_path).download_to_filename(local_panel_path)
    df_actuals = pl.read_parquet(local_panel_path)
    
    if jurisdiction != "all" and "jurisdiction" in df_actuals.columns:
        df_actuals = df_actuals.filter(pl.col("jurisdiction") == jurisdiction)

    # Filter to valid appraisal values and map to standard WorldModel canonical mappings
    rename_map = {
        "parcel_id": "acct",
        "year": "yr",
        "property_value": "tot_appr_val",
        "sqft": "living_area",
        "land_area": "land_ar",
        "year_built": "yr_blt",
        "bedrooms": "bed_cnt",
        "bathrooms": "full_bath",
        "stories": "nbr_story",
        "lat": "gis_lat",
        "lon": "gis_lon",
    }
    actual_renames = {k: v for k, v in rename_map.items() if k in df_actuals.columns}
    
    # Drop any existing columns that clash with our target names
    drop_targets = [v for k, v in actual_renames.items() if v in df_actuals.columns]
    if drop_targets:
        df_actuals = df_actuals.drop(drop_targets)

    df_actuals = df_actuals.rename(actual_renames)
    df_actuals = df_actuals.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))
    # Write back the properly formatted panel for worldmodel to consume
    df_actuals.write_parquet(local_panel_path)
    
    # Patch MIN_YEAR and MAX_YEAR based on the actual dataset
    yr_min = int(df_actuals["yr"].min())
    yr_max = int(df_actuals["yr"].max())
    print(f"[{ts()}] Patching worldmodel constants: yr_min={yr_min}, yr_max={yr_max}")
    
    modified_wm_code = modified_wm_code.replace("MIN_YEAR = 2005", f"MIN_YEAR = {yr_min}")
    modified_wm_code = modified_wm_code.replace("MAX_YEAR = 2025", f"MAX_YEAR = {yr_max}")
    modified_wm_code = modified_wm_code.replace("SEAM_YEAR = 2025", f"SEAM_YEAR = {yr_max}")
    # Patch S_BLOCK to reduce VRAM requirements during inference
    modified_wm_code = modified_wm_code.replace("S_BLOCK = 9999", "S_BLOCK = 16")
    
    exec_globals = {}
    print(f"[{ts()}] Executing worldmodel.py context...")
    exec(modified_wm_code, exec_globals)

    # Resolve required objects
    lf = exec_globals["lf"]
    num_use_local = exec_globals["num_use"]
    cat_use_local = exec_globals["cat_use"]
    create_denoiser = exec_globals["create_denoiser_v11"]
    
    # New v11 objects required for the Token Diffusion model
    create_gating_network = exec_globals["create_gating_network"]
    create_token_persistence = exec_globals["create_token_persistence"]
    create_coherence_scale = exec_globals["create_coherence_scale"]
    sample_token_paths = exec_globals["sample_token_paths_learned"]
    
    GlobalProjection = exec_globals["GlobalProjection"]
    GeoProjection = exec_globals["GeoProjection"]
    SimpleScaler = exec_globals["SimpleScaler"]
    Scheduler = exec_globals["Scheduler"]
    sample_ddim = exec_globals["sample_ddim_v11"]
    build_inference_context = exec_globals["build_inference_context_chunked_v102"]
    
    _device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[{ts()}] Context loaded. Device: {_device}")

    # ─── 3. Load Actuals Ground Truth ───
    df_actuals = pl.read_parquet(local_panel_path)
    
    actual_vals = {}
    for yr in [origin] + [origin + h for h in range(1, 6)]:
        yr_df = df_actuals.filter(pl.col("yr") == yr).select(["acct", "tot_appr_val"])
        actual_vals[yr] = dict(zip(yr_df["acct"].to_list(), yr_df["tot_appr_val"].to_list()))
    print(f"[{ts()}] Actuals loaded for origin {origin} and horizon")

    # ─── 4. Inference loop over checkpoints ───
    variant_raw_results = {}
    MAX_HORIZON = 5
    VALUE_BRACKETS = [
        ("<200K",     0,        200_000),
        ("200K-500K", 200_000,  500_000),
        ("500K-1M",   500_000,  1_000_000),
        ("1M+",       1_000_000, 1e18),
        ("ALL",       0,        1e18),
    ]

    run_group = f"eval_v11_{jurisdiction}"
    
    wandb.init(
        project="homecastr",
        entity="dhardestylewis-columbia-university",
        name=f"{run_group}_{origin}",
        group=run_group,
        tags=["eval", "v11", jurisdiction],
        config={"sample_size": sample_size, "scenarios": scenarios, "origin": origin},
        job_type="evaluation"
    )

    ckpt_dir = f"/output/{jurisdiction}_v11"
    
    import glob
    candidates = glob.glob(os.path.join(ckpt_dir, f"ckpt_v11_origin_{origin}*.pt")) + \
                 glob.glob(os.path.join(ckpt_dir, f"ckpt_origin_{origin}*.pt"))
    if not candidates:
        print(f"[{ts()}] ⚠️ Checkpoint not found for origin {origin} in {ckpt_dir}, skipping")
        return
    ckpt_path = candidates[0]

    print(f"\n[{ts()}] ── Processing Origin {origin} ({os.path.basename(ckpt_path)}) ──")
    
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # Load Model
    ckpt = torch.load(ckpt_path, map_location=_device)
    _cfg = ckpt.get("cfg", {})
    
    def _strip(d): return {k.replace("_orig_mod.", ""): v for k, v in d.items()}
    sd = _strip(ckpt["model_state_dict"])
    hist_len = sd["hist_enc.0.weight"].shape[1]
    num_dim = sd["num_enc.0.weight"].shape[1]
    n_cat = len([k for k in sd if k.startswith("cat_embs.") and k.endswith(".weight")])
    H = int(_cfg.get("H", MAX_HORIZON))

    # CRITICAL: Truncate num_use to match checkpoint's num_dim
    # The worldmodel exec discovers features from the full panel but the checkpoint
    # was trained on a subset. We must align to avoid shape mismatch.
    if len(num_use_local) != num_dim:
        print(f"[{ts()}] ⚠️ num_use mismatch: panel has {len(num_use_local)} features, checkpoint has {num_dim}. Truncating.")
        num_use_local = num_use_local[:num_dim]

    model = create_denoiser(target_dim=H, hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
    model.load_state_dict(sd)
    model = model.to(_device).eval()
    
    # v11 Token Diffusion Components
    gating_net = create_gating_network(hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
    if "gating_state_dict" in ckpt:
        gating_net.load_state_dict(_strip(ckpt["gating_state_dict"]))
    elif "gating_net_state_dict" in ckpt:
        gating_net.load_state_dict(_strip(ckpt["gating_net_state_dict"]))
    gating_net = gating_net.to(_device).eval()
    
    token_persistence = create_token_persistence()
    if "token_persistence_state" in ckpt:
        token_persistence.load_state_dict(ckpt["token_persistence_state"])
    elif "token_persistence_state_dict" in ckpt:
        token_persistence.load_state_dict(ckpt["token_persistence_state_dict"])
        
    coh_scale = create_coherence_scale()
    if "coherence_scale_state" in ckpt:
        coh_scale.load_state_dict(ckpt["coherence_scale_state"])
    elif "coh_scale_state_dict" in ckpt:
        coh_scale.load_state_dict(ckpt["coh_scale_state_dict"])

    # v11 Doesn't have GlobalProjection / GeoProjection
    model._y_scaler = SimpleScaler(mean=np.array(ckpt["y_scaler_mean"]), scale=np.array(ckpt["y_scaler_scale"]))
    model._n_scaler = SimpleScaler(mean=np.array(ckpt["n_scaler_mean"]), scale=np.array(ckpt["n_scaler_scale"]))
    model._t_scaler = SimpleScaler(mean=np.array(ckpt["t_scaler_mean"]), scale=np.array(ckpt["t_scaler_scale"]))
    global_medians = ckpt.get("global_medians", {})

    # Context Setup
    origin_accts = list(actual_vals.get(origin, {}).keys())
    if len(origin_accts) < 100:
        print(f"[{ts()}] ⚠️ Insufficient ground truth at origin {origin}, skipping")
        return

    np.random.seed(42 + origin)
    sample_accts = np.random.choice(origin_accts, min(sample_size, len(origin_accts)), replace=False).tolist()

    ctx = build_inference_context(
        lf=lf, accts=sample_accts, num_use_local=num_use_local, cat_use_local=cat_use_local,
        global_medians=global_medians, anchor_year=origin, max_parcels=len(sample_accts)
    )
    n_valid = len(ctx["acct"])
    print(f"[{ts()}] Built context for {n_valid:,} valid parcels")

    # Scenarios
    sched = Scheduler(int(_cfg.get("DIFF_STEPS_TRAIN", 128)), device=_device)
    
    # Sample shared latent shock paths
    phi_vec = token_persistence.get_phi()
    Z_tokens = sample_token_paths(K=int(_cfg.get("K_TOKENS", 8)), H=H, phi_vec=phi_vec, S=scenarios, device=_device)

    # Inference chunking
    batch_size = min(256, n_valid)
    all_deltas = []
    for b_start in range(0, n_valid, batch_size):
        b_end = min(b_start + batch_size, n_valid)
        b_deltas = sample_ddim(
            model=model, gating_net=gating_net, sched=sched,
            hist_y_b=ctx["hist_y"][b_start:b_end], cur_num_b=ctx["cur_num"][b_start:b_end],
            cur_cat_b=ctx["cur_cat"][b_start:b_end], region_id_b=ctx["region_id"][b_start:b_end],
            Z_tokens=Z_tokens, coh_scale=coh_scale, device=_device
        )
        all_deltas.append(b_deltas)

    deltas = np.concatenate(all_deltas, axis=0)
    accts = ctx["acct"]
    ya = ctx["y_anchor"]
    y_levels = ya[:, None, None] + np.cumsum(deltas, axis=2)  # [N, S, H] log-space
    base_v = actual_vals.get(origin, {})
    base_vals_arr = np.array([base_v.get(str(a).strip(), 0) for a in accts])
    variant_raw_results[("v11", origin)] = {
        "accts": list(accts),
        "y_anchor": ya,
        "y_levels": y_levels,
        "base_val": base_vals_arr
    }

    # ─── 5. W&B Logging Metrics Engine ───
    print(f"\n[{ts()}] ── Computing and Logging Metrics to W&B ──")
    
    key = ("v11", origin)
    if key in variant_raw_results:
        res = variant_raw_results[key]
        y_levels = res["y_levels"]
        y_anchor = res["y_anchor"]
        accts = res["accts"]
        base_v = actual_vals.get(origin, {})
        
        for h in range(1, MAX_HORIZON + 1):
            h_idx = h - 1
            eyr = origin + h
            if eyr not in actual_vals:
                continue
                
            future_v = actual_vals[eyr]
            
            for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
                fan_widths = []
                pits = []
                preds = []
                acts = []
                fan_hits = 0
                fan_checks = 0
                
                for i in range(len(accts)):
                    acct = str(accts[i]).strip()
                    bv = base_v.get(acct, 0)
                    av = future_v.get(acct)
                    
                    if bv <= 0 or av is None or not (bkt_lo <= bv < bkt_hi):
                        continue
                        
                    fan = y_levels[i, :, h_idx]
                    p10 = np.expm1(np.percentile(fan, 10))
                    p50 = np.expm1(np.percentile(fan, 50))
                    p90 = np.expm1(np.percentile(fan, 90))
                    
                    if p50 > 0:
                        fan_widths.append((p90 - p10) / p50 * 100)
                        
                    fan_prices = np.exp(fan)
                    pits.append(float(np.mean(fan_prices <= av)))
                    pred_growth = float(np.expm1(np.nanmedian(fan) - y_anchor[i]) * 100)
                    actual_growth = float((av - bv) / bv * 100)
                    preds.append(pred_growth)
                    acts.append(actual_growth)
                    
                    # Coverage: does actual fall within P10-P90 fan? (dollar-space)
                    p10_dollar = np.exp(np.nanpercentile(fan, 10))
                    p90_dollar = np.exp(np.nanpercentile(fan, 90))
                    fan_checks += 1
                    if p10_dollar <= av <= p90_dollar:
                        fan_hits += 1
                    
                tag = f"o{origin}_h{h}_{bkt_label}"
                preds_arr = np.array(preds)
                acts_arr = np.array(acts)
                
                if fan_widths:
                    avg_fw = float(np.mean(fan_widths))
                    std_fw = float(np.std(fan_widths))
                    wandb.log({f"eval/fan_width/{tag}": avg_fw, f"eval/fan_std/{tag}": std_fw})
                
                if len(pits) > 20:
                    med_pit = float(np.median(pits))
                    ks = float(kstest(pits, 'uniform').statistic)
                    # Handle constant predictions which cause spearmanr to return NaN/warnings
                    if len(set(preds)) > 1 and len(set(acts)) > 1:
                        rho, rho_p = spearmanr(preds, acts)
                        rho = float(rho)
                        rho_p = float(rho_p)
                    else:
                        rho = 0.0
                        rho_p = 1.0
                    
                    # ── New metrics from backtest.py / deep_variant_analysis.py ──
                    abs_err = np.abs(preds_arr - acts_arr)
                    mdae = float(np.median(abs_err))
                    mae = float(np.mean(abs_err))
                    # MAPE (from inference_pipeline.py)
                    nonzero_acts = acts_arr[acts_arr != 0]
                    nonzero_preds = preds_arr[acts_arr != 0]
                    mape = float(np.mean(np.abs(nonzero_preds - nonzero_acts) / np.abs(nonzero_acts))) if len(nonzero_acts) > 0 else float('nan')
                    # Bias: median pred - median actual (from deep_variant_analysis.py)
                    bias = float(np.median(preds_arr) - np.median(acts_arr))
                    # % negative growth predictions (from compare_checkpoint_variants.py)
                    pct_neg = float(np.mean(preds_arr < 0) * 100)
                    # Median predicted growth (from compare_checkpoint_variants.py)
                    med_growth = float(np.median(preds_arr))
                    # Coverage % (from deep_variant_analysis.py + inference_pipeline.py)
                    coverage = float(fan_hits / fan_checks * 100) if fan_checks > 0 else float('nan')
                    
                    wandb.log({
                        # Original 3 core metrics
                        f"eval/rho/{tag}": rho,
                        f"eval/pit_med/{tag}": med_pit,
                        f"eval/pit_ks/{tag}": ks,
                        # New: accuracy metrics
                        f"eval/mdae/{tag}": mdae,
                        f"eval/mae/{tag}": mae,
                        f"eval/mape/{tag}": mape,
                        # New: calibration & bias
                        f"eval/coverage/{tag}": coverage,
                        f"eval/bias/{tag}": bias,
                        f"eval/pct_neg/{tag}": pct_neg,
                        f"eval/med_growth/{tag}": med_growth,
                        # New: significance
                        f"eval/rho_p/{tag}": rho_p,
                    })
                    print(f"  [{tag}] ρ: {rho:+.3f} (p={rho_p:.4f}) | mdPIT: {med_pit:.3f} | avgFW: {avg_fw:.1f}% | MdAE: {mdae:.1f}% | Covg: {coverage:.1f}% | Bias: {bias:+.1f}pp (n={len(pits)})")
    
    # ─── Spatial Coherence (Cross-Parcel Correlation) ───
    key = ("v11", origin)
    if key in variant_raw_results:
        y_levels = variant_raw_results[key]["y_levels"]
        N = y_levels.shape[0]
        
        for h in [1, 3, 5]:
            max_p = min(500, N)
            idx = np.random.choice(N, max_p, replace=False) if N > max_p else np.arange(N)
            y_h = y_levels[idx, :, h - 1]
            corr_mat = np.corrcoef(y_h)
            corrs = corr_mat[np.triu_indices(len(idx), k=1)]
            corrs = corrs[np.isfinite(corrs)]
            if len(corrs) > 0:
                wandb.log({
                    f"eval/parcel_corr/o{origin}_h{h}": float(np.mean(corrs)),
                    f"eval/parcel_corr_std/o{origin}_h{h}": float(np.std(corrs))
                })

    wandb.finish()
    print(f"\n[{ts()}] ✅ Evaluation Complete.")

@app.local_entrypoint()
def main(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    sample_size: int = 20_000,
    scenarios: int = 128,
):
    origins = [2019, 2020, 2021, 2022, 2023, 2024]
    print(f"🚀 Launching parallel v11 evaluation on Modal across {len(origins)} origins")
    print(f"   Jurisdiction: {jurisdiction}")
    print(f"   Origins: {origins}")
    
    # Map across multiple origins concurrently
    params = [(jurisdiction, bucket_name, o, sample_size, scenarios) for o in origins]
    
    results = list(evaluate_checkpoints.starmap(params))

    print(f"\n✅ Parallel Evaluation complete!")

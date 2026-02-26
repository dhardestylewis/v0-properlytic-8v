import functions_framework
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import yaml
import io
import json
from google.cloud import storage

BUCKET = "properlytic-raw-data"
PANEL_PREFIX = "panel"

# Inline schema mappings (subset of schema_registry.yaml)
SOURCES = {
    "hcad_houston": {
        "gcs_path": "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet",
        "format": "parquet",
        "mapping": {
            "parcel_id": "acct", "year": "yr", "assessed_value": "tot_appr_val",
            "land_value": "land_val", "improvement_value": "impr_val",
            "dwelling_type": "state_class", "sqft": "living_area",
            "land_area": "land_ar", "year_built": "yr_blt",
            "bedrooms": "bed_cnt", "address": "site_addr_1",
            "lat": "lat", "lon": "lon",
        },
        "derived": {"bathrooms": "full_bath + 0.5 * half_bath", "stories": "nbr_story"},
    },
    "cook_county_il": {
        "gcs_prefix": "cook_county_il/assessed_values",
        "format": "csv_chunked",
        "mapping": {
            "parcel_id": "pin", "year": "year", "dwelling_type": "class",
            "improvement_value": "certified_bldg", "land_value": "certified_land",
            "assessed_value": "certified_tot",
        },
    },
    "sf_ca": {
        "gcs_prefix": "sf/secured_roll",
        "format": "csv_chunked",
        "mapping": {
            "parcel_id": "parcel_number", "year": "closed_roll_year",
            "dwelling_type": "property_class_code",
            "improvement_value": "assessed_improvement_value",
            "land_value": "assessed_land_value",
            "sqft": "property_area", "land_area": "lot_area",
            "year_built": "year_property_built",
            "bedrooms": "number_of_bedrooms", "bathrooms": "number_of_bathrooms",
            "stories": "number_of_stories", "address": "property_location",
            "sale_date": "current_sales_date",
        },
        "derived": {"assessed_value": "assessed_improvement_value + assessed_land_value"},
    },
    "nyc": {
        "gcs_path": "nyc/nyc_sales_clean.parquet",
        "format": "parquet",
        "mapping": {},  # auto-detect from parquet schema
    },
    "france_dvf": {
        "gcs_prefix": "france_dvf",
        "format": "csv_gz",
        "mapping": {
            "parcel_id": "id_parcelle", "sale_price": "valeur_fonciere",
            "sale_date": "date_mutation", "dwelling_type": "type_local",
            "sqft": "surface_reelle_bati", "land_area": "surface_terrain",
            "bedrooms": "nombre_pieces_principales",
            "lat": "latitude", "lon": "longitude",
        },
    },
}

CANONICAL_COLS = [
    "parcel_id", "jurisdiction", "year", "sale_price", "sale_date",
    "assessed_value", "land_value", "improvement_value", "dwelling_type",
    "sqft", "land_area", "year_built", "bedrooms", "bathrooms",
    "stories", "address", "lat", "lon",
]


@functions_framework.http
def build_panel(request):
    """Build panel partitions from raw GCS data → GCS parquet. All in-cloud."""
    source = request.args.get("source", "all")
    max_chunks = int(request.args.get("max_chunks", "999"))
    dry_run = request.args.get("dry_run", "false") == "true"

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    results = {}

    targets = SOURCES if source == "all" else {source: SOURCES.get(source, {})}

    for name, cfg in targets.items():
        if not cfg:
            results[name] = {"error": f"Unknown source: {name}"}
            continue
        try:
            results[name] = _build_partition(client, bucket, name, cfg, max_chunks, dry_run)
        except Exception as e:
            results[name] = {"error": str(e)}

    return json.dumps(results, indent=2, default=str)


def _build_partition(client, bucket, jurisdiction, cfg, max_chunks, dry_run):
    """Read raw data, map to canonical schema, write as Hive partition."""
    fmt = cfg.get("format", "")
    mapping = cfg.get("mapping", {})

    # --- Read raw data ---
    if fmt == "parquet":
        blob = bucket.blob(cfg["gcs_path"])
        buf = io.BytesIO(blob.download_as_bytes())
        df = pd.read_parquet(buf)
    elif fmt == "csv_chunked":
        dfs = []
        prefix = cfg["gcs_prefix"]
        blobs = list(bucket.list_blobs(prefix=prefix))
        blobs = [b for b in blobs if b.name.endswith(".csv")][:max_chunks]
        for blob in blobs:
            try:
                text = blob.download_as_text()
                chunk = pd.read_csv(io.StringIO(text), low_memory=False)
                dfs.append(chunk)
            except Exception:
                continue
        if not dfs:
            return {"error": "No CSV chunks found"}
        df = pd.concat(dfs, ignore_index=True)
    elif fmt == "csv_gz":
        dfs = []
        prefix = cfg["gcs_prefix"]
        blobs = list(bucket.list_blobs(prefix=prefix))
        blobs = [b for b in blobs if b.name.endswith(".csv.gz") or b.name.endswith(".csv")][:max_chunks]
        for blob in blobs:
            try:
                data = blob.download_as_bytes()
                chunk = pd.read_csv(io.BytesIO(data), compression="gzip" if blob.name.endswith(".gz") else None, low_memory=False)
                dfs.append(chunk)
            except Exception:
                continue
        if not dfs:
            return {"error": "No CSV files found"}
        df = pd.concat(dfs, ignore_index=True)
    else:
        return {"error": f"Unsupported format: {fmt}"}

    if dry_run:
        return {
            "columns": list(df.columns),
            "rows": len(df),
            "dtypes": {c: str(df[c].dtype) for c in df.columns[:20]},
            "sample": df.head(3).to_dict(orient="records"),
        }

    # --- Map to canonical schema ---
    mapped = pd.DataFrame()
    for canon_col, src_col in mapping.items():
        if src_col in df.columns:
            mapped[canon_col] = df[src_col]

    # Handle derived fields
    for canon_col, expr in cfg.get("derived", {}).items():
        try:
            mapped[canon_col] = df.eval(expr)
        except Exception:
            pass

    mapped["jurisdiction"] = jurisdiction

    # Ensure all canonical columns exist
    for col in CANONICAL_COLS:
        if col not in mapped.columns:
            mapped[col] = None

    mapped = mapped[CANONICAL_COLS]

    # --- Write partition to GCS ---
    buf = io.BytesIO()
    mapped.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    size_mb = buf.tell() / 1e6

    blob_path = f"{PANEL_PREFIX}/jurisdiction={jurisdiction}/part.parquet"
    blob = bucket.blob(blob_path)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    years = sorted(mapped["year"].dropna().unique().tolist()) if "year" in mapped.columns and mapped["year"].notna().any() else []

    return {
        "partition": f"gs://{BUCKET}/{blob_path}",
        "rows": len(mapped),
        "size_mb": round(size_mb, 1),
        "years": [int(y) for y in years[:5]] + ["..."] + [int(y) for y in years[-5:]] if len(years) > 10 else [int(y) for y in years],
        "columns_mapped": [c for c in CANONICAL_COLS if mapped[c].notna().any()],
    }

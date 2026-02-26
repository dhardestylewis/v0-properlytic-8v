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
        "mapping": {
            "parcel_id": "BBL", "sale_price": "SALE PRICE",
            "sale_date": "SALE DATE", "dwelling_type": "BUILDING CLASS CATEGORY",
            "sqft": "GROSS SQUARE FEET", "land_area": "LAND SQUARE FEET",
            "year_built": "YEAR BUILT", "address": "ADDRESS",
        },
    },
    "uk_ppd": {
        "gcs_path": "uk_ppd/pp-complete.csv",
        "format": "csv_chunked",
        "mapping": {
            "sale_price": "price", "sale_date": "date_of_transfer",
            "dwelling_type": "property_type",
        },
        "derived": {"parcel_id": "transaction_id",
                    "address": "paon + ' ' + street + ', ' + town_city + ' ' + postcode"},
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
    "philly": {
        "gcs_prefix": "philly",
        "format": "csv_chunked",
        "mapping": {
            "parcel_id": "parcel_number", "assessed_value": "market_value",
            "sale_price": "sale_price", "sale_date": "sale_date",
            "dwelling_type": "category_code", "sqft": "total_livable_area",
            "land_area": "total_area", "year_built": "year_built",
            "bedrooms": "number_of_bedrooms", "bathrooms": "number_of_bathrooms",
            "stories": "number_stories", "address": "location",
            "lat": "lat", "lon": "lng",
        },
    },
}

CANONICAL_COLS = [
    "parcel_id", "jurisdiction", "year",
    "property_value",  # unified target: coalesce(sale_price, assessed_value)
    "sale_price", "sale_date",
    "assessed_value", "land_value", "improvement_value", "dwelling_type",
    "sqft", "land_area", "year_built", "bedrooms", "bathrooms",
    "stories", "address", "lat", "lon",
]

# Jurisdiction → FIPS mapping (no geocoding needed!)
JURISDICTION_FIPS = {
    "hcad_houston":   {"county_fips": "48201", "state_fips": "48"},
    "cook_county_il": {"county_fips": "17031", "state_fips": "17"},
    "sf_ca":          {"county_fips": "06075", "state_fips": "06"},
    "nyc":            {"county_fips": "36061", "state_fips": "36"},
    "france_dvf":     {"county_fips": None,    "state_fips": None},
    "massgis":        {"county_fips": "25017", "state_fips": "25"},
    "uk_ppd":         {"county_fips": None,    "state_fips": None},
    "philly":         {"county_fips": "42101", "state_fips": "42"},
    "dc":             {"county_fips": "11001", "state_fips": "11"},
}

# Contextual source configs for joining
CONTEXTUAL = {
    "fred": {
        "join_level": "national",  # same for all parcels, join by year
        "series": {
            "MORTGAGE30US.csv": "mortgage_rate_30yr",
            "DGS10.csv": "treasury_10yr",
            "FEDFUNDS.csv": "fed_funds_rate",
            "CPIAUCSL.csv": "cpi",
            "UNRATE.csv": "unemployment_rate",
            "CSUSHPINSA.csv": "case_shiller_hpi",
            "USSTHPI.csv": "fhfa_hpi_national",
        },
    },
    "epa_aqi": {
        "join_level": "county",
        "prefix": "epa/",
        "output_fields": ["median_aqi", "good_days", "unhealthy_days"],
    },
    "climate": {
        "join_level": "county",
        "prefix": "climate/tavg_annual_",
        "output_fields": ["tavg_annual"],
    },
}


@functions_framework.http
def build_panel(request):
    """Build panel partitions from raw GCS data → GCS parquet. All in-cloud."""
    source = request.args.get("source", "all")
    max_chunks = int(request.args.get("max_chunks", "999"))
    dry_run = request.args.get("dry_run", "false") == "true"
    skip_context = request.args.get("skip_context", "false") == "true"

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    results = {}

    targets = SOURCES if source == "all" else {source: SOURCES.get(source, {})}

    for name, cfg in targets.items():
        if not cfg:
            results[name] = {"error": f"Unknown source: {name}"}
            continue
        try:
            results[name] = _build_partition(client, bucket, name, cfg, max_chunks, dry_run, skip_context)
        except Exception as e:
            results[name] = {"error": str(e)}

    return json.dumps(results, indent=2, default=str)


def _map_chunk_to_canonical(df_chunk, mapping, derived, jurisdiction):
    """Map a raw DataFrame chunk to canonical schema. Used per-chunk to reduce memory."""
    mapped = pd.DataFrame()
    for canon_col, src_col in mapping.items():
        if src_col in df_chunk.columns:
            mapped[canon_col] = df_chunk[src_col]

    # Handle derived fields
    for canon_col, expr in derived.items():
        try:
            mapped[canon_col] = df_chunk.eval(expr)
        except Exception:
            pass

    mapped["jurisdiction"] = jurisdiction

    # Ensure all canonical columns exist
    for col in CANONICAL_COLS:
        if col not in mapped.columns:
            mapped[col] = None

    return mapped[CANONICAL_COLS]


def _build_partition(client, bucket, jurisdiction, cfg, max_chunks, dry_run, skip_context):
    """Read raw data, map to canonical schema, join contextual, write partition.
    Uses chunked reads to avoid OOM on large sources (HCAD 2.4GB, Cook 1.2GB+, DVF 400MB+)."""
    import traceback
    fmt = cfg.get("format", "")
    mapping = cfg.get("mapping", {})
    derived = cfg.get("derived", {})

    # --- Determine columns we actually need from raw data ---
    needed_raw_cols = set(mapping.values())
    # Also need columns referenced in derived expressions
    for expr in derived.values():
        # Simple heuristic: extract word tokens that could be column names
        for token in expr.replace("+", " ").replace("*", " ").replace("(", " ").replace(")", " ").replace("'", " ").replace(",", " ").split():
            try:
                float(token)
            except ValueError:
                if token not in ("0.5", "0", "1"):
                    needed_raw_cols.add(token.strip())

    # --- Read raw data (CHUNKED to avoid OOM) ---
    if fmt == "parquet":
        blob = bucket.blob(cfg["gcs_path"])
        data = blob.download_as_bytes()
        pf = pq.ParquetFile(io.BytesIO(data))

        if dry_run:
            # Read just first row group for dry run
            first_rg = pf.read_row_group(0).to_pandas()
            return {
                "columns": list(first_rg.columns),
                "rows": pf.metadata.num_rows,
                "row_groups": pf.metadata.num_row_groups,
                "dtypes": {c: str(first_rg[c].dtype) for c in first_rg.columns[:20]},
                "sample": first_rg.head(3).to_dict(orient="records"),
            }

        # Read row groups one at a time, map each to canonical, then concat
        # This way we never hold the full raw DataFrame in memory
        available_cols = [c for c in needed_raw_cols if c in pf.schema.names]
        mapped_chunks = []
        for i in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(i, columns=available_cols if available_cols else None).to_pandas()
            mapped_chunks.append(_map_chunk_to_canonical(rg, mapping, derived, jurisdiction))
            del rg  # free memory immediately
        mapped = pd.concat(mapped_chunks, ignore_index=True)
        del mapped_chunks, data

    elif fmt == "csv_chunked":
        prefix = cfg["gcs_prefix"]
        blobs = list(bucket.list_blobs(prefix=prefix))
        blobs = [b for b in blobs if b.name.endswith(".csv")][:max_chunks]

        if not blobs:
            return {"error": "No CSV chunks found"}

        if dry_run:
            text = blobs[0].download_as_text()
            sample = pd.read_csv(io.StringIO(text), low_memory=False, nrows=100)
            return {
                "columns": list(sample.columns),
                "chunks_found": len(blobs),
                "dtypes": {c: str(sample[c].dtype) for c in sample.columns[:20]},
                "sample": sample.head(3).to_dict(orient="records"),
            }

        # Map each CSV chunk independently — only hold canonical cols in memory
        mapped_chunks = []
        for blob_obj in blobs:
            try:
                text = blob_obj.download_as_text()
                chunk = pd.read_csv(io.StringIO(text), low_memory=False)
                mapped_chunks.append(_map_chunk_to_canonical(chunk, mapping, derived, jurisdiction))
                del chunk  # free raw data immediately
            except Exception:
                continue
        if not mapped_chunks:
            return {"error": "No CSV chunks could be parsed"}
        mapped = pd.concat(mapped_chunks, ignore_index=True)
        del mapped_chunks

    elif fmt == "csv_gz":
        prefix = cfg["gcs_prefix"]
        blobs = list(bucket.list_blobs(prefix=prefix))
        blobs = [b for b in blobs if b.name.endswith(".csv.gz") or b.name.endswith(".csv")][:max_chunks]

        if not blobs:
            return {"error": "No CSV files found"}

        if dry_run:
            data = blobs[0].download_as_bytes()
            sample = pd.read_csv(io.BytesIO(data),
                compression="gzip" if blobs[0].name.endswith(".gz") else None,
                low_memory=False, nrows=100)
            return {
                "columns": list(sample.columns),
                "files_found": len(blobs),
                "dtypes": {c: str(sample[c].dtype) for c in sample.columns[:20]},
                "sample": sample.head(3).to_dict(orient="records"),
            }

        # Map each file independently to canonical schema
        mapped_chunks = []
        for blob_obj in blobs:
            try:
                data = blob_obj.download_as_bytes()
                chunk = pd.read_csv(io.BytesIO(data),
                    compression="gzip" if blob_obj.name.endswith(".gz") else None,
                    low_memory=False)
                mapped_chunks.append(_map_chunk_to_canonical(chunk, mapping, derived, jurisdiction))
                del chunk, data  # free raw data immediately
            except Exception:
                continue
        if not mapped_chunks:
            return {"error": "No CSV files could be parsed"}
        mapped = pd.concat(mapped_chunks, ignore_index=True)
        del mapped_chunks
    else:
        return {"error": f"Unsupported format: {fmt}"}

    # Unified target: property_value = coalesce(sale_price, assessed_value)
    mapped["property_value"] = mapped["sale_price"].fillna(mapped["assessed_value"])

    # Coerce year to int; if year is null, try to derive from sale_date or year_built
    if mapped["year"].isna().all():
        if mapped["sale_date"].notna().any():
            # Try parsing sale_date with multiple strategies
            sd = mapped["sale_date"].astype(str).str.strip()
            parsed = pd.to_datetime(sd, errors="coerce", infer_datetime_format=True)
            if parsed.notna().any():
                mapped["year"] = parsed.dt.year
                print(f"  ✅ Derived year from sale_date: {int(mapped['year'].min())}-{int(mapped['year'].max())}")
        # Fallback: use year_built if sale_date parsing failed
        if mapped["year"].isna().all() and mapped.get("year_built") is not None and mapped["year_built"].notna().any():
            mapped["year"] = pd.to_numeric(mapped["year_built"], errors="coerce").astype("Int64")
            print(f"  ⚠️ Derived year from year_built (fallback): {int(mapped['year'].min())}-{int(mapped['year'].max())}")
    if mapped["year"].notna().any():
        mapped["year"] = pd.to_numeric(mapped["year"], errors="coerce").astype("Int64")

    # --- Join contextual sources ---
    context_cols_added = []
    if not skip_context:
        fips = JURISDICTION_FIPS.get(jurisdiction, {})
        county_fips = fips.get("county_fips")

        # FRED: national, join by year
        context_cols_added += _join_fred(bucket, mapped)

        # EPA AQI: county + year
        if county_fips:
            context_cols_added += _join_epa(bucket, mapped, county_fips)

        # NOAA climate: county + year
        if county_fips:
            context_cols_added += _join_climate(bucket, mapped, county_fips)

        # FEMA NRI: county (static, no year dimension)
        if county_fips:
            context_cols_added += _join_fema_nri(bucket, mapped, county_fips)

        # Census building permits: county + year
        if county_fips:
            context_cols_added += _join_building_permits(bucket, mapped, county_fips)

        # LEHD: state-level jobs aggregate
        state_fips = fips.get("state_fips")
        if state_fips:
            context_cols_added += _join_lehd(bucket, mapped, state_fips)

        # HUD FMR: county
        if county_fips:
            context_cols_added += _join_hud_fmr(bucket, mapped, county_fips)

        # IRS migration: county
        if county_fips:
            context_cols_added += _join_irs_migration(bucket, mapped, county_fips)

        # Census ACS: county-level fallback (population, income, home value)
        if county_fips:
            context_cols_added += _join_census_acs(bucket, mapped, county_fips)

        # FHFA HPI: state-level
        if state_fips:
            context_cols_added += _join_fhfa_hpi(bucket, mapped, state_fips)

        # FEMA Disasters: county + year
        if county_fips:
            context_cols_added += _join_fema_disasters(bucket, mapped, county_fips)

        # International contextual: ECB + INSEE for France, BoE for UK
        if jurisdiction == "france_dvf":
            context_cols_added += _join_ecb_rate(bucket, mapped)
            context_cols_added += _join_insee_hpi(bucket, mapped)
        elif jurisdiction == "uk_ppd":
            context_cols_added += _join_boe_rate(bucket, mapped)

    # --- Write partition to GCS ---
    buf = io.BytesIO()
    mapped.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    size_mb = buf.tell() / 1e6

    blob_path = f"{PANEL_PREFIX}/jurisdiction={jurisdiction}/part.parquet"
    blob_out = bucket.blob(blob_path)
    buf.seek(0)
    blob_out.upload_from_file(buf, content_type="application/octet-stream")

    years = sorted(mapped["year"].dropna().unique().tolist()) if mapped["year"].notna().any() else []

    return {
        "partition": f"gs://{BUCKET}/{blob_path}",
        "rows": len(mapped),
        "size_mb": round(size_mb, 1),
        "years": [int(y) for y in years[:5]] + (["..."] + [int(y) for y in years[-3:]] if len(years) > 8 else []),
        "columns_mapped": [c for c in mapped.columns if mapped[c].notna().any()],
        "context_joined": context_cols_added,
    }


def _join_fred(bucket, panel):
    """Join FRED time series by year (national)."""
    added = []
    for blob_name, output_col in CONTEXTUAL["fred"]["series"].items():
        try:
            blob = bucket.blob(f"fred/{blob_name}")
            text = blob.download_as_text()
            fred = pd.read_csv(io.StringIO(text))
            # FRED format: DATE, VALUE
            fred.columns = [c.strip() for c in fred.columns]
            date_col = [c for c in fred.columns if c.upper() == "DATE"][0]
            val_col = [c for c in fred.columns if c != date_col][0]
            fred["year"] = pd.to_datetime(fred[date_col], errors="coerce").dt.year
            annual = fred.groupby("year")[val_col].mean().reset_index()
            annual.columns = ["year", output_col]
            annual["year"] = annual["year"].astype("Int64")
            panel_before = len(panel.columns)
            merged = panel.merge(annual, on="year", how="left")
            panel[output_col] = merged[output_col]
            added.append(output_col)
        except Exception:
            pass
    return added


def _join_epa(bucket, panel, county_fips):
    """Join EPA AQI by county + year."""
    added = []
    try:
        state_code = county_fips[:2]
        county_code = county_fips[2:]
        dfs = []
        blobs = list(bucket.list_blobs(prefix="epa/"))
        for blob_obj in blobs:
            if not blob_obj.name.endswith(".zip"):
                continue
            try:
                import zipfile
                data = blob_obj.download_as_bytes()
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for fn in zf.namelist():
                        if fn.endswith(".csv"):
                            with zf.open(fn) as f:
                                chunk = pd.read_csv(f)
                                # Filter to our county
                                if "State Code" in chunk.columns and "County Code" in chunk.columns:
                                    mask = (chunk["State Code"].astype(str).str.zfill(2) == state_code) & \
                                           (chunk["County Code"].astype(str).str.zfill(3) == county_code)
                                    dfs.append(chunk[mask])
            except Exception:
                continue
        if dfs:
            epa = pd.concat(dfs, ignore_index=True)
            if "Year" in epa.columns and "Median AQI" in epa.columns:
                epa_annual = epa[["Year", "Median AQI", "Good Days", "Unhealthy Days"]].copy()
                epa_annual.columns = ["year", "median_aqi", "good_days", "unhealthy_days"]
                epa_annual["year"] = epa_annual["year"].astype("Int64")
                for col in ["median_aqi", "good_days", "unhealthy_days"]:
                    merged = panel.merge(epa_annual[["year", col]], on="year", how="left")
                    panel[col] = merged[col]
                    added.append(col)
    except Exception:
        pass
    return added


def _join_climate(bucket, panel, county_fips):
    """Join NOAA climate (annual avg temp) by county + year."""
    added = []
    try:
        blob = bucket.blob(f"climate/tavg_annual_{county_fips}.csv")
        text = blob.download_as_text()
        lines = text.strip().split("\n")
        # NOAA CAG format: skip header lines starting with non-digit
        data_lines = [l for l in lines if l and l[0].isdigit()]
        if data_lines:
            header = "year,tavg_annual\n"
            noaa = pd.read_csv(io.StringIO(header + "\n".join(data_lines)))
            noaa["year"] = noaa["year"].astype("Int64")
            merged = panel.merge(noaa, on="year", how="left")
            panel["tavg_annual"] = merged["tavg_annual"]
            added.append("tavg_annual")
    except Exception:
        pass
    return added


def _join_fema_nri(bucket, panel, county_fips):
    """Join FEMA NRI risk scores by county (static snapshot)."""
    added = []
    try:
        import zipfile
        blob = bucket.blob("fema/NRI_CensusTracts.zip")
        data = blob.download_as_bytes()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if csv_files:
                with zf.open(csv_files[0]) as f:
                    nri = pd.read_csv(f, low_memory=False)
                # NRI has STCOFIPS (state+county FIPS) — aggregate to county
                if "STCOFIPS" in nri.columns:
                    nri["STCOFIPS"] = nri["STCOFIPS"].astype(str).str.zfill(5)
                    county = nri[nri["STCOFIPS"] == county_fips]
                    risk_cols = {"RISK_SCORE": "nri_risk_score", "EAL_SCORE": "nri_eal_score",
                                 "SOVI_SCORE": "nri_sovi_score", "CFLD_RISKR": "nri_flood_risk",
                                 "HWAV_RISKR": "nri_heatwave_risk", "WFIR_RISKR": "nri_wildfire_risk"}
                    for src, dst in risk_cols.items():
                        if src in county.columns and not county.empty:
                            val = county[src].mean()
                            panel[dst] = val
                            added.append(dst)
    except Exception:
        pass
    return added


def _join_building_permits(bucket, panel, county_fips):
    """Join Census building permits by county + year."""
    added = []
    try:
        state_fips = county_fips[:2]
        county_code = county_fips[2:]
        for year in [2022, 2023]:
            blob = bucket.blob(f"census/building_permits_{year}.txt")
            text = blob.download_as_text()
            permits = pd.read_csv(io.StringIO(text), low_memory=False)
            # Census BPS has FIPS columns
            fips_cols = [c for c in permits.columns if "fips" in c.lower() or "code" in c.lower()]
            if fips_cols:
                # Try to match county
                for col in permits.columns:
                    if "1-unit" in col.lower() or "1unit" in col.lower():
                        mask = permits.apply(lambda r: str(r.get("FIPS State", "")).zfill(2) == state_fips and
                                             str(r.get("FIPS County", "")).zfill(3) == county_code, axis=1)
                        row = permits[mask]
                        if not row.empty:
                            panel.loc[panel["year"] == year, "permits_1unit"] = row.iloc[0].get(col, None)
                        break
        if "permits_1unit" in panel.columns:
            added.append("permits_1unit")
    except Exception:
        pass
    return added


def _join_lehd(bucket, panel, state_fips):
    """Join LEHD workplace area characteristics (aggregated to state)."""
    added = []
    STATE_ABBRS = {"48": "tx", "17": "il", "06": "ca", "36": "ny", "04": "az", "53": "wa", "25": "ma"}
    st = STATE_ABBRS.get(state_fips)
    if not st:
        return added
    try:
        blob = bucket.blob(f"lehd/{st}_wac_2021.csv.gz")
        data = blob.download_as_bytes()
        lehd = pd.read_csv(io.BytesIO(data), compression="gzip", low_memory=False)
        # Aggregate to state level — total jobs and key sectors
        if "C000" in lehd.columns:
            total_jobs = lehd["C000"].sum()
            panel["lehd_total_jobs"] = total_jobs
            added.append("lehd_total_jobs")
        if "CNS07" in lehd.columns:
            panel["lehd_retail_jobs"] = lehd["CNS07"].sum()
            added.append("lehd_retail_jobs")
        if "CNS10" in lehd.columns:
            panel["lehd_finance_jobs"] = lehd["CNS10"].sum()
            added.append("lehd_finance_jobs")
    except Exception:
        pass
    return added


def _join_hud_fmr(bucket, panel, county_fips):
    """Join HUD Fair Market Rents by county."""
    added = []
    try:
        import openpyxl
        for fy in [2025, 2024, 2023]:
            try:
                blob = bucket.blob(f"hud/FY{fy}_FMR_county.xlsx")
                data = blob.download_as_bytes()
                fmr = pd.read_excel(io.BytesIO(data), engine="openpyxl")
                # HUD uses fips2010 column
                fips_col = [c for c in fmr.columns if "fips" in c.lower()][0] if any("fips" in c.lower() for c in fmr.columns) else None
                if fips_col:
                    row = fmr[fmr[fips_col].astype(str).str.contains(county_fips)]
                    if not row.empty:
                        for br in [0, 1, 2, 3, 4]:
                            col_match = [c for c in fmr.columns if f"fmr_{br}" in c.lower() or f"br{br}" in c.lower()]
                            if col_match:
                                panel[f"fmr_{br}br"] = row.iloc[0][col_match[0]]
                                added.append(f"fmr_{br}br")
                        break
            except Exception:
                continue
    except ImportError:
        pass  # openpyxl not available
    except Exception:
        pass
    return added


def _join_irs_migration(bucket, panel, county_fips):
    """Join IRS county-to-county migration flows."""
    added = []
    try:
        state_fips = county_fips[:2]
        county_code = county_fips[2:]
        # Inflow
        blob = bucket.blob("irs/migration_inflow_2122.csv")
        text = blob.download_as_text()
        inflow = pd.read_csv(io.StringIO(text), low_memory=False)
        # Filter to destination county
        if "y2_statefips" in inflow.columns and "y2_countyfips" in inflow.columns:
            mask = (inflow["y2_statefips"].astype(str).str.zfill(2) == state_fips) & \
                   (inflow["y2_countyfips"].astype(str).str.zfill(3) == county_code)
            county_in = inflow[mask]
            if not county_in.empty and "n1" in county_in.columns:
                panel["migration_inflow_returns"] = county_in["n1"].sum()
                added.append("migration_inflow_returns")
                if "agi" in county_in.columns:
                    panel["migration_inflow_agi"] = county_in["agi"].sum()
                    added.append("migration_inflow_agi")
    except Exception:
        pass
    return added

def _join_fred(bucket, panel):
    """Join FRED macroeconomic indicators by year."""
    added = []
    try:
        fred_vars = ["CPIAUCSL.csv", "MORTGAGE30US.csv", "UNRATE.csv", "HOUST.csv", "PAYEMS.csv", "DSPIC96.csv", "USSTHPI.csv"]
        for var_file in fred_vars:
            try:
                blob = bucket.blob(f"fred/{var_file}")
                if not blob.exists(): continue
                text = blob.download_as_text()
                var_name = var_file.replace(".csv", "").lower()
                df = pd.read_csv(io.StringIO(text), low_memory=False)
                # Ensure DATE column exists and mapped correctly
                date_col = [c for c in df.columns if "date" in c.lower()][0]
                val_col = [c for c in df.columns if c.lower() != "date"][0]
                
                df["_year"] = pd.to_datetime(df[date_col], errors="coerce").dt.year
                df["_val"] = pd.to_numeric(df[val_col], errors="coerce")
                annual = df.groupby("_year")["_val"].mean().to_dict()
                
                col_name = f"fred_{var_name}"
                panel[col_name] = panel["year"].map(annual)
                added.append(col_name)
            except Exception:
                continue
    except Exception:
        pass
    return added

def _join_climate(bucket, panel, county_fips):
    """Join NOAA climate risk factors by county."""
    added = []
    try:
        blob = bucket.blob("climate/nca_county_risk.csv")
        if blob.exists():
            text = blob.download_as_text()
            clim = pd.read_csv(io.StringIO(text), low_memory=False)
            fips_col = [c for c in clim.columns if "fips" in c.lower()][0] if any("fips" in c.lower() for c in clim.columns) else None
            if fips_col:
                clim[fips_col] = clim[fips_col].astype(str).str.zfill(5)
                row = clim[clim[fips_col] == county_fips]
                if not row.empty:
                    cols_to_add = [c for c in clim.columns if "risk" in c.lower() or "score" in c.lower()]
                    for c in cols_to_add:
                        panel[f"climate_{c.lower()}"] = row.iloc[0][c]
                        added.append(f"climate_{c.lower()}")
    except Exception:
        pass
    return added



def _join_ecb_rate(bucket, panel):
    """Join ECB MRO rate by year (for France/EU parcels)."""
    added = []
    try:
        blob = bucket.blob("ecb/ecb_mro_rate.csv")
        text = blob.download_as_text()
        ecb = pd.read_csv(io.StringIO(text), low_memory=False)
        # ECB CSV has date and value columns
        date_col = [c for c in ecb.columns if "date" in c.lower() or "period" in c.lower()]
        val_col = [c for c in ecb.columns if "obs" in c.lower() or "value" in c.lower() or c == ecb.columns[-1]]
        if date_col and val_col:
            ecb["_year"] = pd.to_datetime(ecb[date_col[0]], errors="coerce").dt.year
            ecb["_val"] = pd.to_numeric(ecb[val_col[0]], errors="coerce")
            annual = ecb.groupby("_year")["_val"].mean().to_dict()
            panel["ecb_mro_rate"] = panel["year"].map(annual)
            added.append("ecb_mro_rate")
    except Exception:
        pass
    return added


def _join_boe_rate(bucket, panel):
    """Join BoE Bank Rate by year (for UK parcels)."""
    added = []
    try:
        blob = bucket.blob("boe/bank_rate.csv")
        text = blob.download_as_text()
        boe = pd.read_csv(io.StringIO(text), low_memory=False)
        date_col = [c for c in boe.columns if "date" in c.lower()]
        val_col = [c for c in boe.columns if "value" in c.lower() or c == boe.columns[-1]]
        if date_col and val_col:
            boe["_year"] = pd.to_datetime(boe[date_col[0]], errors="coerce").dt.year
            boe["_val"] = pd.to_numeric(boe[val_col[0]], errors="coerce")
            annual = boe.groupby("_year")["_val"].mean().to_dict()
            panel["boe_bank_rate"] = panel["year"].map(annual)
            added.append("boe_bank_rate")
    except Exception:
        pass
    return added


def _join_insee_hpi(bucket, panel):
    """Join INSEE HPI national index by year (for France parcels)."""
    added = []
    try:
        blob = bucket.blob("insee/insee_hpi_national.csv")
        text = blob.download_as_text()
        hpi = pd.read_csv(io.StringIO(text), low_memory=False, sep=";")
        # INSEE CSV format: date/period column + value
        date_col = [c for c in hpi.columns if "date" in c.lower() or "period" in c.lower() or "trimestre" in c.lower()]
        val_col = [c for c in hpi.columns if "indice" in c.lower() or "value" in c.lower() or c == hpi.columns[-1]]
        if date_col and val_col:
            hpi["_year"] = pd.to_numeric(hpi[date_col[0]].astype(str).str[:4], errors="coerce")
            hpi["_val"] = pd.to_numeric(hpi[val_col[0]].astype(str).str.replace(",", "."), errors="coerce")
            annual = hpi.groupby("_year")["_val"].mean().to_dict()
            panel["insee_hpi_national"] = panel["year"].map(annual)
            added.append("insee_hpi_national")
    except Exception:
        pass
    return added


def _join_census_acs(bucket, panel, county_fips):
    """Join Census ACS data aggregated to county level (population, income, home value)."""
    added = []
    try:
        import zipfile
        state_fips = county_fips[:2]
        county_code = county_fips[2:]
        # Try to load ACS tables — look for downloaded zip files
        tables = {
            "B01001": "acs_population",
            "B19013": "acs_median_income",
            "B25077": "acs_median_home_value",
        }
        for table, output_col in tables.items():
            try:
                # Find the most recent vintage
                for year in [2022, 2021, 2020, 2019]:
                    blob_name = f"census/{year}_ACS_{table}_{output_col.replace('acs_', '')}.zip"
                    blob = bucket.blob(blob_name)
                    if not blob.exists():
                        continue
                    data = blob.download_as_bytes()
                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                        if csv_files:
                            with zf.open(csv_files[0]) as f:
                                acs = pd.read_csv(f, low_memory=False, dtype=str)
                            # ACS has GEO_ID column containing FIPS
                            geo_col = [c for c in acs.columns if "geo" in c.lower()][0] if any("geo" in c.lower() for c in acs.columns) else None
                            if geo_col:
                                # Filter to our county (county FIPS is in GEO_ID)
                                county_rows = acs[acs[geo_col].astype(str).str.contains(county_fips)]
                                if not county_rows.empty:
                                    # Get the estimate column (usually ends in E)
                                    est_cols = [c for c in acs.columns if c.endswith("E") and c != geo_col]
                                    if est_cols:
                                        val = pd.to_numeric(county_rows[est_cols[0]].iloc[0], errors="coerce")
                                        panel[output_col] = val
                                        added.append(output_col)
                    break  # found a vintage, stop looking
            except Exception:
                continue
    except Exception:
        pass
    return added


def _join_fhfa_hpi(bucket, panel, state_fips):
    """Join FHFA HPI at state level by year."""
    added = []
    try:
        blob = bucket.blob("fhfa/HPI_AT_state.csv")
        text = blob.download_as_text()
        hpi = pd.read_csv(io.StringIO(text), low_memory=False)
        # FHFA state HPI: state, yr, qtr, index_nsa, index_sa
        if "state" in hpi.columns and "yr" in hpi.columns:
            # State FIPS mapping
            STATE_NAMES = {"48": "TX", "17": "IL", "06": "CA", "36": "NY", "04": "AZ",
                           "53": "WA", "25": "MA", "42": "PA", "11": "DC"}
            st_abbr = STATE_NAMES.get(state_fips)
            if st_abbr:
                state_data = hpi[hpi["state"] == st_abbr]
                if not state_data.empty:
                    # Average across quarters for annual
                    idx_col = [c for c in state_data.columns if "index" in c.lower()]
                    if idx_col:
                        annual = state_data.groupby("yr")[idx_col[0]].mean().reset_index()
                        annual.columns = ["year", "fhfa_hpi_state"]
                        annual["year"] = annual["year"].astype("Int64")
                        merged = panel.merge(annual, on="year", how="left")
                        panel["fhfa_hpi_state"] = merged["fhfa_hpi_state"]
                        added.append("fhfa_hpi_state")
    except Exception:
        pass
    return added


def _join_fema_disasters(bucket, panel, county_fips):
    """Join FEMA disaster declaration counts by county + year."""
    added = []
    try:
        blob = bucket.blob("fema/disaster_declarations.csv")
        text = blob.download_as_text()
        fema = pd.read_csv(io.StringIO(text), low_memory=False)
        # FEMA CSV has fipsStateCode and fipsCountyCode
        if "fipsStateCode" in fema.columns and "fipsCountyCode" in fema.columns:
            state_code = county_fips[:2]
            county_code = county_fips[2:]
            fema["_state"] = fema["fipsStateCode"].astype(str).str.zfill(2)
            fema["_county"] = fema["fipsCountyCode"].astype(str).str.zfill(3)
            county_disasters = fema[(fema["_state"] == state_code) & (fema["_county"] == county_code)]
            if not county_disasters.empty and "declarationDate" in county_disasters.columns:
                county_disasters = county_disasters.copy()
                county_disasters["_year"] = pd.to_datetime(
                    county_disasters["declarationDate"], errors="coerce").dt.year
                annual_count = county_disasters.groupby("_year").size().reset_index()
                annual_count.columns = ["year", "fema_disaster_count"]
                annual_count["year"] = annual_count["year"].astype("Int64")
                merged = panel.merge(annual_count, on="year", how="left")
                panel["fema_disaster_count"] = merged["fema_disaster_count"].fillna(0).astype(int)
                added.append("fema_disaster_count")
    except Exception:
        pass
    return added

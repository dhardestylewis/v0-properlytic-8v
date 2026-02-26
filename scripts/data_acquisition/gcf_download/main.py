import functions_framework
import requests
import json
from google.cloud import storage

BUCKET = "properlytic-raw-data"

@functions_framework.http
def download_data(request):
    """Download property data directly to GCS."""
    source = request.args.get("source", "all")
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    results = {}

    dispatch = {
        # Parcel sources
        "cook": _download_cook_county,
        "nyc": _download_nyc,
        "uk": _download_uk_ppd,
        "france": _download_france_dvf,
        "maricopa": _download_maricopa,
        "la": _download_la_county,
        "nystate": _download_ny_state,
        "kingcounty": _download_king_county,
        "sf": _download_sf,
        "massgis": _download_massgis,
        "txgio": _download_txgio,
        # Macro / contextual
        "fred": _download_fred,
        "fhfa": _download_fhfa_hpi,
        "census": _download_census_acs,
        "fema_nri": _download_fema_nri,
        # Additional contextual
        "hud_fmr": _download_hud_fmr,
        "fbi_crime": _download_fbi_crime,
        "permits": _download_building_permits,
        "irs_migration": _download_irs_migration,
        "lehd": _download_lehd,
        "epa_aqi": _download_epa_aqi,
        # Geo-contextual (scoped to our jurisdictions)
        "buildings": _download_ms_buildings,
        "climate": _download_noaa_ghcnd,
        "lulc": _download_nlcd,
    }

    if source == "all":
        for name, fn in dispatch.items():
            try:
                results[name] = fn(bucket)
            except Exception as e:
                results[name] = {"error": str(e)}
    elif source in dispatch:
        try:
            results[source] = dispatch[source](bucket)
        except Exception as e:
            results[source] = {"error": str(e)}
    else:
        return json.dumps({"error": f"Unknown source: {source}", "available": list(dispatch.keys())})

    return json.dumps(results, indent=2)


# ═══════════════════════════════════════════════════════════════
# JURISDICTIONS WE COVER (used to scope contextual downloads)
# ═══════════════════════════════════════════════════════════════
JURISDICTIONS = {
    "houston_tx":    {"state": "Texas",   "state_fips": "48", "county_fips": "48201", "state_abbr": "TX"},
    "cook_county_il": {"state": "Illinois", "state_fips": "17", "county_fips": "17031", "state_abbr": "IL"},
    "sf_ca":         {"state": "California", "state_fips": "06", "county_fips": "06075", "state_abbr": "CA"},
    "nyc_ny":        {"state": "New York",  "state_fips": "36", "county_fips": "36061", "state_abbr": "NY"},
    "maricopa_az":   {"state": "Arizona",  "state_fips": "04", "county_fips": "04013", "state_abbr": "AZ"},
    "king_co_wa":    {"state": "Washington", "state_fips": "53", "county_fips": "53033", "state_abbr": "WA"},
    "la_county_ca":  {"state": "California", "state_fips": "06", "county_fips": "06037", "state_abbr": "CA"},
    "massgis_ma":    {"state": "Massachusetts", "state_fips": "25", "county_fips": "25017", "state_abbr": "MA"},
    "france":        {"state": "France", "state_fips": None, "county_fips": None, "state_abbr": "FR"},
}


def _upload_url_to_gcs(bucket, blob_name, url, timeout=600):
    """Stream a URL directly to GCS blob."""
    headers = {"User-Agent": "Mozilla/5.0 Properlytic-DataBot/1.0"}
    resp = requests.get(url, stream=True, timeout=timeout, headers=headers, allow_redirects=True)
    resp.raise_for_status()
    blob = bucket.blob(blob_name)
    blob.upload_from_string(resp.content, content_type="application/octet-stream")
    size_mb = len(resp.content) / 1e6
    return {"blob": blob_name, "size_mb": round(size_mb, 1)}


def _paginate_socrata(bucket, prefix, dataset_url, page_size=50000, year_field=None, year_range=None):
    """Socrata CSV paginator → GCS chunks. Iterates by year for full history."""
    years = range(year_range[0], year_range[1]+1) if year_range else [None]
    chunk_idx = 0
    total_rows = 0

    for year in years:
        offset = 0
        while True:
            where = f"$where={year_field}={year}&" if (year and year_field) else ""
            url = f"{dataset_url}?{where}$limit={page_size}&$offset={offset}&$order=:id"
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            lines = resp.text.strip().split("\n")
            if len(lines) <= 1:
                break
            chunk_idx += 1
            blob = bucket.blob(f"{prefix}/chunk_{chunk_idx:04d}.csv")
            blob.upload_from_string(resp.text, content_type="text/csv")
            rows = len(lines) - 1
            total_rows += rows
            offset += page_size
            if rows < page_size:
                break

    return {"chunks": chunk_idx, "total_rows": total_rows}


# ═══════════════════════════════════════════════════════════════
# PARCEL-LEVEL SOURCES
# ═══════════════════════════════════════════════════════════════

def _download_cook_county(bucket):
    return _paginate_socrata(bucket, "cook_county_il/assessed_values",
                             "https://datacatalog.cookcountyil.gov/resource/uzyt-m557.csv",
                             year_field="year", year_range=(1999, 2025))

def _download_nyc(bucket):
    """NYC — fixed dataset ID to 8y4t-faws (Property Valuation and Assessment)."""
    results = {}
    # Socrata paginated download for assessment data
    results["assessment"] = _paginate_socrata(
        bucket, "nyc/assessment_roll",
        "https://data.cityofnewyork.us/resource/8y4t-faws.csv")
    # PLUTO (land use) — direct download
    try:
        results["pluto"] = _upload_url_to_gcs(
            bucket, "nyc/pluto.csv",
            "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD",
            timeout=900)
    except Exception as e:
        results["pluto"] = {"error": str(e)}
    return results

def _download_uk_ppd(bucket):
    """UK PPD — use HTTPS direct link (fixed from HTTP S3)."""
    url = "https://prod.publicdata.landregistry.gov.uk/pp-complete.csv"
    return {"complete": _upload_url_to_gcs(bucket, "uk_ppd/pp-complete.csv", url, timeout=900)}

def _download_france_dvf(bucket):
    base = "https://files.data.gouv.fr/geo-dvf/latest/csv"
    results = {}
    for year in [2019, 2020, 2021, 2022, 2023, 2024]:
        try:
            results[str(year)] = _upload_url_to_gcs(bucket, f"france_dvf/{year}_full.csv.gz",
                                                     f"{base}/{year}/full.csv.gz", timeout=600)
        except Exception as e:
            results[str(year)] = {"error": str(e)}
    return results

def _download_maricopa(bucket):
    datasets = {
        "maricopa_az/Residential.csv": "https://mcassessor.maricopa.gov/file/home/MC_RES.csv",
        "maricopa_az/Commercial.csv": "https://mcassessor.maricopa.gov/file/home/MC_COM.csv",
        "maricopa_az/Vacant.csv": "https://mcassessor.maricopa.gov/file/home/MC_VAC.csv",
    }
    results = {}
    for k, v in datasets.items():
        try:
            results[k] = _upload_url_to_gcs(bucket, k, v, timeout=300)
        except Exception as e:
            results[k] = {"error": str(e)}
    return results

def _download_la_county(bucket):
    """LA County — Socrata API (fixed: use API not direct download)."""
    return _paginate_socrata(bucket, "la_county/assessor_parcels",
                             "https://data.lacounty.gov/resource/9trm-uz8i.csv")

def _download_ny_state(bucket):
    """NY State — fixed dataset ID to bnkp-2b2k (property assessment roll)."""
    return _paginate_socrata(bucket, "ny_state/assessment_roll",
                             "https://data.ny.gov/resource/bnkp-2b2k.csv")

def _download_king_county(bucket):
    """King County WA — fixed URLs from assessor data download page."""
    base = "https://aqua.kingcounty.gov/extranet/assessor"
    datasets = {
        "king_county_wa/Real_Property_Sales.csv": f"{base}/Real%20Property%20Sales.csv",
        "king_county_wa/Parcel.csv": f"{base}/Parcel.csv",
        "king_county_wa/Residential_Building.csv": f"{base}/Residential%20Building.csv",
        "king_county_wa/Condo_Unit.csv": f"{base}/Condo%20Unit.csv",
        "king_county_wa/Lookup.csv": f"{base}/Lookup.csv",
    }
    results = {}
    for k, v in datasets.items():
        try:
            results[k] = _upload_url_to_gcs(bucket, k, v, timeout=300)
        except Exception as e:
            results[k] = {"error": str(e)}
    return results

def _download_sf(bucket):
    return _paginate_socrata(bucket, "sf/secured_roll",
                             "https://data.sfgov.org/resource/wv5m-vpq2.csv",
                             year_field="closed_roll_year", year_range=(2007, 2024))

def _download_massgis(bucket):
    url = "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip"
    return {"statewide": _upload_url_to_gcs(bucket, "massgis/MassGIS_L3_Parcels_gdb.zip", url, timeout=900)}

def _download_txgio(bucket):
    """Texas parcels — use Socrata open data endpoint (fixed from TNRIS API)."""
    # Texas Comptroller Property Tax data via data.texas.gov
    return _paginate_socrata(bucket, "txgio/property_tax",
                             "https://data.texas.gov/resource/np4b-9xqm.csv")


# ═══════════════════════════════════════════════════════════════
# MACRO / CONTEXTUAL SOURCES (small tabular)
# ═══════════════════════════════════════════════════════════════

def _download_fred(bucket):
    """FRED economic data — interest rates, CPI. All <1MB each."""
    fred_base = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    series = {
        "fred/MORTGAGE30US.csv": "MORTGAGE30US",     # 30-yr fixed mortgage
        "fred/DGS10.csv": "DGS10",                   # 10-yr Treasury
        "fred/FEDFUNDS.csv": "FEDFUNDS",              # Fed funds rate
        "fred/CPIAUCSL.csv": "CPIAUCSL",              # CPI All Urban
        "fred/UNRATE.csv": "UNRATE",                  # Unemployment rate
        "fred/CSUSHPINSA.csv": "CSUSHPINSA",          # Case-Shiller US National HPI
        "fred/USSTHPI.csv": "USSTHPI",                # FHFA US HPI (all transactions)
    }
    results = {}
    for blob_name, series_id in series.items():
        url = f"{fred_base}?id={series_id}&cosd=1950-01-01&coed=2026-01-01"
        try:
            results[series_id] = _upload_url_to_gcs(bucket, blob_name, url, timeout=30)
        except Exception as e:
            results[series_id] = {"error": str(e)}
    return results


def _download_fhfa_hpi(bucket):
    """FHFA HPI at MSA, State, and ZIP3 level. ~50MB total."""
    datasets = {
        "fhfa/HPI_AT_metro.csv": "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv",
        "fhfa/HPI_AT_state.csv": "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv",
        "fhfa/HPI_AT_3zip.csv": "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_3zip.csv",
    }
    results = {}
    for blob_name, url in datasets.items():
        try:
            results[blob_name] = _upload_url_to_gcs(bucket, blob_name, url, timeout=120)
        except Exception as e:
            results[blob_name] = {"error": str(e)}
    return results


def _download_census_acs(bucket):
    """Census ACS 5-year — key tables at tract level for all states.
    Using pre-built bulk CSV from Census FTP."""
    results = {}
    # ACS 5-year 2022 summary file — tract-level demographics
    url = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/2022_ACS_Detailed_Tables_Group_B01001.zip"
    try:
        results["acs_population"] = _upload_url_to_gcs(
            bucket, "census/2022_ACS_B01001_population.zip", url, timeout=300)
    except Exception as e:
        results["acs_population"] = {"error": str(e)}

    # Median household income
    url2 = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/2022_ACS_Detailed_Tables_Group_B19013.zip"
    try:
        results["acs_income"] = _upload_url_to_gcs(
            bucket, "census/2022_ACS_B19013_income.zip", url2, timeout=300)
    except Exception as e:
        results["acs_income"] = {"error": str(e)}

    # Median home value
    url3 = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/2022_ACS_Detailed_Tables_Group_B25077.zip"
    try:
        results["acs_home_value"] = _upload_url_to_gcs(
            bucket, "census/2022_ACS_B25077_home_value.zip", url3, timeout=300)
    except Exception as e:
        results["acs_home_value"] = {"error": str(e)}

    return results


def _download_fema_nri(bucket):
    """FEMA National Risk Index — census tract level. ~500MB."""
    url = "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip"
    return {"nri_tracts": _upload_url_to_gcs(bucket, "fema/NRI_CensusTracts.zip", url, timeout=600)}


# ═══════════════════════════════════════════════════════════════
# ADDITIONAL CONTEXTUAL SOURCES
# ═══════════════════════════════════════════════════════════════

def _download_hud_fmr(bucket):
    """HUD Fair Market Rents — county and ZIP level. <10MB."""
    datasets = {
        "hud/FY2025_FMR_county.xlsx": "https://www.huduser.gov/portal/datasets/fmr/fmr2025/FY2025_FMRs_revised.xlsx",
        "hud/FY2024_FMR_county.xlsx": "https://www.huduser.gov/portal/datasets/fmr/fmr2024/FY2024_FMRs_revised.xlsx",
        "hud/FY2023_FMR_county.xlsx": "https://www.huduser.gov/portal/datasets/fmr/fmr2023/FY2023_FMRs_revised.xlsx",
    }
    results = {}
    for k, v in datasets.items():
        try:
            results[k] = _upload_url_to_gcs(bucket, k, v, timeout=60)
        except Exception as e:
            results[k] = {"error": str(e)}
    return results


def _download_fbi_crime(bucket):
    """FBI UCR / NIBRS crime data — state and agency level. ~200MB."""
    datasets = {
        "fbi/offenses_known_2022.csv": "https://cde.ucr.cjis.gov/LATEST/webapp/api/bulk-download/offenses-known?format=csv",
    }
    results = {}
    for k, v in datasets.items():
        try:
            results[k] = _upload_url_to_gcs(bucket, k, v, timeout=300)
        except Exception as e:
            results[k] = {"error": str(e)}
    return results


def _download_building_permits(bucket):
    """Census building permits survey — county/MSA level. <50MB."""
    results = {}
    url = "https://www2.census.gov/econ/bps/County/co2023a.txt"
    try:
        results["permits_2023"] = _upload_url_to_gcs(
            bucket, "census/building_permits_2023.txt", url, timeout=60)
    except Exception as e:
        results["permits_2023"] = {"error": str(e)}
    url2 = "https://www2.census.gov/econ/bps/County/co2022a.txt"
    try:
        results["permits_2022"] = _upload_url_to_gcs(
            bucket, "census/building_permits_2022.txt", url2, timeout=60)
    except Exception as e:
        results["permits_2022"] = {"error": str(e)}
    return results


def _download_irs_migration(bucket):
    """IRS SOI county-to-county migration. ~200MB per year."""
    results = {}
    # Latest available: 2021-2022
    base = "https://www.irs.gov/pub/irs-soi"
    files = {
        "irs/migration_inflow_2122.csv": f"{base}/county_migration_2021_2022_inflow.csv",
        "irs/migration_outflow_2122.csv": f"{base}/county_migration_2021_2022_outflow.csv",
    }
    for k, v in files.items():
        try:
            results[k] = _upload_url_to_gcs(bucket, k, v, timeout=120)
        except Exception as e:
            results[k] = {"error": str(e)}
    return results


def _download_lehd(bucket):
    """Census LEHD/LODES — workplace-residence flows. ~2GB total but we grab summary only."""
    results = {}
    # LODES workplace area characteristics (WAC) — by census block
    # Grab a few key states only (matching our parcel data)
    states = ["il", "wa", "az", "ny", "ca", "ma", "tx"]
    for st in states:
        url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{st}/wac/{st}_wac_S000_JT00_2021.csv.gz"
        try:
            results[st] = _upload_url_to_gcs(
                bucket, f"lehd/{st}_wac_2021.csv.gz", url, timeout=120)
        except Exception as e:
            results[st] = {"error": str(e)}
    return results


def _download_epa_aqi(bucket):
    """EPA annual AQI by county. <5MB per year. Full history 2005-2023."""
    results = {}
    for year in range(2005, 2024):
        url = f"https://aqs.epa.gov/aqsweb/airdata/annual_aqi_by_county_{year}.zip"
        try:
            results[str(year)] = _upload_url_to_gcs(
                bucket, f"epa/aqi_county_{year}.zip", url, timeout=60)
        except Exception as e:
            results[str(year)] = {"error": str(e)}
    return results


# ═══════════════════════════════════════════════════════════════
# GEO-CONTEXTUAL (scoped to our jurisdictions)
# ═══════════════════════════════════════════════════════════════

def _download_ms_buildings(bucket):
    """Microsoft Building Footprints — by state GeoJSON. ~200MB per state.
    Only download states matching our jurisdictions."""
    # MS Footprints are organized by state
    states = set(j["state"] for j in JURISDICTIONS.values() if j["state"] != "France")
    results = {}
    for state in states:
        fname = state.replace(" ", "")
        url = f"https://usbuildingdata.blob.core.windows.net/usbuildings-v2/{fname}.geojson.zip"
        try:
            results[state] = _upload_url_to_gcs(
                bucket, f"buildings/ms_footprints_{fname}.geojson.zip", url, timeout=600)
        except Exception as e:
            results[state] = {"error": str(e)}
    return results


def _download_noaa_ghcnd(bucket):
    """NOAA GHCN-Daily — climate normals and recent data.
    Download the inventory + station data for our counties."""
    results = {}
    # Station inventory (all US)
    url_inv = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    try:
        results["inventory"] = _upload_url_to_gcs(
            bucket, "climate/ghcnd_inventory.txt", url_inv, timeout=60)
    except Exception as e:
        results["inventory"] = {"error": str(e)}

    # Climate normals (1991-2020) — summary CSVs
    normals_url = "https://www.ncei.noaa.gov/data/normals-monthly/2006-2020/archive/us-climate-normals_2006-2020_v1.0.0_monthly_multivariate_by-station.csv"
    try:
        results["normals"] = _upload_url_to_gcs(
            bucket, "climate/normals_monthly_2006_2020.csv", normals_url, timeout=120)
    except Exception as e:
        results["normals"] = {"error": str(e)}

    # Recent annual summaries per county (FIPS-based)
    for jname, jinfo in JURISDICTIONS.items():
        fips = jinfo.get("county_fips")
        if not fips:
            continue
        # NOAA Climate-at-a-Glance county time series
        url = f"https://www.ncei.noaa.gov/cag/county/time-series/{fips}/tavg/ann/12/1895-2025.csv"
        try:
            results[f"temp_{jname}"] = _upload_url_to_gcs(
                bucket, f"climate/tavg_annual_{fips}.csv", url, timeout=60)
        except Exception as e:
            results[f"temp_{jname}"] = {"error": str(e)}

    return results


def _download_nlcd(bucket):
    """NLCD Land Use / Land Cover — 2021 CONUS (~1GB).
    Download the national dataset; spatial filtering done in ETL."""
    results = {}
    # NLCD 2021 CONUS land cover — MRLC direct download
    url = "https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2021_land_cover_l48_20230630.zip"
    try:
        results["nlcd_2021"] = _upload_url_to_gcs(
            bucket, "lulc/nlcd_2021_land_cover_conus.zip", url, timeout=900)
    except Exception as e:
        results["nlcd_2021"] = {"error": str(e)}
    return results

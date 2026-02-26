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
        # French contextual
        "ecb_rate": _download_ecb_rate,
        "insee_hpi": _download_insee_hpi,
        # UK
        "uk_ppd": _download_uk_ppd,
        "boe_rate": _download_boe_rate,
        # Mortgage
        "hmda": _download_hmda,
        # Additional US cities
        "philly": _download_philly,
        "dc": _download_dc,
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
    for year in range(2014, 2025):  # DVF available 2014-2024
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
    """Census ACS 5-year — key tables at tract level. 2012-2022."""
    results = {}
    tables = {
        "B01001": "population",
        "B19013": "income",
        "B25077": "home_value",
    }
    for year in range(2012, 2023):  # ACS 5-year available 2009-2022, table-based from 2012
        for table, label in tables.items():
            url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/{year}_ACS_Detailed_Tables_Group_{table}.zip"
            try:
                results[f"{label}_{year}"] = _upload_url_to_gcs(
                    bucket, f"census/{year}_ACS_{table}_{label}.zip", url, timeout=300)
            except Exception as e:
                results[f"{label}_{year}"] = {"error": str(e)}
    return results


def _download_fema_nri(bucket):
    """FEMA National Risk Index — census tract level. ~500MB."""
    url = "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip"
    return {"nri_tracts": _upload_url_to_gcs(bucket, "fema/NRI_CensusTracts.zip", url, timeout=600)}


# ═══════════════════════════════════════════════════════════════
# ADDITIONAL CONTEXTUAL SOURCES
# ═══════════════════════════════════════════════════════════════

def _download_hud_fmr(bucket):
    """HUD Fair Market Rents — county and ZIP level. FY2010-2025."""
    results = {}
    for fy in range(2010, 2026):  # HUD FMR available ~FY2006-FY2025
        url = f"https://www.huduser.gov/portal/datasets/fmr/fmr{fy}/FY{fy}_FMRs_revised.xlsx"
        try:
            results[f"fmr_{fy}"] = _upload_url_to_gcs(
                bucket, f"hud/FY{fy}_FMR_county.xlsx", url, timeout=60)
        except Exception as e:
            results[f"fmr_{fy}"] = {"error": str(e)}
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
    """Census building permits survey — county/MSA level. 2004-2023."""
    results = {}
    for year in range(2004, 2024):  # BPS available ~2004-2023
        url = f"https://www2.census.gov/econ/bps/County/co{year}a.txt"
        try:
            results[f"permits_{year}"] = _upload_url_to_gcs(
                bucket, f"census/building_permits_{year}.txt", url, timeout=60)
        except Exception as e:
            results[f"permits_{year}"] = {"error": str(e)}
    return results


def _download_irs_migration(bucket):
    """IRS SOI county-to-county migration. 2011-2022."""
    results = {}
    base = "https://www.irs.gov/pub/irs-soi"
    for y1 in range(2011, 2022):  # SOI available ~2011-2022
        y2 = y1 + 1
        tag = f"{str(y1)[-2:]}{str(y2)[-2:]}"
        for flow in ["inflow", "outflow"]:
            key = f"irs/migration_{flow}_{tag}.csv"
            url = f"{base}/county_migration_{y1}_{y2}_{flow}.csv"
            try:
                results[key] = _upload_url_to_gcs(bucket, key, url, timeout=120)
            except Exception as e:
                results[key] = {"error": str(e)}
    return results


def _download_lehd(bucket):
    """Census LEHD/LODES — workplace area characteristics. 2015-2021."""
    results = {}
    states = ["il", "wa", "az", "ny", "ca", "ma", "tx"]
    for year in range(2015, 2022):  # LODES8 typically 2015-2021
        for st in states:
            url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{st}/wac/{st}_wac_S000_JT00_{year}.csv.gz"
            try:
                results[f"{st}_{year}"] = _upload_url_to_gcs(
                    bucket, f"lehd/{st}_wac_{year}.csv.gz", url, timeout=120)
            except Exception as e:
                results[f"{st}_{year}"] = {"error": str(e)}
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
    url = "https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2021_land_cover_l48_20230630.zip"
    try:
        results["nlcd_2021"] = _upload_url_to_gcs(
            bucket, "lulc/nlcd_2021_land_cover_conus.zip", url, timeout=900)
    except Exception as e:
        results["nlcd_2021"] = {"error": str(e)}
    return results


def _download_ecb_rate(bucket):
    """ECB key interest rates — Main Refinancing Operations rate.
    From ECB Statistical Data Warehouse CSV export."""
    results = {}
    # MRO rate (Main Refinancing Operations)
    url = "https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=143.FM.B.U2.EUR.4F.KR.MRR_FR.LEV&type=csv"
    try:
        results["ecb_mro"] = _upload_url_to_gcs(
            bucket, "ecb/ecb_mro_rate.csv", url, timeout=60)
    except Exception as e:
        results["ecb_mro"] = {"error": str(e)}
    # Deposit facility rate
    url2 = "https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=143.FM.B.U2.EUR.4F.KR.DFR.LEV&type=csv"
    try:
        results["ecb_deposit"] = _upload_url_to_gcs(
            bucket, "ecb/ecb_deposit_rate.csv", url2, timeout=60)
    except Exception as e:
        results["ecb_deposit"] = {"error": str(e)}
    return results


def _download_insee_hpi(bucket):
    """INSEE Indice des prix des logements anciens — by département.
    Quarterly house price indices from INSEE open data."""
    results = {}
    # INSEE apartment price index (quarterly, département level)
    url = "https://www.insee.fr/fr/statistiques/serie/telecharger/csv/010605958"
    try:
        results["insee_hpi_national"] = _upload_url_to_gcs(
            bucket, "insee/insee_hpi_national.csv", url, timeout=60)
    except Exception as e:
        results["insee_hpi_national"] = {"error": str(e)}
    # Indices by building type (houses vs apartments)
    url2 = "https://www.insee.fr/fr/statistiques/serie/telecharger/csv/010605959"
    try:
        results["insee_hpi_apartments"] = _upload_url_to_gcs(
            bucket, "insee/insee_hpi_apartments.csv", url2, timeout=60)
    except Exception as e:
        results["insee_hpi_apartments"] = {"error": str(e)}
    return results


def _download_uk_ppd(bucket):
    """UK Land Registry Price Paid Data — complete dataset (~4GB CSV).
    All residential property sales in England and Wales since 1995."""
    results = {}
    url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
    try:
        results["ppd_complete"] = _upload_url_to_gcs(
            bucket, "uk_ppd/pp-complete.csv", url, timeout=1200)
    except Exception as e:
        results["ppd_complete"] = {"error": str(e)}
    return results


def _download_boe_rate(bucket):
    """Bank of England Bank Rate — official interest rate since 1975."""
    results = {}
    url = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/1975&Dateto=01/Jan/2026&SeriesCodes=IUDBEDR&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
    try:
        results["bank_rate"] = _upload_url_to_gcs(
            bucket, "boe/bank_rate.csv", url, timeout=60)
    except Exception as e:
        results["bank_rate"] = {"error": str(e)}
    return results


def _download_hmda(bucket):
    """CFPB HMDA mortgage data — tract-level originations. ~500MB/yr.
    Covers denial rates, loan amounts, lender activity by census tract."""
    results = {}
    for year in range(2018, 2024):  # HMDA bulk available 2018-2023
        url = f"https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/{year}/{year}_public_lar_csv.zip"
        try:
            results[str(year)] = _upload_url_to_gcs(
                bucket, f"hmda/hmda_lar_{year}.zip", url, timeout=900)
        except Exception as e:
            results[str(year)] = {"error": str(e)}
    return results


def _download_philly(bucket):
    """Philadelphia OPA — property assessments via Open Data Philly (Socrata)."""
    return _paginate_socrata(bucket, "philly/opa_assessments",
                             "https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+opa_properties_public&format=csv",
                             paginate=False)


def _download_dc(bucket):
    """Washington DC — CAMA property data via opendata.dc.gov (Socrata)."""
    return _paginate_socrata(bucket, "dc/cama_residential",
                             "https://opendata.dc.gov/api/views/a2bh-cepn/rows.csv?accessType=DOWNLOAD",
                             paginate=False)

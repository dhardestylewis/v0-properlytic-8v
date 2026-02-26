# Properlytic — Multi-Jurisdiction Data Acquisition Tracker
**GCS:** `gs://properlytic-raw-data` | **Project:** `properlytic-data`  
**Total:** ~20 GB | **Directories:** 22  
**Updated:** 2026-02-26T13:45Z  
**Download CF:** `gcf_download/main.py` (29 sources) | **Panel CF:** `gcf_build_panel/main.py` (8GB, 11 contextual joins)

---

## Full Pipeline Status (per source)

Legend: ⬜ Not started | 🔄 In progress | ✅ Done | ❌ Failed | ⏸️ Blocked

### Parcel Sources

| # | Source | Download | GCS Size | Years in GCS | Schema Verified | YAML Mapping | Panel Builder | Panel Built |
|---|--------|----------|----------|-------------|-----------------|-------------|---------------|-------------|
| 1 | **HCAD Houston** | ✅ | 2.4 GB | 2005-2025 | ✅ GCS inspect 2/25 | ✅ 16 cols | ✅ | ⬜ (OOM→8GB retry) |
| 2 | **Cook County IL** | 🔄 re-dl 1999-2025 | 1.2 GB+ | ⚠️ partial years | ✅ GCS inspect 2/25 | ✅ 6 cols | ✅ | ⬜ |
| 3 | **SF Assessor** | 🔄 re-dl 2007-2024 | 1.6 GB | ⚠️ partial years | ✅ GCS inspect 2/25 | ✅ 12 cols | ✅ | ⬜ |
| 4 | **France DVF** | ✅ (expanded 2014-2024) | 400 MB+ | 2014-2024 | ✅ data.gouv docs | ✅ 8 cols | ✅ | ⬜ |
| 5 | **NYC DoF** | ✅ (from Drive) | 51 MB | TBD | ⬜ Need parquet header | ✅ 7 cols | ✅ (newly mapped) | ⬜ |
| 6 | **MassGIS L3** | ✅ | 1.4 GB | 2020-2024 | ✅ Mass.gov docs | ✅ 10 cols | ❌ GDB not handled | ⬜ |
| 7 | **UK PPD** | ❌ S3 503 | 0 | — | ✅ Land Registry docs | ✅ 5 cols | ✅ (newly added) | ⬜ (no data) |
| 8 | **NY State ORPTS** | 🔄 | ~200 MB | TBD | ⬜ | ⬜ | ⬜ | ⬜ |
| 9 | Maricopa AZ | ❌ URL changed | 0 | — | ❌ | ❌ | ❌ | ❌ |
| 10 | King County WA | ❌ bot-blocked | 0 | — | ❌ | ❌ | ❌ | ❌ |
| 11 | LA County | ❌ Socrata error | 0 | — | ❌ | ❌ | ❌ | ❌ |

### Financial / Market Context  

| # | Source | Download | Years | Join Level | Join Key | Panel Join | Verified Against |
|---|--------|----------|-------|------------|----------|------------|-----------------|
| 12 | **FRED 30yr mortgage** | ✅ | 1971-2025 | national | year | ✅ | fred.stlouisfed.org |
| 13 | **FRED 10yr treasury** | ✅ | 1962-2025 | national | year | ✅ | fred.stlouisfed.org |
| 14 | **FRED fed funds** | ✅ | 1954-2025 | national | year | ✅ | fred.stlouisfed.org |
| 15 | **FRED CPI** | ✅ | 1947-2025 | national | year | ✅ | fred.stlouisfed.org |
| 16 | **FRED unemployment** | ✅ | 1948-2025 | national | year | ✅ | fred.stlouisfed.org |
| 17 | **FRED Case-Shiller** | ✅ | 1987-2025 | national | year | ✅ | fred.stlouisfed.org |
| 18 | **FRED FHFA HPI** | ✅ | 1975-2025 | national | year | ✅ | fred.stlouisfed.org |
| 19 | **ECB MRO rate** | ✅ | 1999-2025 | national_eu | year | ✅ (France only) | sdw.ecb.europa.eu |
| 20 | **BoE Bank Rate** | ✅ | 1975-2026 | national_uk | year | ✅ (UK only) | bankofengland.co.uk |
| 21 | **INSEE HPI** | ✅ | 1996-2024 | national_fr | year | ✅ (France only) | insee.fr |

### Census / Demographics

| # | Source | Download | Years | Join Level | Join Key | Panel Join | Verified Against |
|---|--------|----------|-------|------------|----------|------------|-----------------|
| 22 | **Census ACS (pop/income/value)** | 🔄 2012-2022 | 11 vintages × 3 tables | tract | tract_fips | ⬜ (needs geocoding) | census.gov |
| 23 | **Building permits** | ✅ | 2004-2023 | county | county_fips+year | ✅ | census.gov/econ/bps |
| 24 | **LEHD** jobs | 🔄 2015-2021 | 7yr × 7 states | state (agg) | state_fips | ✅ | lehd.ces.census.gov |

### Climate / Environment

| # | Source | Download | Years | Join Level | Join Key | Panel Join | Verified Against |
|---|--------|----------|-------|------------|----------|------------|-----------------|
| 25 | **EPA AQI** | 🔄 2005-2023 | 19 years | county | county_fips+year | ✅ | aqs.epa.gov |
| 26 | **NOAA Climate** | ✅ | 1895-2025 | county | county_fips+year | ✅ | ncdc.noaa.gov |
| 27 | **FEMA NRI** | ✅ | 2024 snapshot | county (agg from tract) | county_fips | ✅ | hazards.fema.gov |

### Housing Market

| # | Source | Download | Years | Join Level | Join Key | Panel Join | Verified Against |
|---|--------|----------|-------|------------|----------|------------|-----------------|
| 28 | **HUD FMR** | ✅ FY2010-2025 | 16 years | county | county_fips | ✅ | huduser.gov |
| 29 | **IRS Migration** | ✅ 2011-2022 | 11 year-pairs | county | county_fips | ✅ | irs.gov/pub/irs-soi |
| 30 | **HMDA mortgage** | ⬜ deploying | 2018-2023 | tract | tract_fips | ⬜ | ffiec.cfpb.gov |

### Geospatial

| # | Source | Download | Years | Join Level | Join Key | Panel Join | Verified Against |
|---|--------|----------|-------|------------|----------|------------|-----------------|
| 31 | **MS Buildings** | ✅ | 2023 | parcel (spatial) | spatial overlay | ⬜ (needs geopandas) | usbuildingdata.blob.core.windows.net |
| 32 | **NLCD LULC** | ✅ | 2021 | parcel (raster) | lat/lon lookup | ⬜ (needs rasterio) | mrlc.gov |

---

## Canonical Schema (18 parcel fields + contextual)

**Parcel:** `parcel_id, jurisdiction, year, sale_price, sale_date, assessed_value, land_value, improvement_value, dwelling_type, sqft, land_area, year_built, bedrooms, bathrooms, stories, address, lat, lon`

**US Context (by county FIPS + year):** `mortgage_rate_30yr, treasury_10yr, fed_funds_rate, cpi, unemployment_rate, case_shiller_hpi, fhfa_hpi_national, median_aqi, good_days, unhealthy_days, tavg_annual, nri_risk_score, nri_eal_score, nri_sovi_score, nri_flood_risk, nri_heatwave_risk, nri_wildfire_risk, permits_1unit, lehd_total_jobs, lehd_retail_jobs, lehd_finance_jobs, fmr_0br..fmr_4br, migration_inflow_returns, migration_inflow_agi`

**France Context:** `ecb_mro_rate, insee_hpi_national`

**UK Context:** `boe_bank_rate`

---

## Pipeline Architecture

```
Download CF (29 sources, parallel per-source)
  → gs://properlytic-raw-data/{source}/ (raw)

Panel Builder CF (8GB, GCS→GCS, reads raw + joins context)
  → gs://properlytic-raw-data/panel/jurisdiction={X}/part.parquet

Training reads:
  pd.read_parquet("gs://properlytic-raw-data/panel/")
```

**Jurisdiction → FIPS mapping** for contextual joins:
```
hcad_houston   → 48201 (Harris County TX)
cook_county_il → 17031 (Cook County IL)
sf_ca          → 06075 (San Francisco CA)
nyc            → 36061 (New York County NY)
massgis        → 25017 (Middlesex MA)
france_dvf     → None (uses ECB/INSEE instead)
uk_ppd         → None (uses BoE instead)
```

---

## Active Jobs (as of 13:45Z)

| Job | Status | Started |
|-----|--------|---------|
| Cook County re-download (1999-2025) | 🔄 Running | 12:50Z |
| SF re-download (2007-2024) | 🔄 Running | 12:50Z |
| Census ACS expanded (2012-2022) | 🔄 Running | 13:37Z |
| LEHD expanded (2015-2021, 7 states) | 🔄 Running | 13:37Z |
| France DVF expanded (2014-2024) | ✅ Done | 13:37Z |
| Building permits (2004-2023) | ✅ Done | 13:37Z |
| IRS migration (2011-2022) | ✅ Done | 13:37Z |
| HUD FMR (FY2010-2025) | ✅ Done | 13:37Z |
| Both CFs redeploying (HMDA + intl joins) | 🔄 Deploying | 13:43Z |
| Panel dry-runs (5 sources) | 🔄 Running | 13:42Z |

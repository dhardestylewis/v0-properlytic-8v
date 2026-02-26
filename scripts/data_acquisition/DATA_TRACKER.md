# Properlytic — Multi-Jurisdiction Data Acquisition Tracker
**GCS:** `gs://properlytic-raw-data` | **Project:** `properlytic-data`  
**Total:** ~14 GB | **Directories:** 20  
**Updated:** 2026-02-26T13:27Z

---

## A. Canonical Covariate Ontology

| # | Canonical Field | Description |
|---|----------------|-------------|
| 1 | `parcel_id` | Unique parcel identifier (str) |
| 2 | `jurisdiction` | Source jurisdiction key |
| 3 | `year` | Assessment / transaction year |
| 4 | `sale_price` | Most recent arm's-length sale price |
| 5 | `sale_date` | Sale date (YYYY-MM-DD) |
| 6 | `assessed_value` | Total assessed / appraised value |
| 7 | `land_value` | Land-only assessed value |
| 8 | `improvement_value` | Improvement (building) assessed value |
| 9 | `dwelling_type` | Normalized: SF, CONDO, MULTI, COMMERCIAL, VACANT |
| 10 | `sqft` | Living area (sq ft) |
| 11 | `land_area` | Lot / land area |
| 12 | `year_built` | Year structure built |
| 13 | `bedrooms` | Bedroom count |
| 14 | `bathrooms` | Bathroom count (half=0.5) |
| 15 | `stories` | Story count |
| 16 | `address` | Street address |
| 17 | `lat` | Latitude |
| 18 | `lon` | Longitude |

---

## B. Parcel-Level Sources

| # | Source | Country | GCS Path | Years Available | Years Confirmed in GCS | Size | Schema | Status |
|---|--------|---------|----------|-----------------|----------------------|------|--------|--------|
| 1 | **HCAD (Houston)** | US | `hcad/` | 2005-2025 | ✅ 2005-2025 (full panel) | 2.4 GB | ✅ 16 cols mapped | ✅ **Partition created** |
| 2 | **Cook County IL** | US | `cook_county_il/` ~100 chunks | 1999-2025 | ⚠️ 2002, 2016, 2020 +(re-dl running) | 1.2 GB | ✅ 19 cols verified | 🔄 **Re-downloading full 1999-2025 year-by-year** |
| 3 | **SF Assessor** | US | `sf/` ~57 chunks | 2007-2024 | ⚠️ 2016, 2019-2021 +(re-dl running) | 1.3 GB | ✅ 45 cols verified | 🔄 **Re-downloading full 2007-2024 year-by-year** |
| 4 | **France DVF** | FR | `france_dvf/` 6 gzips | 2019-2024 | ✅ 2019-2024 (yearly files) | 400 MB | ✅ ~35 cols | ✅ **Full history** |
| 5 | **NY State ORPTS** | US | `ny_state/` | 2015-2025 | 🔲 Not verified | ~200 MB | 🔲 | 🔄 Retrying (fixed dataset ID bnkp-2b2k) |
| 6 | **MassGIS L3** | US | `massgis/` 1 ZIP | 2020-2024 | 🔲 GDB format, not inspected | 1.4 GB | ✅ ~35 cols (docs) | ✅ **Landed** |
| 7 | **NYC DoF** | US | `nyc/` | 2003-2024 | 🔲 From thesis EDA, years TBD | 51 MB | 🔲 To verify | ✅ **Pushed from Drive** + 🔄 Cloud retry |
| 8 | Maricopa AZ | US | `maricopa_az/` | ~2000-2025 | ❌ Got HTML not CSV | — | ❌ | ❌ URL scheme changed |
| 9 | **UK PPD** | UK | `uk_ppd/` | 1995-2025 | 🔄 Re-downloading (S3 URL) | ~4 GB | ~16 cols | 🔄 **Downloading via Land Registry S3** |
| 10 | King County WA | US | — | 1989-2025 | — | — | — | ❌ Server blocks bots |
| 11 | LA County | US | — | 2006-2025 | — | — | — | ❌ Socrata error |
| 12 | TXGIO | US | — | 2019-2025 | — | — | — | ❌ API empty |
| 13 | Florida DOR | US | — | 2002-2025 | — | — | — | ⏸️ Email required |
| 14 | NJ MOD-IV | US | — | 1989-2025 | — | — | — | ⏸️ Registration |
| 15 | NSW Australia | AU | — | 1990-2025 | — | — | — | ⏸️ Email required |
| 16 | BC Canada | CA | — | 2016-2025 | — | — | — | ⏸️ Email required |
| 17 | Denmark BBR | DK | — | 1992-2025 | — | — | — | ⏸️ API key required |

**Parcel: 6 landed ✅ + 3 downloading 🔄 (Cook, SF year-fix + UK PPD). ~7 GB + growing.**

---

## C. Financial / Market Context

| # | Source | GCS Path | Years | Frequency | Grain | Size | Status |
|---|--------|----------|-------|-----------|-------|------|--------|
| 18 | **FRED 30yr mortgage** | `fred/MORTGAGE30US.csv` | ✅ **1971-2025** | Weekly | National | <1 MB | ✅ **Landed, full history** |
| 19 | **FRED 10yr treasury** | `fred/DGS10.csv` | ✅ **1962-2025** | Daily | National | <1 MB | ✅ **Landed, full history** |
| 20 | **FRED fed funds** | `fred/FEDFUNDS.csv` | ✅ **1954-2025** | Monthly | National | <1 MB | ✅ **Landed, full history** |
| 21 | **FRED CPI** | `fred/CPIAUCSL.csv` | ✅ **1947-2025** | Monthly | National | <1 MB | ✅ **Landed, full history** |
| 22 | **FRED unemployment** | `fred/UNRATE.csv` | ✅ **1948-2025** | Monthly | National | <1 MB | ✅ **Landed, full history** |
| 23 | **FRED Case-Shiller** | `fred/CSUSHPINSA.csv` | ✅ **1987-2025** | Monthly | National | <1 MB | ✅ **Landed, full history** |
| 24 | **FRED FHFA HPI** | `fred/USSTHPI.csv` | ✅ **1975-2025** | Quarterly | National | <1 MB | ✅ **Landed, full history** |
| 25 | FHFA HPI (metro/state/ZIP3) | — | 1975-2025 | Quarterly | MSA/State/ZIP3 | ~50 MB | 🔄 Retrying (fixed URL) |
| 26 | **BoE Bank Rate** (UK) | `boe/bank_rate.csv` | 1975-2026 | Monthly | National | <1 MB | 🔄 **Downloading** |
| 27 | **ECB Key Rate** (EU) | `ecb/ecb_mro_rate.csv` | 1999-2025 | Monthly | National | <1 MB | 🔄 **Downloading** |
| 28 | **INSEE HPI** (FR) | `insee/insee_hpi_*.csv` | 1996-2024 | Quarterly | Département | <1 MB | 🔄 **Downloading** |

---

## D. Census / Demographics

| # | Source | GCS Path | Years | Grain | Size | Status |
|---|--------|----------|-------|-------|------|--------|
| 29 | **Census building permits** | `census/building_permits_20{22,23}.txt` | 2022-2023 only | County/MSA | <5 MB | ⚠️ **Landed but only 2 years — need 2005-2021** |
| 30 | Census ACS 5-year (pop) | `census/` (not verified) | 2022 only | Tract | ~50 MB | 🔄 Triggered |
| 31 | Census ACS 5-year (income) | `census/` (not verified) | 2022 only | Tract | ~50 MB | 🔄 Triggered |
| 32 | Census ACS 5-year (home value) | `census/` (not verified) | 2022 only | Tract | ~50 MB | 🔄 Triggered |
| 33 | UK Census 2021 | — | 2021 | OA | — | 🔲 Not coded |
| 34 | France INSEE census | — | 2020 | Commune | — | 🔲 Not coded |

---

## E. LULC / Land Cover

| # | Source | GCS Path | Years | Resolution | Coverage | Status |
|---|--------|----------|-------|------------|----------|--------|
| 35 | **NLCD** (USGS) | `lulc/` | 2021 | 30m | US | ✅ **Landed** |
| 36 | Copernicus GLC | — | 2015-2019 | 100m | Global | 🔲 Not coded |

---

## F. Flood / Hazard / Risk

| # | Source | GCS Path | Years | Grain | Size | Status |
|---|--------|----------|-------|-------|------|--------|
| 37 | **FEMA NRI** (risk scores) | `fema/NRI_CensusTracts.zip` | 2024 v1.20 | Tract | ~500 MB | ✅ **Landed** |
| 38 | OpenFEMA claims | — | 1978-2025 | Policy | ~2 GB | 🔲 Not coded |
| 39 | USFS wildfire | — | 1992-2020 | Raster | ~500 MB | 🔲 Not coded |
| 40 | USGS earthquake | — | Realtime | Point | API | 🔲 Not coded |

---

## G. Climate / Weather

| # | Source | GCS Path | Years | Grain | Coverage | Status |
|---|--------|----------|-------|-------|----------|--------|
| 41 | **NOAA GHCN-D** | `climate/` | 1850-2025 | County/Annual avg | Our counties | ✅ **Landed** |
| 42 | ERA5-Land | — | 1950-now | 9km grid | Global | 🔲 Not coded (CDS API) |
| 43 | **EPA AQI** | `epa/aqi_county_20{05-23}.zip` | 2005-2023 | County/Annual | US | 🔄 **Expanding from 4yr to 19yr** |

---

## H. Building Footprints / Proximity

| # | Source | GCS Path | Years | Grain | Coverage | Status |
|---|--------|----------|-------|-------|----------|--------|
| 44 | **MS Building Footprints** | `buildings/` | 2023 | Polygon | Our 7 states | ✅ **Landed** |
| 45 | Google Open Buildings | — | 2022 | Polygon | Global 1.8B | 🔲 Not coded |
| 46 | **OSM / Geofabrik PBF** | — | Current | Polygon/POI | Global | 🔲 Not coded → OSMnx features |
| 47 | Geofabrik regional PBFs | — | Current | All | Global | 🔲 Not coded |

---

## I. Other Context

| # | Source | GCS Path | Years | Status |
|---|--------|----------|-------|--------|
| 48 | **LEHD** workplace area | `lehd/` (AZ, WA landed) | 2021 | ⚠️ **Only 2 of 7 states — need IL, NY, CA, MA, TX** |
| 49 | HUD Fair Market Rents | — | 2023-2025 | 🔄 Retrying |
| 50 | FBI UCR Crime | — | 2022 | ❌ URL error |
| 51 | IRS SOI Migration | — | 2021-2022 | 🔄 Retrying |

---

## J. Verified Schemas (verbatim from GCS)

### Cook County IL — 19 columns
```
pin, year, class, township_code, township_name, nbhd,
mailed_bldg, mailed_land, mailed_tot, mailed_hie,
certified_bldg, certified_land, certified_tot, certified_hie,
board_bldg, board_land, board_tot, board_hie, row_id
```

### SF Assessor — 45 columns
```
closed_roll_year, property_location, parcel_number, block, lot,
volume_number, use_code, use_definition, property_class_code,
property_class_code_definition, year_property_built,
number_of_bathrooms, number_of_bedrooms, number_of_rooms,
number_of_stories, number_of_units, zoning_code, construction_type,
lot_depth, lot_frontage, property_area, basement_area, lot_area,
lot_code, tax_rate_area_code, percent_of_ownership, exemption_code,
exemption_code_definition, status_code, misc_exemption_value,
homeowner_exemption_value, current_sales_date,
assessed_fixtures_value, assessed_improvement_value,
assessed_land_value, assessed_personal_property_value,
assessor_neighborhood_district, assessor_neighborhood_code,
assessor_neighborhood, supervisor_district, supervisor_district_2012,
analysis_neighborhood, the_geom, row_id, data_as_of, data_loaded_at
```

### FRED — 7 time series (all full history)
```
MORTGAGE30US.csv (1971-2025), DGS10.csv, FEDFUNDS.csv,
CPIAUCSL.csv, UNRATE.csv, CSUSHPINSA.csv, USSTHPI.csv
```

### HCAD — 16 columns (2005-2025 panel)
```
acct, yr, tot_appr_val, land_val, impr_val, state_class,
living_area, land_ar, yr_blt, bed_cnt, full_bath, half_bath,
nbr_story, site_addr_1, lat, lon
```

---

## K. Pipeline Architecture

```
Download (Cloud Functions, parallel)
  → gs://properlytic-raw-data/{source}/ (raw)

ETL Workers (local or Colab, parallel per source)
  → gs://properlytic-raw-data/panel/jurisdiction={X}/part.parquet

Training reads:
  pd.read_parquet("gs://properlytic-raw-data/panel/")
```

**Config:** `schema_registry.yaml` (machine-readable column mappings)  
**Panel Builder:** `gcf_build_panel/main.py` (Cloud Function, GCS→GCS, 5 parcel + 8 contextual joins)  
**Download:** `gcf_download/main.py` (Cloud Function, 28 sources, JURISDICTIONS scoped)

---

## L. Active Jobs

| Job | Status | ETA |
|-----|--------|-----|
| HCAD panel → partition | ✅ Done | — |
| NYC push from Drive | ✅ Done | — |
| MS Buildings download | ✅ Done | — |
| NOAA Climate download | ✅ Done | — |
| NLCD LULC download | ✅ Done | — |
| Cook County re-download (1999-2025) | 🔄 Running | ~30 min |
| SF re-download (2007-2024) | 🔄 Running | ~30 min |
| EPA AQI (2005-2023 expanded) | 🔄 Running | ~10 min |
| UK PPD download (S3) | 🔄 Deploying | After deploy |
| BoE Bank Rate | 🔄 Deploying | After deploy |
| ECB Rate + INSEE HPI | 🔄 Deploying | After deploy |
| Panel builder: dry-run verify | 🔄 Pending deploy | After deploy |

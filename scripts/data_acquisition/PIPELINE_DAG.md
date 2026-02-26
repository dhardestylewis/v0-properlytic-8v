# ETL Pipeline DAG — Status Board
**Updated:** 2026-02-26T13:53Z

## Pipeline DAG

```mermaid
graph LR
    subgraph Download["⬇️ Download (Cloud Function, 29 sources)"]
        direction TB
        P_HCAD["🟢 HCAD Houston<br/>2.4GB, 2005-2025"]
        P_COOK["🟡 Cook County IL<br/>1.2GB, re-dl 1999-2025"]
        P_SF["🟡 SF Assessor<br/>1.6GB, re-dl 2007-2024"]
        P_DVF["🟢 France DVF<br/>400MB, 2014-2024"]
        P_NYC["🟢 NYC DoF<br/>51MB, from Drive"]
        P_MASS["🟢 MassGIS<br/>1.4GB, GDB"]
        P_UK["🔴 UK PPD<br/>S3 503"]
        P_PHILLY["⚪ Philly OPA<br/>not coded"]
        P_DC["⚪ DC CAMA<br/>not coded"]
        C_FRED["🟢 FRED 7 series<br/>full history"]
        C_EPA["🟡 EPA AQI<br/>2005-2023 expanding"]
        C_NOAA["🟢 NOAA Climate"]
        C_FEMA["🟢 FEMA NRI"]
        C_PERMITS["🟢 Census permits<br/>2004-2023"]
        C_LEHD["🟡 LEHD<br/>2015-2021 expanding"]
        C_HUD["🟢 HUD FMR<br/>FY2010-2025"]
        C_IRS["🟢 IRS Migration<br/>2011-2022"]
        C_ACS["🟡 Census ACS<br/>2012-2022 expanding"]
        C_ECB["🟢 ECB Rate"]
        C_BOE["🟢 BoE Rate"]
        C_INSEE["🟢 INSEE HPI"]
        C_HMDA["⚪ HMDA<br/>deploying"]
        G_BLDG["🟢 MS Buildings"]
        G_NLCD["🟢 NLCD LULC"]
    end

    subgraph Schema["📋 Schema Mapping (YAML)"]
        direction TB
        M_HCAD["🟢 HCAD: 16 cols"]
        M_COOK["🟢 Cook: 6 cols"]
        M_SF["🟢 SF: 12 cols"]
        M_DVF["🟢 DVF: 8 cols"]
        M_NYC["🟢 NYC: 7 cols (fixed from dry-run)"]
        M_UK["🟢 UK PPD: 5 cols"]
        M_MASS["🟢 MassGIS: 10 cols"]
    end

    subgraph Panel["🔧 Panel Builder (Cloud Function, 8GB)"]
        direction TB
        B_MAP["Map to canonical 19 cols"]
        B_VALUE["property_value = coalesce(sale, assessed)"]
        B_FIPS["Jurisdiction → FIPS lookup"]
        B_CONTEXT["Join 11 contextual sources"]
        B_WRITE["Write Hive parquet"]
    end

    subgraph Output["📦 Master Panel"]
        O_PANEL["gs://properlytic-raw-data/panel/<br/>jurisdiction=X/part.parquet"]
    end

    subgraph Train["🧠 Training"]
        T_READ["pd.read_parquet('panel/')"]
    end

    P_HCAD --> M_HCAD --> B_MAP
    P_COOK --> M_COOK --> B_MAP
    P_SF --> M_SF --> B_MAP
    P_DVF --> M_DVF --> B_MAP
    P_NYC --> M_NYC --> B_MAP
    P_UK --> M_UK --> B_MAP
    P_MASS --> M_MASS --> B_MAP

    B_MAP --> B_VALUE --> B_FIPS --> B_CONTEXT --> B_WRITE --> O_PANEL --> T_READ

    C_FRED --> B_CONTEXT
    C_EPA --> B_CONTEXT
    C_NOAA --> B_CONTEXT
    C_FEMA --> B_CONTEXT
    C_PERMITS --> B_CONTEXT
    C_LEHD --> B_CONTEXT
    C_HUD --> B_CONTEXT
    C_IRS --> B_CONTEXT
    C_ECB --> B_CONTEXT
    C_BOE --> B_CONTEXT
    C_INSEE --> B_CONTEXT
```

## Per-Source Pipeline Status

🟢 Done | 🟡 In progress | 🔴 Failed | ⚪ Not started

### Parcel Sources

| Source | ⬇️ Download | 📋 Schema | 🔧 Builder | 🧪 Dry-run | 📦 Panel |
|--------|------------|-----------|------------|----------|---------|
| HCAD Houston | 🟢 2.4GB | 🟢 16 cols | 🟢 | 🔴 OOM→8GB retry | ⚪ |
| Cook County IL | 🟡 re-dl | 🟢 19 cols | 🟢 | 🔴 OOM→8GB retry | ⚪ |
| SF Assessor | 🟡 re-dl | 🟢 45 cols | 🟢 | 🟢 **verified** | ⚪ |
| France DVF | 🟢 2014-24 | 🟢 35 cols | 🟢 | 🔴 OOM→8GB retry | ⚪ |
| NYC DoF | 🟢 51MB | 🟢 7 cols (BBL) | 🟢 (fixed) | 🟢 **verified** | ⚪ |
| MassGIS L3 | 🟢 1.4GB | 🟢 docs | 🔴 GDB | ⚪ | ⚪ |
| UK PPD | 🔴 S3 503 | 🟢 docs | 🟢 | ⚪ | ⚪ |
| Philly OPA | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| Washington DC | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |

### Contextual Sources

| Source | ⬇️ Download | Join Level | Join Key | 🔧 In Builder | Years |
|--------|------------|-----------|----------|--------------|-------|
| FRED (7 series) | 🟢 | national | year | 🟢 | 1947-2025 |
| ECB MRO rate | 🟢 | national_eu | year | 🟢 (France) | 1999-2025 |
| BoE Bank Rate | 🟢 | national_uk | year | 🟢 (UK) | 1975-2026 |
| INSEE HPI | 🟢 | national_fr | year | 🟢 (France) | 1996-2024 |
| EPA AQI | 🟡 2005-23 | county | FIPS+year | 🟢 | 2005-2023 |
| NOAA Climate | 🟢 | county | FIPS+year | 🟢 | 1895-2025 |
| FEMA NRI | 🟢 | county | FIPS | 🟢 | static |
| Census permits | 🟢 2004-23 | county | FIPS+year | 🟢 | 2004-2023 |
| LEHD jobs | 🟡 2015-21 | state | state_fips | 🟢 | 2015-2021 |
| HUD FMR | 🟢 FY10-25 | county | FIPS | 🟢 | 2010-2025 |
| IRS Migration | 🟢 2011-22 | county | FIPS | 🟢 | 2011-2022 |
| Census ACS | 🟡 2012-22 | **tract** | **needs geocode** | ⚪ | 2012-2022 |
| HMDA mortgage | ⚪ deploying | **tract** | **needs geocode** | ⚪ | 2018-2023 |
| MS Buildings | 🟢 | **parcel** | **spatial** | ⚪ | 2023 |
| NLCD LULC | 🟢 | **parcel** | **raster** | ⚪ | 2021 |

### Target Variable

```
property_value = COALESCE(sale_price, assessed_value)
```

| Source | sale_price | assessed_value | property_value |
|--------|-----------|---------------|----------------|
| HCAD | ❌ | ✅ tot_appr_val | ✅ assessed |
| Cook County | ❌ | ✅ certified_tot | ✅ assessed |
| SF | ❌ | ✅ land+improvement | ✅ assessed |
| France DVF | ✅ valeur_fonciere | ❌ | ✅ sale |
| NYC | ✅ SALE_PRICE | ❌ | ✅ sale |
| UK PPD | ✅ price | ❌ | ✅ sale |

## Active Jobs

| Job | Status | Started |
|-----|--------|---------|
| Cook County re-download (1999-2025) | 🟡 | 12:50Z |
| SF re-download (2007-2024) | 🟡 | 12:50Z |
| Census ACS (2012-2022, 33 files) | 🟡 | 13:37Z |
| LEHD (2015-2021, 49 files) | 🟡 | 13:37Z |
| Both CFs redeploying | 🟡 | pending |
| Panel build (all sources) | ⚪ | after deploy |

## Blockers

| Issue | Impact | Fix |
|-------|--------|-----|
| HCAD/Cook/DVF OOM at 4GB | Can't build 3 of 6 panels | ✅ Upgraded to 8GB, redeploy pending |
| UK PPD download 503 | No UK data | Need alt URL (gov.uk HTTPS) |
| Census ACS needs tract geocoding | County-level fallback possible | Can aggregate to county for now |
| MassGIS is GDB format | Skipped in panel builder | Needs geopandas/fiona |
| MS Buildings needs spatial join | Not in panel | Needs geopandas |

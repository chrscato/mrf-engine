# MRF ETL Data Dictionary

## Overview

This document describes the data schema and structure for the MRF (Machine Readable Files) ETL pipeline output. The data is organized in a star schema with fact and dimension tables, optimized for healthcare rate analysis and provider network queries.

## Data Architecture

The ETL pipeline processes healthcare rate and provider data into a structured data warehouse with the following layers:

- **Gold Layer**: Fact tables containing the core business metrics
- **Dimensions**: Lookup tables for entities like providers, codes, and payers
- **Cross-References**: Bridge tables linking provider groups to individual providers

## File Structure

```
prod_etl/core/data/
├── gold/
│   └── fact_rate.parquet          # Main fact table (2.97M rows)
├── dims/
│   ├── dim_code.parquet           # Medical procedure codes (3,696 rows)
│   ├── dim_code_cat.parquet       # Code categorization (8,992 rows)
│   ├── dim_npi.parquet            # Provider information (1,214 rows)
│   ├── dim_npi_address.parquet    # Provider addresses (2,428 rows)
│   ├── dim_payer.parquet          # Insurance payer info (1 row)
│   ├── dim_pos_set.parquet        # Place of service sets (4 rows)
│   └── dim_provider_group.parquet # Provider groups (637 rows)
└── xrefs/
    ├── xref_pg_member_npi.parquet # Provider group → NPI mapping (16,999 rows)
    └── xref_pg_member_tin.parquet # Provider group → TIN mapping (9,486 rows)
```

## Core Tables

### Fact Table

#### `fact_rate.parquet` (2,967,105 rows)
**Purpose**: Central fact table containing negotiated healthcare rates

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `fact_uid` | String | Unique identifier for each rate record | `2c6a6a035d7ab8b5558f9b422ace9a32` |
| `state` | String | US state code | `GA` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-08` |
| `payer_slug` | String | Normalized payer identifier | `unitedhealthcare-of-georgia-inc` |
| `billing_class` | String | Type of billing (professional/institutional) | `professional` |
| `code_type` | String | Code system (CPT, HCPCS, etc.) | `CPT` |
| `code` | String | Medical procedure code | `33216` |
| `pg_uid` | String | Provider group unique identifier | `049049fa50d881db5db61293fa01cb5e` |
| `pos_set_id` | String | Place of service set identifier | `d41d8cd98f00b204e9800998ecf8427e` |
| `negotiated_type` | String | Rate type (negotiated, fee schedule, etc.) | `negotiated` |
| `negotiation_arrangement` | String | Contract arrangement type | `ffs` |
| `negotiated_rate` | Float64 | **The negotiated rate amount** | `752.23` |
| `expiration_date` | String | Rate expiration date | `9999-12-31` |
| `provider_group_id_raw` | String | Original provider group ID | `222` |
| `reporting_entity_name` | String | Full payer name | `UnitedHealthcare of Georgia Inc.` |

### Dimension Tables

#### `dim_payer.parquet` (1 row)
**Purpose**: Insurance payer information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `payer_slug` | String | Normalized payer identifier | `unitedhealthcare-of-georgia-inc` |
| `reporting_entity_name` | String | Full payer name | `UnitedHealthcare of Georgia Inc.` |
| `version` | String | Data version | `1.0.0` |

#### `dim_code.parquet` (3,696 rows)
**Purpose**: Medical procedure codes and descriptions

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `code_type` | String | Code system type | `CPT` |
| `code` | String | Procedure code | `31614` |
| `code_description` | String | Full procedure description | `Tracheostoma revision; complex, with flap rotation` |
| `code_name` | String | Shortened procedure name | `TRACHEOSTOMA REVJ CPLX W/FLAP ROTATION` |

#### `dim_code_cat.parquet` (8,992 rows)
**Purpose**: Procedure code categorization and grouping

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `proc_cd` | String | Procedure code | `99201` |
| `proc_set` | String | High-level procedure category | `Evaluation and Management` |
| `proc_class` | String | Procedure class | `Office/ outpatient services` |
| `proc_group` | String | Specific procedure group | `New office visits` |

#### `dim_provider_group.parquet` (637 rows)
**Purpose**: Provider group information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `b8b29688e92394ea7cc3d736446337d0` |
| `payer_slug` | String | Associated payer | `unitedhealthcare-of-georgia-inc` |
| `provider_group_id_raw` | Int64 | Original provider group ID | `772` |
| `version` | String | Data version | `1.0.0` |

#### `dim_pos_set.parquet` (4 rows)
**Purpose**: Place of service groupings

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pos_set_id` | String | Place of service set identifier | `17b00c58b3dcdb9c20cb2a70b52a4cc1` |
| `pos_members` | List[String] | List of place of service codes | `["02", "05", "06", "07", "08", "19", "21", "22", "23", "24", "26", "31", "34", "41", "42", "51", "52", "53", "56", "61"]` |

#### `dim_npi.parquet` (1,214 rows)
**Purpose**: Individual provider (NPI) information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `npi` | String | National Provider Identifier | `1003070913` |
| `first_name` | String | Provider first name (23.48% null) | `STEPHANIE` |
| `last_name` | String | Provider last name (23.48% null) | `LEONI` |
| `organization_name` | String | Organization name (76.52% null) | `HEADWAY COLORADO BEHAVIORAL HEALTH SERVICES, INC.` |
| `enumeration_type` | String | NPI type (NPI-1 individual, NPI-2 organization) | `NPI-1` |
| `status` | String | Provider status | `A` |
| `primary_taxonomy_code` | String | Primary specialty code | `101YP2500X` |
| `primary_taxonomy_desc` | String | Primary specialty description | `Counselor, Professional` |
| `primary_taxonomy_state` | String | License state (19.36% null) | `GA` |
| `primary_taxonomy_license` | String | License number (20.35% null) | `LPC005043` |
| `credential` | String | Professional credential (31.47% null) | `LPC` |
| `sole_proprietor` | String | Sole proprietor status (23.48% null) | `YES` |
| `enumeration_date` | String | NPI enumeration date | `2008-07-10` |
| `last_updated` | String | Last update date | `2021-09-03` |
| `nppes_fetched` | Boolean | Whether NPPES data was fetched | `True` |
| `nppes_fetch_date` | String | NPPES fetch date | `2021-09-03` |
| `replacement_npi` | String | Replacement NPI (100% null) | `null` |

#### `dim_npi_address.parquet` (2,428 rows)
**Purpose**: Provider address information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `npi` | String | National Provider Identifier | `1235233776` |
| `address_purpose` | String | Address type (MAILING/LOCATION) | `MAILING` |
| `address_type` | String | Address format | `DOM` |
| `address_1` | String | Primary address line | `582 MOUNT GERIZIM RD SE` |
| `address_2` | String | Secondary address line (78.42% null) | `BLDG 400, STE 102` |
| `city` | String | City | `MABLETON` |
| `state` | String | State code | `GA` |
| `postal_code` | String | ZIP code | `301266410` |
| `country_code` | String | Country code | `US` |
| `telephone_number` | String | Phone number (5.19% null) | `4047301650` |
| `fax_number` | String | Fax number (45.35% null) | `7062868442` |
| `last_updated` | String | Last update date | `2007-07-08` |
| `address_hash` | String | Unique address identifier | `cd0237207cae95b80fa11879df9fb182` |

### Cross-Reference Tables

#### `xref_pg_member_npi.parquet` (16,999 rows)
**Purpose**: Maps provider groups to individual NPIs

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `33c5ebf41b7fe9461b8ccf3202cb6604` |
| `npi` | String | National Provider Identifier | `1780875781` |

#### `xref_pg_member_tin.parquet` (9,486 rows)
**Purpose**: Maps provider groups to Tax Identification Numbers

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `11ce3cbdcf491bc5ea76386e84a55b4d` |
| `tin_type` | String | TIN type | `ein` |
| `tin_value` | String | Tax ID number | `881009565` |

## Data Relationships

### Primary Relationships
- **Fact → Dimensions**: All foreign keys validated with 100% integrity
  - `fact_rate.payer_slug` → `dim_payer.payer_slug`
  - `fact_rate.code_type,code` → `dim_code.code_type,code`
  - `fact_rate.pg_uid` → `dim_provider_group.pg_uid`
  - `fact_rate.pos_set_id` → `dim_pos_set.pos_set_id`

### Cross-Reference Relationships
- **Provider Groups → NPIs**: 637 provider groups mapped to 15,885 unique NPIs
- **Provider Groups → TINs**: 637 provider groups mapped to 8,188 unique TINs

## Key Identifiers

### `pg_uid` (Provider Group UID)
- **Format**: MD5 hash of `payer_slug|version|provider_group_id|""`
- **Purpose**: Unique identifier for provider groups across all tables
- **Usage**: Links fact table to provider group dimensions and cross-references

### `fact_uid` (Fact UID)
- **Format**: MD5 hash of all fact dimensions plus rate amount
- **Purpose**: Unique identifier for each rate record
- **Usage**: Enables idempotent upserts and deduplication

### `pos_set_id` (Place of Service Set ID)
- **Format**: MD5 hash of normalized place of service code list
- **Purpose**: Groups related place of service codes
- **Usage**: Simplifies place of service filtering and analysis

## Data Quality

### Completeness
- **Fact Table**: 0% nulls in key fields
- **Dimensions**: High completeness (>95%) for critical fields
- **Cross-References**: 100% complete for linking fields

### Uniqueness
- **Fact UIDs**: 100% unique (2,967,105 unique values)
- **Dimension Keys**: 100% unique across all dimension tables
- **Provider Groups**: 637 unique groups with complete NPI/TIN mappings

### Referential Integrity
- **All foreign keys validated**: 0 orphaned records
- **Cross-reference integrity**: All provider groups in fact table have NPI/TIN mappings

## Usage Examples

### Basic Rate Query
```sql
SELECT 
    f.negotiated_rate,
    c.code_description,
    p.reporting_entity_name,
    pg.provider_group_id_raw
FROM fact_rate f
JOIN dim_code c ON f.code_type = c.code_type AND f.code = c.code
JOIN dim_payer p ON f.payer_slug = p.payer_slug
JOIN dim_provider_group pg ON f.pg_uid = pg.pg_uid
WHERE f.state = 'GA' 
  AND f.code = '99213'
  AND f.negotiated_rate > 100
```

### Provider Network Analysis
```sql
SELECT 
    pg.provider_group_id_raw,
    COUNT(DISTINCT npi.npi) as provider_count,
    COUNT(DISTINCT tin.tin_value) as tin_count
FROM dim_provider_group pg
LEFT JOIN xref_pg_member_npi npi ON pg.pg_uid = npi.pg_uid
LEFT JOIN xref_pg_member_tin tin ON pg.pg_uid = tin.pg_uid
GROUP BY pg.provider_group_id_raw
```

### Rate Analysis by Procedure Category
```sql
SELECT 
    cat.proc_set,
    cat.proc_class,
    AVG(f.negotiated_rate) as avg_rate,
    COUNT(*) as rate_count
FROM fact_rate f
JOIN dim_code_cat cat ON f.code = cat.proc_cd
GROUP BY cat.proc_set, cat.proc_class
ORDER BY avg_rate DESC
```

## Data Sources

- **Rates Data**: UnitedHealthcare of Georgia Inc. MRF files
- **Provider Data**: NPPES (National Plan and Provider Enumeration System)
- **Code Data**: CPT/HCPCS procedure codes with categorization
- **Geographic Scope**: Georgia (GA) state
- **Time Period**: August 2025 rates

## Technical Notes

- **File Format**: Parquet with ZSTD compression
- **Processing Engine**: Polars with DuckDB for complex operations
- **Update Strategy**: Idempotent upserts based on unique identifiers
- **Data Versioning**: Version 1.0.0 across all dimensions

# TIN ↔ NPI Relationship Analysis
## Raw MRF Data Investigation for Provider Database Redesign

**Date:** 2025-10-30  
**Data Source:** UHC, Aetna, FloridaBlue MRF Provider Files  
**Total Records Analyzed:** 2,551,827 (after cleaning)  
**Files Analyzed:** 16 provider parquet files across CT, FL, GA, MO, NJ, TX

---

## Executive Summary

### Key Finding: **Many-to-Many Relationship**
- **74.1%** of NPIs have exactly **1 TIN**
- **25.9%** of NPIs have **2+ TINs** (up to 28 TINs per NPI)
- **Average:** 1.41 TINs per NPI | **Median:** 1.0

### Scale
- **454,896** unique NPIs
- **94,987** unique TINs  
- **640,895** unique (NPI, TIN) pairs

---

## Question 1: Can a Single NPI Appear with Multiple TINs?

### ✅ YES - This is common (25.9% of NPIs)

**Distribution of TINs per NPI:**
```
1 TIN:      337,055 NPIs (74.1%)
2 TINs:      79,325 NPIs (17.4%)
3-5 TINs:    35,226 NPIs (7.74%)
6-10 TINs:    3,013 NPIs (0.66%)
11+ TINs:       277 NPIs (0.06%)
Max:             28 TINs per NPI
```

### Real Examples:

**Example 1: NPI with Multiple TINs Across States**
```
NPI 1801621438 has 4 TINs:
┌────────────┬───────────┬─────────────────┬───────────────────────────────┐
│ NPI        │ TIN       │ Provider Group  │ Payer                         │
├────────────┼───────────┼─────────────────┼───────────────────────────────┤
│ 1801621438 │ 853953549 │ 196293         │ Aetna Health Inc. - Texas     │
│ 1801621438 │ 853953549 │ 24070          │ Aetna Health Inc. - Florida   │
│ 1801621438 │ 462531252 │ 140304         │ Aetna Health Inc. - Florida   │
│ 1801621438 │ 462531252 │ 225245         │ Aetna Health Inc. - New Jersey│
└────────────┴───────────┴─────────────────┴───────────────────────────────┘
```
**Pattern:** Same provider practicing in multiple states, different TINs per location

**Example 2: NPI with Multiple TINs Within Same State**
```
NPI 1295374601 has 6 TINs (all Texas Aetna):
┌────────────┬───────────┬─────────────────┐
│ NPI        │ TIN       │ Provider Group  │
├────────────┼───────────┼─────────────────┤
│ 1295374601 │ 850750688 │ 196293         │
│ 1295374601 │ 384030151 │ 275994         │
│ 1295374601 │ 741201585 │ 135308         │
│ 1295374601 │ 760482007 │ 87966          │
│ 1295374601 │ 741201585 │ 284851         │
│ 1295374601 │ 384030151 │ 191987         │
└────────────┴───────────┴─────────────────┘
```
**Pattern:** Provider affiliates with multiple practices/groups using different TINs

**Example 3: NPI with 2 TINs (Most Common Multi-TIN Pattern)**
```
NPI 1861435638 has 2 TINs:
┌────────────┬───────────┬─────────────────┬───────────────────────────────┐
│ NPI        │ TIN       │ Provider Group  │ Payer                         │
├────────────┼───────────┼─────────────────┼───────────────────────────────┤
│ 1861435638 │ 273654710 │ 358702         │ Aetna Health Inc. - New Jersey│
│ 1861435638 │ 223442866 │ 2318           │ Aetna Health Inc. - New Jersey│
└────────────┴───────────┴─────────────────┴───────────────────────────────┘
```

---

## Question 2: Can a Single TIN Have Multiple NPIs?

### ✅ YES - This is very common (54% of TINs)

**Distribution of NPIs per TIN:**
```
1 NPI:        43,668 TINs (46.0%)
2-10 NPIs:    45,665 TINs (48.1%)
11-100 NPIs:   5,031 TINs (5.3%)
101+ NPIs:       623 TINs (0.7%)
Max:          13,347 NPIs per TIN
```

### Real Example - Large Practice Group:

**TIN 852938829 (Florida UHC) - 157 NPIs (showing first 15)**
```
┌───────────┬────────────┬─────────────────┐
│ TIN       │ NPI        │ Provider Group  │
├───────────┼────────────┼─────────────────┤
│ 852938829 │ 1720314263 │ 216            │
│ 852938829 │ 1053757179 │ 216            │
│ 852938829 │ 1265703565 │ 216            │
│ 852938829 │ 1831385020 │ 216            │
│ 852938829 │ 1700972940 │ 216            │
│ 852938829 │ 1528178514 │ 216            │
│ 852938829 │ 1093234486 │ 216            │
│ 852938829 │ 1215958111 │ 216            │
│ 852938829 │ 1881954527 │ 216            │
│ 852938829 │ 1376860759 │ 216            │
│ 852938829 │ 1376041640 │ 216            │
│ 852938829 │ 1396261814 │ 216            │
│ 852938829 │ 1427003771 │ 216            │
│ 852938829 │ 1457860017 │ 216            │
│ 852938829 │ 1851578033 │ 216            │
│    ...    │    ...     │      ...        │
└───────────┴────────────┴─────────────────┘
(157 total NPIs)
```
**Pattern:** Large hospital system or medical group with many employed providers

### Top 10 Largest TINs by NPI Count (Florida UHC):
```
TIN          │ NPIs
─────────────┼──────
852938829    │ 157
862493019    │ 150
592579938    │ 116
953372911    │ 116
834257679    │ 109
841856765    │ 106
920986068    │  78
593214635    │  69
260609255    │  62
952977916    │  61
```

---

## Question 3: Data Structure in Raw Files

### Provider File Schema
```
Column                    Type     Description
────────────────────────  ───────  ─────────────────────────────────────
provider_group_id         int64    Unique ID for each contract/rate tier
npi                       string   National Provider Identifier (10 digits)
tin_type                  string   "ein" (99%) or "npi" (1%, solo practitioners)
tin_value                 string   Tax ID Number (9 digits) or NPI (10 digits)
location                  string   Empty for most records
reporting_entity_name     string   Payer name (e.g., "Aetna Health Inc. - Florida")
reporting_entity_type     string   Usually "Insurer"
last_updated_on           string   Date (e.g., "2025-10-01")
version                   string   Usually "1.0.0"
network_id                string   Network identifier (if provided)
```

### Sample Rows (20 random examples from real data):
```
provider_group_id        npi tin_type tin_value           reporting_entity_name
─────────────────  ──────────  ────────  ─────────  ───────────────────────────────
             76622  1912265638       ein  854252623     Aetna Health Inc. - Florida
            209392  1245593573       ein  844504483       Aetna Health Inc. - Texas
            140304  1114793866       ein  331874162     Aetna Health Inc. - Florida
            177766  1306370010       ein  371911194 Aetna Health Inc. - Connecticut
            155463  1144687047       ein  841856765       Aetna Health Inc. - Texas
            112767  1619054137       ein  454832340  Aetna Health Inc. - New Jersey
            247637  1164606034       ein  223052989  Aetna Health Inc. - New Jersey
            123252  1780921031       ein  852938829     Aetna Health Inc. - Florida
            113040  1730495623       ein  201966531     Aetna Health Inc. - Florida
            258795  1497149462       ein  591289802     Aetna Health Inc. - Florida
             69209  1881025567       ein  205268843     Aetna Health Inc. - Florida
            240972  1154983229       ein  471617423 Aetna Health Inc. - Connecticut
            236141  1447282025       ein  204865566     Aetna Health Inc. - Florida
             37939  1134147523       ein  760459500       Aetna Health Inc. - Texas
             15270  1285941690       ein  841856765     Aetna Health Inc. - Florida
            142900  1508510868       ein  880640297     Aetna Health Inc. - Florida
            268923  1932305844       ein  832675429       Aetna Health Inc. - Texas
            107828  1174707863       ein  884197075     Aetna Health Inc. - Florida
            388415  1932744422       ein  841856765     Aetna Health Inc. - Florida
            355306  1063041762       ein  854252623     Aetna Health Inc. - Florida
```

### File Sizes by Source:
```
Source        │ Unique NPIs │ Unique TINs │ Total Rows
──────────────┼─────────────┼─────────────┼────────────
fl_aetna      │     156,458 │      29,219 │  1,802,177
tx_aetna      │     159,494 │      27,390 │    459,155
nj_aetna      │      93,339 │      16,169 │    168,818
ct_aetna      │      50,353 │       7,769 │     68,912
fl_uhc        │       8,729 │       4,685 │      9,534
tx_uhc        │      10,329 │       6,487 │     11,456
nj_uhc        │       8,347 │       5,859 │      9,193
mo_uhc        │       9,146 │       4,691 │     10,381
ga_uhc        │       5,227 │       2,822 │      5,797
fl_blue_v1    │       6,344 │       5,510 │      6,404
```

---

## Question 4: Data Quality Issues

### ⚠️ Critical Issue: NPI = "0" (Placeholder for Missing Values)

**Prevalence:** 27,619 records (1.08% of total data)

**Distribution by Payer:**
- **Aetna:** ~1.1% of records have NPI="0"
- **UHC:** 0% (no NPI="0" records)

**Example:**
```
provider_group_id   npi  tin_type tin_value  reporting_entity_name
─────────────────  ────  ────────  ─────────  ──────────────────────────
           346500     0       ein  474290233  Aetna Health Inc. - Florida
           340975     0       ein  592013191  Aetna Health Inc. - Florida
```

**Impact:** NPI="0" creates false "worst case" of 2979 TINs per NPI. After removing NPI="0":
- Max TINs per NPI drops from 2979 → 28 (reasonable)

### ✅ Clean Fields (UHC Data)
```
NULL values:               0 across all columns
Empty strings:             0 in npi, tin_value, tin_type
Invalid NPI format:        0 (all are 10 digits, numeric)
Invalid TIN format:      142 (10-digit "NPIs used as TINs", not 9-digit EINs)
```

### ℹ️ Solo Practitioners Using NPI as TIN

**Pattern:** `tin_type = "npi"` instead of `"ein"`

**Prevalence (UHC Florida):** 140 records (1.5%)

**Example:**
```
       npi tin_type  tin_value  provider_group_id
──────────  ────────  ──────────  ─────────────────
1568083616       npi  1568083616                 89
1366455693       npi  1366455693                 89
1497570022       npi  1497570022                 89
```
**100% of tin_type="npi" records have NPI == TIN_VALUE**

**Business Logic:** Individual practitioners can use their NPI as their billing TIN. This is valid per CMS guidelines.

### ✅ No Validation Issues Found
- No empty NPIs or TINs (after excluding NPI="0")
- No malformed values
- All NPIs are 10 digits
- All EIN-type TINs are 9 digits
- All NPI-type TINs are 10 digits (matching the NPI)

---

## Question 5: Why Do NPIs Have Multiple TINs?

Based on data patterns, here are the reasons:

### Pattern 1: **Multi-State Practice** (Different TINs per State)
```
NPI 1801621438:
  - TIN 853953549 in Texas + Florida
  - TIN 462531252 in Florida + New Jersey
```
**Interpretation:** Provider practices in multiple states, different billing entities per region

### Pattern 2: **Multiple Group Affiliations** (Same State, Different TINs)
```
NPI 1295374601 in Texas:
  - TIN 850750688 (provider_group 196293)
  - TIN 384030151 (provider_groups 275994, 191987)
  - TIN 741201585 (provider_groups 135308, 284851)
  - TIN 760482007 (provider_group 87966)
```
**Interpretation:** Provider has hospital privileges or affiliations with multiple medical groups

### Pattern 3: **Different Contracted Rates Within Same TIN**
```
Same (NPI, TIN) pair → Multiple provider_group_ids
Average: 2.94 provider_groups per (NPI, TIN) pair
```
**Interpretation:** Same provider+TIN combination can have different negotiated rates depending on contract tier/plan/network

### Pattern 4: **Solo vs. Group Practice**
```
tin_type="ein":  Provider bills under group/organization TIN
tin_type="npi":  Solo practitioner bills under own NPI
```

---

## Question 6: TIN Selection Strategy for dim_npi.tin_value

### ❌ Option A: JSON Array `["123456789", "987654321"]`
**Pros:** Preserves all TINs  
**Cons:** Complex querying, non-standard SQL, harder for analysts  
**Verdict:** ❌ Not recommended unless your DB has strong JSON support

### ❌ Option B: Comma-separated String `"123456789,987654321"`
**Pros:** Simple to store  
**Cons:** Violates 1NF, horrible for filtering/joins, parsing required  
**Verdict:** ❌ Anti-pattern

### ⚠️ Option C: Pick Most Common/Primary TIN
**Pros:** Simple, clean schema  
**Cons:** **Loses 25.9% of TIN relationships**, arbitrary choice  
**Verdict:** ⚠️ Only if you can accept data loss

### ✅ **Option D: Keep Multiple Rows (Recommended)**
**Structure:**
```sql
dim_npi (NPI is NOT unique - denormalized by design)
  npi              string  (not unique!)
  tin_value        string
  tin_type         string
  -- NPI metadata from NPPES (duplicated) --
  first_name       string
  last_name        string
  taxonomy_code    string
  ...
  -- Unique constraint --
  UNIQUE(npi, tin_value)
```

**Example:**
```
npi         tin_value  tin_type  first_name  last_name  taxonomy
──────────  ─────────  ────────  ──────────  ─────────  ────────
1801621438  853953549  ein       JOHN        SMITH      207R00000X
1801621438  462531252  ein       JOHN        SMITH      207R00000X
1295374601  850750688  ein       JANE        DOE        208D00000X
1295374601  384030151  ein       JANE        DOE        208D00000X
1295374601  741201585  ein       JANE        DOE        208D00000X
```

**Why This Works:**
1. ✅ Preserves ALL relationships (no data loss)
2. ✅ Easy to query: `WHERE npi = ? AND tin_value = ?`
3. ✅ Easy to aggregate: `SELECT npi, COUNT(DISTINCT tin_value) as tin_count`
4. ✅ Matches your extraction data structure (already 1 row per NPI+TIN pair)
5. ✅ Standard SQL, no parsing needed
6. ✅ NPPES data duplicated is negligible (454K NPIs → 640K rows = 1.41x)

**Storage Impact:**
- 454,896 NPIs with all metadata → 640,895 rows
- Only 41% increase in storage for complete relationship preservation
- NPPES enrichment data (names, addresses) duplicated 1.41x on average

---

## Question 7: Scale & Coverage Summary

### Aggregate Statistics Across All Files:

```
Total Records (cleaned):       2,551,827
Unique NPIs:                     454,896
Unique TINs:                      94,987
Unique (NPI, TIN) pairs:         640,895

Average TINs per NPI:              1.41
Median TINs per NPI:               1.0

Average NPIs per TIN:              6.75
Median NPIs per TIN:               2.0
```

### Data Quality Score: **98.9%**
- 98.92% of records have valid NPIs (not "0")
- 100% of UHC records are clean
- 98.9% of Aetna records are clean

---

## Recommendations for dim_npi Design

### ✅ **Recommended Schema:**

```sql
-- Master NPI table (denormalized, one row per NPI+TIN pair)
CREATE TABLE dim_npi (
    -- Primary key --
    npi_tin_id          STRING PRIMARY KEY,  -- MD5(npi||tin_value)
    
    -- Foreign keys --
    npi                 STRING NOT NULL,
    tin_value           STRING NOT NULL,
    tin_type            STRING NOT NULL,     -- 'ein' or 'npi'
    
    -- NPPES enrichment (duplicated per NPI) --
    first_name          STRING,
    last_name           STRING,
    organization_name   STRING,
    enumeration_type    STRING,              -- 'NPI-1' or 'NPI-2'
    primary_taxonomy_code STRING,
    primary_taxonomy_desc STRING,
    credential          STRING,
    status              STRING,
    enumeration_date    DATE,
    
    -- CMS facility enrichment (duplicated per NPI) --
    facility_name       STRING,
    facility_address_1  STRING,
    facility_city       STRING,
    facility_state      STRING,
    facility_zip        STRING,
    
    -- Metadata --
    nppes_fetched       BOOLEAN,
    nppes_fetch_date    DATE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    
    -- Constraints --
    UNIQUE(npi, tin_value)
);

-- Indexes for common queries
CREATE INDEX idx_npi ON dim_npi(npi);
CREATE INDEX idx_tin_value ON dim_npi(tin_value);
CREATE INDEX idx_enumeration_type ON dim_npi(enumeration_type);
```

### Migration Strategy:

```python
# Pseudocode for migration
def migrate_to_new_schema():
    """
    1. Extract all (NPI, TIN) pairs from provider files
    2. Deduplicate to unique (NPI, TIN) pairs
    3. For each unique NPI:
         - Fetch NPPES data ONCE
         - Fetch CMS facility data ONCE (if NPI-2)
         - Create N rows (one per TIN for this NPI)
         - Duplicate NPPES/CMS data in each row
    4. Upsert to dim_npi with UNIQUE(npi, tin_value)
    """
    
    # Load all provider files
    npi_tin_pairs = load_all_provider_files()
    npi_tin_pairs = npi_tin_pairs[npi_tin_pairs['npi'] != '0']  # Filter out bad data
    
    # Get unique NPIs for NPPES fetch
    unique_npis = npi_tin_pairs['npi'].unique()
    
    # Fetch NPPES data in batches
    nppes_data = fetch_nppes_bulk(unique_npis)
    
    # Join and create denormalized rows
    dim_npi = npi_tin_pairs.merge(nppes_data, on='npi', how='left')
    
    # Generate surrogate key
    dim_npi['npi_tin_id'] = dim_npi.apply(
        lambda x: hashlib.md5(f"{x['npi']}{x['tin_value']}".encode()).hexdigest(),
        axis=1
    )
    
    # Write to database with UPSERT
    dim_npi.to_sql('dim_npi', engine, if_exists='replace', index=False)
```

### Deprecate dim_tin_npi:
```sql
-- Old junction table (buggy, incomplete) --
DROP TABLE dim_tin_npi;  -- No longer needed

-- Old dim_tin (optional: keep for aggregations) --
-- Can keep dim_tin as a summary/rollup table if needed for reporting
```

---

## Usage Examples with New Schema

### Query 1: Get all TINs for an NPI
```sql
SELECT npi, tin_value, tin_type
FROM dim_npi
WHERE npi = '1801621438';

-- Returns:
-- npi         tin_value  tin_type
-- 1801621438  853953549  ein
-- 1801621438  462531252  ein
```

### Query 2: Get all NPIs for a TIN
```sql
SELECT npi, first_name, last_name
FROM dim_npi
WHERE tin_value = '852938829';

-- Returns 157 rows (all providers in that group)
```

### Query 3: Count providers with multiple TINs
```sql
SELECT 
    npi,
    first_name,
    last_name,
    COUNT(DISTINCT tin_value) as tin_count
FROM dim_npi
GROUP BY npi, first_name, last_name
HAVING tin_count > 1
ORDER BY tin_count DESC;
```

### Query 4: Join rates to enriched provider data
```sql
SELECT 
    r.billing_code,
    r.negotiated_rate,
    n.npi,
    n.first_name,
    n.last_name,
    n.tin_value,
    n.primary_taxonomy_desc
FROM fact_rate r
JOIN dim_provider_group pg ON r.pg_uid = pg.pg_uid
JOIN xref_pg_member_npi x ON pg.pg_uid = x.pg_uid
JOIN dim_npi n ON x.npi = n.npi AND pg.tin_value = n.tin_value  -- Match both!
WHERE r.code = '99213';
```

---

## Conclusion

### Key Insights:
1. **NPI↔TIN is Many-to-Many** (not 1:1)
2. **26% of NPIs have multiple TINs** (up to 28)
3. **Data is clean** (98.9%) except for NPI="0" placeholder
4. **Denormalization is optimal** for this use case
5. **Storage overhead is minimal** (1.41x rows vs. 1 row per NPI)

### Recommended Approach:
- ✅ Use **Option D**: Multiple rows per NPI (one per TIN)
- ✅ Add `UNIQUE(npi, tin_value)` constraint
- ✅ Denormalize NPPES/CMS data (duplicate per NPI+TIN row)
- ✅ Filter out `npi = '0'` during migration
- ✅ Keep `tin_type` column to identify solo practitioners

This approach preserves all relationships, maintains query simplicity, and has negligible storage overhead.

---

**Report Generated:** 2025-10-30  
**Agent:** MRF Extraction System  
**Files Analyzed:** 16 provider parquet files  
**Total Records:** 2.5M+ cleaned records


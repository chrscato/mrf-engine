# MRF Index-Based Extraction Guide

## Overview

Some payers (Aetna, UHC with index files) provide plan metadata in separate index JSON files rather than in the MRF files themselves. This guide covers the two-step process:
1. Parse the index JSON file to extract plan metadata and MRF URLs
2. Run extraction commands with plan metadata included

## Why This Approach?

**Index File Structure (CMS Standard):**
- Index files contain plan metadata (`plan_name`, `plan_id`, etc.)
- MRF files may NOT contain plan metadata
- Multiple plans may share the same MRF file OR have separate files

**Our Solution:**
- `parse_mrf_index.py` generates extraction commands with plan metadata (works with any payer)
- `run_extraction.py` accepts plan metadata as CLI arguments
- Plan metadata is written to the rates output file

## Usage

### Step 1: Parse Index File

```bash
python parse_mrf_index.py <index_file.json>

# Or with custom prefix:
python parse_mrf_index.py <index_file.json> --state-prefix fl_aetna
```

**Output:** Ready-to-run extraction commands with plan metadata included

### Step 2: Run Extraction Commands

Copy/paste the generated commands. Example:

```bash
python -m src.run_extraction \
  https://mrf.healthsparq.com/.../Aetna-Health-Inc.---Florida.json.gz \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix fl_aetna_plan1 \
  --rate-batch-size 20000 \
  --plan-name "Aetna HMO_60179" \
  --plan-id-type "EIN" \
  --plan-id "592411584" \
  --plan-market-type "group"
```

## Examples

### Aetna Florida (3 separate plans, 3 endpoints)
```bash
python parse_mrf_index.py aetna_florida_index.json
# Generates 3 commands, one for each plan
```

### Aetna Connecticut (2 plans, 1 endpoint)
```bash
python parse_mrf_index.py aetna_connecticut_index.json
# Generates 1 command covering both plans
```

### UHC Georgia (Multiple plans with complex structure)
```bash
python parse_mrf_index.py uhc_georgia_index.json
# Auto-detects state and payer, generates commands for all plans
```

## Output

Rates files will include populated plan fields:
- `plan_name`: e.g., "Aetna HMO_60179"
- `plan_id_type`: "EIN" or "HIOS"
- `plan_id`: e.g., "592411584"
- `plan_market_type`: "group" or "individual"

## When to Use This Approach

**Use index parser when:**
- You have an index JSON file with `reporting_structure` array
- You want plan metadata populated in your output
- The payer provides plan info in index files (Aetna, some UHC files)

**Use direct extraction when:**
- You have direct MRF URLs (like standard UHC endpoints)
- No index file is available
- Plan fields will be empty (which is fine for many use cases)


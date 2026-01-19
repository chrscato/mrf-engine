# MRF Index-Based Extraction Guide

## Overview

Some payers (Aetna, UHC with index files, Cigna) provide plan metadata in separate index JSON files rather than in the MRF files themselves. This guide covers how to extract data from these index files using the unified extraction orchestrator.

## Why This Approach?

**Index File Structure (CMS Standard):**
- Index files contain plan metadata (`plan_name`, `plan_id`, `plan_id_type`, `plan_market_type`)
- MRF files may NOT contain plan metadata in their root
- Multiple plans may share the same MRF file OR have separate files
- Each `reporting_structure` can have multiple plans and multiple MRF file URLs

**Our Solution:**
- The `extraction_orchestrator` automatically parses index files and extracts plan metadata
- Plan metadata is passed to the extraction workflow and written to output files
- No manual command generation needed - the orchestrator handles everything

## Usage

### Basic Index File Extraction

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt
```

This will:
1. Parse the index file to extract all `reporting_structure` entries
2. For each structure, extract plan metadata and MRF file URLs
3. Run extraction for each MRF file with plan metadata included
4. Generate output files with populated `plan_name`, `plan_id`, `plan_id_type`, `plan_market_type` fields

### Filtering Structures

Process only specific structures (useful for testing or selective extraction):

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt \
  --structures 1,4,7
```

### Limiting Files Per Structure

For testing, limit the number of files processed per structure:

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt \
  --max-files 5
```

This processes only the first 5 files from each structure.

### Parallel Execution

Use multiple workers for faster batch processing:

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt \
  --workers 4
```

In parallel mode:
- Console shows summary output
- Detailed logs written to `logs/{timestamp}/` directory
- Manifest file tracks all extractions

## Examples

### Aetna Florida (Multiple Plans, Multiple Files)

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/2025-10-05_Aetna-Health-Inc----Florida_index.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt
```

**What happens:**
- Index file contains multiple `reporting_structure` entries
- Each structure has multiple plans (e.g., "Aetna HMO_60179", "Aetna PPO_60180")
- Each structure has multiple MRF file URLs
- Each MRF file is extracted with all plans from its structure
- Output files have `plan_name`, `plan_id`, etc. populated as lists

### Aetna Connecticut (Multiple Plans, Single File)

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/2025-10-05_Aetna-Health-Inc----Connecticut_index.json \
  --output-dir output/ct_aetna \
  --cpt-whitelist src/cpt_codes.txt
```

**What happens:**
- Index file has structures with multiple plans
- Some structures have only one MRF file
- That single file is extracted with all plans from the structure
- Output file has multiple plan names in the `plan_name` list field

### Cigna National (Complex Structure)

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/2025-11-01_cigna-health-life-insurance-company_index.json \
  --output-dir output/cigna_national \
  --cpt-whitelist src/cpt_codes.txt \
  --workers 8
```

**What happens:**
- Large index file with many structures
- Each structure may have different plan configurations
- Multiple MRF files per structure
- Parallel execution speeds up processing

## Output

### Plan Metadata Fields

Rates files will include populated plan fields:

- **`plan_name`**: List of plan names (e.g., `["Aetna HMO_60179", "Aetna PPO_60180"]`)
- **`plan_id_type`**: List of plan ID types (e.g., `["EIN", "EIN"]`)
- **`plan_id`**: List of plan IDs (e.g., `["592411584", "592411585"]`)
- **`plan_market_type`**: List of market types (e.g., `["group", "individual"]`)

**Note**: These are list fields because one MRF file can be associated with multiple plans.

### Network ID

The `network_id` field is automatically extracted from MRF filenames/URLs:
- Aetna: Extracted from filename patterns like `pl-29k-hr23`
- UHC: Extracted from network identifiers in filenames
- Other payers: Custom extraction logic based on naming conventions

### Output File Naming

Files are named with auto-detected prefixes:
- `{state}_{payer}` format (e.g., `fl_aetna`, `ct_aetna`)
- Extracted from `reporting_entity_name` in index file
- Can be overridden with `--output-prefix`

Example output:
```
output/fl_aetna/
├── providers_fl_aetna_pl-2im-hr23_20251008_173510.parquet
├── rates_fl_aetna_pl-2im-hr23_20251008_173713.parquet
├── providers_fl_aetna_pl-3mk-hr23_20251008_174402.parquet
├── rates_fl_aetna_pl-3mk-hr23_20251008_174625.parquet
└── manifest.json
```

## When to Use This Approach

**Use index-based extraction when:**
- ✅ You have an index JSON file with `reporting_structure` array
- ✅ You want plan metadata populated in your output
- ✅ The payer provides plan info in index files (Aetna, some UHC files, Cigna)
- ✅ You need to process multiple structures/plans systematically

**Use direct extraction when:**
- ✅ You have direct MRF URLs (like standard UHC endpoints)
- ✅ No index file is available
- ✅ Plan fields can be empty (which is fine for many use cases)
- ✅ You're processing a single file or small batch

## Index File Structure

Index files follow CMS transparency schema:

```json
{
  "reporting_entity_name": "Aetna Health Inc. - Florida",
  "reporting_entity_type": "Insurer",
  "last_updated_on": "2025-10-05",
  "version": "1.0.0",
  "reporting_structure": [
    {
      "reporting_plans": [
        {
          "plan_name": "Aetna HMO_60179",
          "plan_id_type": "EIN",
          "plan_id": "592411584",
          "plan_market_type": "group"
        }
      ],
      "in_network_files": [
        {
          "location": "https://mrf.healthsparq.com/.../file1.json.gz"
        },
        {
          "location": "https://mrf.healthsparq.com/.../file2.json.gz"
        }
      ]
    }
  ]
}
```

The orchestrator:
1. Extracts `reporting_entity_name` to infer state and payer (for prefix)
2. Iterates through `reporting_structure` array
3. For each structure, collects all plans and all file URLs
4. Associates each file URL with all plans from its structure
5. Passes plan metadata to extraction workflow

## Comparison with Direct Extraction

### Index-Based (This Guide)

**Pros:**
- ✅ Plan metadata automatically populated
- ✅ Handles complex multi-plan structures
- ✅ Systematic processing of all structures
- ✅ Network IDs extracted from filenames

**Cons:**
- ❌ Requires index file
- ❌ More complex setup
- ❌ Slower for single-file extractions

### Direct Extraction

**Pros:**
- ✅ Simple: just provide URL
- ✅ Fast for single files
- ✅ No index file needed

**Cons:**
- ❌ Plan fields empty (unless in MRF root, which is rare)
- ❌ Manual plan identification needed
- ❌ No automatic structure handling

## Troubleshooting

### No Plans Extracted

**Symptom**: Output files have empty `plan_name`, `plan_id` lists

**Causes:**
- Index file structure different than expected
- No `reporting_plans` in structure
- Parsing error

**Solutions:**
- Check index file structure manually
- Verify `reporting_structure` array exists
- Check orchestrator logs for parsing errors

### Wrong Prefix Generated

**Symptom**: Output prefix doesn't match expected format

**Causes:**
- `reporting_entity_name` format not recognized
- State/payer extraction logic doesn't match naming convention

**Solutions:**
- Use `--output-prefix` to override auto-detection
- Check `reporting_entity_name` in index file
- Report naming pattern for enhancement

### Missing Files

**Symptom**: Some structures or files not processed

**Causes:**
- `--structures` filter excludes them
- `--max-files` limit reached
- File URLs inaccessible

**Solutions:**
- Check `manifest.json` for failed extractions
- Verify URL accessibility
- Remove filters to process all files

## Advanced Usage

### Custom Output Prefix

Override auto-detected prefix:

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/custom \
  --output-prefix custom_prefix \
  --cpt-whitelist src/cpt_codes.txt
```

### Combining with Other Options

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt \
  --structures 1,2,3 \
  --max-files 10 \
  --workers 4 \
  --provider-batch-size 20000 \
  --rate-batch-size 50000
```

## See Also

- [Main README.md](README.md) - Complete documentation
- [Batch Extraction Guide](README.md#batch-extraction) - General batch processing
- [Output Schema](README.md#output-schema) - Field descriptions

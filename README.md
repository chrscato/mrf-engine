# MRF Extraction Engine

A high-performance, production-ready tool for extracting provider and rate information from Machine Readable Files (MRF) published by health insurance payers under CMS Transparency in Coverage regulations. This engine handles large-scale batch extractions with automatic schema detection, memory-efficient streaming processing, and support for multiple payer formats.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Usage Guide](#usage-guide)
  - [Single File Extraction](#single-file-extraction)
  - [Batch Extraction](#batch-extraction)
  - [Command Line Options](#command-line-options)
- [Output Schema](#output-schema)
- [Multi-Payer Schema Support](#multi-payer-schema-support)
- [Performance & Optimization](#performance--optimization)
- [Troubleshooting](#troubleshooting)
- [Advanced Topics](#advanced-topics)

## Overview

The MRF Extraction Engine processes large JSON.GZ files (often 10GB+ compressed) containing negotiated healthcare rates and provider network information. The engine:

- **Automatically detects** two different MRF schema variants (standard vs. inline)
- **Streams data** efficiently using `ijson` to handle files larger than available RAM
- **Filters intelligently** using provider groups and CPT code whitelists
- **Outputs Parquet** files optimized for downstream analytics
- **Supports batch processing** with parallel execution for large-scale extractions

### Key Features

- ✅ **Zero-configuration schema detection** - Works with Aetna, UHC, Cigna, Florida Blue, BCBS, and more
- ✅ **Memory-efficient streaming** - Processes 200M+ rate records without loading entire files
- ✅ **Automatic provider synthesis** - Handles inline provider schemas (Florida Blue, BCBS)
- ✅ **Batch orchestration** - Unified tool for CSV, index files, URL lists, and single URLs
- ✅ **Parallel execution** - Multi-worker support for faster batch processing
- ✅ **Comprehensive logging** - Detailed manifests and error tracking
- ✅ **Plan metadata extraction** - Captures plan names, IDs, and market types from index files

## Repository Structure

```
mrf-engine/
├── src/                    # Core extraction modules
│   ├── run_extraction.py   # Main extraction workflow
│   ├── extraction_orchestrator.py  # Batch extraction orchestrator
│   ├── extract_providers_pro.py   # Provider extraction
│   ├── extract_rates.py    # Rate extraction
│   └── cpt_codes.txt       # CPT code whitelist
├── input/                  # Input files for batch extractions
│   ├── *.csv              # CSV files with URLs and plan metadata
│   └── *.txt              # URL list files
├── index_files/           # MRF index JSON files
├── notebooks/             # Jupyter notebooks for exploratory analysis
├── output/                # Extraction output (Parquet files)
├── docs/                  # Documentation
│   └── archive/           # Archived files and old documentation
└── requirements.txt       # Python dependencies
```

## Quick Start

### Installation

1. **Clone the repository** (if not already done):
   ```bash
   git clone <repository-url>
   cd mrf-engine
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Basic Single File Extraction

Extract providers and rates from a single MRF file:

```bash
python -m src.run_extraction \
  "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-12-01/2025-12-01_UnitedHealthcare-Insurance-Company-of-Illinois_Insurer_Choice-Plus-POS_8_in-network-rates.json.gz" \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix il_uhc_choice_plus \
  --output-dir output/il_uhc
```

This will create:
- `output/il_uhc/providers_il_uhc_choice_plus_YYYYMMDD_HHMMSS.parquet`
- `output/il_uhc/rates_il_uhc_choice_plus_YYYYMMDD_HHMMSS.parquet`

### Basic Batch Extraction

Extract from multiple files using the unified orchestrator:

```bash
# From a CSV file with URLs
python -m src.extraction_orchestrator \
  --source csv \
  --input batch_urls.csv \
  --output-dir output/batch_run \
  --cpt-whitelist src/cpt_codes.txt \
  --workers 4

# From an MRF index file
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt
```

## Architecture

### Two-Step Extraction Workflow

The engine uses a two-step process optimized for different MRF schema types:

#### Step 1: Provider Extraction (`extract_providers_pro.py`)

- **Purpose**: Extract provider references from top-level `provider_references` array
- **Output**: `providers_{prefix}_{timestamp}.parquet`
- **Schema Detection**: If no `provider_references` found, detects inline schema and skips (takes ~1 second)
- **Filtering**: Optional TIN whitelist filtering
- **For Standard Schema** (Aetna, UHC, Cigna): Extracts all provider groups with NPIs and TINs
- **For Inline Schema** (Florida Blue, BCBS): Detects absence and skips (providers extracted in Step 2)

#### Step 2: Rate Extraction (`extract_rates.py`)

- **Purpose**: Extract negotiated rates with provider and CPT filtering
- **Output**: `rates_{prefix}_{timestamp}.parquet`
- **Provider Filtering**: Uses provider groups from Step 1 (or synthesizes for inline schema)
- **CPT Filtering**: Filters rates by billing codes in whitelist
- **For Standard Schema**: Uses existing `provider_reference_id` integers from MRF
- **For Inline Schema**: Synthesizes deterministic provider IDs and writes providers file during this step

### Data Flow

```
MRF File (JSON.GZ)
    ↓
[Step 1] Provider Extraction
    ├─→ Detects schema type
    ├─→ Standard: Extracts provider_references → providers.parquet
    └─→ Inline: Detects and skips (~1 second)
    ↓
[Step 2] Rate Extraction
    ├─→ Standard: Uses provider_reference_ids from Step 1
    ├─→ Inline: Synthesizes provider IDs from embedded provider_groups
    ├─→ Filters by CPT whitelist
    └─→ Outputs rates.parquet (and providers.parquet for inline)
```

## Usage Guide

### Single File Extraction

The `run_extraction.py` module is the core extraction workflow for individual MRF files.

#### Basic Command Structure

```bash
python -m src.run_extraction <mrf_url_or_path> [OPTIONS]
```

#### Common Use Cases

**1. Extract all data from a URL:**
```bash
python -m src.run_extraction \
  "https://example.com/mrf_file.json.gz" \
  --output-prefix my_extraction
```

**2. Extract with CPT filtering:**
```bash
python -m src.run_extraction \
  "https://example.com/mrf_file.json.gz" \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix filtered_extraction
```

**3. Extract with provider filtering (TIN whitelist):**
```bash
python -m src.run_extraction \
  "https://example.com/mrf_file.json.gz" \
  --tin-whitelist src/optim_tins.txt \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix targeted_extraction
```

**4. Test extraction (limited data):**
```bash
python -m src.run_extraction \
  "https://example.com/mrf_file.json.gz" \
  --max-providers 1000 \
  --max-items 5000 \
  --max-time 30 \
  --output-prefix test_run
```

**5. Extract with plan metadata (for index-based extractions):**
```bash
python -m src.run_extraction \
  "https://example.com/mrf_file.json.gz" \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix aetna_plan1 \
  --plan-name "Aetna HMO_60179" \
  --plan-id-type "EIN" \
  --plan-id "592411584" \
  --plan-market-type "group" \
  --network-id "pl-29k-hr23"
```

### Batch Extraction

The `extraction_orchestrator.py` module provides a unified interface for batch processing from multiple input sources.

#### Input Source Types

**1. CSV Files** (`--source csv`)

CSV files should have a `file_url` column and optional plan metadata columns. Place CSV files in the `input/` directory:

```csv
file_url,plan_name,plan_id_type,plan_id,plan_market_type
https://example.com/file1.json.gz,Plan A,EIN,123456789,group
https://example.com/file2.json.gz,Plan B,HIOS,987654321,individual
```

```bash
python -m src.extraction_orchestrator \
  --source csv \
  --input input/batch_urls.csv \
  --output-dir output/batch \
  --cpt-whitelist src/cpt_codes.txt \
  --workers 4
```

**2. MRF Index Files** (`--source index`)

Index files follow CMS transparency schema with `reporting_structure` arrays:

```bash
python -m src.extraction_orchestrator \
  --source index \
  --input index_files/aetna_florida.json \
  --output-dir output/fl_aetna \
  --cpt-whitelist src/cpt_codes.txt \
  --structures 1,2,3 \
  --max-files 10
```

**3. URL List Files** (`--source urls`)

Text file with one URL per line. Place text files in the `input/` directory:

```bash
python -m src.extraction_orchestrator \
  --source urls \
  --input input/url_list.txt \
  --output-dir output/url_batch \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix uhc_batch
```

**4. Single URL** (`--source url`)

Process a single URL through the orchestrator:

```bash
python -m src.extraction_orchestrator \
  --source url \
  --input "https://example.com/mrf_file.json.gz" \
  --output-dir output/single \
  --cpt-whitelist src/cpt_codes.txt \
  --output-prefix single_extraction
```

#### Parallel Execution

Use `--workers N` to run extractions in parallel:

```bash
python -m src.extraction_orchestrator \
  --source csv \
  --input batch_urls.csv \
  --output-dir output/parallel \
  --workers 8 \
  --cpt-whitelist src/cpt_codes.txt
```

- **Sequential mode** (`--workers 1` or default): Detailed output to console
- **Parallel mode** (`--workers > 1`): Summary to console, detailed logs to `logs/{timestamp}/` directory

### Command Line Options

#### Single File Extraction (`run_extraction.py`)

**Required Arguments:**
- `mrf_url` - URL or local path to MRF file

**Output Options:**
- `--output-dir` / `-o` - Output directory (default: `output`)
- `--output-prefix` / `-op` - Prefix for output files (required for identification)

**Provider Extraction Options:**
- `--max-providers` / `-mp` - Maximum provider references to process (for testing)
- `--provider-batch-size` / `-pbs` - Batch size for provider extraction (default: 10000)
- `--tin-whitelist` / `-t` - Path to TIN whitelist file (one TIN per line)

**Rate Extraction Options:**
- `--max-items` / `-mi` - Maximum rate items to process (for testing)
- `--max-time` / `-mt` - Maximum time in minutes (for testing)
- `--rate-batch-size` / `-rbs` - Batch size for rate extraction (default: 20000)
- `--cpt-whitelist` / `-c` - Path to CPT code whitelist file (one code per line)

**Plan Metadata Options:**
- `--plan-name` - Plan name (can be specified multiple times for multiple plans)
- `--plan-id-type` - Plan ID type: "EIN" or "HIOS" (can be specified multiple times)
- `--plan-id` - Plan ID value (can be specified multiple times)
- `--plan-market-type` - Market type: "group" or "individual" (can be specified multiple times)
- `--plan-name-alt` - Alternate plan name (for files with multiple plans)
- `--network-id` - Network identifier (extracted from filename/URL)

**Output Control:**
- `--quiet` - Suppress detailed output (auto-enabled in batch mode)

#### Batch Extraction (`extraction_orchestrator.py`)

**Required Arguments:**
- `--source` - Input source type: `csv`, `index`, `urls`, or `url`
- `--input` - Path to input file or URL string

**Output Options:**
- `--output-dir` - Output directory (default: `output`)
- `--output-prefix` - Optional prefix (auto-detected for index files)

**Execution Options:**
- `--workers` - Number of parallel workers (default: 1 = sequential)
- `--cpt-whitelist` - Path to CPT code whitelist file
- `--provider-batch-size` - Batch size for provider extraction (default: 10000)
- `--rate-batch-size` - Batch size for rate extraction (default: 20000)

**Index File Options:**
- `--structures` - Comma-separated structure numbers (e.g., "1,4,7")
- `--max-files` - Maximum files per structure (for testing)

## Output Schema

### Provider File Schema

**File**: `providers_{prefix}_{timestamp}.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `provider_group_id` | int64 | Unique provider group identifier (from MRF or synthesized) |
| `npi` | string | National Provider Identifier (10 digits) |
| `tin_type` | string | TIN type: "ein" or "npi" |
| `tin_value` | string | Tax Identification Number (9 digits for EIN, 10 for NPI) |
| `reporting_entity_name` | string | Payer name (e.g., "Aetna Health Inc. - Florida") |
| `reporting_entity_type` | string | Entity type (usually "Insurer") |
| `last_updated_on` | string | File last updated date (ISO format) |
| `version` | string | Schema version (usually "1.0.0") |
| `network_id` | string | Network identifier (if provided) |

**Notes:**
- For standard schema: `provider_group_id` comes directly from MRF
- For inline schema: `provider_group_id` is synthesized deterministically
- Multiple rows per `provider_group_id` (one per NPI in the group)
- `npi` may be "0" for TIN-level contracts without NPI information

### Rate File Schema

**File**: `rates_{prefix}_{timestamp}.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `provider_reference_id` | int64 | Provider group reference (links to providers file) |
| `billing_code` | string | Billing code (CPT, HCPCS, ICD, etc.) |
| `billing_code_type` | string | Code type: "CPT", "HCPCS", "ICD", "MS-DRG", "RC", etc. |
| `description` | string | Service description |
| `negotiated_rate` | float64 | Negotiated rate amount |
| `negotiated_type` | string | Rate type: "negotiated", "derived", "fee schedule", "percentage", "per diem" |
| `billing_class` | string | Billing class: "professional", "institutional", "both" |
| `expiration_date` | string | Rate expiration date (ISO format, "9999-12-31" = evergreen) |
| `service_codes` | list[string] | Place of service codes (CMS two-digit codes) |
| `billing_code_modifier` | list[string] | Billing code modifiers (if applicable) |
| `name` | string | Service name |
| `negotiation_arrangement` | string | Arrangement type: "ffs", "bundle", "capitation" |
| `plan_name` | list[string] | Plan names (from index file or MRF root) |
| `plan_id_type` | list[string] | Plan ID types: "EIN" or "HIOS" |
| `plan_id` | list[string] | Plan IDs |
| `plan_market_type` | list[string] | Market types: "group" or "individual" |
| `plan_name_alt` | string | Alternate plan name |
| `network_id` | string | Network identifier |
| `reporting_entity_name` | string | Payer name |
| `reporting_entity_type` | string | Entity type |
| `last_updated_on` | string | File last updated date |
| `version` | string | Schema version |

**Notes:**
- `provider_reference_id` joins to `provider_group_id` in providers file
- `service_codes` may contain `["CSTM-00"]` for "all service locations"
- `plan_name`, `plan_id_type`, `plan_id`, `plan_market_type` are lists (one file can have multiple plans)
- These plan fields are populated from index files or CLI arguments, not always from MRF root

## Multi-Payer Schema Support

The engine automatically detects and handles two MRF schema variants without any configuration.

### Standard Schema (by_reference)

**Used by**: Aetna, UnitedHealthcare, Cigna, most payers

**Structure:**
- Top-level `provider_references` array with integer `provider_group_id` values
- Rates reference providers via `provider_references` field containing integer IDs
- Provider data is separate from rate data

**Processing:**
1. Step 1 extracts all provider references → `providers.parquet`
2. Step 2 uses `provider_reference_id` integers to filter rates
3. Clean join between providers and rates via `provider_group_id` ↔ `provider_reference_id`

**Example:**
```json
{
  "provider_references": [
    {"provider_group_id": 123, "provider_groups": [...]}
  ],
  "in_network": [
    {
      "negotiated_rates": [
        {"provider_references": [123, 456], "negotiated_prices": [...]}
      ]
    }
  ]
}
```

### Inline Schema (inline_groups)

**Used by**: Florida Blue, Blue Cross Blue Shield, some BCBS carriers

**Structure:**
- No top-level `provider_references` array
- Provider data embedded within each `negotiated_rates[*].provider_groups`
- Each rate group contains its own provider information

**Processing:**
1. Step 1 detects absence of `provider_references` in ~1 second and skips
2. Step 2 synthesizes deterministic `provider_group_id` values from embedded provider groups
3. Writes both `providers.parquet` and `rates.parquet` during Step 2
4. Provider IDs are deterministic (same NPI+TIN combination = same ID across files)

**Example:**
```json
{
  "in_network": [
    {
      "negotiated_rates": [
        {
          "provider_groups": [
            {"npi": ["1234567890"], "tin": {"type": "ein", "value": "123456789"}}
          ],
          "negotiated_prices": [...]
        }
      ]
    }
  ]
}
```

**Detection Logic:**
- Engine checks for `provider_references` in MRF root
- If present → Standard schema
- If absent → Inline schema (fast detection, no full file scan needed)

## Performance & Optimization

### Performance Characteristics

**Typical Extraction Times:**
- **Small files** (< 1GB compressed): 5-15 minutes
- **Medium files** (1-5GB compressed): 15-60 minutes
- **Large files** (5-10GB compressed): 1-3 hours
- **Very large files** (10GB+ compressed): 3-8 hours

**Memory Usage:**
- **Peak memory**: 500MB - 2GB (depending on batch sizes)
- **Streaming processing**: Files larger than RAM can be processed
- **Batch sizes**: Default 10K providers, 20K rates (tunable)

**Processing Rates:**
- **Provider extraction**: ~10K-50K providers/second
- **Rate extraction**: ~2K-5K rates/second (varies by filtering)

### Optimization Tips

**1. Batch Size Tuning**

Larger batch sizes = faster processing but more memory:

```bash
# High-memory system (16GB+ RAM)
python -m src.run_extraction "file.json.gz" \
  --provider-batch-size 20000 \
  --rate-batch-size 50000

# Low-memory system (8GB RAM)
python -m src.run_extraction "file.json.gz" \
  --provider-batch-size 5000 \
  --rate-batch-size 10000
```

**2. Filtering Strategy**

Always use filtering to reduce output size:

```bash
# Most efficient: Filter by both providers and CPT codes
python -m src.run_extraction "file.json.gz" \
  --tin-whitelist src/optim_tins.txt \
  --cpt-whitelist src/cpt_codes.txt
```

**3. Parallel Batch Processing**

For multiple files, use parallel workers:

```bash
# 8 parallel workers (adjust based on CPU cores and memory)
python -m src.extraction_orchestrator \
  --source csv \
  --input batch.csv \
  --workers 8 \
  --cpt-whitelist src/cpt_codes.txt
```

**4. Testing Before Full Runs**

Always test with limits before full extraction:

```bash
# Quick test (1000 providers, 5000 rates, 30 minutes max)
python -m src.run_extraction "file.json.gz" \
  --max-providers 1000 \
  --max-items 5000 \
  --max-time 30 \
  --cpt-whitelist src/cpt_codes.txt
```

**5. Storage Optimization**

- Use SSD storage for output directory (faster Parquet writes)
- Ensure sufficient disk space (output files are typically 10-50% of compressed input size)

### Performance Benchmarks

**Florida Blue BCR File** (11GB compressed, 198K items):
- Total extraction time: **~93 minutes**
- Rates extracted: **22.1 million**
- Unique providers: **5,581 groups, 6,399 NPIs**
- Peak memory: **597 MB**
- Processing rate: **~4K rates/second**

**UHC Choice Plus** (varies by state, ~220M rates):
- Total extraction time: **2-4 hours**
- Rates extracted: **~220 million**
- Unique providers: **~37K groups, ~1.1M NPIs**
- Peak memory: **1-2 GB**
- Processing rate: **~15K-20K rates/second**

## Troubleshooting

### Common Issues

**1. Memory Errors**

**Symptoms**: `MemoryError` or system becomes unresponsive

**Solutions**:
- Reduce batch sizes: `--provider-batch-size 5000 --rate-batch-size 10000`
- Add limits: `--max-providers 10000 --max-items 50000`
- Use filtering to reduce data volume
- Close other applications to free memory

**2. Slow Performance**

**Symptoms**: Extraction takes much longer than expected

**Solutions**:
- Increase batch sizes (if memory allows): `--provider-batch-size 20000 --rate-batch-size 50000`
- Use SSD storage for output directory
- Check network speed (for URL-based extractions)
- Use parallel workers for batch extractions: `--workers 4`

**3. No Rates Extracted**

**Symptoms**: Providers file created but rates file is empty or has very few rows

**Possible Causes**:
- CPT whitelist too restrictive (no codes match)
- Provider filter excludes all providers
- File structure different than expected

**Solutions**:
- Check CPT whitelist file exists and has valid codes
- Try extraction without CPT whitelist to verify file structure
- Inspect providers file to ensure provider groups exist
- Check extraction logs for filtering statistics

**4. Schema Detection Issues**

**Symptoms**: "0 providers written" for standard schema files, or errors about missing fields

**Solutions**:
- Check MRF file structure (should have `provider_references` for standard schema)
- Verify file is valid JSON.GZ format
- Check extraction logs for schema detection messages

**5. Network/Download Errors**

**Symptoms**: `requests.exceptions.HTTPError` or connection timeouts

**Solutions**:
- Check URL is accessible
- Retry extraction (network issues are often transient)
- Download file locally first, then use local path
- Check for signed URLs that may have expired

### Error Messages Reference

| Error | Cause | Solution |
|-------|-------|----------|
| `Column 'provider_group_id' not found` | Wrong MRF format or schema detection failed | Verify file structure, check logs |
| `Memory usage high` | Batch sizes too large | Reduce `--provider-batch-size` and `--rate-batch-size` |
| `File not found` | Invalid path or URL | Check file path/URL is correct and accessible |
| `JSONDecodeError` | Corrupted or invalid JSON | Verify file integrity, try re-downloading |
| `ArrowInvalid: Rescaling Decimal value` | Type mismatch in provider_group_id | Should be fixed in current version |
| `Unsupported cast from string to list` | List field contains string | Should be fixed with normalize_list_field |

### Debugging Tips

**1. Enable Verbose Output**

Remove `--quiet` flag to see detailed processing information:

```bash
python -m src.run_extraction "file.json.gz" --cpt-whitelist src/cpt_codes.txt
```

**2. Check Extraction Statistics**

Look for these in the output:
- Provider examination counts
- Filtering statistics (how many filtered vs. written)
- Memory usage tracking
- Processing rates

**3. Inspect Output Files**

Use pandas to quickly inspect output:

```python
import pandas as pd

# Check providers
providers = pd.read_parquet("output/providers_*.parquet")
print(f"Providers: {len(providers):,} rows")
print(f"Unique provider_group_ids: {providers['provider_group_id'].nunique():,}")

# Check rates
rates = pd.read_parquet("output/rates_*.parquet")
print(f"Rates: {len(rates):,} rows")
print(f"Unique billing codes: {rates['billing_code'].nunique():,}")
```

**4. Check Manifest Files**

For batch extractions, check `manifest.json` for:
- Success/failure status
- Error messages
- Duration and statistics
- Output file paths

## Advanced Topics

### Whitelist File Formats

#### CPT Whitelist (`cpt_codes.txt`)

Plain text file with one billing code per line. Codes can be:
- CPT codes: `99213`, `27447`, `29881`
- HCPCS codes: `G0453`, `C9292`
- Other billing codes: `360` (Revenue Code), `A0425` (HCPCS)

Example:
```
99213
99214
99215
27447
29881
G0453
```

**Note**: The whitelist filters by `billing_code` value only, not `billing_code_type`. Code `360` will match whether it's CPT, HCPCS, or RC type.

#### TIN Whitelist (`optim_tins.txt`)

Plain text file with one TIN per line (9 digits for EIN, 10 digits for NPI):

```
123456789
987654321
555666777
```

### Plan Metadata Handling

Plan metadata (`plan_name`, `plan_id`, etc.) can come from three sources:

1. **MRF Index Files**: Extracted automatically by orchestrator
2. **CLI Arguments**: Passed via `--plan-name`, `--plan-id`, etc.
3. **MRF Root**: Sometimes present in MRF file root (rare)

The orchestrator automatically extracts plan metadata from index files and passes it to the extraction workflow.

### Provider Group ID Determinism

**Standard Schema**: `provider_group_id` comes directly from MRF (payer-provided)

**Inline Schema**: `provider_group_id` is synthesized deterministically using:
- NPI list (sorted)
- TIN type and value
- MD5 hash of normalized provider group data

This ensures the same provider group (same NPIs + TIN) gets the same ID across different files, enabling joins and comparisons.

### Network ID Extraction

The `network_id` field is extracted from MRF filenames/URLs using patterns like:
- `pl-29k-hr23` from Aetna filenames
- `GANETWORKEXGN` from UHC filenames
- Custom extraction logic for different payer naming conventions

This identifier helps categorize plans and networks in downstream analysis.

### Long-Running Extractions

For very large extractions (several hours):

1. **Use `caffeinate` on macOS** to prevent sleep:
   ```bash
   caffeinate -d python -m src.run_extraction "file.json.gz" ...
   ```

2. **Monitor progress**: Check output directory for growing parquet files

3. **Use time limits**: Set `--max-time` as a safety net

4. **Check logs**: For batch extractions, monitor `manifest.json` and log files

### Data Quality Considerations

**NPI = "0"**: Some MRF files use `"0"` as a placeholder for TIN-level contracts without NPI information. This is valid per CMS guidelines.

**Expiration Dates**: Many payers use `"9999-12-31"` to indicate "evergreen" rates with no expiration. Treat as valid.

**Service Codes**: `["CSTM-00"]` means "all service locations" (Florida Blue convention), not an unknown location.

**List Fields**: Fields like `service_codes`, `billing_code_modifier`, `plan_name` are stored as lists. Single values are normalized to single-item lists during extraction.

## Support & Contributing

For issues, questions, or contributions:

1. Check extraction logs and error messages
2. Review this README and troubleshooting section
3. Inspect output files to verify data quality
4. Check manifest files for batch extraction status

## License

[Add license information here]

# Rate Extraction Module

This module (`extract_rates.py`) handles rate extraction from Machine-Readable Files (MRF) as part of the two-step extraction workflow. It extracts negotiated rates with efficient filtering by provider groups and CPT codes, and supports both standard and inline MRF schemas.

**Note**: This module is typically called as part of the `run_extraction.py` workflow, not directly. For single-file extractions, use `python -m src.run_extraction`. For batch extractions, use `python -m src.extraction_orchestrator`.

## Quick Start

### Basic Usage
```bash
# Extract all rates from a local file
python extract_rates.py path/to/file.json.gz

# Extract rates from a URL
python extract_rates.py https://example.com/mrf-file.json.gz

# Process with time limit (30 minutes)
python extract_rates.py file.json.gz --time 30

# Process limited number of items
python extract_rates.py file.json.gz --items 10000
```

## Filtering Options

### 1. Filter by Provider Groups from Parquet File (Recommended)
Use the output from `extract_providers.py` to efficiently filter rates:

```bash
# Basic provider group filtering
python extract_rates.py file.json.gz --provider-groups-parquet output/providers_20250108_143022.parquet

# With additional limits
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers_filtered.parquet \
  --items 50000 \
  --time 60
```

### 2. Filter by Specific Provider Group IDs
Manually specify provider group IDs:

```bash
# Single provider group
python extract_rates.py file.json.gz --provider-groups 12345

# Multiple provider groups
python extract_rates.py file.json.gz --provider-groups 12345 67890 11111 22222
```

### 3. Filter by CPT Codes
Create a text file with CPT codes (one per line):

```bash
# Create CPT whitelist
cat > cpt_codes.txt << EOF
99213
99214
99215
27447
29881
EOF

# Run with CPT filtering
python extract_rates.py file.json.gz --cpt-whitelist cpt_codes.txt
```

### 4. Combine Multiple Filters
```bash
# Provider groups from Parquet + manual IDs + CPT codes
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers_filtered.parquet \
  --provider-groups 99999 88888 \
  --cpt-whitelist orthopedic_cpts.txt \
  --items 25000
```

## Command Line Arguments

| Argument | Short | Type | Default | Description |
|----------|-------|------|---------|-------------|
| `source` | - | str | - | **Required.** URL or local path to MRF file |
| `--items` | `-i` | int | None | Maximum number of items to process |
| `--time` | `-t` | int | None | Maximum time to run (minutes) |
| `--provider-groups` | `-p` | int+ | None | Provider group IDs to filter for |
| `--provider-groups-parquet` | `-pp` | str | None | Path to Parquet file with provider_group_id column |
| `--cpt-whitelist` | `-c` | str | None | Path to text file with CPT codes (one per line) |
| `--batch-size` | `-b` | int | 5 | Batch size for writing |

## Usage Examples

### Example 1: Quick Rate Sample
```bash
# Get a sample of rates for analysis
python extract_rates.py file.json.gz --items 1000 --time 5
```

### Example 2: Orthopedic Practice Rates
```bash
# Step 1: Create orthopedic CPT codes file
cat > orthopedic_cpts.txt << EOF
27447
27448
29881
29882
99213
99214
EOF

# Step 2: Extract rates for specific providers and procedures
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/orthopedic_providers.parquet \
  --cpt-whitelist orthopedic_cpts.txt
```

### Example 3: High-Volume Processing
```bash
# Process large file with provider filtering and larger batches
python extract_rates.py large_file.json.gz \
  --provider-groups-parquet output/filtered_providers.parquet \
  --batch-size 20 \
  --time 120
```

### Example 4: Targeted Analysis
```bash
# Focus on specific high-value procedures for select providers
python extract_rates.py file.json.gz \
  --provider-groups 12345 67890 \
  --cpt-whitelist high_cost_procedures.txt \
  --items 5000
```

### Example 5: URL Processing with Filtering
```bash
# Download and process remote file with filtering
python extract_rates.py \
  "https://transparency.example.com/mrf-file.json.gz" \
  --provider-groups-parquet output/target_providers.parquet \
  --time 45
```

## Multi-Payer Schema Support

The tool **automatically handles two MRF schema variants**:

### Standard Schema (by_reference)
- **Payers:** Aetna, UnitedHealthcare, most carriers
- **Structure:** Top-level `provider_references` array
- **Behavior:** Uses existing integer provider IDs for filtering

### Inline Schema (inline_groups)
- **Payers:** Florida Blue, Blue Cross Blue Shield carriers
- **Structure:** Provider data embedded in `negotiated_rates[*].provider_groups`
- **Behavior:** Synthesizes deterministic provider IDs and writes `providers_*.parquet` during rate extraction

**Detection is automatic** - no configuration needed!

## Performance Optimization

### Batch Size Guidelines
- **Light filtering** (many providers): 5-10 (default: 5)
- **Heavy filtering** (few providers): 10-50
- **Memory constrained**: 1-5
- **Inline schema** (Florida Blue): 20K (default for unified pipeline)

### Processing Limits
- **Large files**: Use `--time` to prevent runaway processing
- **Exploration**: Use `--items 1000-10000` for quick analysis
- **Production**: Remove limits or use generous time limits

### Filtering Strategy
1. **Most Efficient**: `--provider-groups-parquet` (pre-filtered providers)
2. **Moderate**: `--provider-groups` (manual provider IDs)
3. **Least Efficient**: `--cpt-whitelist` only (processes all rates first)

**Best Practice**: Always use provider group filtering when possible!

## Pipeline Integration

### Typical Workflow
```bash
# Step 1: Extract and filter providers
python extract_providers.py file.json.gz --tin-whitelist my_tins.txt

# Step 2: Extract rates using provider output (most efficient)
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers_20250108_143022.parquet
```

### Advanced Pipeline
```bash
# Step 1: Extract providers with multiple filters
python extract_providers.py file.json.gz \
  --tin-whitelist target_tins.txt \
  --max-providers 5000

# Step 2: Extract rates with combined filtering
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers_filtered.parquet \
  --cpt-whitelist priority_codes.txt \
  --batch-size 15
```

## Output

### File Location
Files are saved to `output/` directory with timestamps:
```
output/rates_20250108_144511.parquet
```

### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| `provider_reference_id` | int | Provider group reference ID |
| `billing_code` | str | Billing/CPT code |
| `billing_code_type` | str | Type of billing code |
| `description` | str | Service description |
| `negotiated_rate` | float | Negotiated rate amount |
| `negotiated_type` | str | Type of negotiation |
| `billing_class` | str | Billing class |
| `expiration_date` | str | Rate expiration date |
| `service_codes` | str | Additional service codes |
| `name` | str | Service name |
| `negotiation_arrangement` | str | Negotiation arrangement type |
| `reporting_entity_name` | str | Name of reporting entity |
| `reporting_entity_type` | str | Type of reporting entity |
| `last_updated_on` | str | Last update timestamp |
| `version` | str | File version |

## Monitoring & Statistics

The tool provides real-time feedback:

```
💰 EXTRACTING RATES
📊 Initial memory: 45.2 MB
🔍 Added 1,234 provider groups from Parquet file
🎯 Total provider groups to filter for: 1,234
📄 Max items to process: 10,000
⏱️  Max time to run: 30 minutes

✅ RATE EXTRACTION COMPLETE
⏱️  Time elapsed: 156.3 seconds
📊 Items processed: 8,543
📊 Rates generated: 45,231
📊 Rates passed provider filter: 12,876
📊 CPT codes filtered for: 15
📊 Rates written: 12,876
🧠 Peak memory: 78.4 MB
📁 Output: output/rates_20250108_144511.parquet
```

## Troubleshooting

### Common Issues

**No rates extracted:**
- Check that provider groups exist in the MRF file
- Verify Parquet file has `provider_group_id` column
- Try without filtering first to confirm file structure

**Memory errors:**
- Reduce `--batch-size` to 1-3
- Add provider group filtering to reduce data volume
- Use `--items` limit for testing

**Slow processing:**
- Always use `--provider-groups-parquet` when possible
- Avoid CPT-only filtering on large files
- Use `--time` limits for exploration

### Validation Commands

```bash
# Test file accessibility
python extract_rates.py file.json.gz --items 10

# Validate provider groups file
python -c "import pandas as pd; df=pd.read_parquet('output/providers.parquet'); print(f'Groups: {len(df.provider_group_id.unique())}')"

# Quick rate sample
python extract_rates.py file.json.gz --items 100 --time 2
```

## Advanced Usage

### Memory Monitoring
```bash
# Monitor memory usage during processing
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers.parquet \
  --batch-size 10 | grep "memory"
```

### Incremental Processing
```bash
# Process in smaller chunks for very large files
python extract_rates.py file.json.gz --items 25000 --time 30
python extract_rates.py file.json.gz --items 50000 --time 60
# Combine outputs as needed
```

For additional help, check the main repository documentation or examine console output for detailed processing statistics.
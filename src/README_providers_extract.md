# Provider Extraction Tool

This directory contains tools for extracting and processing provider data from Machine-Readable Files (MRF). The main tool is `extract_providers.py`, which efficiently extracts provider references from MRF files with optional filtering capabilities.

## Overview

The `extract_providers.py` script streams through large MRF files and extracts provider information including NPIs, TIN values, and provider group IDs. It supports memory-efficient processing and flexible filtering options.

## Quick Start

### Basic Usage
```bash
# Extract all providers from a local file
python extract_providers.py path/to/file.json.gz

# Extract providers from a URL
python extract_providers.py https://example.com/mrf-file.json.gz

# Limit processing to first 1000 provider groups
python extract_providers.py file.json.gz --max-providers 1000
```

## Filtering Options

### Filter by TIN Values
Create a text file with TIN values (one per line) and use it to filter providers:

```bash
# Create TIN whitelist file
echo "123456789" > my_tins.txt
echo "987654321" >> my_tins.txt
echo "555666777" >> my_tins.txt

# Run extraction with TIN filtering
python extract_providers.py file.json.gz --tin-whitelist my_tins.txt
```

### Filter by Provider Group IDs
Use an existing Parquet file containing `provider_reference_id` column:

```bash
python extract_providers.py file.json.gz --provider-whitelist existing_providers.parquet
```

### Combine Both Filters
```bash
python extract_providers.py file.json.gz \
  --provider-whitelist existing_providers.parquet \
  --tin-whitelist my_tins.txt
```

## Command Line Arguments

| Argument | Short | Type | Description |
|----------|-------|------|-------------|
| `source` | - | str | **Required.** URL or local path to MRF file |
| `--max-providers` | `-m` | int | Maximum number of provider groups to process |
| `--provider-whitelist` | `-p` | str | Path to Parquet file with `provider_reference_id` column |
| `--tin-whitelist` | `-t` | str | Path to text file with TIN values (one per line) |
| `--batch-size` | `-b` | int | Batch size for writing (default: 1000) |

## Output

The script creates a Parquet file in the `output/` directory with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `provider_group_id` | int | Unique identifier for provider group |
| `npi` | str | National Provider Identifier |
| `tin_type` | str | Type of Tax Identification Number |
| `tin_value` | str | Tax Identification Number value |
| `reporting_entity_name` | str | Name of reporting entity |
| `reporting_entity_type` | str | Type of reporting entity |
| `last_updated_on` | str | Last update timestamp |
| `version` | str | File version |

## Examples

### Example 1: Extract Specific TINs for Analysis
```bash
# Create whitelist for specific medical groups
cat > orthopedic_tins.txt << EOF
123456789
234567890
345678901
EOF

# Extract only these providers
python extract_providers.py \
  https://transparency-in-coverage.humana.com/machine-readable/2025-01-01_humana-medical-plan-inc_index.json.gz \
  --tin-whitelist orthopedic_tins.txt \
  --max-providers 5000
```

### Example 2: Incremental Processing
```bash
# First, extract a subset to identify relevant provider groups
python extract_providers.py large_file.json.gz --max-providers 1000

# Analyze the output to create a provider group whitelist
# Then process the full file with filtering
python extract_providers.py large_file.json.gz \
  --provider-whitelist output/providers_20250108_143022.parquet
```

### Example 3: Memory-Efficient Processing
```bash
# For very large files, use smaller batch size
python extract_providers.py huge_file.json.gz \
  --batch-size 500 \
  --tin-whitelist important_tins.txt
```

## Performance Tips

1. **Use Filtering**: Always use TIN or provider group filtering when possible to reduce output size and processing time.

2. **Batch Size**: Adjust `--batch-size` based on available memory:
   - Small files: 1000-5000 (default: 1000)
   - Large files: 500-1000
   - Memory constrained: 100-500

3. **Incremental Processing**: For very large files, process in chunks using `--max-providers` to identify relevant data first.

4. **Local Processing**: Download files locally when processing multiple times to avoid repeated network requests.

## Output Files

Files are saved to the `output/` directory with timestamps:
```
output/
└── providers_20250108_143022.parquet
```

The timestamp format is `YYYYMMDD_HHMMSS`.

## Error Handling

The script handles common errors gracefully:

- **File not found**: Clear error message with suggestions
- **Invalid URLs**: Automatic retry with exponential backoff
- **Memory issues**: Automatic garbage collection and batch writing
- **Invalid whitelist files**: Warning messages with fallback to processing all data

## Integration with Other Tools

The output Parquet files can be used with other tools in this repository:

```bash
# Use provider output as input for rate extraction
python extract_rates.py file.json.gz \
  --provider-groups $(python -c "import pandas as pd; df=pd.read_parquet('output/providers_latest.parquet'); print(' '.join(map(str, df['provider_group_id'].unique())))")
```

## Troubleshooting

### Common Issues

1. **Memory Errors**: Reduce `--batch-size` or use more specific filtering
2. **No Output**: Check that whitelist files contain valid data
3. **Slow Processing**: Ensure filtering is applied; consider `--max-providers` for testing

### Debugging

Enable verbose output by examining the console logs:
- Provider examination counts
- Filtering statistics
- Memory usage tracking
- Batch writing confirmations

For additional help or issues, check the main repository documentation.
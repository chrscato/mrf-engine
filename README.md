# MRF Extraction Tool

A comprehensive tool for extracting provider and rate information from Machine Readable Files (MRF) with NPPES API integration and data cleaning capabilities.

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Basic usage:**
   ```bash
   python src/run_extraction.py "https://example.com/mrf_index.json.gz"
   ```

## Command Line Options

### Required Arguments

- **`mrf_url`** - URL or local path to the MRF index file
  ```bash
  # From URL
  python src/run_extraction.py "https://example.com/mrf_index.json.gz"
  
  # From local file
  python src/run_extraction.py "local_mrf_file.json.gz"
  ```

### Output Options

- **`--output-dir` / `-o`** - Output directory for all files (default: `output`)
  ```bash
  python src/run_extraction.py "mrf_url" --output-dir "my_results"
  python src/run_extraction.py "mrf_url" -o "my_results"
  ```

- **`--output-prefix` / `-op`** - Prefix for output files
  ```bash
  python src/run_extraction.py "mrf_url" --output-prefix "aetna_2024"
  python src/run_extraction.py "mrf_url" -op "aetna_2024"
  ```

### Provider Extraction Options

- **`--max-providers` / `-mp`** - Maximum number of provider references to process
  ```bash
  python src/run_extraction.py "mrf_url" --max-providers 5000
  python src/run_extraction.py "mrf_url" -mp 5000
  ```

- **`--provider-batch-size` / `-pbs`** - Batch size for provider extraction (default: 1000)
  ```bash
  python src/run_extraction.py "mrf_url" --provider-batch-size 500
  python src/run_extraction.py "mrf_url" -pbs 500
  ```

- **`--tin-whitelist` / `-t`** - Path to TIN whitelist file (one TIN per line)
  ```bash
  python src/run_extraction.py "mrf_url" --tin-whitelist "src/my_tins.txt"
  python src/run_extraction.py "mrf_url" -t "src/my_tins.txt"
  ```

### Rate Extraction Options

- **`--max-items` / `-mi`** - Maximum number of rate items to process
  ```bash
  python src/run_extraction.py "mrf_url" --max-items 10000
  python src/run_extraction.py "mrf_url" -mi 10000
  ```

- **`--max-time` / `-mt`** - Maximum time to run rate extraction in minutes
  ```bash
  python src/run_extraction.py "mrf_url" --max-time 120
  python src/run_extraction.py "mrf_url" -mt 120
  ```

- **`--rate-batch-size` / `-rbs`** - Batch size for rate extraction (default: 5)
  ```bash
  python src/run_extraction.py "mrf_url" --rate-batch-size 10
  python src/run_extraction.py "mrf_url" -rbs 10
  ```

- **`--cpt-whitelist` / `-c`** - Path to CPT code whitelist file (one code per line)
  ```bash
  python src/run_extraction.py "mrf_url" --cpt-whitelist "src/cpt_codes.txt"
  python src/run_extraction.py "mrf_url" -c "src/cpt_codes.txt"
  ```

### NPPES API Options

- **`--nppes-workers` / `-nw`** - Number of concurrent NPPES API workers (default: 6)
  ```bash
  python src/run_extraction.py "mrf_url" --nppes-workers 10
  python src/run_extraction.py "mrf_url" -nw 10
  ```

## Complete Examples

### Basic Extraction (All Data)
```bash
python src/run_extraction.py "https://example.com/mrf_index.json.gz"
```

### Filtered Extraction with Custom Output
```bash
python src/run_extraction.py "https://example.com/mrf_index.json.gz" \
  --output-dir "aetna_results" \
  --output-prefix "aetna_2024" \
  --tin-whitelist "src/my_tins.txt" \
  --cpt-whitelist "src/cpt_codes.txt"
```

### Limited Extraction (Testing)
```bash
python src/run_extraction.py "https://example.com/mrf_index.json.gz" \
  --max-providers 1000 \
  --max-items 5000 \
  --max-time 30 \
  --output-dir "test_run"
```

### High-Performance Extraction
```bash
python src/run_extraction.py "https://example.com/mrf_index.json.gz" \
  --provider-batch-size 2000 \
  --rate-batch-size 10 \
  --nppes-workers 12 \
  --output-dir "fast_run"
```

## Output Files

The script generates several output files:

1. **Providers (raw)** - `{prefix}_providers_{timestamp}.parquet`
2. **Providers (cleaned)** - `{prefix}_providers_{timestamp}_cleaned.parquet`
3. **Rates** - `{prefix}_rates_{timestamp}.parquet`
4. **Merged** - `merged_rates_providers_{timestamp}.parquet`

## Workflow Steps

1. **Download MRF Index** - Downloads the MRF file if it's a URL
2. **Extract Providers** - Extracts provider references with optional TIN filtering
3. **Clean Providers** - Filters to NPI-2 providers using NPPES API
4. **Extract Rates** - Extracts rates using cleaned providers as filter
5. **Merge Data** - Combines rates and providers into final dataset

## Whitelist File Formats

### TIN Whitelist (`my_tins.txt`)


### CPT Whitelist (`cpt_codes.txt`)



## Memory and Performance Tips

- **Batch sizes**: Larger batch sizes use more memory but are faster
- **NPPES workers**: More workers = faster NPPES API processing (be mindful of rate limits)
- **Max limits**: Use `--max-providers` and `--max-items` for testing before full runs
- **Output directory**: Use SSD storage for better I/O performance

## Troubleshooting

### Common Issues

1. **Memory errors**: Reduce batch sizes or use max limits
2. **Slow performance**: Increase batch sizes and NPPES workers
3. **File permission errors**: Ensure write access to output directory
4. **NPPES API errors**: Reduce NPPES workers to avoid rate limiting

### Error Messages

- `Column 'provider_group_id' not found`: Ensure you're using the correct MRF format
- `NPPES API errors`: Check internet connection and reduce worker count
- `Memory usage high`: Reduce batch sizes or add memory limits

## Dependencies

- **Python 3.8+**
- **Core packages**: requests, ijson, pyarrow, pandas, numpy, psutil, tqdm
- **Optional**: Additional packages for advanced features

## Support

For issues or questions, check the script output for detailed error messages and ensure all dependencies are properly installed.
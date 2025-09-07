# MRF Extraction Tool

A high-performance tool for extracting provider and rate information from Machine Readable Files (MRF). Optimized for speed with performance improvements through efficient I/O and streaming processing.

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

- **`--provider-batch-size` / `-pbs`** - Batch size for provider extraction (default: 10000)
  ```bash
  python src/run_extraction.py "mrf_url" --provider-batch-size 5000
  python src/run_extraction.py "mrf_url" -pbs 5000
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

- **`--rate-batch-size` / `-rbs`** - Batch size for rate extraction (default: 20000)
  ```bash
  python src/run_extraction.py "mrf_url" --rate-batch-size 10000
  python src/run_extraction.py "mrf_url" -rbs 10000
  ```

- **`--cpt-whitelist` / `-c`** - Path to CPT code whitelist file (one code per line)
  ```bash
  python src/run_extraction.py "mrf_url" --cpt-whitelist "src/cpt_codes.txt"
  python src/run_extraction.py "mrf_url" -c "src/cpt_codes.txt"
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
  --provider-batch-size 10000 \
  --rate-batch-size 20000 \
  --output-dir "fast_run"
```

## Output Files

The script generates two main output files:

1. **Providers** - `{prefix}_providers_{timestamp}.parquet` - Raw provider references from MRF
2. **Rates** - `rates_{timestamp}.parquet` - Extracted rate data with provider filtering

## Workflow Steps

1. **Download MRF Index** - Downloads the MRF file if it's a URL
2. **Extract Providers** - Extracts provider references with optional TIN filtering
3. **Extract Rates** - Extracts rates using raw providers as filter with CPT code filtering

## Whitelist File Formats

### TIN Whitelist (`my_tins.txt`)


### CPT Whitelist (`cpt_codes.txt`)



## Performance Features

- **Speed Improvement**: Optimized I/O using PyArrow ParquetWriter for O(N) complexity
- **Streaming Processing**: Uses ijson for memory-efficient JSON parsing
- **Large Batch Sizes**: Default 20K rate batches for optimal performance
- **Memory Management**: Automatic garbage collection and memory tracking
- **Fast JSON Parsing**: Uses C-extension backend (yajl2_c) when available

## Memory and Performance Tips

- **Batch sizes**: Larger batch sizes use more memory but are significantly faster
- **Max limits**: Use `--max-providers` and `--max-items` for testing before full runs
- **Output directory**: Use SSD storage for better I/O performance
- **Memory monitoring**: Built-in memory tracking shows peak usage

## Troubleshooting

### Common Issues

1. **Memory errors**: Reduce batch sizes or use max limits
2. **Slow performance**: Increase batch sizes for better performance
3. **File permission errors**: Ensure write access to output directory
4. **JSON parsing errors**: Ensure MRF file is valid JSON format

### Error Messages

- `Column 'provider_group_id' not found`: Ensure you're using the correct MRF format
- `Memory usage high`: Reduce batch sizes or add memory limits
- `File not found`: Check MRF URL or file path is correct

## Dependencies

- **Python 3.8+**
- **Core packages**: requests, ijson, pyarrow, pandas, numpy, psutil, tqdm
- **Optional**: Additional packages for advanced features

## Support

For issues or questions, check the script output for detailed error messages and ensure all dependencies are properly installed.
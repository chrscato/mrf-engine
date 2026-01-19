# Batch Orchestrator Design

## Purpose

Unified tool for batch MRF extraction that handles all input types (CSV, index files, URL lists, single URLs) with optional parallelization.

## API Design

### Command-Line Interface

```bash
python -m src.batch_orchestrator \
  --source <csv|index|urls|url> \
  --input <input_file_or_url> \
  [--output-dir <directory>] \
  [--workers <number>] \
  [--cpt-whitelist <file>] \
  [--provider-batch-size <number>] \
  [--rate-batch-size <number>]
```

### Input Sources

1. **CSV** (`--source csv`): CSV file with `file_url` column and optional plan metadata
2. **Index** (`--source index`): MRF index JSON file (parse structures, extract URLs/plans)
3. **URLs** (`--source urls`): Text file with one URL per line
4. **URL** (`--source url`): Single URL string

### Internal Structure

```python
# Standardized task format
Task = {
    'url': str,
    'network_id': str,
    'output_prefix': str,
    'plan_metadata': List[Dict],  # Optional
    'extra_args': Dict  # Optional payer-specific args
}

# Execution engine
execute_batch(
    tasks: List[Task],
    output_dir: Path,
    workers: int = 1,
    cpt_whitelist: str = None,
    **kwargs
) -> Dict[str, Any]  # Returns manifest summary
```

## Input Parsers

### 1. CSV Parser
- Reads CSV with `file_url` column
- Optionally reads plan metadata columns
- Groups plans by URL (one URL can have multiple plans)
- Returns list of tasks

### 2. Index Parser
- Reuses logic from `parse_mrf_index.py`
- Parses MRF index JSON structure
- Extracts URLs and plan metadata from `reporting_structure`
- Generates network IDs from filenames
- Returns list of tasks

### 3. URL List Parser
- Reads text file line-by-line
- Validates URLs
- Extracts network IDs from filenames
- Returns list of tasks

### 4. Single URL Parser
- Takes URL string directly
- Extracts network ID
- Returns single task

## Execution Engine

- Handles both sequential and parallel execution
- Manages logging (console vs files based on workers)
- Tracks manifest incrementally (sequential) or at end (parallel)
- Returns results summary

## Logging Strategy

- **Sequential (workers=1)**: Detailed output to console
- **Parallel (workers>1)**: Summary to console, detailed logs to `logs/{timestamp}/` files

## Manifest Format

```json
{
  "extraction_run": "ISO timestamp",
  "source_type": "csv|index|urls|url",
  "source_input": "input file/URL",
  "total_tasks": 22,
  "extractions": [
    {
      "url": "...",
      "network_id": "...",
      "status": "success|failed",
      "duration_seconds": 1234.5,
      "output_files": {...},
      "log_file": "logs/.../...log"  // Only in parallel mode
    }
  ],
  "summary": {
    "successful": 20,
    "failed": 2
  }
}
```

## Integration Points

- Uses `src.run_extraction` as subprocess (same as current scripts)
- Reuses `parse_mrf_index.py` parsing logic (can import or copy)
- Reuses plan metadata handling from existing batch scripts


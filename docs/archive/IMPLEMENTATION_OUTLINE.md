# Parallelization Implementation Outline
## batch_extract_aetna_national.py

## CHANGE 1: Add imports
**Location:** Top of file (after existing imports)

```python
from concurrent.futures import ProcessPoolExecutor
import argparse  # Add this if not already imported
```

---

## CHANGE 2: Add CLI argument parsing
**Location:** At start of `main()` function

**Current:** No argument parsing (hardcoded)

**New:**
```python
def main():
    parser = argparse.ArgumentParser(description='Batch extract Aetna national MRF endpoints')
    parser.add_argument('--workers', type=int, default=1,
                       help='Number of parallel workers (default: 1 = sequential)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed output in console when using parallel workers')
    args = parser.parse_args()
    
    workers = args.workers
    verbose = args.verbose
```

---

## CHANGE 3: Extract extraction logic into function
**Location:** Before `main()` function

**New function:**
```python
def process_one_extraction(task_info):
    """Process a single extraction. Returns result dict.
    
    Args:
        task_info: Tuple of (index, url, network_id, plans, prefix, output_dir, 
                   cpt_whitelist, log_dir, workers, verbose)
    
    Returns:
        Dict with extraction results
    """
    (i, url, network_id, plans, prefix, output_dir, cpt_whitelist, 
     log_dir, workers, verbose) = task_info
    
    # Extract filename
    filename = url.split('/')[-1].split('?')[0]
    
    # Build command (same as current code, lines 130-149)
    cmd = [
        sys.executable, '-m', 'src.run_extraction',
        url,
        '--output-dir', str(output_dir),
        '--output-prefix', prefix,
        '--cpt-whitelist', cpt_whitelist,
        '--provider-batch-size', '20000',
        '--rate-batch-size', '50000',
        '--network-id', network_id
    ]
    
    # Add plan metadata (same as current code, lines 143-149)
    for plan in plans:
        cmd.extend(['--plan-name', plan['plan_name']])
        cmd.extend(['--plan-id-type', plan['plan_id_type']])
        cmd.extend(['--plan-id', plan['plan_id']])
        cmd.extend(['--plan-market-type', plan['plan_market_type']])
    
    # Initialize result dict
    result_dict = {
        'index': i,
        'url': url,
        'filename': filename,
        'network_id': network_id,
        'prefix': prefix,
        'plans': plans,
        'success': False,
        'duration_seconds': None,
        'extraction_timestamp': None,
        'error': None,
        'output_files': {'providers': None, 'rates': None},
        'log_file': None
    }
    
    # Determine logging behavior
    log_file_path = None
    if workers > 1:
        # Parallel mode: write to log file
        log_file_path = log_dir / f'{network_id}_extraction.log'
        log_file_handle = open(log_file_path, 'w')
        stdout_dest = log_file_handle
        stderr_dest = log_file_handle
    else:
        # Sequential mode: print to console
        stdout_dest = None  # Don't capture, let it print
        stderr_dest = subprocess.PIPE  # Capture for error detection
    
    # Run extraction
    start_time = datetime.now()
    try:
        result = subprocess.run(
            cmd,
            stdout=stdout_dest,
            stderr=stderr_dest,
            text=True
        )
        
        if workers > 1:
            log_file_handle.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result_dict['duration_seconds'] = duration
        result_dict['extraction_timestamp'] = start_time.isoformat()
        
        if result.returncode == 0:
            result_dict['success'] = True
            
            # Find output files (same as current code, lines 177-186)
            provider_pattern = f"providers_{prefix}_*.parquet"
            rates_pattern = f"rates_{prefix}_*.parquet"
            
            provider_files = list(output_dir.glob(provider_pattern))
            rates_files = list(output_dir.glob(rates_pattern))
            
            if provider_files:
                result_dict['output_files']['providers'] = provider_files[0].name
            if rates_files:
                result_dict['output_files']['rates'] = rates_files[0].name
        else:
            result_dict['success'] = False
            result_dict['error'] = result.stderr[:500] if result.stderr else 'Unknown error'
    
    except Exception as e:
        if workers > 1 and log_file_handle:
            log_file_handle.close()
        result_dict['success'] = False
        result_dict['error'] = str(e)[:200]
        result_dict['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        result_dict['extraction_timestamp'] = start_time.isoformat()
    
    if log_file_path:
        result_dict['log_file'] = str(log_file_path.relative_to(output_dir))
    
    return result_dict
```

---

## CHANGE 4: Set up log directory structure
**Location:** In `main()`, after creating output_dir and before the extraction loop

**New code:**
```python
# Create output directory
output_dir.mkdir(parents=True, exist_ok=True)

# Create log directory structure (for parallel mode)
log_dir = None
if workers > 1:
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = output_dir / 'logs' / run_timestamp
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"📝 Log directory: logs/{run_timestamp}/")
```

---

## CHANGE 5: Refactor main extraction loop
**Location:** Replace current `for i, url in enumerate(urls, 1):` loop (lines 104-204)

**Current:** Single loop with inline subprocess.run()

**New:** Two paths - sequential vs parallel

```python
# Prepare task list
tasks = []
for i, url in enumerate(urls, 1):
    filename = url.split('/')[-1].split('?')[0]
    network_id = extract_network_id(filename) or 'unknown'
    prefix = f"aetna_national_{network_id}"
    plans = plan_metadata.get(url, [])
    
    task_info = (
        i, url, network_id, plans, prefix, output_dir, 
        cpt_whitelist, log_dir, workers, verbose
    )
    tasks.append(task_info)

# Execute extractions
if workers == 1:
    # SEQUENTIAL MODE (current behavior)
    print("\n" + "="*60)
    print("RUNNING EXTRACTIONS (Sequential)")
    print("="*60)
    
    results = []
    for task in tasks:
        i, url, network_id, plans, prefix = task[:5]
        
        print(f"\n[{i}/{len(urls)}] {network_id}")
        print(f"   URL: {url.split('/')[-1][:70]}...")
        if plans:
            print(f"   Plans: {len(plans)} plan(s) - {', '.join([p['plan_id'] for p in plans])}")
        else:
            print(f"   ⚠️  No plan metadata found for this URL")
        
        # Process extraction (prints to console)
        result = process_one_extraction(task)
        results.append(result)
        
        # Show result
        if result['success']:
            print(f"   ✅ Success ({result['duration_seconds']:.1f}s)")
            successful += 1
        else:
            print(f"   ❌ Failed")
            failed += 1
        
        # Update manifest incrementally (same as before)
        manifest_entry = {
            'source_url': result['url'],
            'filename': result['filename'],
            'output_prefix': result['prefix'],
            'network_id': result['network_id'],
            'extraction_timestamp': result['extraction_timestamp'],
            'status': 'success' if result['success'] else 'failed',
            'output_files': result['output_files'],
            'error': result['error'],
            'duration_seconds': result['duration_seconds'],
            'plans': result['plans'],
            'log_file': result.get('log_file')
        }
        manifest['extractions'].append(manifest_entry)
        
        # Save manifest incrementally
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)

else:
    # PARALLEL MODE (new)
    print("\n" + "="*60)
    print(f"RUNNING EXTRACTIONS (Parallel - {workers} workers)")
    print("="*60)
    
    # Process all tasks in parallel
    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(process_one_extraction, tasks))
    
    # Aggregate results and update manifest
    for result in results:
        i = result['index']
        
        # Console summary (clean output)
        if result['success']:
            duration = result['duration_seconds']
            log_file = result.get('log_file', '')
            log_msg = f" | Log: {log_file}" if log_file else ""
            print(f"✅ [{i}/{len(urls)}] {result['network_id']} completed ({duration:.1f}s){log_msg}")
            successful += 1
        else:
            log_file = result.get('log_file', '')
            log_msg = f" | Log: {log_file}" if log_file else ""
            print(f"❌ [{i}/{len(urls)}] {result['network_id']} failed{log_msg}")
            failed += 1
        
        # Add to manifest
        manifest_entry = {
            'source_url': result['url'],
            'filename': result['filename'],
            'output_prefix': result['prefix'],
            'network_id': result['network_id'],
            'extraction_timestamp': result['extraction_timestamp'],
            'status': 'success' if result['success'] else 'failed',
            'output_files': result['output_files'],
            'error': result['error'],
            'duration_seconds': result['duration_seconds'],
            'plans': result['plans'],
            'log_file': result.get('log_file')
        }
        manifest['extractions'].append(manifest_entry)
    
    # Sort manifest by index (since parallel execution order may differ)
    manifest['extractions'].sort(key=lambda x: next(
        (t['index'] for t in tasks if t[1] == x['source_url']), 0
    ))
    
    # Write manifest once at end (parallel mode)
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
```

---

## CHANGE 6: Update final summary output
**Location:** After extraction loop, before final manifest save

**Current:** Always shows same summary

**New:** Include log directory info if parallel mode was used

```python
# Final summary
print("\n" + "="*60)
print("EXTRACTION SUMMARY")
print("="*60)
print(f"\n✅ Successful: {successful}/{len(urls)}")
print(f"❌ Failed: {failed}/{len(urls)}")
print(f"\n📁 Output directory: {output_dir}/")
print(f"📋 Manifest file: {manifest_file}")

if workers > 1 and log_dir:
    print(f"📝 Log directory: {log_dir.relative_to(Path.cwd())}/")
```

---

## CHANGE 7: Update manifest schema (optional)
**Location:** Manifest initialization

**No code change needed**, but manifest will now include:
- `log_file` field (path to log file, relative to output_dir)
- Only populated when `workers > 1`

---

## SUMMARY OF CHANGES

### Files Modified:
- `batch_extract_aetna_national.py` (one file)

### Lines Changed:
- ~50 lines refactored (extract function)
- ~80 lines added (parallel execution logic)
- ~10 lines added (CLI args, log dir setup)
- **Total: ~140 lines changed/added**

### Backward Compatibility:
- ✅ Default `--workers=1` preserves current sequential behavior
- ✅ All existing functionality unchanged when sequential
- ✅ No changes to core extraction code (`src/run_extraction.py`)

### New Features:
- ✅ `--workers N` flag for parallel execution
- ✅ `--verbose` flag (for future use, currently not used)
- ✅ Automatic log directory creation (`logs/{timestamp}/`)
- ✅ Per-extraction log files in parallel mode
- ✅ Clean console summary in parallel mode

### Testing Checklist:
1. Test sequential mode (`--workers 1` or default) - should match current behavior
2. Test parallel mode (`--workers 2`) - verify log files created
3. Test parallel mode (`--workers 4`) - verify all extractions complete
4. Verify manifest includes log_file paths
5. Verify log files contain full detailed output
6. Verify console output is clean in parallel mode


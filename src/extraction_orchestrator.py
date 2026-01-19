#!/usr/bin/env python3
"""
Unified MRF extraction orchestrator.

Handles batch extraction from multiple input sources:
- CSV files with plan metadata
- MRF index JSON files (extracts endpoints + metadata)
- URL list text files
- Single URLs

Supports both sequential and parallel execution with optional logging to files.

This provides a single entry point for all MRF extraction scenarios, replacing
ad-hoc batch scripts and providing consistent execution behavior.
"""

import argparse
import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_state_and_payer(entity_name: str) -> Tuple[str, str]:
    """
    Extract state and payer from reporting entity name.
    
    Handles common naming patterns:
    - "Aetna Health Inc. - Florida" → ("fl", "aetna")
    - "UnitedHealthcare-of-Georgia-Inc" → ("ga", "uhc")
    - "FloridaBlue" → ("fl", "floridablue")
    
    Args:
        entity_name: Reporting entity name from index file
    
    Returns:
        Tuple of (state_code, payer_name) - both lowercase, state is 2 chars
    """
    entity_lower = entity_name.lower()
    
    # Extract state from various patterns
    if ' - ' in entity_name:
        # Pattern: "Company Name - State"
        state = entity_name.split(' - ')[-1].lower()
    elif '-of-' in entity_lower:
        # Pattern: "Company-of-State-Inc"
        parts = entity_lower.split('-of-')
        state = parts[1].split('-')[0] if len(parts) > 1 else "unknown"
    elif 'floridablue' in entity_lower or 'florida blue' in entity_lower:
        state = "florida"
    else:
        state = "unknown"
    
    # Detect payer from entity name
    if 'aetna' in entity_lower:
        payer = 'aetna'
    elif 'united' in entity_lower or 'uhc' in entity_lower:
        payer = 'uhc'
    elif 'floridablue' in entity_lower or 'florida blue' in entity_lower:
        payer = 'floridablue'
    else:
        payer = 'payer'
    
    return state[:2], payer


def extract_network_id(url: str) -> str:
    """
    Extract unique network identifier from MRF URL filename.
    
    Handles multiple payer-specific filename patterns:
    - FloridaBlue: "2025-08_020_02E0_in-network-rates_1_of_5.json.gz" → "020_02E0"
    - UHC: "UHC-Vision_in-network-rates.json.gz" → "UHC-Vision"
    - Aetna: "2025-10-05_pl-3mk-hr23_Aetna-Health.json.gz" → "pl-3mk-hr23"
    
    Args:
        url: Full URL or filename to extract network ID from
    
    Returns:
        Network identifier string (e.g., "pl-12e-hr23", "020_02E0")
    """
    # Extract filename from URL
    filename = url.split('/')[-1].split('?')[0].replace('.json.gz', '')
    
    if '_in-network-rates' in filename:
        # Pattern used by FloridaBlue, BCBS, UHC
        base = filename.split('_in-network-rates')[0]
        
        # Remove date prefix (YYYY-MM or YYYY-MM-DD format)
        if base.startswith(('2025-', '2024-', '2026-')):
            base = '_'.join(base.split('_')[1:])
        
        # FloridaBlue pattern: Look for XXX_YYYY (3 digits, underscore, 4 alphanumeric)
        match = re.search(r'\b(\d{3}_\w{4})\b', base)
        if match:
            return match.group(1)
        
        # FloridaBlue_XXX pattern: extract suffix only
        if 'FloridaBlue_' in base:
            return base.split('FloridaBlue_')[-1]
        
        # UHC-style or other: return cleaned base
        return base if base else 'network'
    else:
        # Aetna pattern: first non-date part (e.g., pl-XXX-hr23)
        parts = [p for p in filename.split('_') if p and not p.startswith('20')]
        return parts[0] if parts else 'network'


# ============================================================================
# INPUT PARSERS
# ============================================================================

def parse_csv_input(csv_path: str, output_prefix: Optional[str] = None) -> Tuple[List[Dict], str]:
    """
    Parse CSV file with file_url column and optional plan metadata.
    
    Expected CSV format:
    - Required: file_url column with MRF file URLs
    - Optional: plan_name, plan_id_type, plan_id, plan_market_type columns
    
    Multiple plans can share the same URL (multiple rows with same file_url).
    
    Args:
        csv_path: Path to CSV file
        output_prefix: Optional prefix for output files (defaults to 'extracted')
    
    Returns:
        Tuple of (list of task dictionaries, inferred prefix)
    
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV lacks required columns or has no URLs
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    url_to_plans = defaultdict(list)
    unique_urls = []
    seen_urls = set()
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Validate required column
        if 'file_url' not in reader.fieldnames:
            raise ValueError(f"CSV must have 'file_url' column. Found: {reader.fieldnames}")
        
        for row in reader:
            url = row['file_url'].strip()
            if not url:
                continue
            
            # Extract plan metadata if available
            if 'plan_name' in reader.fieldnames:
                plan = {
                    'plan_name': row.get('plan_name', '').strip(),
                    'plan_id_type': row.get('plan_id_type', '').strip(),
                    'plan_id': row.get('plan_id', '').strip(),
                    'plan_market_type': row.get('plan_market_type', '').strip(),
                }
                # Only add plan if it has at least a plan_id
                if plan.get('plan_id'):
                    url_to_plans[url].append(plan)
            
            # Track unique URLs (preserve order)
            if url not in seen_urls:
                unique_urls.append(url)
                seen_urls.add(url)
    
    if not unique_urls:
        raise ValueError(f"No URLs found in CSV file: {csv_path}")
    
    # Generate tasks from URLs
    tasks = []
    default_prefix = output_prefix or 'extracted'
    
    for url in unique_urls:
        filename = url.split('/')[-1].split('?')[0]
        network_id = extract_network_id(url) or 'unknown'
        prefix = f"{default_prefix}_{network_id}"
        
        task = {
            'url': url,
            'filename': filename,
            'network_id': network_id,
            'output_prefix': prefix,
            'plan_metadata': url_to_plans.get(url, []),
            'extra_args': {}
        }
        tasks.append(task)
    
    return tasks, default_prefix


def parse_index_input(index_path: str, state_prefix: Optional[str] = None,
                     structures_filter: Optional[List[int]] = None,
                     max_files: Optional[int] = None) -> Tuple[List[Dict], str]:
    """
    Parse MRF index JSON file to extract URLs and plan metadata.
    
    Index files follow CMS transparency schema with reporting_structures containing:
    - reporting_plans: List of plans with metadata (plan_name, plan_id, etc.)
    - in_network_files: List of file locations (URLs)
    
    Each structure can have multiple plans and multiple files. Each file in a structure
    is associated with all plans in that structure.
    
    Args:
        index_path: Path to MRF index JSON file
        state_prefix: Optional prefix (auto-detected from entity name if not provided)
        structures_filter: Optional list of structure indices to process (1-based)
        max_files: Optional limit on files per structure
    
    Returns:
        Tuple of (list of task dictionaries, inferred state_prefix)
    
    Raises:
        FileNotFoundError: If index file doesn't exist
    """
    index_file = Path(index_path)
    if not index_file.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}")
    
    with open(index_file, 'r') as f:
        data = json.load(f)
    
    # Auto-generate prefix from reporting entity if not provided
    if not state_prefix:
        entity_name = data.get('reporting_entity_name', '')
        state, payer = extract_state_and_payer(entity_name)
        state_prefix = f"{state}_{payer}"
    
    all_structures = data.get('reporting_structure', [])
    
    # Filter structures if specified (1-based indexing for user-friendliness)
    if structures_filter:
        structures_to_process = [
            (idx, all_structures[idx-1])
            for idx in structures_filter
            if 0 < idx <= len(all_structures)
        ]
    else:
        structures_to_process = list(enumerate(all_structures, 1))
    
    tasks = []
    
    for struct_idx, structure in structures_to_process:
        plans = structure.get('reporting_plans', [])
        in_network_files = structure.get('in_network_files', [])
        
        if not in_network_files:
            continue  # Skip structures with no files
        
        structure_id = f"{state_prefix}_{struct_idx}"
        
        # Apply max_files limit if specified (useful for testing)
        files_to_process = in_network_files[:max_files] if max_files else in_network_files
        
        for file_info in files_to_process:
            url = file_info.get('location', '')
            if not url:
                continue
            
            filename = url.split('/')[-1].split('?')[0]
            network_id = extract_network_id(url)
            
            # Convert plan metadata to standard format
            # All plans in this structure are associated with each file
            plan_metadata = []
            for plan in plans:
                plan_metadata.append({
                    'plan_name': plan.get('plan_name', ''),
                    'plan_id_type': plan.get('plan_id_type', ''),
                    'plan_id': plan.get('plan_id', ''),
                    'plan_market_type': plan.get('plan_market_type', ''),
                })
            
            task = {
                'url': url,
                'filename': filename,
                'network_id': network_id,
                'output_prefix': f"{state_prefix}_{network_id}",
                'plan_metadata': plan_metadata,
                'extra_args': {
                    'structure_id': structure_id,
                }
            }
            tasks.append(task)
    
    return tasks, state_prefix


def parse_url_list_input(txt_path: str, output_prefix: str = 'extracted') -> List[Dict]:
    """
    Parse text file with one URL per line.
    
    Simple format - one URL per line, empty lines and comments (starting with #) are skipped.
    URLs are deduplicated automatically.
    
    Args:
        txt_path: Path to text file with URLs
        output_prefix: Prefix for output files
    
    Returns:
        List of task dictionaries
    
    Raises:
        FileNotFoundError: If text file doesn't exist
        ValueError: If file has no valid URLs
    """
    txt_file = Path(txt_path)
    if not txt_file.exists():
        raise FileNotFoundError(f"URL list file not found: {txt_path}")
    
    tasks = []
    seen_urls = set()
    
    with open(txt_file, 'r') as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith('#'):
                continue  # Skip empty lines and comments
            
            # Basic URL validation
            if not url.startswith(('http://', 'https://')):
                continue
            
            # Deduplicate
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
            filename = url.split('/')[-1].split('?')[0]
            network_id = extract_network_id(url) or 'unknown'
            prefix = f"{output_prefix}_{network_id}"
            
            task = {
                'url': url,
                'filename': filename,
                'network_id': network_id,
                'output_prefix': prefix,
                'plan_metadata': [],
                'extra_args': {}
            }
            tasks.append(task)
    
    if not tasks:
        raise ValueError(f"No valid URLs found in file: {txt_path}")
    
    return tasks


def parse_single_url_input(url: str, output_prefix: str = 'extracted') -> Dict:
    """
    Parse single URL input.
    
    Args:
        url: Single MRF file URL
        output_prefix: Prefix for output files
    
    Returns:
        Single task dictionary
    """
    filename = url.split('/')[-1].split('?')[0]
    network_id = extract_network_id(url) or 'unknown'
    prefix = f"{output_prefix}_{network_id}"
    
    return {
        'url': url,
        'filename': filename,
        'network_id': network_id,
        'output_prefix': prefix,
        'plan_metadata': [],
        'extra_args': {}
    }


# ============================================================================
# EXECUTION ENGINE
# ============================================================================

def _process_extraction_task(exec_task_tuple: Tuple) -> Dict:
    """
    Helper function to unpack execution task tuple and call process_one_extraction.
    
    This is a module-level function (not a lambda) so it can be pickled by
    ProcessPoolExecutor for parallel execution.
    
    Args:
        exec_task_tuple: Tuple of (task, output_dir, cpt_whitelist, provider_batch_size,
                                 rate_batch_size, log_dir, workers)
    
    Returns:
        Result dictionary from process_one_extraction
    """
    return process_one_extraction(*exec_task_tuple)


def process_one_extraction(task: Dict, output_dir: Path, cpt_whitelist: Optional[str],
                          provider_batch_size: int, rate_batch_size: int,
                          log_dir: Optional[Path], workers: int) -> Dict:
    """
    Process a single extraction task.
    
    This function is designed to work in both sequential and parallel modes.
    In parallel mode, each worker process calls this function independently.
    
    Args:
        task: Task dictionary with url, network_id, plan_metadata, etc.
        output_dir: Output directory for Parquet files
        cpt_whitelist: Path to CPT code whitelist file (optional)
        provider_batch_size: Batch size for provider extraction
        rate_batch_size: Batch size for rate extraction
        log_dir: Directory for log files (only used in parallel mode)
        workers: Number of workers (determines logging behavior)
    
    Returns:
        Dictionary with extraction results (success status, duration, output files, errors)
    """
    url = task['url']
    network_id = task['network_id']
    prefix = task['output_prefix']
    plans = task.get('plan_metadata', [])
    extra_args = task.get('extra_args', {})
    
    # Initialize result dictionary
    result = {
        'url': url,
        'filename': task['filename'],
        'network_id': network_id,
        'output_prefix': prefix,
        'success': False,
        'duration_seconds': None,
        'extraction_timestamp': None,
        'error': None,
        'output_files': {'providers': None, 'rates': None},
        'plan_metadata': plans,
        'log_file': None
    }
    
    # Build extraction command - same arguments regardless of mode
    cmd = [
        sys.executable, '-m', 'src.run_extraction',
        url,
        '--output-dir', str(output_dir),
        '--output-prefix', prefix,
        '--provider-batch-size', str(provider_batch_size),
        '--rate-batch-size', str(rate_batch_size),
        '--network-id', network_id,
    ]
    
    # Add CPT whitelist if provided
    if cpt_whitelist:
        cmd.extend(['--cpt-whitelist', cpt_whitelist])
    
    # Add plan metadata (multiple plans can share one URL)
    for plan in plans:
        if plan.get('plan_name'):
            cmd.extend(['--plan-name', plan['plan_name']])
        if plan.get('plan_id_type'):
            cmd.extend(['--plan-id-type', plan['plan_id_type']])
        if plan.get('plan_id'):
            cmd.extend(['--plan-id', plan['plan_id']])
        if plan.get('plan_market_type'):
            cmd.extend(['--plan-market-type', plan['plan_market_type']])
    
    # Add extra arguments (e.g., structure_id from index files)
    if 'structure_id' in extra_args:
        cmd.extend(['--structure-id', extra_args['structure_id']])
    
    # Determine output destination based on execution mode
    # Sequential: print to console | Parallel: write to log file
    log_file_path = None
    log_file_handle = None
    
    if workers > 1 and log_dir:
        # Parallel mode: write all output to log file
        log_file_path = log_dir / f'{network_id}_extraction.log'
        log_file_handle = open(log_file_path, 'w')
        stdout_dest = log_file_handle
        stderr_dest = log_file_handle
    else:
        # Sequential mode: print to console, capture stderr for errors
        stdout_dest = None  # Let stdout print normally
        stderr_dest = subprocess.PIPE  # Capture stderr for error detection
    
    # Run extraction subprocess
    start_time = datetime.now()
    try:
        subprocess_result = subprocess.run(
            cmd,
            stdout=stdout_dest,
            stderr=stderr_dest,
            text=True
        )
        
        if log_file_handle:
            log_file_handle.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        result['duration_seconds'] = duration
        result['extraction_timestamp'] = start_time.isoformat()
        
        if subprocess_result.returncode == 0:
            result['success'] = True
            
            # Find generated output files
            provider_pattern = f"providers_{prefix}_*.parquet"
            rates_pattern = f"rates_{prefix}_*.parquet"
            
            provider_files = list(output_dir.glob(provider_pattern))
            rates_files = list(output_dir.glob(rates_pattern))
            
            if provider_files:
                result['output_files']['providers'] = provider_files[0].name
            if rates_files:
                result['output_files']['rates'] = rates_files[0].name
        else:
            result['success'] = False
            error_msg = subprocess_result.stderr[:500] if subprocess_result.stderr else 'Unknown error'
            result['error'] = error_msg
    
    except Exception as e:
        if log_file_handle:
            log_file_handle.close()
        result['success'] = False
        result['error'] = str(e)[:200]  # Truncate long error messages
        result['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        result['extraction_timestamp'] = start_time.isoformat()
    
    # Store relative path to log file (for manifest)
    if log_file_path:
        result['log_file'] = str(log_file_path.relative_to(output_dir))
    
    return result


def execute_batch(tasks: List[Dict], output_dir: Path, workers: int = 1,
                 cpt_whitelist: Optional[str] = None,
                 provider_batch_size: int = 20000,
                 rate_batch_size: int = 50000) -> Dict:
    """
    Execute batch of extraction tasks with optional parallelization.
    
    Handles both sequential (workers=1) and parallel (workers>1) execution.
    - Sequential: Detailed output goes to console, manifest saved incrementally
    - Parallel: Detailed output goes to log files, manifest written once at end
    
    Args:
        tasks: List of task dictionaries to execute
        output_dir: Output directory for Parquet files
        workers: Number of parallel workers (1 = sequential)
        cpt_whitelist: Optional path to CPT code whitelist
        provider_batch_size: Batch size for provider extraction
        rate_batch_size: Batch size for rate extraction
    
    Returns:
        Dictionary with execution summary (successful count, failed count, manifest path)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = output_dir / 'manifest.json'
    
    # Set up log directory for parallel mode
    # Each run gets a timestamped subdirectory to avoid conflicts
    log_dir = None
    run_timestamp = None
    if workers > 1:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_dir = output_dir / 'logs' / run_timestamp
        log_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize manifest
    manifest = {
        'extraction_run': datetime.now().isoformat(),
        'total_tasks': len(tasks),
        'workers': workers,
        'extractions': []
    }
    
    successful = 0
    failed = 0
    
    # Prepare tasks for execution (add execution parameters)
    execution_tasks = []
    for task in tasks:
        exec_task = (task, output_dir, cpt_whitelist, provider_batch_size,
                    rate_batch_size, log_dir, workers)
        execution_tasks.append(exec_task)
    
    # Execute based on mode
    if workers == 1:
        # SEQUENTIAL MODE: Process one at a time with detailed console output
        # This preserves the original behavior with full logging in console
        print("\n" + "="*60)
        print("RUNNING EXTRACTIONS (Sequential)")
        print("="*60)
        
        for i, exec_task in enumerate(execution_tasks, 1):
            task, _, _, _, _, _, _ = exec_task
            network_id = task['network_id']
            plans = task.get('plan_metadata', [])
            
            print(f"\n[{i}/{len(tasks)}] {network_id}")
            print(f"   URL: {task['filename'][:70]}...")
            if plans:
                plan_ids = ', '.join([p.get('plan_id', 'N/A') for p in plans if p.get('plan_id')])
                print(f"   Plans: {len(plans)} plan(s) - {plan_ids}")
            
            # Process extraction (output prints to console)
            result = process_one_extraction(*exec_task)
            
            # Show result and update counters
            if result['success']:
                print(f"   ✅ Success ({result['duration_seconds']:.1f}s)")
                successful += 1
            else:
                print(f"   ❌ Failed")
                failed += 1
            
            # Build manifest entry
            manifest_entry = {
                'source_url': result['url'],
                'filename': result['filename'],
                'output_prefix': result['output_prefix'],
                'network_id': result['network_id'],
                'extraction_timestamp': result['extraction_timestamp'],
                'status': 'success' if result['success'] else 'failed',
                'output_files': result['output_files'],
                'error': result['error'],
                'duration_seconds': result['duration_seconds'],
                'plan_metadata': result['plan_metadata']
            }
            manifest['extractions'].append(manifest_entry)
            
            # Save manifest incrementally (allows progress tracking)
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f, indent=2)
    
    else:
        # PARALLEL MODE: Process multiple extractions concurrently
        # Detailed output goes to log files, console shows clean summary
        print("\n" + "="*60)
        print(f"RUNNING EXTRACTIONS (Parallel - {workers} workers)")
        print("="*60)
        if log_dir:
            print(f"📝 Log directory: logs/{run_timestamp}/")
        
        # Execute all tasks in parallel - ProcessPoolExecutor manages worker pool
        # Use module-level function instead of lambda for pickling compatibility
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(
                _process_extraction_task,
                execution_tasks
            ))
        
        # Aggregate results and build manifest entries
        # Note: Results preserve order from executor.map() even though execution is parallel
        for i, result in enumerate(results, 1):
            # Show clean summary in console (detailed logs are in files)
            if result['success']:
                duration = result['duration_seconds']
                log_file = result.get('log_file', '')
                log_msg = f" | Log: {log_file}" if log_file else ""
                print(f"✅ [{i}/{len(tasks)}] {result['network_id']} completed ({duration:.1f}s){log_msg}")
                successful += 1
            else:
                log_file = result.get('log_file', '')
                log_msg = f" | Log: {log_file}" if log_file else ""
                print(f"❌ [{i}/{len(tasks)}] {result['network_id']} failed{log_msg}")
                failed += 1
            
            # Build manifest entry
            manifest_entry = {
                'source_url': result['url'],
                'filename': result['filename'],
                'output_prefix': result['output_prefix'],
                'network_id': result['network_id'],
                'extraction_timestamp': result['extraction_timestamp'],
                'status': 'success' if result['success'] else 'failed',
                'output_files': result['output_files'],
                'error': result['error'],
                'duration_seconds': result['duration_seconds'],
                'plan_metadata': result['plan_metadata'],
                'log_file': result.get('log_file')  # Only populated in parallel mode
            }
            manifest['extractions'].append(manifest_entry)
        
        # Write manifest once at end (no need for incremental writes in parallel mode)
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
    
    # Add summary to manifest (same format for both modes)
    manifest['summary'] = {
        'successful': successful,
        'failed': failed,
        'total': len(tasks)
    }
    
    # Final manifest write (includes summary)
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    return {
        'successful': successful,
        'failed': failed,
        'total': len(tasks),
        'manifest_file': str(manifest_file)
    }


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point for extraction orchestrator."""
    parser = argparse.ArgumentParser(
        description='Unified MRF extraction orchestrator - handles CSV, index files, URL lists, and single URLs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # CSV-based batch extraction
  python -m src.extraction_orchestrator --source csv --input aetna_national_plans.csv --output-dir output/aetna_national --workers 4

  # Index file extraction (extracts endpoints + metadata from index)
  python -m src.extraction_orchestrator --source index --input index_files/aetna_florida.json --output-dir output/fl_aetna

  # URL list extraction
  python -m src.extraction_orchestrator --source urls --input nj_bcbs.txt --output-dir output/nj_bcbs --workers 4

  # Single URL extraction
  python -m src.extraction_orchestrator --source url --input https://example.com/file.json.gz --output-dir output/single
        """
    )
    
    parser.add_argument('--source', required=True, choices=['csv', 'index', 'urls', 'url'],
                       help='Input source type: csv, index (MRF index JSON), urls (URL list file), or url (single URL)')
    parser.add_argument('--input', required=True,
                       help='Input file path or URL (depending on source type)')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory for Parquet files (default: output)')
    parser.add_argument('--workers', type=int, default=1,
                       help='Number of parallel workers (default: 1 = sequential mode)')
    parser.add_argument('--cpt-whitelist', type=str, default='src/cpt_codes.txt',
                       help='Path to CPT code whitelist file (default: src/cpt_codes.txt)')
    parser.add_argument('--provider-batch-size', type=int, default=20000,
                       help='Batch size for provider extraction (default: 20000)')
    parser.add_argument('--rate-batch-size', type=int, default=50000,
                       help='Batch size for rate extraction (default: 50000)')
    parser.add_argument('--output-prefix', type=str, default=None,
                       help='Optional prefix for output files (auto-detected for index files)')
    parser.add_argument('--structures', type=str, default=None,
                       help='Comma-separated structure numbers for index files (e.g., "1,4,7")')
    parser.add_argument('--max-files', type=int, default=None,
                       help='Maximum files per structure for index files (useful for testing)')
    
    args = parser.parse_args()
    
    # Parse input based on source type
    print("="*60)
    print("MRF EXTRACTION ORCHESTRATOR")
    print("="*60)
    print(f"\n📋 Source type: {args.source}")
    print(f"📋 Input: {args.input}")
    
    try:
        if args.source == 'csv':
            tasks, prefix = parse_csv_input(args.input, args.output_prefix)
            print(f"📊 Parsed {len(tasks)} tasks from CSV")
        
        elif args.source == 'index':
            structures_filter = None
            if args.structures:
                structures_filter = [int(s.strip()) for s in args.structures.split(',')]
            tasks, prefix = parse_index_input(
                args.input,
                state_prefix=args.output_prefix,
                structures_filter=structures_filter,
                max_files=args.max_files
            )
            print(f"📊 Parsed {len(tasks)} tasks from index file")
            print(f"📋 Inferred prefix: {prefix}")
        
        elif args.source == 'urls':
            prefix = args.output_prefix or 'extracted'
            tasks = parse_url_list_input(args.input, prefix)
            print(f"📊 Parsed {len(tasks)} tasks from URL list")
        
        elif args.source == 'url':
            prefix = args.output_prefix or 'extracted'
            task = parse_single_url_input(args.input, prefix)
            tasks = [task]
            print(f"📊 Parsed 1 task from URL")
        
    except Exception as e:
        print(f"\n❌ Error parsing input: {e}")
        sys.exit(1)
    
    # Set output directory
    output_dir = Path(args.output_dir)
    
    # Check if CPT whitelist exists (optional)
    cpt_whitelist = args.cpt_whitelist if Path(args.cpt_whitelist).exists() else None
    
    print(f"\n📁 Output directory: {output_dir}/")
    print(f"⚙️  Workers: {args.workers} ({'parallel' if args.workers > 1 else 'sequential'})")
    if cpt_whitelist:
        print(f"📋 CPT whitelist: {args.cpt_whitelist}")
    
    # Execute batch
    try:
        summary = execute_batch(
            tasks=tasks,
            output_dir=output_dir,
            workers=args.workers,
            cpt_whitelist=cpt_whitelist,
            provider_batch_size=args.provider_batch_size,
            rate_batch_size=args.rate_batch_size
        )
        
        # Final summary
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"\n✅ Successful: {summary['successful']}/{summary['total']}")
        print(f"❌ Failed: {summary['failed']}/{summary['total']}")
        print(f"\n📁 Output directory: {output_dir}/")
        print(f"📋 Manifest file: {summary['manifest_file']}")
        
        if args.workers > 1 and (output_dir / 'logs').exists():
            log_dirs = sorted(output_dir.glob('logs/*'))
            if log_dirs:
                latest_log_dir = log_dirs[-1]
                # Print path relative to output_dir (resolve both to absolute for reliable comparison)
                rel_path = latest_log_dir.resolve().relative_to(output_dir.resolve())
                print(f"📝 Log directory: {rel_path}/")
    
    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()


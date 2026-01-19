#!/usr/bin/env python3
"""
Parse MRF index JSON files to generate extraction commands with plan metadata.
Works with any payer (UHC, Aetna, FloridaBlue, etc.) that follows CMS transparency schema.

Usage:
    # Generate commands only (default)
    python parse_mrf_index.py index_files/aetna_florida_index.json
    
    # Generate and execute commands
    python parse_mrf_index.py index_files/FloridaBlue_index.json --execute
    
    # Execute only specific structures
    python parse_mrf_index.py index_files/FloridaBlue_index.json --execute --structures 1
    python parse_mrf_index.py index_files/FloridaBlue_index.json --execute --structures 1,4,7
"""

import json
import sys
import argparse
import subprocess
import re
from pathlib import Path
from datetime import datetime
from collections import Counter


def extract_state_and_payer(entity_name: str) -> tuple:
    """Extract state and payer from reporting entity name."""
    # Extract state (works for "Aetna Health Inc. - Florida" or "UnitedHealthcare-of-Georgia-Inc")
    if ' - ' in entity_name:
        state = entity_name.split(' - ')[-1].lower()
    elif '-of-' in entity_name.lower():
        parts = entity_name.lower().split('-of-')
        state = parts[1].split('-')[0] if len(parts) > 1 else "unknown"
    elif 'floridablue' in entity_name.lower() or 'florida blue' in entity_name.lower():
        state = "florida"
    else:
        state = "unknown"
    
    # Detect payer from entity name
    entity_lower = entity_name.lower()
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
    Extract unique network identifier from MRF URL.
    
    Handles multiple patterns:
    - FloridaBlue: "2025-08_020_02E0_in-network-rates_1_of_5.json.gz" → "020_02E0"
    - UHC: "UHC-Vision_in-network-rates.json.gz" → "UHC-Vision"
    - Aetna: "2025-10-05_pl-3mk-hr23_Aetna-Health.json.gz" → "pl-3mk-hr23"
    """
    filename = url.split('/')[-1].split('?')[0].replace('.json.gz', '')
    
    if '_in-network-rates' in filename:
        # Extract the part before "_in-network-rates"
        base = filename.split('_in-network-rates')[0]
        
        # Remove date prefix (YYYY-MM format)
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
        # Aetna pattern: first non-date part
        parts = [p for p in filename.split('_') if p and not p.startswith('20')]
        return parts[0] if parts else 'network'


def build_command(url: str, structure_id: str, plans: list, network_id: str, 
                  state_prefix: str) -> str:
    """Build extraction command for a single in-network file."""
    # Build plan metadata arguments (all plans in this structure)
    plan_args = []
    for plan in plans:
        plan_args.append(f"--plan-name \"{plan.get('plan_name', '')}\"")
        plan_args.append(f"--plan-id-type \"{plan.get('plan_id_type', '')}\"")
        plan_args.append(f"--plan-id \"{plan.get('plan_id', '')}\"")
        plan_args.append(f"--plan-market-type \"{plan.get('plan_market_type', '')}\"")
    
    plan_args_str = " \\\n  ".join(plan_args)
    
    # Build complete command
    cmd = (
        f"python -m src.run_extraction \\\n"
        f"  {url} \\\n"
        f"  --cpt-whitelist src/cpt_codes.txt \\\n"
        f"  --output-prefix {state_prefix} \\\n"
        f"  --rate-batch-size 20000 \\\n"
        f"  --structure-id \"{structure_id}\" \\\n"
        f"  {plan_args_str} \\\n"
        f"  --network-id \"{network_id}\" \\\n"
        f"  --quiet"
    )
    
    return cmd


def parse_index(index_path: str, state_prefix: str = None, execute: bool = False, 
                structures_filter: list = None, max_files: int = None, summary_only: bool = False):
    """
    Parse MRF index file and generate extraction commands.
    
    Args:
        index_path: Path to index JSON file
        state_prefix: Custom prefix for output files (auto-detected if not provided)
        execute: If True, run the extraction commands; if False, just print them
        structures_filter: List of structure numbers to process (e.g., [1, 4, 7]); None = all
        max_files: Maximum number of files to process per structure (None = all)
        summary_only: If True, only print summary statistics and exit
    
    Returns:
        List of command strings that were generated
    """
    with open(index_path, 'r') as f:
        data = json.load(f)
    
    # Auto-generate prefix from reporting entity if not provided
    if not state_prefix:
        entity_name = data.get('reporting_entity_name', '')
        state, payer = extract_state_and_payer(entity_name)
        state_prefix = f"{state}_{payer}"
    
    print(f"📋 MRF Index Parser")
    print(f"=" * 80)
    print(f"Reporting Entity: {data.get('reporting_entity_name', 'N/A')}")
    print(f"Version: {data.get('version', 'N/A')}")
    print(f"Output Prefix: {state_prefix}")
    
    all_structures = data.get('reporting_structure', [])
    
    # If summary_only, print statistics and exit
    if summary_only:
        print(f"\n{'='*80}")
        print(f"INDEX FILE SUMMARY")
        print(f"{'='*80}\n")
        
        total_plans = 0
        structures_with_files = 0
        structures_without_files = 0
        plans_per_structure = []
        files_per_structure = []
        unique_filenames = set()
        unique_network_ids = set()
        plan_market_types = Counter()
        plan_id_types = Counter()
        
        for structure in all_structures:
            plans = structure.get('reporting_plans', [])
            in_network_files = structure.get('in_network_files', [])
            
            total_plans += len(plans)
            plans_per_structure.append(len(plans))
            files_per_structure.append(len(in_network_files))
            
            if in_network_files:
                structures_with_files += 1
                for file_info in in_network_files:
                    url = file_info.get('location', '')
                    filename = url.split('/')[-1].split('?')[0] if url else ''
                    if filename:
                        unique_filenames.add(filename)
                        network_id = extract_network_id(url)
                        unique_network_ids.add(network_id)
            else:
                structures_without_files += 1
            
            for plan in plans:
                plan_market_types[plan.get('plan_market_type', 'N/A')] += 1
                plan_id_types[plan.get('plan_id_type', 'N/A')] += 1
        
        print(f"Total Structures: {len(all_structures):,}")
        print(f"Total Plans: {total_plans:,}")
        print(f"\nStructures:")
        print(f"  With in-network files: {structures_with_files:,}")
        print(f"  Without in-network files: {structures_without_files:,}")
        print(f"\nFiles:")
        print(f"  Unique filenames: {len(unique_filenames):,}")
        print(f"  Unique network IDs: {len(unique_network_ids):,}")
        print(f"\nPlans per structure:")
        print(f"  Min: {min(plans_per_structure)}")
        print(f"  Max: {max(plans_per_structure):,}")
        print(f"  Average: {sum(plans_per_structure)/len(plans_per_structure):.1f}")
        print(f"  Median: {sorted(plans_per_structure)[len(plans_per_structure)//2]}")
        print(f"\nFiles per structure:")
        file_counts = [f for f in files_per_structure if f > 0]
        if file_counts:
            print(f"  Min: {min(file_counts)}")
            print(f"  Max: {max(file_counts)}")
            print(f"  Average: {sum(file_counts)/len(file_counts):.2f}")
        print(f"\nPlan Market Types:")
        for market_type, count in plan_market_types.most_common(10):
            print(f"  {market_type}: {count:,}")
        print(f"\nPlan ID Types:")
        for id_type, count in plan_id_types.most_common(10):
            print(f"  {id_type}: {count:,}")
        
        return []
    
    print(f"\n🔍 Found {len(all_structures)} reporting structure(s)\n")
    
    # Filter structures if specified
    if structures_filter:
        print(f"🎯 Filtering to structures: {structures_filter}\n")
        structures_to_process = [
            (idx, all_structures[idx-1]) 
            for idx in structures_filter 
            if 0 < idx <= len(all_structures)
        ]
    else:
        structures_to_process = list(enumerate(all_structures, 1))
    
    commands = []
    
    for struct_idx, structure in structures_to_process:
        plans = structure.get('reporting_plans', [])
        in_network_files = structure.get('in_network_files', [])
        
        if not in_network_files:
            continue
        
        structure_id = f"{state_prefix}_{struct_idx}"
        
        if not execute:
            print(f"\n{'='*80}")
            print(f"STRUCTURE {struct_idx}: {len(plans)} plan(s), {len(in_network_files)} in-network file(s)")
            print(f"{'='*80}")
            for plan in plans:
                print(f"  • {plan.get('plan_name', 'N/A')} "
                      f"({plan.get('plan_id_type', 'N/A')}: {plan.get('plan_id', 'N/A')}, "
                      f"{plan.get('plan_market_type', 'N/A')})")
            print()
        
        # Apply max_files limit if specified
        files_to_process = in_network_files[:max_files] if max_files else in_network_files
        
        # Generate command for each in-network file in this structure
        for file_idx, network_file in enumerate(files_to_process, 1):
            url = network_file.get('location', '')
            if not url:
                continue
                
            network_id = extract_network_id(url)
            cmd = build_command(url, structure_id, plans, network_id, state_prefix)
            commands.append(cmd)
            
            if not execute:
                print(f"## File {file_idx}/{len(files_to_process)}: {network_id}")
                print(cmd)
                print()
    
    if execute:
        print(f"\n🚀 EXECUTING {len(commands)} extraction command(s)")
        print(f"=" * 80)
        
        start_time = datetime.now()
        successful = 0
        failed = 0
        results_log = []  # Track position, status, filename, error
        
        for i, cmd in enumerate(commands, 1):
            # Extract info for progress display
            struct_id = cmd.split('--structure-id "')[1].split('"')[0] if '--structure-id' in cmd else 'unknown'
            url = cmd.split('\n')[1].strip()
            filename = url.split('/')[-1].split('?')[0]
            
            print(f"\n[{i}/{len(commands)}] {struct_id} - {filename}")
            print("-" * 80)
            
            # Run the command
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                successful += 1
                # Extract success stats from stdout (last line with SUCCESS message)
                success_msg = [line for line in result.stdout.split('\n') if '✅ SUCCESS' in line]
                stats = success_msg[-1] if success_msg else 'No stats available'
                results_log.append({
                    'position': i,
                    'status': 'SUCCESS',
                    'filename': filename,
                    'structure_id': struct_id,
                    'error': stats
                })
                print(stats)
            else:
                failed += 1
                # Extract error message from stderr
                error_msg = result.stderr.split('\n')[-2] if result.stderr else 'Unknown error'
                results_log.append({
                    'position': i,
                    'status': 'FAILED',
                    'filename': filename,
                    'structure_id': struct_id,
                    'error': error_msg
                })
                print(f"❌ FAILED: {filename}")
                print(f"   Error: {error_msg}")
        
        # Summary
        elapsed = datetime.now() - start_time
        print(f"\n" + "=" * 80)
        print(f"EXTRACTION COMPLETE")
        print(f"=" * 80)
        print(f"✅ Successful: {successful}/{len(commands)}")
        print(f"❌ Failed: {failed}/{len(commands)}")
        print(f"⏱️  Total time: {elapsed}")
        
        # Save detailed results log
        log_filename = f"extraction_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_filename, 'w') as f:
            f.write(f"EXTRACTION RUN SUMMARY\n")
            f.write(f"=" * 80 + "\n")
            f.write(f"Total files: {len(commands)}\n")
            f.write(f"Successful: {successful}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Duration: {elapsed}\n")
            f.write(f"\n" + "=" * 80 + "\n\n")
            
            f.write("DETAILED RESULTS:\n")
            f.write("-" * 80 + "\n")
            for result in results_log:
                f.write(f"[{result['position']:3d}] {result['status']:8s} | {result['structure_id']:20s} | {result['filename']}\n")
                if result['error']:
                    f.write(f"      Error: {result['error']}\n")
            
            # Summary by structure
            f.write(f"\n" + "=" * 80 + "\n")
            f.write("SUMMARY BY STRUCTURE:\n")
            f.write("-" * 80 + "\n")
            structure_summary = {}
            for result in results_log:
                sid = result['structure_id']
                if sid not in structure_summary:
                    structure_summary[sid] = {'success': 0, 'failed': 0}
                if result['status'] == 'SUCCESS':
                    structure_summary[sid]['success'] += 1
                else:
                    structure_summary[sid]['failed'] += 1
            
            for sid, counts in structure_summary.items():
                total = counts['success'] + counts['failed']
                f.write(f"{sid:20s}: {counts['success']:3d} success, {counts['failed']:3d} failed ({total} total)\n")
        
        print(f"\n📝 Detailed log saved to: {log_filename}")
        print(f"📁 Output: output/{state_prefix}/")
        
        return commands
    else:
        print(f"=" * 80)
        print(f"✅ Generated {len(commands)} extraction command(s)")
        print(f"\n💡 Tip: Copy/paste commands to run, or use --execute flag to run automatically")
        
        return commands


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse MRF index JSON to generate extraction commands (works with any payer)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate commands only (default)
  python parse_mrf_index.py index_files/aetna_fl_index.json
  
  # Generate and execute all structures
  python parse_mrf_index.py index_files/FloridaBlue_index.json --execute
  
  # Execute only Structure 1 (for testing)
  python parse_mrf_index.py index_files/FloridaBlue_index.json --execute --structures 1
  
  # Execute specific structures
  python parse_mrf_index.py index_files/FloridaBlue_index.json --execute --structures 1,4,7
  
  # Execute first 100 files from Structure 1
  python parse_mrf_index.py index_files/FloridaBlue_index.json --execute --structures 1 --max-files 100
        """
    )
    parser.add_argument('index_file', help='Path to MRF index JSON file')
    parser.add_argument('--state-prefix', 
                       help='Custom prefix for output files (e.g., fl_aetna, floridablue_fl)', 
                       default=None)
    parser.add_argument('--execute', action='store_true', 
                       help='Execute the extraction commands (default: just print commands)')
    parser.add_argument('--structures', type=str,
                       help='Comma-separated structure numbers to process (e.g., "1,4,7"; default: all)')
    parser.add_argument('--max-files', type=int, default=None,
                       help='Maximum number of files to process per structure (default: all)')
    parser.add_argument('--summary', action='store_true',
                       help='Print summary statistics only (no commands generated)')
    
    args = parser.parse_args()
    
    if not Path(args.index_file).exists():
        print(f"❌ Error: File not found: {args.index_file}")
        sys.exit(1)
    
    # Parse structures filter
    structures_filter = None
    if args.structures:
        try:
            structures_filter = [int(s.strip()) for s in args.structures.split(',')]
        except ValueError:
            print(f"❌ Error: Invalid structures format. Use comma-separated numbers (e.g., '1,4,7')")
            sys.exit(1)
    
    parse_index(args.index_file, args.state_prefix, args.execute, structures_filter, args.max_files, args.summary)

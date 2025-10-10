#!/usr/bin/env python3
"""
Parse MRF index JSON files to generate extraction commands with plan metadata.
Works with any payer (UHC, Aetna, etc.) that follows CMS transparency schema.

Usage:
    python parse_mrf_index.py aetna_florida_index.json
    python parse_mrf_index.py uhc_georgia_index.json --state-prefix ga_uhc
"""

import json
import sys
import argparse
from pathlib import Path

def parse_index(index_path: str, state_prefix: str = None):
    """Parse MRF index file and generate extraction commands."""
    
    with open(index_path, 'r') as f:
        data = json.load(f)
    
    reporting_entity = data.get('reporting_entity_name', '').replace(' ', '_').replace('.', '')
    
    # Auto-generate prefix from reporting entity if not provided
    if not state_prefix:
        entity_name = data.get('reporting_entity_name', '')
        # Extract state (works for "Aetna Health Inc. - Florida" or "UnitedHealthcare-of-Georgia-Inc")
        if ' - ' in entity_name:
            state = entity_name.split(' - ')[-1].lower()
        elif '-of-' in entity_name.lower():
            # Extract from "UnitedHealthcare-of-Georgia-Inc"
            parts = entity_name.lower().split('-of-')
            if len(parts) > 1:
                state = parts[1].split('-')[0]
            else:
                state = "unknown"
        else:
            state = "unknown"
        
        # Detect payer from entity name
        if 'aetna' in entity_name.lower():
            payer = 'aetna'
        elif 'united' in entity_name.lower() or 'uhc' in entity_name.lower():
            payer = 'uhc'
        else:
            payer = 'payer'
        
        state_prefix = f"{state[:2]}_{payer}"
    
    # Helper to extract unique identifier from URL
    def get_unique_id(url):
        """Extract unique part from URL filename (network/variant identifier)"""
        # Get filename: "2025-10-05_pl-3mk-hr23_Aetna-Health.json.gz" or
        #              "2025-10-01_...Insurer_UHC-Vision_in-network-rates.json.gz"
        filename = url.split('/')[-1].replace('.json.gz', '')
        
        # Check if this is the "_in-network-rates" pattern (UHC style)
        if '_in-network-rates' in filename:
            # UHC pattern: the network code is right before "_in-network-rates"
            # "2025-10-01_..._UHC-Vision_in-network-rates" -> "UHC-Vision"
            parts_before = filename.split('_in-network-rates')[0].split('_')
            # Return the last part before "_in-network-rates" (the network identifier)
            return parts_before[-1] if parts_before else 'network'
        else:
            # Aetna pattern: first non-date part after removing common suffixes
            parts = [p for p in filename.split('_') if p and not p.startswith('20')]  # Skip dates
            return parts[0] if parts else 'network'
    
    print(f"📋 MRF Index Parser")
    print(f"=" * 80)
    print(f"Reporting Entity: {data.get('reporting_entity_name', 'N/A')}")
    print(f"Version: {data.get('version', 'N/A')}")
    print(f"\n🔍 Found {len(data.get('reporting_structure', []))} reporting structure(s)\n")
    
    cmd_count = 0
    
    for idx, structure in enumerate(data.get('reporting_structure', []), 1):
        plans = structure.get('reporting_plans', [])
        in_network_files = structure.get('in_network_files', [])
        
        if not in_network_files:
            continue
        
        # Each in-network file gets its own command
        for file_idx, network_file in enumerate(in_network_files, 1):
            cmd_count += 1
            url = network_file.get('location', '')
            
            # Extract unique identifier from URL
            unique_id = get_unique_id(url)
            
            plan_suffix = unique_id
            
            # Determine which plan(s) this file represents
            if len(plans) == 1:
                plan = plans[0]
                plan_name_alt = None
                print(f"## Command {cmd_count}: {plan.get('plan_name', 'N/A')} [{plan_suffix}]")
            else:
                # Multiple plans in one file - use first plan, store others in alt
                plan = plans[0]
                # Join additional plan names with " | " separator
                plan_name_alt = " | ".join([p.get('plan_name', '') for p in plans[1:]])
                print(f"## Command {cmd_count}: Multiple plans ({len(plans)} plans) [{plan_suffix}]")
                for p in plans:
                    print(f"   - {p.get('plan_name', 'N/A')}")
            
            # Generate command
            cmd = f"python -m src.run_extraction \\\n"
            cmd += f"  {url} \\\n"
            cmd += f"  --cpt-whitelist src/cpt_codes.txt \\\n"
            cmd += f"  --output-prefix {state_prefix}_{plan_suffix} \\\n"
            cmd += f"  --rate-batch-size 20000 \\\n"
            cmd += f"  --plan-name \"{plan.get('plan_name', '')}\" \\\n"
            cmd += f"  --plan-id-type \"{plan.get('plan_id_type', '')}\" \\\n"
            cmd += f"  --plan-id \"{plan.get('plan_id', '')}\" \\\n"
            cmd += f"  --plan-market-type \"{plan.get('plan_market_type', '')}\" \\\n"
            cmd += f"  --network-id \"{unique_id}\""
            
            # Add plan_name_alt only if multiple plans exist
            if plan_name_alt:
                cmd += f" \\\n  --plan-name-alt \"{plan_name_alt}\""
            cmd += "\n"
            
            print(cmd)
    
    print(f"=" * 80)
    print(f"✅ Generated {cmd_count} extraction command(s)")
    print(f"\n💡 Tip: Copy/paste commands to run, or redirect output to a shell script")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse MRF index JSON to generate extraction commands (works with any payer)')
    parser.add_argument('index_file', help='Path to MRF index JSON file (UHC, Aetna, etc.)')
    parser.add_argument('--state-prefix', help='Custom prefix for output files (e.g., fl_aetna, ga_uhc)', default=None)
    
    args = parser.parse_args()
    
    if not Path(args.index_file).exists():
        print(f"❌ Error: File not found: {args.index_file}")
        sys.exit(1)
    
    parse_index(args.index_file, args.state_prefix)


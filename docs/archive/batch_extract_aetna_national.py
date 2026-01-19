#!/usr/bin/env python3
"""
Batch extraction script for Aetna National MRF endpoints.

Simple script to process Aetna national files with consistent naming.
"""

import re
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from collections import defaultdict


def extract_network_id(filename: str) -> Optional[str]:
    """Extract network ID from Aetna filename (e.g., pl-12e-hr23)."""
    # Pattern: pl-XXX-hr23 or similar
    match = re.search(r'(pl-[a-z0-9]+-hr23)', filename)
    if match:
        return match.group(1)
    return None


def load_plan_metadata(csv_path: str = 'aetna_national_plans.csv') -> tuple[Dict[str, List[Dict[str, str]]], List[str]]:
    """Load plan metadata mapping from URL to plans from CSV file.
    
    Returns:
        Tuple of (url_to_plans dict, list of unique URLs)
    """
    import csv
    
    url_to_plans = defaultdict(list)
    unique_urls = []
    csv_file = Path(csv_path)
    
    if not csv_file.exists():
        print(f"⚠️  Warning: Plan metadata CSV not found: {csv_path}")
        print("   Continuing without plan metadata...")
        return {}, []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        seen_urls = set()
        for row in reader:
            url = row['file_url'].strip()
            plan = {
                'plan_name': row['plan_name'].strip(),
                'plan_id_type': row['plan_id_type'].strip(),
                'plan_id': row['plan_id'].strip(),
                'plan_market_type': row['plan_market_type'].strip(),
            }
            url_to_plans[url].append(plan)
            
            # Track unique URLs (preserve order)
            if url not in seen_urls:
                unique_urls.append(url)
                seen_urls.add(url)
    
    return dict(url_to_plans), unique_urls


def main():
    output_dir = Path('output/aetna_national')
    manifest_file = output_dir / 'manifest.json'
    cpt_whitelist = 'src/cpt_codes.txt'
    plan_csv = 'aetna_national_plans.csv'
    
    # Load plan metadata and URLs from CSV
    plan_metadata, urls = load_plan_metadata(plan_csv)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not urls:
        print(f"❌ Error: No URLs found in {plan_csv}")
        print("   Make sure the CSV file exists and has 'file_url' column")
        sys.exit(1)
    
    print("="*60)
    print("AETNA NATIONAL BATCH EXTRACTION")
    print("="*60)
    print(f"\n📋 URLs loaded from: {plan_csv}")
    print(f"📋 Total URLs: {len(urls)}")
    print(f"📁 Output directory: {output_dir}/")
    print(f"📊 Plan metadata loaded for {len(plan_metadata)} unique URLs")
    
    # Initialize manifest
    manifest = {
        'extraction_run': datetime.now().isoformat(),
        'total_urls': len(urls),
        'extractions': []
    }
    
    successful = 0
    failed = 0
    
    print("\n" + "="*60)
    print("RUNNING EXTRACTIONS")
    print("="*60)
    
    for i, url in enumerate(urls, 1):
        # Extract network_id from filename
        filename = url.split('/')[-1].split('?')[0]
        network_id = extract_network_id(filename) or 'unknown'
        prefix = f"aetna_national_{network_id}"
        
        print(f"\n[{i}/{len(urls)}] {network_id}")
        print(f"   URL: {filename[:70]}...")
        
        # Initialize manifest entry
        manifest_entry = {
            'source_url': url,
            'filename': filename,
            'output_prefix': prefix,
            'network_id': network_id,
            'extraction_timestamp': None,
            'status': 'pending',
            'output_files': {
                'providers': None,
                'rates': None
            },
            'error': None,
            'duration_seconds': None
        }
        
        # Build command
        cmd = [
            sys.executable, '-m', 'src.run_extraction',
            url,
            '--output-dir', str(output_dir),
            '--output-prefix', prefix,
            '--cpt-whitelist', cpt_whitelist,
            '--provider-batch-size', '20000',
            '--rate-batch-size', '50000',
            '--network-id', network_id
            # Removed --quiet to show detailed logging
        ]
        
        # Add plan metadata if available
        plans = plan_metadata.get(url, [])
        if plans:
            for plan in plans:
                cmd.extend(['--plan-name', plan['plan_name']])
                cmd.extend(['--plan-id-type', plan['plan_id_type']])
                cmd.extend(['--plan-id', plan['plan_id']])
                cmd.extend(['--plan-market-type', plan['plan_market_type']])
            manifest_entry['plans'] = plans
            print(f"   Plans: {len(plans)} plan(s) - {', '.join([p['plan_id'] for p in plans])}")
        else:
            manifest_entry['plans'] = []
            print(f"   ⚠️  No plan metadata found for this URL")
        
        try:
            start_time = datetime.now()
            # Let stdout print normally for detailed logging, but capture stderr for error detection
            result = subprocess.run(
                cmd,
                stderr=subprocess.PIPE,
                # Don't capture stdout - let it print in real-time
                text=True
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            manifest_entry['extraction_timestamp'] = start_time.isoformat()
            manifest_entry['duration_seconds'] = duration
            
            if result.returncode == 0:
                print(f"   ✅ Success ({duration:.1f}s)")
                successful += 1
                manifest_entry['status'] = 'success'
                
                # Find output files
                provider_pattern = f"providers_{prefix}_*.parquet"
                rates_pattern = f"rates_{prefix}_*.parquet"
                
                provider_files = list(output_dir.glob(provider_pattern))
                rates_files = list(output_dir.glob(rates_pattern))
                
                if provider_files:
                    manifest_entry['output_files']['providers'] = provider_files[0].name
                if rates_files:
                    manifest_entry['output_files']['rates'] = rates_files[0].name
            else:
                print(f"   ❌ Failed")
                failed += 1
                manifest_entry['status'] = 'failed'
                manifest_entry['error'] = result.stderr[:500] if result.stderr else 'Unknown error'
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:200]}")
            failed += 1
            manifest_entry['status'] = 'failed'
            manifest_entry['error'] = str(e)
        
        # Add to manifest
        manifest['extractions'].append(manifest_entry)
        
        # Save manifest incrementally
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
    
    # Final summary
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)
    print(f"\n✅ Successful: {successful}/{len(urls)}")
    print(f"❌ Failed: {failed}/{len(urls)}")
    print(f"\n📁 Output directory: {output_dir}/")
    print(f"📋 Manifest file: {manifest_file}")
    
    manifest['summary'] = {
        'successful': successful,
        'failed': failed,
        'total': len(urls)
    }
    
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✅ Manifest saved to: {manifest_file}")


if __name__ == '__main__':
    main()


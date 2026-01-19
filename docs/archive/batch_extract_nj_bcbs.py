#!/usr/bin/env python3
"""
Batch extraction script for NJ BCBS MRF endpoints.

Features:
- URL cleaning and validation
- Deduplication
- Smart prefix generation with collision detection
- Multipart file handling (includes part info in prefix)
- Manifest tracking (JSON)
- Signed URL expiration detection (403 → failed_expired)
"""

import re
import subprocess
import sys
import json
import hashlib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional, Tuple

def is_valid_url(url: str) -> bool:
    """Check if URL is valid (has a file path, not just base domain)."""
    if not url.startswith('http'):
        return False
    
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    
    # Must have a path and end with .json.gz
    if not path or not path.endswith('.json.gz'):
        return False
    
    # Reject base domains
    if url == 'https://horizonblue.sapphiremrfhub.com' or url.endswith('horizonblue.sapphiremrfhub.com'):
        return False
    
    # Reject URLs with just domain
    if not path or path == '':
        return False
    
    return True

def normalize_url(url: str) -> str:
    """Normalize URL (ensure https:// prefix if missing)."""
    url = url.strip()
    if not url.startswith('http'):
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('horizon'):
            url = 'https://' + url
    return url

def is_signed_url(url: str) -> bool:
    """Check if URL has signed parameters (Expires, Signature)."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return 'Expires' in query and 'Signature' in query

def extract_network_id(filename: str) -> Optional[str]:
    """Extract network ID from filename patterns."""
    patterns = [
        r'_(MGCN|MCEX|OEX\d+|OEX\d+|TRAD|OMT\d+)_',
        r'_(MGCN|MCEX|OEX\d+|OEX\d+|TRAD|OMT\d+)\.',
        r'-([A-Z0-9]{4})_in-network',  # 4-char codes like 58B0, 17B0
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group(1)
    
    return None

def extract_part_info(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract part number and total from filename (e.g., _01_of_02)."""
    pattern = r'_(\d+)_of_(\d+)'
    match = re.search(pattern, filename)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def extract_date_from_path(url: str) -> Optional[str]:
    """Extract date/month from URL path (e.g., /mrfs/202601/ -> 2026-01)."""
    match = re.search(r'/mrfs/(\d{6})/', url)
    if match:
        yyyymm = match.group(1)
        year = yyyymm[:4]
        month = yyyymm[4:]
        return f"{year}-{month}"
    
    # Try filename date format
    match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
    if match:
        date_str = match.group(1)
        year, month, _ = date_str.split('-')
        return f"{year}-{month}"
    
    return None

def sanitize_prefix(text: str) -> str:
    """Sanitize text for prefix (only [a-zA-Z0-9._-])."""
    # Replace non-allowed chars with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9._-]', '_', text)
    # Remove multiple underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Trim underscores from ends
    sanitized = sanitized.strip('_')
    return sanitized

def generate_url_hash(url: str, length: int = 8) -> str:
    """Generate short hash from URL."""
    return hashlib.md5(url.encode()).hexdigest()[:length]

def generate_prefix(
    filename: str,
    url: str,
    part_number: Optional[int],
    parts_total: Optional[int],
    existing_prefixes: set,
    max_length: int = 120
) -> str:
    """Generate unique prefix from filename with collision detection."""
    HASH_SUFFIX_LEN = len("_hash") + 8  # 13 chars: "_hash" (5) + 8 hash chars
    
    # Remove extension and query params
    base = filename.split('?')[0].replace('.json.gz', '')
    
    # Split into parts and filter
    parts = base.split('_')
    filtered = []
    for p in parts:
        if p.lower() not in ['in', 'network', 'rates', 'of', 'json', 'gz']:
            # Skip pure numbers unless they're meaningful
            if not re.match(r'^\d+$', p) or len(p) > 4:
                filtered.append(p)
    
    # Build base prefix
    base_prefix = '_'.join(filtered[:10])  # Limit parts
    base_prefix = sanitize_prefix(base_prefix.lower())
    
    # Add part suffix if multipart
    if part_number is not None and parts_total is not None:
        part_suffix = f"_p{part_number:02d}of{parts_total:02d}"
        base_prefix = base_prefix + part_suffix
    
    # Truncate if needed
    needs_hash = False
    if len(base_prefix) > max_length:
        # Truncate to leave room for hash suffix
        base_prefix = base_prefix[:max_length - HASH_SUFFIX_LEN]
        needs_hash = True
    
    # Check for collision
    if base_prefix in existing_prefixes:
        needs_hash = True
    
    # Add hash if needed
    if needs_hash:
        url_hash = generate_url_hash(url)
        base_prefix = f"{base_prefix}_hash{url_hash}"
    
    # Final collision check
    counter = 1
    final_prefix = base_prefix
    while final_prefix in existing_prefixes:
        final_prefix = f"{base_prefix}_{counter}"
        counter += 1
    
    existing_prefixes.add(final_prefix)
    return final_prefix

def main():
    input_file = Path('nj_bcbs.txt')
    output_dir = Path('output/nj_bcbs')
    manifest_file = output_dir / 'manifest.json'
    cpt_whitelist = 'src/cpt_codes.txt'
    
    if not input_file.exists():
        print(f"Error: {input_file} not found!")
        sys.exit(1)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read and filter URLs
    print("="*60)
    print("LOADING AND FILTERING URLs")
    print("="*60)
    
    with open(input_file, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    # Normalize first (to handle truncated URLs), then filter
    normalized_urls = [normalize_url(line) for line in lines if line]
    valid_urls = [url for url in normalized_urls if is_valid_url(url)]
    # Use dict.fromkeys() to preserve insertion order while deduplicating
    unique_urls = list(dict.fromkeys(valid_urls))
    
    print(f"\nTotal lines: {len(lines)}")
    print(f"Normalized URLs: {len(normalized_urls)}")
    print(f"Valid URLs: {len(valid_urls)}")
    print(f"Unique valid URLs: {len(unique_urls)}")
    
    # Initialize manifest
    manifest = {
        'extraction_run': datetime.now().isoformat(),
        'total_urls': len(unique_urls),
        'extractions': []
    }
    
    # Generate prefixes with collision detection
    print("\n" + "="*60)
    print("GENERATING OUTPUT PREFIXES (with collision detection)")
    print("="*60)
    
    existing_prefixes = set()
    tasks = []
    
    for url in unique_urls:
        filename = urlparse(url).path.split('/')[-1].split('?')[0]
        network_id = extract_network_id(filename) or 'UNKNOWN'
        part_number, parts_total = extract_part_info(filename)
        as_of_month = extract_date_from_path(url)
        is_signed = is_signed_url(url)
        
        prefix = generate_prefix(filename, url, part_number, parts_total, existing_prefixes)
        
        tasks.append({
            'url': url,
            'filename': filename,
            'prefix': prefix,
            'network_id': network_id,
            'part_number': part_number,
            'parts_total': parts_total,
            'as_of_month': as_of_month,
            'is_signed': is_signed,
        })
    
    print(f"\n✅ Generated {len(tasks)} unique prefixes")
    print(f"   No collisions (all prefixes unique)")
    
    # Show sample tasks
    print(f"\nSample tasks (first 5):")
    for i, task in enumerate(tasks[:5], 1):
        print(f"\n  {i}. {task['filename'][:60]}...")
        print(f"     Prefix: {task['prefix']}")
        print(f"     Network ID: {task['network_id']}")
        if task['part_number']:
            print(f"     Part: {task['part_number']}/{task['parts_total']}")
        if task['is_signed']:
            print(f"     ⚠️  Signed URL (may expire)")
    
    # Confirm before proceeding
    print("\n" + "="*60)
    response = input(f"\nReady to extract {len(tasks)} files to {output_dir}/ ? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        sys.exit(0)
    
    # Run extractions
    print("\n" + "="*60)
    print("RUNNING EXTRACTIONS")
    print("="*60)
    
    successful = 0
    failed = 0
    failed_expired = 0
    
    for i, task in enumerate(tasks, 1):
        url = task['url']
        prefix = task['prefix']
        network_id = task['network_id']
        filename = task['filename']
        
        print(f"\n[{i}/{len(tasks)}] Extracting: {filename[:60]}...")
        print(f"   Prefix: {prefix}")
        print(f"   Network ID: {network_id}")
        
        # Initialize manifest entry
        manifest_entry = {
            'source_url': url,
            'filename': filename,
            'output_prefix': prefix,
            'network_id': network_id,
            'part_number': task['part_number'],
            'parts_total': task['parts_total'],
            'as_of_month': task['as_of_month'],
            'is_signed_url': task['is_signed'],
            'extraction_timestamp': None,
            'status': 'pending',
            'output_files': {
                'providers': None,
                'rates': None
            },
            'error': None
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
            '--network-id', network_id,
        ]
        
        try:
            start_time = datetime.now()
            # Let stdout print normally, but capture stderr for error detection
            result = subprocess.run(
                cmd,
                stderr=subprocess.PIPE,  # Capture stderr for error detection
                text=True,
                timeout=3600  # 1 hour timeout per file
            )
            
            end_time = datetime.now()
            manifest_entry['extraction_timestamp'] = start_time.isoformat()
            manifest_entry['duration_seconds'] = (end_time - start_time).total_seconds()
            
            if result.returncode == 0:
                print(f"   ✅ Success")
                successful += 1
                manifest_entry['status'] = 'success'
                
                # Try to find output files
                provider_pattern = f"providers_{prefix}_*.parquet"
                rates_pattern = f"rates_{prefix}_*.parquet"
                
                provider_files = list(output_dir.glob(provider_pattern))
                rates_files = list(output_dir.glob(rates_pattern))
                
                if provider_files:
                    manifest_entry['output_files']['providers'] = str(provider_files[0].name)
                if rates_files:
                    manifest_entry['output_files']['rates'] = str(rates_files[0].name)
                    
            else:
                # Check for 403/AccessDenied
                stderr = result.stderr.lower()
                if '403' in stderr or 'access denied' in stderr or 'forbidden' in stderr:
                    print(f"   ❌ Failed (403 - Expired URL)")
                    failed_expired += 1
                    manifest_entry['status'] = 'failed_expired'
                    manifest_entry['error'] = '403 Access Denied (likely expired signed URL)'
                else:
                    print(f"   ❌ Failed: {result.stderr[:200]}")
                    failed += 1
                    manifest_entry['status'] = 'failed'
                    manifest_entry['error'] = result.stderr  # No truncation - capture full error
                
        except subprocess.TimeoutExpired:
            print(f"   ⏱️  Timeout (>1 hour)")
            failed += 1
            manifest_entry['status'] = 'failed'
            manifest_entry['error'] = 'Timeout (>1 hour)'
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:200]}")
            failed += 1
            manifest_entry['status'] = 'failed'
            manifest_entry['error'] = str(e)  # No truncation - capture full error
        
        # Add to manifest
        manifest['extractions'].append(manifest_entry)
        
        # Save manifest incrementally (after each extraction)
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
    
    # Final summary
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)
    print(f"\n✅ Successful: {successful}/{len(tasks)}")
    print(f"❌ Failed: {failed}/{len(tasks)}")
    print(f"⏰ Expired (403): {failed_expired}/{len(tasks)}")
    print(f"\n📁 Output directory: {output_dir}/")
    print(f"📋 Manifest file: {manifest_file}")
    
    # Update manifest summary
    manifest['summary'] = {
        'successful': successful,
        'failed': failed,
        'failed_expired': failed_expired,
        'total': len(tasks)
    }
    
    # Final manifest save
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✅ Manifest saved to: {manifest_file}")

if __name__ == '__main__':
    main()


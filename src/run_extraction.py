#!/usr/bin/env python3
"""
Combined MRF extraction workflow that:
1. Extracts providers from MRF index file
2. Cleans providers by filtering to NPI-2 only using NPPES API
3. Extracts rates using cleaned provider groups as filter
4. Merges rates and providers data together

This is a marriage script that coordinates the existing extract_providers_pro.py and extract_rates.py modules.
"""

import argparse
import os
import sys
import time
import json
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, List, Dict, Any
import pandas as pd
from tqdm import tqdm

# Import our existing modules
from extract_providers_pro import ProviderExtractor, load_tin_whitelist
from extract_rates import RateExtractor, load_cpt_whitelist, load_provider_groups_from_parquet
from utils import download_to_temp, get_output_slug

# NPPES API configuration
NPPES_BASE = "https://npiregistry.cms.hhs.gov/api/"
HEADERS = {"User-Agent": "NPPES-Lookup/1.0 (+your_email@example.com)"}

def build_nppes_url(npi: str) -> str:
    """Build the NPPES API URL for a given NPI."""
    return f"{NPPES_BASE}?number={npi}&version=2.1"

def get_enumeration_type(npi: str, timeout: int = 10) -> str:
    """
    Check the enumeration_type for a given NPI.
    Returns 'NPI-1', 'NPI-2', or None if not found/error.
    """
    url = build_nppes_url(npi)
    tries = 0
    while True:
        tries += 1
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 429:
                time.sleep(1.5 * tries)
                continue
            r.raise_for_status()
            payload = r.json()
            if not payload or not payload.get("result_count"):
                return None
            res = payload["results"][0]
            return res.get("enumeration_type")
        except (requests.RequestException, json.JSONDecodeError):
            if tries < 3:
                time.sleep(1.2 * tries)
                continue
            return None

def clean_providers_by_nppes(providers_parquet_path: str, max_workers: int = 6) -> str:
    """
    Clean provider parquet by filtering to NPI-2 providers only.
    
    Args:
        providers_parquet_path: Path to the providers parquet file
        max_workers: Number of concurrent NPPES API calls
        
    Returns:
        Path to the cleaned providers parquet file
    """
    print(f"\n🧹 CLEANING PROVIDERS BY NPPES API")
    print(f"📖 Reading providers from: {providers_parquet_path}")
    
    # Read providers parquet
    providers_df = pd.read_parquet(providers_parquet_path)
    print(f"📊 Loaded {len(providers_df):,} provider records")
    
    # Get unique NPIs
    unique_npis = providers_df['npi'].unique().tolist()
    print(f"🔍 Found {len(unique_npis):,} unique NPIs to check")
    
    # Check enumeration types in parallel
    print(f"🌐 Checking NPPES API for enumeration types...")
    npi_enumeration_types = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(get_enumeration_type, npi): npi for npi in unique_npis}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="NPPES Lookup", unit="NPI"):
            npi = futures[future]
            enum_type = future.result()
            npi_enumeration_types[npi] = enum_type
    
    # Filter to NPI-2 only
    npi2_npis = {npi for npi, enum_type in npi_enumeration_types.items() if enum_type == "NPI-2"}
    print(f"✅ Found {len(npi2_npis):,} NPI-2 providers out of {len(unique_npis):,} total")
    
    # Filter the dataframe
    cleaned_df = providers_df[providers_df['npi'].isin(npi2_npis)].copy()
    print(f"📊 Filtered to {len(cleaned_df):,} provider records")
    
    # Get unique provider group IDs from cleaned data
    unique_provider_groups = set(cleaned_df['provider_group_id'].unique())
    print(f"🏥 Unique provider groups after NPI-2 filtering: {len(unique_provider_groups):,}")
    
    # Save cleaned providers
    cleaned_path = providers_parquet_path.replace('.parquet', '_cleaned.parquet')
    cleaned_df.to_parquet(cleaned_path, index=False)
    print(f"💾 Saved cleaned providers to: {cleaned_path}")
    
    return cleaned_path

def merge_rates_and_providers(rates_parquet_path: str, providers_parquet_path: str, output_dir: Path) -> str:
    """
    Merge rates and providers data together.
    
    Args:
        rates_parquet_path: Path to rates parquet file
        providers_parquet_path: Path to providers parquet file
        output_dir: Output directory for merged file
        
    Returns:
        Path to the merged parquet file
    """
    print(f"\n🔗 MERGING RATES AND PROVIDERS")
    print(f"📖 Reading rates from: {rates_parquet_path}")
    print(f"📖 Reading providers from: {providers_parquet_path}")
    
    # Read data
    rates_df = pd.read_parquet(rates_parquet_path)
    prov_df = pd.read_parquet(providers_parquet_path)
    
    print(f"📊 Rates records: {len(rates_df):,}")
    print(f"📊 Provider records: {len(prov_df):,}")
    
    # Merge
    print(f"🔗 Merging on provider_reference_id = provider_group_id...")
    merged_df = pd.merge(
        rates_df, 
        prov_df, 
        left_on='provider_reference_id', 
        right_on='provider_group_id', 
        how='left'
    )
    
    print(f"📊 Merged records: {len(merged_df):,}")
    
    # Save merged data
    slug = get_output_slug()
    merged_path = output_dir / f"merged_rates_providers_{slug}.parquet"
    merged_df.to_parquet(merged_path, index=False)
    print(f"💾 Saved merged data to: {merged_path}")
    
    return str(merged_path)

def run_extraction_workflow(
    mrf_url: str,
    output_dir: Path = Path("output"),
    max_providers: int = None,
    max_items: int = None,
    max_time_minutes: int = None,
    provider_batch_size: int = 1000,
    rate_batch_size: int = 5,
    tin_whitelist_path: str = None,
    cpt_whitelist_path: str = None,
    output_prefix: str = None,
    nppes_workers: int = 6
) -> Dict[str, Any]:
    """
    Run the complete extraction workflow.
    
    Args:
        mrf_url: URL to the MRF index file
        output_dir: Output directory for all files
        max_providers: Max number of provider references to process
        max_items: Max number of rate items to process
        max_time_minutes: Max time to run rate extraction
        provider_batch_size: Batch size for provider extraction
        rate_batch_size: Batch size for rate extraction
        tin_whitelist_path: Path to TIN whitelist file
        cpt_whitelist_path: Path to CPT whitelist file
        output_prefix: Prefix for output files
        nppes_workers: Number of concurrent NPPES API workers
        
    Returns:
        Dictionary with paths to all output files and statistics
    """
    print(f"🚀 STARTING COMPLETE MRF EXTRACTION WORKFLOW")
    print(f"📥 MRF URL: {mrf_url}")
    print(f"📁 Output directory: {output_dir}")
    
    start_time = datetime.now()
    results = {}
    
    try:
        # Step 1: Download MRF index if it's a URL
        if mrf_url.startswith(('http://', 'https://')):
            print(f"\n📥 Downloading MRF index...")
            temp_index = download_to_temp(mrf_url)
            print(f"📦 Downloaded to: {temp_index}")
            mrf_path = temp_index
        else:
            mrf_path = mrf_url
            print(f"📄 Using local file: {mrf_path}")
        
        # Step 2: Extract providers
        print(f"\n{'='*60}")
        print(f"STEP 1: EXTRACTING PROVIDERS")
        print(f"{'='*60}")
        
        # Load whitelists
        tin_whitelist = load_tin_whitelist(tin_whitelist_path) if tin_whitelist_path else set()
        
        provider_extractor = ProviderExtractor(
            batch_size=provider_batch_size,
            tin_whitelist=tin_whitelist
        )
        
        provider_results = provider_extractor.process_file(
            file_path=mrf_path,
            output_dir=output_dir,
            max_providers=max_providers,
            output_prefix=output_prefix
        )
        
        providers_path = provider_results["output_path"]
        results["providers_path"] = providers_path
        results["provider_stats"] = provider_results["stats"]
        
        # Step 3: Clean providers by NPPES API
        print(f"\n{'='*60}")
        print(f"STEP 2: CLEANING PROVIDERS BY NPPES API")
        print(f"{'='*60}")
        
        cleaned_providers_path = clean_providers_by_nppes(providers_path, nppes_workers)
        results["cleaned_providers_path"] = cleaned_providers_path
        
        # Step 4: Extract rates using cleaned providers as filter
        print(f"\n{'='*60}")
        print(f"STEP 3: EXTRACTING RATES")
        print(f"{'='*60}")
        
        # Load provider groups from cleaned providers
        provider_groups = load_provider_groups_from_parquet(cleaned_providers_path)
        
        # Load CPT whitelist
        cpt_whitelist = load_cpt_whitelist(cpt_whitelist_path) if cpt_whitelist_path else set()
        
        rate_extractor = RateExtractor(
            batch_size=rate_batch_size,
            provider_group_filter=provider_groups,
            cpt_whitelist=cpt_whitelist
        )
        
        rate_results = rate_extractor.process_file(
            file_path=mrf_path,
            output_dir=output_dir,
            max_items=max_items,
            max_time_minutes=max_time_minutes
        )
        
        rates_path = rate_results["output_path"]
        results["rates_path"] = rates_path
        results["rate_stats"] = rate_results["stats"]
        
        # Step 5: Merge rates and providers
        print(f"\n{'='*60}")
        print(f"STEP 4: MERGING RATES AND PROVIDERS")
        print(f"{'='*60}")
        
        merged_path = merge_rates_and_providers(rates_path, cleaned_providers_path, output_dir)
        results["merged_path"] = merged_path
        
        # Final statistics
        elapsed = (datetime.now() - start_time).total_seconds()
        results["total_elapsed_seconds"] = elapsed
        
        print(f"\n{'='*60}")
        print(f"✅ WORKFLOW COMPLETE")
        print(f"{'='*60}")
        print(f"⏱️  Total time: {elapsed:.1f} seconds")
        print(f"📁 Output files:")
        print(f"   📊 Providers (raw): {providers_path}")
        print(f"   🧹 Providers (cleaned): {cleaned_providers_path}")
        print(f"   💰 Rates: {rates_path}")
        print(f"   🔗 Merged: {merged_path}")
        
        return results
        
    finally:
        # Clean up temp file if downloaded
        if mrf_url.startswith(('http://', 'https://')) and 'temp_index' in locals():
            try:
                os.unlink(temp_index)
                print(f"\n🧹 Cleaned up temporary index file")
            except Exception:
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Complete MRF extraction workflow: providers → NPPES cleaning → rates → merge"
    )
    parser.add_argument("mrf_url", help="URL or path to MRF index file")
    parser.add_argument("--output-dir", "-o", type=str, default="output", 
                       help="Output directory (default: output)")
    parser.add_argument("--max-providers", "-mp", type=int, 
                       help="Max number of provider references to process")
    parser.add_argument("--max-items", "-mi", type=int, 
                       help="Max number of rate items to process")
    parser.add_argument("--max-time", "-mt", type=int, 
                       help="Max time to run rate extraction in minutes")
    parser.add_argument("--provider-batch-size", "-pbs", type=int, default=1000,
                       help="Batch size for provider extraction (default: 1000)")
    parser.add_argument("--rate-batch-size", "-rbs", type=int, default=5,
                       help="Batch size for rate extraction (default: 5)")
    parser.add_argument("--tin-whitelist", "-t", type=str,
                       help="Path to TIN whitelist file (one TIN per line)")
    parser.add_argument("--cpt-whitelist", "-c", type=str,
                       help="Path to CPT whitelist file (one code per line)")
    parser.add_argument("--output-prefix", "-op", type=str,
                       help="Prefix for output files")
    parser.add_argument("--nppes-workers", "-nw", type=int, default=6,
                       help="Number of concurrent NPPES API workers (default: 6)")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run the workflow
    results = run_extraction_workflow(
        mrf_url=args.mrf_url,
        output_dir=output_dir,
        max_providers=args.max_providers,
        max_items=args.max_items,
        max_time_minutes=args.max_time,
        provider_batch_size=args.provider_batch_size,
        rate_batch_size=args.rate_batch_size,
        tin_whitelist_path=args.tin_whitelist,
        cpt_whitelist_path=args.cpt_whitelist,
        output_prefix=args.output_prefix,
        nppes_workers=args.nppes_workers
    )
    
    print(f"\n🎉 Workflow completed successfully!")
    print(f"📊 Final merged dataset: {results['merged_path']}")
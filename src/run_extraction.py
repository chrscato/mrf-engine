#!/usr/bin/env python3
"""
Simplified MRF extraction workflow that:
1. Extracts providers from MRF index file
2. Extracts rates using raw provider groups as filter

This is a simplified script that coordinates the existing extract_providers_pro.py and extract_rates.py modules.
NPPES API filtering and merging steps have been removed for simplicity and speed.
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Set, List, Dict, Any

# Import our existing modules
from .extract_providers_pro import ProviderExtractor, load_tin_whitelist
from .extract_rates import RateExtractor, load_cpt_whitelist, load_provider_groups_from_parquet
from .utils import download_to_temp, get_output_slug

def run_extraction_workflow(
    mrf_url: str,
    output_dir: Path = Path("output"),
    max_providers: int = None,
    max_items: int = None,
    max_time_minutes: int = None,
    provider_batch_size: int = 10000,
    rate_batch_size: int = 5,
    tin_whitelist_path: str = None,
    cpt_whitelist_path: str = None,
    output_prefix: str = None
) -> Dict[str, Any]:
    """
    Run the simplified extraction workflow.
    
    Args:
        mrf_url: URL to MRF file
        output_dir: Output directory for results
        max_providers: Optional limit on number of providers to extract
        max_items: Optional limit on number of items to process for rates
        max_time_minutes: Optional time limit in minutes
        provider_batch_size: Batch size for provider extraction
        rate_batch_size: Batch size for rate extraction
        tin_whitelist_path: Path to TIN whitelist file
        cpt_whitelist_path: Path to CPT whitelist file
        output_prefix: Prefix for output files
        
    Returns:
        Dictionary with results and file paths
    """
    start_time = datetime.now()
    results = {}
    
    try:
        # Download MRF file if it's a URL
        if mrf_url.startswith(('http://', 'https://')):
            print(f"📥 Downloading MRF file from: {mrf_url}")
            mrf_path = download_to_temp(mrf_url)
            print(f"✅ Downloaded to: {mrf_path}")
        else:
            mrf_path = mrf_url
            print(f"📄 Using local file: {mrf_path}")
        
        # Step 1: Extract providers
        print(f"\n{'='*60}")
        print(f"STEP 1: EXTRACTING PROVIDERS")
        print(f"{'='*60}")
        
        # Load TIN whitelist if specified
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
        
        # Step 2: Extract rates using raw providers as filter
        print(f"\n{'='*60}")
        print(f"STEP 2: EXTRACTING RATES")
        print(f"{'='*60}")
        
        # Load provider groups from raw providers (no NPPES filtering)
        provider_groups = load_provider_groups_from_parquet(providers_path)
        
        # Load CPT whitelist
        cpt_whitelist = load_cpt_whitelist(cpt_whitelist_path) if cpt_whitelist_path else set()
        
        rate_extractor = RateExtractor(
            batch_size=rate_batch_size,
            provider_group_filter=provider_groups,
            cpt_whitelist=cpt_whitelist,
            output_prefix=output_prefix
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
        
        # Final statistics
        elapsed = (datetime.now() - start_time).total_seconds()
        results["total_elapsed_seconds"] = elapsed
        
        print(f"\n{'='*60}")
        print(f"✅ WORKFLOW COMPLETE")
        print(f"{'='*60}")
        print(f"⏱️  Total time: {elapsed:.1f} seconds")
        print(f"📁 Output files:")
        print(f"   📊 Providers: {providers_path}")
        print(f"   💰 Rates: {rates_path}")
        
        return results
        
    finally:
        # Clean up temp file if downloaded
        if mrf_url.startswith(('http://', 'https://')) and 'mrf_path' in locals():
            try:
                Path(mrf_path).unlink()
                print(f"🗑️ Cleaned up temp file: {mrf_path}")
            except:
                pass

def main():
    """Main entry point for the extraction workflow."""
    parser = argparse.ArgumentParser(description="Extract providers and rates from MRF files")
    
    # Required arguments
    parser.add_argument("mrf_url", help="URL or path to MRF file")
    
    # Optional arguments
    parser.add_argument("--output-dir", "-o", type=str, default="output",
                       help="Output directory (default: output)")
    parser.add_argument("--output-prefix", "-p", type=str,
                       help="Prefix for output files")
    parser.add_argument("--max-providers", type=int,
                       help="Maximum number of providers to extract")
    parser.add_argument("--max-items", type=int,
                       help="Maximum number of items to process for rates")
    parser.add_argument("--max-time", type=int,
                       help="Maximum time in minutes for rate extraction")
    parser.add_argument("--provider-batch-size", type=int, default=10000,
                       help="Batch size for provider extraction (default: 10000)")
    parser.add_argument("--rate-batch-size", type=int, default=20000,
                       help="Batch size for rate extraction (default: 20000)")
    parser.add_argument("--tin-whitelist", type=str,
                       help="Path to TIN whitelist file")
    parser.add_argument("--cpt-whitelist", type=str,
                       help="Path to CPT whitelist file")
    
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
        output_prefix=args.output_prefix
    )
    
    print(f"\n🎉 Extraction completed successfully!")
    print(f"📊 Provider records: {results['provider_stats']['providers_written']:,}")
    print(f"📊 Rate records: {results['rate_stats']['rates_written']:,}")

if __name__ == "__main__":
    main()
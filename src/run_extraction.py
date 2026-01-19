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
    rate_batch_size: int = 20000,
    tin_whitelist_path: str = None,
    cpt_whitelist_path: str = None,
    output_prefix: str = None,
    structure_id: str = None,
    plan_name: list = None,
    plan_id_type: list = None,
    plan_id: list = None,
    plan_market_type: list = None,
    plan_name_alt: str = None,
    network_id: str = None
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
        structure_id: Optional structure identifier (e.g., "floridablue_fl_1")
        plan_name: Optional list of plan names in this structure
        plan_id_type: Optional list of plan ID types (EIN or HIOS)
        plan_id: Optional list of plan IDs
        plan_market_type: Optional list of plan market types (group or individual)
        plan_name_alt: Optional alternate plan names (when multiple plans share endpoint)
        network_id: Optional network identifier from source URL
        
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
            tin_whitelist=tin_whitelist,
            network_id=network_id
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
        
        # Check if inline schema detected (0 providers from Step 1)
        if provider_results["stats"]["providers_written"] == 0:
            print(f"\n💡 Inline schema detected (no top-level provider_references)")
            print(f"   → Providers will be synthesized and written during Step 2 (rate extraction)")
        
        # Step 2: Extract rates using raw providers as filter
        print(f"\n{'='*60}")
        print(f"STEP 2: EXTRACTING RATES")
        print(f"{'='*60}")
        
        # Load provider groups from raw providers (no NPPES filtering)
        provider_groups = load_provider_groups_from_parquet(providers_path)
        
        # Load CPT whitelist
        cpt_whitelist = load_cpt_whitelist(cpt_whitelist_path) if cpt_whitelist_path else set()
        
        # Build plan metadata dict if provided
        plan_metadata = {}
        if structure_id:
            plan_metadata['structure_id'] = structure_id
        if plan_name:
            plan_metadata['plan_name'] = plan_name
        if plan_id_type:
            plan_metadata['plan_id_type'] = plan_id_type
        if plan_id:
            plan_metadata['plan_id'] = plan_id
        if plan_market_type:
            plan_metadata['plan_market_type'] = plan_market_type
        if plan_name_alt:
            plan_metadata['plan_name_alt'] = plan_name_alt
        if network_id:
            plan_metadata['network_id'] = network_id
        
        rate_extractor = RateExtractor(
            batch_size=rate_batch_size,
            provider_group_filter=provider_groups,
            cpt_whitelist=cpt_whitelist,
            output_prefix=output_prefix,
            plan_metadata=plan_metadata if plan_metadata else None
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
        
        # === INLINE MODE: Providers created during rate extraction ===
        if rate_results["stats"].get("linkage_mode") == "inline_groups":
            providers_path = rate_extractor.providers_output_path
            results["providers_path"] = str(providers_path)
            print(f"\n📋 Inline schema: providers written during rate extraction")
        
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
        # Clean up temp file if downloaded (and transformed file if created)
        if mrf_url.startswith(('http://', 'https://')) and 'mrf_path' in locals():
            try:
                Path(mrf_path).unlink()
                print(f"🗑️  Cleaned up temp file: {mrf_path}")
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
    
    # Plan metadata (for index-based extractions)
    parser.add_argument("--structure-id", type=str,
                       help="Structure identifier (e.g., floridablue_fl_1)")
    parser.add_argument("--plan-name", type=str, action='append',
                       help="Plan name(s) in this structure (can specify multiple times)")
    parser.add_argument("--plan-id-type", type=str, action='append',
                       help="Plan ID type(s) - EIN or HIOS (can specify multiple times)")
    parser.add_argument("--plan-id", type=str, action='append',
                       help="Plan ID(s) (can specify multiple times)")
    parser.add_argument("--plan-market-type", type=str, action='append',
                       help="Plan market type(s) - group or individual (can specify multiple times)")
    parser.add_argument("--plan-name-alt", type=str,
                       help="Alternate plan names (when multiple plans share endpoint)")
    parser.add_argument("--network-id", type=str,
                       help="Network identifier from source URL")
    parser.add_argument("--quiet", action='store_true',
                       help="Suppress tracebacks, only show clean error messages")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    start_time = datetime.now()
    
    try:
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
            structure_id=args.structure_id,
            plan_name=args.plan_name,
            plan_id_type=args.plan_id_type,
            plan_id=args.plan_id,
            plan_market_type=args.plan_market_type,
            plan_name_alt=args.plan_name_alt,
            network_id=args.network_id
        )
        
        # Calculate total time
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Get provider count based on linkage mode
        if results['rate_stats'].get('linkage_mode') == 'inline_groups':
            try:
                import pyarrow.parquet as pq
                provider_count = pq.read_table(results['providers_path']).num_rows
            except:
                provider_count = 0
        else:
            provider_count = results['provider_stats']['providers_written']
        
        # Get rate count
        rate_count = results['rate_stats']['rates_written']
        
        if args.quiet:
            # Compact success message for batch processing
            print(f"✅ SUCCESS ({total_time:.1f}s | {rate_count:,} rates | {provider_count:,} providers)")
        else:
            # Detailed success message for interactive use
            print(f"\n🎉 Extraction completed successfully!")
            print(f"📊 Provider records: {provider_count:,}")
            print(f"📊 Rate records: {rate_count:,}")
        
    except Exception as e:
        if args.quiet:
            # Clean error message for batch processing
            print(f"❌ EXTRACTION FAILED: {str(e)}")
            sys.exit(1)
        else:
            # Full traceback for debugging
            raise

if __name__ == "__main__":
    main()
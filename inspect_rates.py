#!/usr/bin/env python3
"""
Simple inspector to peek into rates parquet files and compare against schema.
"""

import pandas as pd
import json
import gzip
import ijson
import io
from pathlib import Path
import requests

def inspect_rates_file(parquet_path: str):
    """Peek into a rates parquet file and show structure + sample data."""
    
    # Load the parquet file
    df = pd.read_parquet(parquet_path)
    
    print(f"📊 RATES FILE INSPECTION: {Path(parquet_path).name}")
    print("=" * 60)
    
    # Part 1: Basic stats
    print(f"Total records: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")
    print()
    
    # Part 2: Sample rate record (show first record in readable format)
    print("📋 SAMPLE RATE RECORD:")
    print("-" * 30)
    sample_record = df.iloc[0].to_dict()
    for key, value in sample_record.items():
        # Truncate long values for readability
        if isinstance(value, str) and len(value) > 100:
            value = value[:100] + "..."
        print(f"{key}: {value}")
    print()
    
    # Part 3: Field analysis
    print("🔍 FIELD ANALYSIS:")
    print("-" * 20)
    print("Columns in our file:")
    for i, col in enumerate(sorted(df.columns), 1):
        print(f"  {i:2d}. {col}")
    print()
    
    # Part 4: Data type check
    print("📝 DATA TYPES:")
    print("-" * 15)
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = df[col].isnull().sum()
        print(f"{col:25} | {dtype:15} | {null_count:6,} nulls")
    
    print()
    print("✅ Inspection complete!")

def inspect_raw_mrf(mrf_url: str, max_items: int = 3):
    """Peek into raw MRF file to see all available fields."""
    
    print(f"🔍 RAW MRF INSPECTION: {mrf_url.split('/')[-1]}")
    print("=" * 60)
    
    try:
        # Stream the first few items from the MRF
        response = requests.get(mrf_url, stream=True)
        response.raise_for_status()
        
        # Decompress the gzipped content
        import io
        gz_content = gzip.decompress(response.content)
        
        # Use ijson to parse just the first few items
        items = ijson.items(io.BytesIO(gz_content), 'in_network.item')
        
        print(f"📋 SAMPLE RAW MRF ITEMS (first {max_items}):")
        print("-" * 40)
        
        for i, item in enumerate(items):
            if i >= max_items:
                break
                
            print(f"\n--- ITEM {i+1} ---")
            print(f"Item fields: {list(item.keys())}")
            
            # Show negotiated_rates structure
            if 'negotiated_rates' in item:
                rates = item['negotiated_rates']
                print(f"  Negotiated rates count: {len(rates)}")
                if rates:
                    rate = rates[0]
                    print(f"  Rate fields: {list(rate.keys())}")
                    
                    # Show negotiated_prices structure
                    if 'negotiated_prices' in rate:
                        prices = rate['negotiated_prices']
                        print(f"    Prices count: {len(prices)}")
                        if prices:
                            price = prices[0]
                            print(f"    Price fields: {list(price.keys())}")
                            # Show service_code structure specifically
                            if 'service_code' in price:
                                print(f"    Service codes: {price['service_code']} (type: {type(price['service_code'])})")
            
            # Show provider structure
            if 'negotiated_rates' in item and item['negotiated_rates']:
                rate = item['negotiated_rates'][0]
                if 'provider_groups' in rate:
                    print(f"  Provider groups: {len(rate['provider_groups'])}")
                    if rate['provider_groups']:
                        provider = rate['provider_groups'][0]
                        print(f"    Provider fields: {list(provider.keys())}")
        
        print("\n✅ Raw MRF inspection complete!")
        
    except Exception as e:
        print(f"❌ Error inspecting raw MRF: {e}")

def compare_sources(parquet_path: str, mrf_url: str):
    """Side-by-side comparison of our extraction vs raw MRF vs schema."""
    
    print(f"📊 SIDE-BY-SIDE COMPARISON")
    print("=" * 60)
    
    # Load our parquet data
    df = pd.read_parquet(parquet_path)
    our_fields = set(df.columns)
    
    # Get raw MRF fields (from first few items)
    raw_fields = set()
    sample_raw_record = None
    
    try:
        response = requests.get(mrf_url, stream=True)
        response.raise_for_status()
        gz_content = gzip.decompress(response.content)
        items = ijson.items(io.BytesIO(gz_content), 'in_network.item')
        
        for i, item in enumerate(items):
            if i >= 3:  # Just first 3 items
                break
            raw_fields.update(item.keys())
            
            # Get a sample with negotiated_prices for detailed comparison
            if 'negotiated_rates' in item and item['negotiated_rates']:
                rate = item['negotiated_rates'][0]
                if 'negotiated_prices' in rate and rate['negotiated_prices']:
                    price = rate['negotiated_prices'][0]
                    raw_fields.update(price.keys())
                    if sample_raw_record is None:
                        sample_raw_record = {
                            'item_fields': list(item.keys()),
                            'price_fields': list(price.keys()),
                            'sample_price': price
                        }
    except Exception as e:
        print(f"❌ Error getting raw fields: {e}")
        return
    
    # Schema required fields (from the sample JSON)
    schema_fields = {
        'negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version',
        'billing_code', 'description', 'negotiated_rates', 'negotiated_prices',
        'provider_references', 'provider_groups', 'negotiated_rate', 'negotiated_type',
        'expiration_date', 'billing_class', 'service_code', 'billing_code_modifier',
        'additional_information'
    }
    
    # Comparison table
    print("🔍 FIELD COMPARISON:")
    print("-" * 50)
    all_fields = our_fields | raw_fields | schema_fields
    all_fields = sorted(all_fields)
    
    for field in all_fields:
        status = []
        if field in our_fields:
            status.append("✅ OUR")
        if field in raw_fields:
            status.append("🌐 RAW")
        if field in schema_fields:
            status.append("📋 SCHEMA")
        
        status_str = " | ".join(status) if status else "❌ MISSING"
        print(f"{field:25} | {status_str}")
    
    print("\n📋 SAMPLE RECORD COMPARISON:")
    print("-" * 40)
    
    # Our sample record
    our_sample = df.iloc[0].to_dict()
    print("OUR EXTRACTED:")
    for key, value in our_sample.items():
        if isinstance(value, str) and len(value) > 80:
            value = value[:80] + "..."
        print(f"  {key}: {value}")
    
    print("\nRAW MRF (negotiated_price):")
    if sample_raw_record:
        for key, value in sample_raw_record['sample_price'].items():
            print(f"  {key}: {value}")
    
    print("\n✅ Comparison complete!")

if __name__ == "__main__":
    # Inspect the UHC Missouri rates file
    inspect_rates_file("output/uhc_mo/rates_mo_uhc_20250921_140551.parquet")
    
    print("\n" + "="*80 + "\n")
    
    # Inspect the raw MRF source
    mrf_url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-09-01/2025-09-01_UnitedHealthcare-Insurance-Company_Insurer_Missouri-Provider-Network_MONETWORKEXGN_in-network-rates.json.gz"
    inspect_raw_mrf(mrf_url)
    
    print("\n" + "="*80 + "\n")
    
    # Side-by-side comparison
    compare_sources("output/uhc_mo/rates_mo_uhc_20250921_140551.parquet", mrf_url)

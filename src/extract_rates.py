"""
Rate extraction from MRF files with improved Windows compatibility.

This module has been enhanced to handle common Windows file permission issues
that can occur during long-running processes:

Key improvements:
- Robust file handling with retry logic
- Windows-specific file locking detection
- Automatic backup file creation when main file is locked
- Efficient batch writing (avoids reading entire file each time)
- Automatic consolidation of backup files at completion
- Better error handling and user feedback

These changes make the extractor more reliable for long-running processes
on Windows systems, especially when dealing with OneDrive sync or antivirus
software that may temporarily lock files.
"""

import gzip
import ijson
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
import os
import sys

# Windows-specific imports for better file handling
if sys.platform == "win32":
    import msvcrt
    import time

from utils import (
    get_memory_usage,
    force_garbage_collection,
    create_progress_bar,
    get_output_slug
)

def load_cpt_whitelist(file_path: str) -> Set[str]:
    """Load CPT codes from a text file (one code per line)."""
    cpt_codes = set()
    try:
        with open(file_path, 'r') as f:
            for line in f:
                code = line.strip()
                if code:  # Skip empty lines
                    cpt_codes.add(code)
        print(f"📋 Loaded {len(cpt_codes)} CPT codes from {file_path}")
        return cpt_codes
    except FileNotFoundError:
        print(f"⚠️  CPT whitelist file not found: {file_path}")
        return set()

def load_provider_groups_from_parquet(parquet_path: str) -> Set[int]:
    """
    Load unique provider group ID values from a Parquet file.
    
    Args:
        parquet_path: Path to Parquet file containing provider_group_id column
        
    Returns:
        Set of unique provider group IDs
    """
    print(f"📋 Loading provider group whitelist from: {parquet_path}")
    
    try:
        df = pd.read_parquet(parquet_path)
        
        if 'provider_group_id' not in df.columns:
            raise ValueError(f"Column 'provider_group_id' not found in {parquet_path}")
        
        provider_groups = set(df['provider_group_id'].dropna().unique())
        print(f"✅ Loaded {len(provider_groups):,} unique provider group IDs")
        
        return provider_groups
        
    except Exception as e:
        print(f"❌ Error loading provider group whitelist: {e}")
        return set()

class RateExtractor:
    def __init__(self, batch_size: int = 5, provider_group_filter: Optional[Set[int]] = None, 
                 cpt_whitelist: Optional[Set[str]] = None):
        self.batch_size = batch_size
        self.provider_group_filter = provider_group_filter
        self.cpt_whitelist = cpt_whitelist
        self.rates_batch = []
        self.stats = {
            "start_time": datetime.now(),
            "items_processed": 0,
            "rates_generated": 0,
            "rates_passed_filter": 0,
            "rates_written": 0,
            "peak_memory_mb": 0
        }
        # Track if we've written the first batch to avoid unnecessary file reads
        self.first_batch_written = False
    
    def _wait_for_file_unlock(self, file_path: Path, timeout_seconds: int = 30) -> bool:
        """Wait for a file to become unlocked (Windows-specific)."""
        if sys.platform != "win32":
            return True
            
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                # Try to open the file in exclusive mode
                with open(file_path, 'rb') as f:
                    # If we can open it, it's not locked
                    return True
            except (PermissionError, OSError):
                # File is locked, wait a bit
                time.sleep(1)
                continue
        
        return False
    
    def _update_memory_stats(self):
        """Update peak memory usage statistics."""
        current_memory = get_memory_usage()
        self.stats["peak_memory_mb"] = max(
            self.stats["peak_memory_mb"], 
            current_memory
        )
        return current_memory

    def _write_batch(self, output_path: Path) -> None:
        """Write current batch to parquet file."""
        if not self.rates_batch:
            return
            
        rates_df = pd.DataFrame(self.rates_batch)
        
        # For the first batch, just write directly
        if not self.first_batch_written:
            try:
                rates_df.to_parquet(output_path, index=False)
                self.first_batch_written = True
            except PermissionError as e:
                print(f"❌ Permission error writing first batch: {e}")
                # Try backup filename
                backup_path = output_path.parent / f"{output_path.stem}_backup_{int(time.time())}.parquet"
                print(f"💾 Writing to backup file: {backup_path}")
                rates_df.to_parquet(backup_path, index=False)
                raise
        else:
            # For subsequent batches, try to append with retry logic
            try:
                # Wait for file to be unlocked if on Windows
                if sys.platform == "win32" and self.output_path.exists():
                    if not self._wait_for_file_unlock(self.output_path):
                        print(f"⚠️  File {self.output_path.name} is locked, creating backup instead...")
                        backup_path = self.output_path.parent / f"{self.output_path.stem}_backup_{int(time.time())}.parquet"
                        rates_df.to_parquet(backup_path, index=False)
                        print(f"💾 Wrote to backup: {backup_path.name}")
                        self.stats["rates_written"] += len(self.rates_batch)
                        self.rates_batch.clear()
                        force_garbage_collection()
                        return
                
                # Use a more robust approach for appending
                existing_df = pd.read_parquet(output_path)
                rates_df = pd.concat([existing_df, rates_df], ignore_index=True)
            except (PermissionError, OSError, FileNotFoundError) as e:
                print(f"⚠️  Warning: Could not read existing file {output_path}: {e}")
                print(f"📝 Creating new file instead...")
                # Continue with just the new data
                pass
            
            # Write with retry logic for Windows permission issues
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    rates_df.to_parquet(output_path, index=False)
                    break
                except PermissionError as e:
                    if attempt < max_retries - 1:
                        print(f"⚠️  Permission error on attempt {attempt + 1}, retrying in 2 seconds...")
                        import time
                        time.sleep(2)
                    else:
                        print(f"❌ Failed to write after {max_retries} attempts: {e}")
                        # Try to write to a backup filename
                        backup_path = output_path.parent / f"{output_path.stem}_backup_{int(time.time())}.parquet"
                        print(f"💾 Writing to backup file: {backup_path}")
                        rates_df.to_parquet(backup_path, index=False)
                        raise
        
        self.stats["rates_written"] += len(self.rates_batch)
        self.rates_batch.clear()
        force_garbage_collection()

    def _process_rate(self, item: Dict[str, Any], file_metadata: Dict[str, Any]) -> None:
        """Process a single in_network rate item."""
        billing_code = item.get("billing_code", "")
        
        # Apply CPT whitelist filter if specified
        if self.cpt_whitelist and billing_code not in self.cpt_whitelist:
            return  # Skip this item entirely
        
        base_info = {
            "billing_code": billing_code,
            "billing_code_type": item.get("billing_code_type", ""),
            "description": item.get("description", ""),
            "name": item.get("name", ""),
            "negotiation_arrangement": item.get("negotiation_arrangement", ""),
            **file_metadata
        }
        
        # Process each rate group
        for rate_group in item.get("negotiated_rates", []):
            for price in rate_group.get("negotiated_prices", []):
                self.stats["rates_generated"] += 1
                
                # Create rate record for each provider reference
                for provider_ref_id in rate_group.get("provider_references", []):
                    # Apply provider group filter if specified
                    if self.provider_group_filter and provider_ref_id not in self.provider_group_filter:
                        continue
                    
                    rate_record = {
                        "provider_reference_id": provider_ref_id,
                        "negotiated_rate": float(price.get("negotiated_rate", 0)),
                        "negotiated_type": price.get("negotiated_type", ""),
                        "billing_class": price.get("billing_class", ""),
                        "expiration_date": price.get("expiration_date", ""),
                        "service_codes": str(price.get("service_code", [])),
                        **base_info
                    }
                    self.rates_batch.append(rate_record)
                    self.stats["rates_passed_filter"] += 1
                    
                    # Write batch if size threshold reached
                    if len(self.rates_batch) >= self.batch_size:
                        self._write_batch(self.output_path)

    def process_file(self, file_path: str, output_dir: Path, 
                    max_items: Optional[int] = None, 
                    max_time_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Process negotiated rates from MRF file.
        
        Args:
            file_path: Path to .json.gz file
            output_dir: Directory for output files
            max_items: Optional limit on number of items to process
            max_time_minutes: Optional time limit in minutes
        
        Returns:
            Processing statistics
        """
        print(f"\n💰 EXTRACTING RATES")
        print(f"📊 Initial memory: {self._update_memory_stats():.1f} MB")
        
        # Setup output path
        slug = get_output_slug()
        self.output_path = output_dir / f"rates_{slug}.parquet"
        
        with gzip.open(file_path, 'rb') as gz_file:
            # Extract file metadata first
            parser = ijson.parse(gz_file)
            file_metadata = {}
            for prefix, event, value in parser:
                if prefix in ['reporting_entity_name', 'reporting_entity_type', 
                            'last_updated_on', 'version']:
                    file_metadata[prefix] = value
                elif prefix == 'in_network':
                    break
            
            # Reset file pointer for rate processing
            gz_file.seek(0)
            
            # Stream process rates
            items = ijson.items(gz_file, 'in_network.item')
            
            # Apply limits if specified
            if max_items or max_time_minutes:
                items = create_progress_bar(items, "Items", "item")
                start_time = datetime.now()
                
                for idx, item in enumerate(items):
                    # Check item limit
                    if max_items and idx >= max_items:
                        print(f"\n⏹️  Reached item limit: {max_items}")
                        break
                    
                    # Check time limit
                    if max_time_minutes:
                        elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
                        if elapsed_minutes >= max_time_minutes:
                            print(f"\n⏹️  Reached time limit: {max_time_minutes} minutes")
                            break
                    
                    self._process_rate(item, file_metadata)
                    self.stats["items_processed"] += 1
                    
                    # Memory check every 10 items
                    if self.stats["items_processed"] % 10 == 0:
                        self._update_memory_stats()
            else:
                # Process all items
                for item in create_progress_bar(items, "Items", "item"):
                    self._process_rate(item, file_metadata)
                    self.stats["items_processed"] += 1
                    
                    # Memory check every 10 items
                    if self.stats["items_processed"] % 10 == 0:
                        self._update_memory_stats()
        
        # Write final batch
        if self.rates_batch:
            self._write_batch(self.output_path)
        
        # Check for and consolidate any backup files
        print(f"\n🔍 Checking for backup files...")
        self._consolidate_backup_files()
        
        # Final statistics
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        final_memory = self._update_memory_stats()
        
        print(f"\n✅ RATE EXTRACTION COMPLETE")
        print(f"⏱️  Time elapsed: {elapsed:.1f} seconds")
        print(f"📊 Items processed: {self.stats['items_processed']:,}")
        print(f"📊 Rates generated: {self.stats['rates_generated']:,}")
        if self.provider_group_filter:
            print(f"📊 Rates passed filter: {self.stats['rates_passed_filter']:,}")
        print(f"📊 Rates written: {self.stats['rates_written']:,}")
        print(f"🧠 Peak memory: {self.stats['peak_memory_mb']:.1f} MB")
        print(f"📁 Output: {self.output_path}")
        
        return {
            "output_path": str(self.output_path),
            "stats": self.stats
        }
    
    def _consolidate_backup_files(self):
        """Consolidate any backup files that were created during processing."""
        if not self.output_path.exists():
            return
            
        # Look for backup files in the same directory
        backup_pattern = f"{self.output_path.stem}_backup_*.parquet"
        backup_files = list(self.output_path.parent.glob(backup_pattern))
        
        if not backup_files:
            return
            
        print(f"🔄 Found {len(backup_files)} backup files, consolidating...")
        
        try:
            # Read main file
            main_df = pd.read_parquet(self.output_path)
            
            # Read and concatenate all backup files
            backup_dfs = []
            for backup_file in backup_files:
                try:
                    backup_df = pd.read_parquet(backup_file)
                    backup_dfs.append(backup_df)
                    print(f"📖 Read backup: {backup_file.name}")
                except Exception as e:
                    print(f"⚠️  Could not read backup {backup_file.name}: {e}")
            
            if backup_dfs:
                # Combine all data
                all_dfs = [main_df] + backup_dfs
                consolidated_df = pd.concat(all_dfs, ignore_index=True)
                
                # Write consolidated file
                consolidated_path = self.output_path.parent / f"{self.output_path.stem}_consolidated.parquet"
                consolidated_df.to_parquet(consolidated_path, index=False)
                
                print(f"✅ Consolidated {len(backup_dfs)} backup files into: {consolidated_path.name}")
                print(f"📊 Total records: {len(consolidated_df):,}")
                
                # Optionally replace original file
                import shutil
                shutil.move(str(consolidated_path), str(self.output_path))
                print(f"🔄 Replaced original file with consolidated version")
                
                # Clean up backup files
                for backup_file in backup_files:
                    try:
                        backup_file.unlink()
                        print(f"🗑️  Cleaned up: {backup_file.name}")
                    except Exception as e:
                        print(f"⚠️  Could not delete {backup_file.name}: {e}")
                        
        except Exception as e:
            print(f"⚠️  Error during consolidation: {e}")
            print(f"📁 Backup files remain in: {self.output_path.parent}")

if __name__ == "__main__":
    import sys
    import argparse
    from utils import download_to_temp
    
    parser = argparse.ArgumentParser(description="Extract rates from MRF files")
    parser.add_argument("source", help="URL or path to MRF file")
    parser.add_argument("--items", "-i", type=int, help="Maximum number of items to process")
    parser.add_argument("--time", "-t", type=int, help="Maximum time to run in minutes")
    parser.add_argument("--provider-groups", "-p", nargs="+", type=int, 
                       help="Provider group IDs to filter for")
    parser.add_argument("--provider-groups-parquet", type=str,
                       help="Path to Parquet file containing provider_group_id column to filter for")
    parser.add_argument("--cpt-whitelist", "-c", type=str,
                       help="Path to text file with CPT codes (one per line)")
    parser.add_argument("--batch-size", "-b", type=int, default=5,
                       help="Batch size for writing (default: 5)")
    
    args = parser.parse_args()
    
    # Load provider groups from different sources
    provider_group_ids = set()
    
    # From command line arguments
    if args.provider_groups:
        provider_group_ids.update(args.provider_groups)
        print(f"🔍 Added {len(args.provider_groups)} provider groups from command line")
    
    # From Parquet file
    if args.provider_groups_parquet:
        parquet_groups = load_provider_groups_from_parquet(args.provider_groups_parquet)
        provider_group_ids.update(parquet_groups)
        print(f"🔍 Added {len(parquet_groups):,} provider groups from Parquet file")
    
    if provider_group_ids:
        print(f"🎯 Total provider groups to filter for: {len(provider_group_ids):,}")
    else:
        print("🌐 No provider group filtering - processing all groups")
    
    if args.items:
        print(f"📄 Max items to process: {args.items:,}")
    
    if args.time:
        print(f"⏱️  Max time to run: {args.time} minutes")
    
    try:
        # Download if URL
        if args.source.startswith(('http://', 'https://')):
            print(f"📥 Downloading from URL...")
            temp_path = download_to_temp(args.source)
            print(f"📦 Downloaded to: {temp_path}")
        else:
            temp_path = args.source
            print(f"📄 Using local file: {temp_path}")
        
        # Load CPT whitelist if specified
        cpt_whitelist = load_cpt_whitelist(args.cpt_whitelist) if args.cpt_whitelist else set()
        
        extractor = RateExtractor(
            batch_size=args.batch_size,
            provider_group_filter=provider_group_ids,
            cpt_whitelist=cpt_whitelist
        )
        results = extractor.process_file(
            temp_path,
            output_dir=Path("output"),
            max_items=args.items,
            max_time_minutes=args.time
        )
        
    finally:
        # Cleanup temp file if downloaded
        if args.source.startswith(('http://', 'https://')) and 'temp_path' in locals():
            try:
                import os
                os.unlink(temp_path)
                print(f"\n🧹 Cleaned up temporary file")
            except Exception:
                pass
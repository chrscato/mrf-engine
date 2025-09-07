# extract_providers.py
"""
Extract provider references from MRF files with memory-efficient streaming.

Enhancement: supports Cigna-style references where each entry in
'provider_references' includes a {provider_group_id, location} and the actual
'provider_groups' live in a separate JSON at that 'location' URL.
"""

import gzip
import ijson
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
import os
import sys

from .utils import (
    get_memory_usage,
    force_garbage_collection,
    create_progress_bar,
    get_output_slug,
    download_to_temp,  # used to fetch per-reference JSONs
)

def load_provider_group_whitelist(parquet_path: str) -> Set[int]:
    """
    Load unique provider_reference_id values from a Parquet file.
    Expects a 'provider_reference_id' column (output of your rate extractor).
    """
    print(f"📋 Loading provider group whitelist from: {parquet_path}")
    try:
        df = pd.read_parquet(parquet_path)
        if 'provider_reference_id' not in df.columns:
            raise ValueError(f"Column 'provider_reference_id' not found in {parquet_path}")
        groups = set(int(x) for x in df['provider_reference_id'].dropna().unique())
        print(f"✅ Loaded {len(groups):,} unique provider group IDs")
        return groups
    except Exception as e:
        print(f"❌ Error loading provider group whitelist: {e}")
        return set()

def load_tin_whitelist(file_path: str) -> Set[str]:
    """Load TIN values from a text file (one TIN per line)."""
    tin_values = set()
    try:
        with open(file_path, 'r') as f:
            for line in f:
                tin = line.strip()
                if tin:
                    tin_values.add(tin)
        print(f"📋 Loaded {len(tin_values)} TIN values from {file_path}")
        return tin_values
    except FileNotFoundError:
        print(f"⚠️  TIN whitelist file not found: {file_path}")
        return set()

class ProviderExtractor:
    def __init__(
        self,
        batch_size: int = 10000,
        provider_group_whitelist: Optional[Set[int]] = None,
        tin_whitelist: Optional[Set[str]] = None,
    ):
        self.batch_size = batch_size
        self.provider_group_whitelist = provider_group_whitelist or set()
        self.tin_whitelist = tin_whitelist or set()
        self.providers_batch: List[Dict[str, Any]] = []

        self.stats = {
            "providers_examined": 0,
            "providers_processed": 0,
            "providers_written": 0,
            "providers_filtered_by_group": 0,
            "providers_filtered_by_tin": 0,
            "ref_files_fetched": 0,
            "ref_files_errors": 0,
            "peak_memory_mb": 0,
            "start_time": datetime.now(),
        }

        # Avoid re-fetching the same provider reference URL or group id
        self._seen_ref_urls: Set[str] = set()
        self._seen_group_ids: Set[int] = set()

    def _update_memory_stats(self) -> float:
        current = get_memory_usage()
        self.stats["peak_memory_mb"] = max(self.stats["peak_memory_mb"], current)
        return current

    def _write_batch(self, output_path: Path) -> None:
        if not self.providers_batch:
            return
        df = pd.DataFrame(self.providers_batch)

        if output_path.exists():
            existing = pd.read_parquet(output_path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(output_path, index=False)
        self.stats["providers_written"] += len(self.providers_batch)
        self.providers_batch.clear()
        force_garbage_collection()

    def _iter_provider_groups_from_inline(self, provider_obj) -> Any:
        # Inline case used by some issuers (original behavior)
        for group in provider_obj.get("provider_groups", []):
            yield group

    def _iter_provider_groups_from_url(self, location_url: str) -> Any:
        """
        Cigna-style: fetch provider reference JSON at 'location' and stream its provider_groups.
        Handles both .json and .json.gz. Uses utils.download_to_temp for robust signed-URL support.
        """
        if not location_url:
            return

        if location_url in self._seen_ref_urls:
            # We've processed this reference file already (e.g., duplicate entries).
            return
        self._seen_ref_urls.add(location_url)

        temp_path = None
        try:
            temp_path = download_to_temp(location_url)
            self.stats["ref_files_fetched"] += 1
            is_gz = temp_path.lower().endswith(".gz")

            opener = gzip.open if is_gz else open
            mode = "rb"

            with opener(temp_path, mode) as f:
                # Expect structure: {"provider_groups": [{ "npi": [...], "tin": {...}}, ...]}
                for group in ijson.items(f, "provider_groups.item"):
                    yield group
        except Exception as e:
            self.stats["ref_files_errors"] += 1
            print(f"⚠️  Error fetching/parsing provider reference @ {location_url}: {e}")
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass

    def _iter_provider_groups(self, provider_obj) -> Any:
        """
        Decide whether provider groups are inline or at a remote 'location' URL.
        """
        if "provider_groups" in provider_obj and provider_obj["provider_groups"]:
            yield from self._iter_provider_groups_from_inline(provider_obj)
            return

        location_url = provider_obj.get("location")
        if location_url:
            yield from self._iter_provider_groups_from_url(location_url)
            return

        # Some issuers put a bare reference with only an id and no location (shouldn’t happen for Cigna refs).
        # We simply skip if no data source is present.
        return

    def _process_provider_reference(self, provider_ref: Dict[str, Any], file_meta: Dict[str, Any]) -> None:
        """
        Process a single entry under top-level 'provider_references' in the MRF index.
        This can be inline (has 'provider_groups') or Cigna-style (has 'location' URL).
        """
        provider_group_id = provider_ref.get("provider_group_id")
        self.stats["providers_examined"] += 1

        # Filter early by provider_group_whitelist if provided
        if self.provider_group_whitelist and provider_group_id not in self.provider_group_whitelist:
            self.stats["providers_filtered_by_group"] += 1
            return

        # Avoid double-processing same group id if encountered via duplicates
        if provider_group_id in self._seen_group_ids:
            return
        self._seen_group_ids.add(provider_group_id)

        # Iterate provider_groups (inline or fetched)
        any_emitted = False
        for group in self._iter_provider_groups(provider_ref):
            tin_info = group.get("tin", {}) or {}
            tin_value = str(tin_info.get("value", "")).strip()

            # Optional TIN filtering
            if self.tin_whitelist and tin_value and tin_value not in self.tin_whitelist:
                self.stats["providers_filtered_by_tin"] += 1
                continue

            # Emit one row per NPI in this provider_group
            for npi in group.get("npi", []):
                rec = {
                    "provider_group_id": provider_group_id,
                    "npi": str(npi),
                    "tin_type": tin_info.get("type", ""),
                    "tin_value": tin_value,
                    **file_meta,
                }
                self.providers_batch.append(rec)
                self.stats["providers_processed"] += 1
                any_emitted = True

                if len(self.providers_batch) >= self.batch_size:
                    self._write_batch(self.output_path)

        # Occasional progress log (less frequent)
        if any_emitted and (self.stats["providers_processed"] % 10000 == 0):
            print(
                f"  ✅ provider_group_id={provider_group_id} | "
                f"processed={self.stats['providers_processed']:,} | "
                f"written={self.stats['providers_written']:,}"
            )

        # Memory check
        if self.stats["providers_processed"] % 100 == 0:
            self._update_memory_stats()

    def process_file(self, file_path: str, output_dir: Path, max_providers: Optional[int] = None, output_prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Process provider references from a .json.gz MRF (index) file.
        If entries include 'location', fetch each referenced JSON and stream its provider_groups.
        """
        print(f"\n🔍 EXTRACTING PROVIDERS")
        print(f"📊 Initial memory: {self._update_memory_stats():.1f} MB")

        # Setup output path
        if output_prefix:
            slug = f"{output_prefix}_{get_output_slug()}"
        else:
            slug = get_output_slug()
        self.output_path = output_dir / f"providers_{slug}.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)

        with gzip.open(file_path, "rb") as gz_file:
            # Pull file metadata first
            parser = ijson.parse(gz_file)
            file_meta = {}
            for prefix, event, value in parser:
                if prefix in ["reporting_entity_name", "reporting_entity_type", "last_updated_on", "version"]:
                    file_meta[prefix] = value
                elif prefix == "provider_references":
                    break

            # Reset and stream over provider_references
            gz_file.seek(0)
            refs = ijson.items(gz_file, "provider_references.item")

            if max_providers:
                for idx, provider_ref in enumerate(refs):
                    if idx >= max_providers:
                        print(f"⏹️  Reached provider ref limit: {max_providers}")
                        break
                    self._process_provider_reference(provider_ref, file_meta)
            else:
                for idx, provider_ref in enumerate(refs):
                    self._process_provider_reference(provider_ref, file_meta)

        # Final write
        if self.providers_batch:
            self._write_batch(self.output_path)

        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        final_mem = self._update_memory_stats()

        print(f"\n✅ PROVIDER EXTRACTION COMPLETE")
        print(f"⏱️  Time elapsed: {elapsed:.1f}s")
        print(f"📊 Provider refs examined: {self.stats['providers_examined']:,}")
        print(f"📊 Filtered by group: {self.stats['providers_filtered_by_group']:,}")
        print(f"📊 Filtered by TIN: {self.stats['providers_filtered_by_tin']:,}")
        print(f"📊 Ref files fetched: {self.stats['ref_files_fetched']:,} (errors: {self.stats['ref_files_errors']:,})")
        print(f"📊 Provider rows written: {self.stats['providers_written']:,}")
        print(f"🧠 Peak memory: {self.stats['peak_memory_mb']:.1f} MB")
        print(f"📁 Output: {self.output_path}")

        return {"output_path": str(self.output_path), "stats": self.stats}

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract providers from MRF files (supports Cigna-style references).")
    parser.add_argument("source", help="URL or path to top-level MRF .json.gz index")
    parser.add_argument("--max-providers", "-m", type=int, help="Max number of provider references to process")
    parser.add_argument("--provider-whitelist", "-p", type=str,
                        help="Path to Parquet with 'provider_reference_id' column (to limit which refs to fetch)")
    parser.add_argument("--tin-whitelist", "-t", type=str,
                        help="Path to text file of TINs (one per line)")
    parser.add_argument("--output", "-o", type=str,
                        help="Output filename prefix (e.g., 'cigna_ga' leads to 'cigna_ga_providers_{datetime}.parquet')")
    parser.add_argument("--batch-size", "-b", type=int, default=10000, help="Batch size for Parquet writes (default: 10000)")
    args = parser.parse_args()

    # Download index if URL
    if args.source.startswith(("http://", "https://")):
        print("📥 Downloading MRF index...")
        temp_index = download_to_temp(args.source)
        print(f"📦 Downloaded to: {temp_index}")
        source_path = temp_index
    else:
        source_path = args.source
        print(f"📄 Using local file: {source_path}")

    # Load whitelists (strongly recommended to reduce ref fetches)
    provider_group_whitelist = load_provider_group_whitelist(args.provider_whitelist) if args.provider_whitelist else set()
    tin_whitelist = load_tin_whitelist(args.tin_whitelist) if args.tin_whitelist else set()

    try:
        extractor = ProviderExtractor(
            batch_size=args.batch_size,
            provider_group_whitelist=provider_group_whitelist,
            tin_whitelist=tin_whitelist,
        )
        extractor.process_file(
            file_path=source_path,
            output_dir=Path("output"),
            max_providers=args.max_providers,
            output_prefix=args.output,
        )
    finally:
        # Clean up index temp file if we downloaded it
        if args.source.startswith(("http://", "https://")) and "temp_index" in locals():
            try:
                os.unlink(temp_index)
                print("🧹 Cleaned up temporary index file")
            except Exception:
                pass

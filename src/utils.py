"""Shared utilities for MRF processing."""

import os
import gc
import psutil
import tempfile
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("📋 Note: Install tqdm for better progress bars: pip install tqdm")

def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def force_garbage_collection():
    """Force garbage collection and return memory usage."""
    gc.collect()
    return get_memory_usage()

def get_cloudfront_headers() -> Dict[str, str]:
    """Get headers for CloudFront requests."""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, application/octet-stream',
        'Accept-Encoding': 'gzip, deflate, br'
    }

def download_to_temp(url: str) -> str:
    """Download file to temp location and return path."""
    print(f"📥 Downloading from {url}...")
    headers = get_cloudfront_headers()
    response = requests.get(url, headers=headers, stream=True, timeout=300)
    response.raise_for_status()
    
    suffix = '.json.gz' if url.endswith('.gz') else '.json'
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                temp_file.write(chunk)
        return temp_file.name

def create_progress_bar(items, desc: str, unit: str):
    """Create a progress bar if tqdm is available."""
    if TQDM_AVAILABLE:
        return tqdm(items, desc=desc, unit=unit,
                   bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
    return items

def get_output_slug() -> str:
    """Generate a timestamp-based slug for output files."""
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def setup_output_dir(base_dir: str = "output") -> Path:
    """Create and return output directory path."""
    output_dir = Path(base_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir
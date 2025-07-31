"""
Local Data Manager - Handle local data cache operations
"""
import os
import glob
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

from ..engine.data_config import DataConfig


class LocalDataManager:
    """Manages local data cache for AWS data exports."""
    
    def __init__(self, config: DataConfig):
        """Initialize local data manager with configuration."""
        self.config = config
    
    def discover_data_files(self) -> List[str]:
        """
        Discover available local data files.
        
        Returns:
            List of local file paths
        """
        if not self.config.local_bucket_path:
            return []
        
        # Search for parquet files in local cache
        parquet_pattern = os.path.join(self.config.local_bucket_path, "**", "*.parquet")
        parquet_files = glob.glob(parquet_pattern, recursive=True)
        
        # Filter by date range if specified
        if self.config.date_start or self.config.date_end:
            parquet_files = self._filter_files_by_date(parquet_files)
        
        # Sort files for consistent ordering
        parquet_files.sort()
        
        if parquet_files:
            print(f"💾 Found {len(parquet_files)} local data files")
        
        return parquet_files
    
    def _filter_files_by_date(self, files: List[str]) -> List[str]:
        """Filter local files based on date range from file paths."""
        filtered_files = []
        
        for file_path in files:
            # Extract partition information from path
            path_parts = file_path.split(os.sep)
            
            # Look for partition format in path
            partition_date = None
            for part in path_parts:
                if part.startswith(self.config.partition_format + '='):
                    partition_date = part.split('=')[1]
                    break
            
            if partition_date and self._is_date_in_range(partition_date):
                filtered_files.append(file_path)
        
        return filtered_files
    
    def _is_date_in_range(self, partition_date: str) -> bool:
        """Check if a partition date is within the specified range."""
        # Handle different date formats
        if self.config.data_export_type.value == 'COH':
            # Daily format: YYYY-MM-DD
            try:
                partition_dt = datetime.strptime(partition_date, '%Y-%m-%d')
                
                if self.config.date_start:
                    start_dt = datetime.strptime(self.config.date_start, '%Y-%m-%d')
                    if partition_dt < start_dt:
                        return False
                
                if self.config.date_end:
                    end_dt = datetime.strptime(self.config.date_end, '%Y-%m-%d')
                    if partition_dt > end_dt:
                        return False
                
                return True
            except ValueError:
                return False
        else:
            # Monthly format: YYYY-MM
            if self.config.date_start and partition_date < self.config.date_start:
                return False
            
            if self.config.date_end and partition_date > self.config.date_end:
                return False
            
            return True
    
    def check_data_status(self) -> Dict[str, Any]:
        """
        Check the status of local data cache.
        
        Returns:
            Dictionary with cache status information
        """
        if not self.config.local_data_path:
            return {
                "local_cache_configured": False,
                "has_data": False,
                "total_files": 0,
                "total_size_mb": 0,
                "cache_path": None
            }
        
        # Check if cache directory exists
        cache_path = Path(self.config.local_bucket_path)
        if not cache_path.exists():
            return {
                "local_cache_configured": True,
                "has_data": False,
                "total_files": 0,
                "total_size_mb": 0,
                "cache_path": str(cache_path),
                "cache_exists": False
            }
        
        # Count files and calculate total size
        data_files = self.discover_data_files()
        total_size = 0
        
        for file_path in data_files:
            try:
                file_size = os.path.getsize(file_path)
                total_size += file_size
            except OSError:
                continue
        
        total_size_mb = total_size / (1024 * 1024)
        
        # Get last modified time of newest file
        last_updated = None
        if data_files:
            newest_file = max(data_files, key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0)
            if os.path.exists(newest_file):
                last_updated = datetime.fromtimestamp(os.path.getmtime(newest_file)).isoformat()
        
        return {
            "local_cache_configured": True,
            "has_data": len(data_files) > 0,
            "total_files": len(data_files),
            "total_size_mb": round(total_size_mb, 2),
            "cache_path": str(cache_path),
            "cache_exists": True,
            "last_updated": last_updated,
            "date_range": {
                "start": self.config.date_start,
                "end": self.config.date_end
            }
        }
    
    def clear_cache(self, confirm: bool = False) -> None:
        """
        Clear local data cache.
        
        Args:
            confirm: If True, proceed with deletion without prompting
        """
        if not self.config.local_bucket_path:
            print("❌ No local cache configured")
            return
        
        cache_path = Path(self.config.local_bucket_path)
        if not cache_path.exists():
            print("ℹ️  Local cache directory doesn't exist")
            return
        
        # Get status before deletion
        status = self.check_data_status()
        
        if not confirm:
            print(f"⚠️  This will delete {status['total_files']} files ({status['total_size_mb']:.1f} MB)")
            print(f"   Path: {cache_path}")
            response = input("Are you sure? (y/N): ")
            if response.lower() != 'y':
                print("❌ Cache clear cancelled")
                return
        
        # Delete files
        try:
            import shutil
            shutil.rmtree(cache_path)
            print(f"✅ Local cache cleared: {status['total_files']} files deleted ({status['total_size_mb']:.1f} MB freed)")
        except Exception as e:
            print(f"❌ Error clearing cache: {e}")
    
    def get_cache_info(self) -> None:
        """Print detailed information about the local cache."""
        status = self.check_data_status()
        
        print("\n" + "="*50)
        print("💾 Local Data Cache Status")
        print("="*50)
        
        if not status["local_cache_configured"]:
            print("❌ Local cache not configured")
            print("   Set local_data_path to enable caching")
            return
        
        print(f"📂 Cache Path: {status['cache_path']}")
        print(f"📊 Files: {status['total_files']}")
        print(f"💽 Size: {status['total_size_mb']:.1f} MB")
        
        if status["has_data"]:
            print(f"🕒 Last Updated: {status.get('last_updated', 'Unknown')}")
            if status["date_range"]["start"] or status["date_range"]["end"]:
                print(f"📅 Date Range: {status['date_range']['start'] or 'earliest'} to {status['date_range']['end'] or 'latest'}")
        else:
            print("📭 No data in cache")
            print("   Run download_data_locally() to populate cache")
        
        print("="*50)
"""
Data Downloader - Download S3 data to local cache for cost optimization
"""
import os
import shutil
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..engine.data_config import DataConfig
from ..auth import get_boto3_client


class DataDownloader:
    """Downloads S3 data to local cache for cost optimization."""
    
    def __init__(self, config: DataConfig):
        """Initialize data downloader with configuration."""
        self.config = config
    
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        return get_boto3_client(service_name, **creds)
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """
        Download S3 data to local storage for cost optimization.
        
        Args:
            overwrite: If True, overwrite existing local files
            show_progress: If True, show download progress
        """
        if not self.config.local_data_path:
            raise ValueError("❌ local_data_path must be configured for data download")
        
        print("🚀 Starting S3 to local data download...")
        print("💡 This one-time download eliminates future S3 query costs!")
        print()
        
        # Create local directory structure
        local_path = Path(self.config.local_bucket_path)
        local_path.mkdir(parents=True, exist_ok=True)
        
        # Get S3 files to download
        from .s3_data_manager import S3DataManager
        s3_manager = S3DataManager(self.config)
        s3_files = s3_manager.discover_data_files()
        
        if not s3_files:
            print("❌ No S3 files found to download")
            return
        
        print(f"📥 Found {len(s3_files)} files to download")
        
        # Convert S3 URIs to file paths and local paths
        download_tasks = []
        for s3_uri in s3_files:
            # Extract S3 key from URI
            s3_key = s3_uri.replace(f"s3://{self.config.s3_bucket}/", "")
            
            # Create local file path maintaining S3 structure
            local_file_path = os.path.join(self.config.local_data_path, self.config.s3_bucket, s3_key)
            
            # Check if file already exists
            if os.path.exists(local_file_path) and not overwrite:
                if show_progress:
                    print(f"⏭️  Skipping existing file: {os.path.basename(local_file_path)}")
                continue
            
            download_tasks.append((s3_key, local_file_path))
        
        if not download_tasks:
            print("✅ All files already exist locally. Use overwrite=True to re-download.")
            return
        
        print(f"⬇️  Downloading {len(download_tasks)} files...")
        print()
        
        # Download files
        s3_client = self._get_boto3_client('s3')
        total_size = 0
        failed_downloads = []
        
        if len(download_tasks) > 1 and show_progress:
            # Multi-threaded download for better performance
            total_size = self._download_files_parallel(s3_client, download_tasks, show_progress)
        else:
            # Single-threaded download
            for i, (s3_key, local_file_path) in enumerate(download_tasks, 1):
                try:
                    file_size = self._download_single_file(s3_client, s3_key, local_file_path, i, len(download_tasks), show_progress)
                    total_size += file_size
                except Exception as e:
                    failed_downloads.append((s3_key, str(e)))
                    if show_progress:
                        print(f"❌ Failed to download {s3_key}: {e}")
        
        # Summary
        print()
        print("📊 Download Summary:")
        print(f"   ✅ Successful: {len(download_tasks) - len(failed_downloads)} files")
        print(f"   📦 Total Size: {total_size / (1024*1024):.1f} MB")
        
        if failed_downloads:
            print(f"   ❌ Failed: {len(failed_downloads)} files")
            for s3_key, error in failed_downloads[:3]:  # Show first 3 errors
                print(f"      {s3_key}: {error}")
            if len(failed_downloads) > 3:
                print(f"      ... and {len(failed_downloads) - 3} more")
        
        print()
        print("🎉 Download complete!")
        print("💾 Future queries will use local data automatically (no S3 costs)")
    
    def _download_files_parallel(self, s3_client, download_tasks: List, show_progress: bool) -> int:
        """Download files in parallel for better performance."""
        total_size = 0
        failed_downloads = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all download tasks
            future_to_task = {
                executor.submit(self._download_single_file, s3_client, s3_key, local_file_path, 0, len(download_tasks), False): (s3_key, local_file_path)
                for s3_key, local_file_path in download_tasks
            }
            
            # Process completed downloads
            for future in as_completed(future_to_task):
                s3_key, local_file_path = future_to_task[future]
                completed += 1
                
                try:
                    file_size = future.result()
                    total_size += file_size
                    
                    if show_progress:
                        progress = (completed / len(download_tasks)) * 100
                        print(f"✅ [{completed}/{len(download_tasks)}] ({progress:.1f}%) {os.path.basename(local_file_path)}")
                        
                except Exception as e:
                    failed_downloads.append((s3_key, str(e)))
                    if show_progress:
                        print(f"❌ [{completed}/{len(download_tasks)}] Failed: {os.path.basename(local_file_path)}")
        
        return total_size
    
    def _download_single_file(self, s3_client, s3_key: str, local_file_path: str, file_num: int, total_files: int, show_progress: bool) -> int:
        """Download a single file from S3."""
        # Create local directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Download file
        if show_progress and file_num > 0:
            progress = (file_num / total_files) * 100
            print(f"⬇️  [{file_num}/{total_files}] ({progress:.1f}%) {os.path.basename(local_file_path)}")
        
        s3_client.download_file(self.config.s3_bucket, s3_key, local_file_path)
        
        # Get file size
        file_size = os.path.getsize(local_file_path)
        
        if show_progress and file_num > 0:
            print(f"✅ Downloaded: {file_size / (1024*1024):.1f} MB")
        
        return file_size
    
    def estimate_download_size(self) -> dict:
        """
        Estimate the size and cost of downloading data.
        
        Returns:
            Dictionary with size estimates and potential savings
        """
        print("📊 Estimating download requirements...")
        
        s3_client = self._get_boto3_client('s3')
        
        # Get S3 files
        from .s3_data_manager import S3DataManager
        s3_manager = S3DataManager(self.config)
        s3_files = s3_manager.discover_data_files()
        
        if not s3_files:
            return {
                "total_files": 0,
                "total_size_mb": 0,
                "estimated_download_time": "N/A",
                "potential_monthly_savings": 0
            }
        
        # Calculate total size
        total_size = 0
        for s3_uri in s3_files[:10]:  # Sample first 10 files for estimation
            s3_key = s3_uri.replace(f"s3://{self.config.s3_bucket}/", "")
            try:
                response = s3_client.head_object(Bucket=self.config.s3_bucket, Key=s3_key)
                total_size += response['ContentLength']
            except Exception:
                continue
        
        # Estimate total size based on sample
        if len(s3_files) > 10:
            avg_file_size = total_size / min(10, len(s3_files))
            total_size = avg_file_size * len(s3_files)
        
        total_size_mb = total_size / (1024 * 1024)
        
        # Rough estimates
        estimated_download_time = self._estimate_download_time(total_size_mb)
        potential_savings = self._estimate_query_cost_savings(total_size_mb)
        
        return {
            "total_files": len(s3_files),
            "total_size_mb": round(total_size_mb, 1),
            "estimated_download_time": estimated_download_time,
            "potential_monthly_savings": potential_savings
        }
    
    def _estimate_download_time(self, size_mb: float) -> str:
        """Estimate download time based on file size."""
        # Assume average download speed of 10 MB/s
        seconds = size_mb / 10
        
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            return f"{int(seconds / 60)} minutes"
        else:
            return f"{int(seconds / 3600)} hours"
    
    def _estimate_query_cost_savings(self, size_mb: float) -> float:
        """Estimate potential monthly cost savings from using local data."""
        # Very rough estimate: $5 per TB of S3 queries per month
        # This assumes multiple queries per month on the dataset
        size_tb = size_mb / (1024 * 1024)
        estimated_monthly_queries = 20  # Assume 20 queries per month
        cost_per_tb_query = 5.0
        
        return round(size_tb * estimated_monthly_queries * cost_per_tb_query, 2)
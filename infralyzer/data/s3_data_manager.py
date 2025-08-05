"""
S3 Data Manager - Handle S3 data discovery and access
"""
import re
from typing import List, Optional
from pathlib import Path

from ..engine.data_config import DataConfig
from ..auth import get_boto3_client


class S3DataManager:
    """Manages S3 data discovery and access for AWS data exports."""
    
    def __init__(self, config: DataConfig):
        """Initialize S3 data manager with configuration."""
        self.config = config
    
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        return get_boto3_client(service_name, **creds)
    
    def discover_data_files(self) -> List[str]:
        """
        Discover available parquet files in S3 based on date filters and partitioning.
        
        Returns:
            List of S3 URIs for discovered data files
        """
        s3_client = self._get_boto3_client('s3')
        
        # Get target partitions based on date filters
        target_partitions = self._get_target_partitions()
        
        all_files = []
        
        if target_partitions:
            # Query specific partitions
            for partition in target_partitions:
                partition_prefix = f"{self.config.s3_data_prefix}/{partition}/"
                files = self._scan_partition_directory(s3_client, partition_prefix)
                all_files.extend(files)
        else:
            # Query all partitions if no date filter
            files = self._scan_all_partitions(s3_client, self.config.s3_data_prefix)
            all_files.extend(files)
        
        # Filter and detect format
        if all_files:
            file_format = self._detect_file_format(all_files)
            print(f"Detected file format: {file_format}")
            
            # Filter files by format
            if file_format == 'parquet':
                all_files = [f for f in all_files if f.endswith('.parquet')]
            elif file_format == 'gzip':
                all_files = [f for f in all_files if f.endswith('.gz')]
        
        # Convert to S3 URIs
        s3_uris = [f"s3://{self.config.s3_bucket}/{file_path}" for file_path in all_files]
        
        return s3_uris
    
    def list_available_partitions(self) -> List[str]:
        """
        List all available partitions in the S3 data export.
        
        Returns:
            List of partition names (e.g., ['billing_period=2025-01', 'billing_period=2025-02'])
        """
        s3_client = self._get_boto3_client('s3')
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=self.config.s3_bucket,
                Prefix=f"{self.config.s3_data_prefix}/",
                Delimiter='/'
            )
            
            partitions = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    # Extract partition name from prefix
                    partition_path = prefix['Prefix'].rstrip('/')
                    partition_name = partition_path.split('/')[-1]
                    
                    # Validate partition format
                    if '=' in partition_name and partition_name.startswith(self.config.partition_format):
                        partitions.append(partition_name)
            
            partitions.sort()
            print(f"📅 Found {len(partitions)} partitions: {partitions[:5]}{'...' if len(partitions) > 5 else ''}")
            
            return partitions
            
        except Exception as e:
            print(f"Error listing partitions: {e}")
            return []
    
    def _get_target_partitions(self) -> List[str]:
        """
        Get target partitions based on date_start and date_end filters.
        
        Returns:
            List of partition directory names to query
        """
        if not self.config.date_start and not self.config.date_end:
            return []  # No filtering, scan all partitions
        
        # Get all available partitions
        all_partitions = self.list_available_partitions()
        
        if not all_partitions:
            return []
        
        # Filter partitions based on date range
        target_partitions = []
        
        for partition in all_partitions:
            # Extract date from partition name (e.g., "billing_period=2025-07" -> "2025-07")
            if '=' in partition:
                partition_date = partition.split('=')[1]
                
                # Check if partition date is within our range
                if self._is_date_in_range(partition_date):
                    target_partitions.append(partition)
        
        print(f"📅 Target partitions: {target_partitions}")
        return target_partitions
    
    def _is_date_in_range(self, partition_date: str) -> bool:
        """Check if a partition date is within the specified range."""
        # Handle different date formats
        if self.config.data_export_type.value == 'COH':
            # Daily format: YYYY-MM-DD
            from datetime import datetime
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
    
    def _scan_partition_directory(self, s3_client, partition_prefix: str) -> List[str]:
        """Scan a specific partition directory for data files."""
        files = []
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=partition_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith(('.parquet', '.gz')) and obj['Size'] > 0:
                            files.append(key)
            
            print(f"📂 Partition {partition_prefix}: {len(files)} files")
            
        except Exception as e:
            print(f"Error scanning partition {partition_prefix}: {e}")
        
        return files
    
    def _scan_all_partitions(self, s3_client, base_prefix: str) -> List[str]:
        """Scan all partitions for data files."""
        files = []
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=f"{base_prefix}/"):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith(('.parquet', '.gz')) and obj['Size'] > 0:
                            files.append(key)
            
            print(f"📂 All partitions: {len(files)} files")
            
        except Exception as e:
            print(f"Error scanning all partitions: {e}")
        
        return files
    
    def _detect_file_format(self, files: List[str]) -> str:
        """Detect whether files are parquet or gzip format."""
        parquet_count = sum(1 for f in files if f.endswith('.parquet'))
        gzip_count = sum(1 for f in files if f.endswith('.gz'))
        
        if parquet_count > gzip_count:
            return 'parquet'
        elif gzip_count > 0:
            return 'gzip'
        else:
            return 'unknown'
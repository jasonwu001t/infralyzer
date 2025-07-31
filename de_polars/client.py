"""
Backward-compatible DataExportsPolars client using new modular architecture
"""
import polars as pl
from typing import Optional, Dict, Any

from .engine import DuckDBEngine, DataConfig, DataExportType


class DataExportsPolars:
    """
    Backward-compatible interface for DataExportsPolars using new modular architecture.
    
    This maintains the same API as the original DataExportsPolars class but is now
    built on top of the new modular engine architecture for better maintainability.
    
    Usage (unchanged from original):
        data = DataExportsPolars(
            s3_bucket='my-bucket',
            s3_data_prefix='cur2/cur2/data', 
            data_export_type='CUR2.0',
            table_name='CUR'
        )
        result = data.query("SELECT * FROM CUR LIMIT 10")
    """
    
    def __init__(self, s3_bucket: str, s3_data_prefix: str, 
                 data_export_type: str,
                 table_name: str = "CUR",
                 date_start: Optional[str] = None, 
                 date_end: Optional[str] = None,
                 local_data_path: Optional[str] = None,
                 prefer_local_data: bool = True,
                 aws_region: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 aws_profile: Optional[str] = None,
                 role_arn: Optional[str] = None,
                 external_id: Optional[str] = None,
                 expiration: Optional[str] = None):
        """
        Initialize DataExportsPolars with backward-compatible interface.
        
        All parameters remain the same as the original implementation.
        """
        # Create DataConfig from parameters
        config = DataConfig(
            s3_bucket=s3_bucket,
            s3_data_prefix=s3_data_prefix,
            data_export_type=DataExportType(data_export_type),
            table_name=table_name,
            date_start=date_start,
            date_end=date_end,
            local_data_path=local_data_path,
            prefer_local_data=prefer_local_data,
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_profile=aws_profile,
            role_arn=role_arn,
            external_id=external_id,
            expiration=expiration
        )
        
        # Initialize the underlying engine
        self.engine = DuckDBEngine(config)
        
        # Store configuration for backward compatibility
        self.s3_bucket = s3_bucket
        self.s3_data_prefix = s3_data_prefix
        self.data_export_type = data_export_type
        self.table_name = table_name
        self.date_start = date_start
        self.date_end = date_end
        self.local_data_path = local_data_path
        self.prefer_local_data = prefer_local_data
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_profile = aws_profile
        self.role_arn = role_arn
        self.external_id = external_id
        self.expiration = expiration
    
    def query(self, sql: str, force_s3: bool = False) -> pl.DataFrame:
        """Execute SQL query on data export data using DuckDB SQL engine."""
        return self.engine.query(sql, force_s3)
    
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        return self.engine.has_local_data()
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """Download S3 data to local storage for cost optimization."""
        return self.engine.download_data_locally(overwrite, show_progress)
    
    def check_local_data_status(self) -> Dict[str, Any]:
        """Check status of local data cache."""
        return self.engine.check_local_data_status()
    
    def list_available_partitions(self):
        """List all available data partitions."""
        return self.engine.list_available_partitions()
    
    def schema(self) -> Dict[str, str]:
        """Get schema information for the data."""
        return self.engine.schema()
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        return self.engine.catalog()
    
    def sample(self, n: int = 10) -> pl.DataFrame:
        """Get a sample of the data."""
        return self.engine.sample(n)
    
    def info(self) -> None:
        """Print information about the data source and configuration."""
        return self.engine.info()
    
    # Additional properties for backward compatibility
    @property
    def config(self):
        """Access to underlying engine configuration."""
        return self.engine.config
    
    @property
    def partition_format(self):
        """Get partition format for this data export type."""
        return self.engine.config.partition_format
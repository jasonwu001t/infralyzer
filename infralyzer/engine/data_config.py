"""
Data configuration and export type definitions
"""
from enum import Enum
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


class DataExportType(Enum):
    """Supported AWS Data Export types"""
    FOCUS_1_0 = "FOCUS1.0"
    CUR_2_0 = "CUR2.0"  
    COH = "COH"
    CARBON_EMISSION = "CARBON_EMISSION"


# AWS Data Export type partition format mapping
DATA_EXPORT_PARTITION_FORMATS = {
    DataExportType.FOCUS_1_0: 'billing_period',      # lowercase for FOCUS 1.0 (monthly: billing_period=YYYY-MM)
    DataExportType.CUR_2_0: 'BILLING_PERIOD',        # uppercase for CUR 2.0 (monthly: BILLING_PERIOD=YYYY-MM)
    DataExportType.COH: 'date',                       # Cost Optimization Hub (daily: date=YYYY-MM-DD)
    DataExportType.CARBON_EMISSION: 'BILLING_PERIOD' # uppercase for Carbon Emissions (monthly: BILLING_PERIOD=YYYY-MM)
}

# Date format requirements by export type
DATA_EXPORT_DATE_FORMATS = {
    DataExportType.FOCUS_1_0: 'YYYY-MM',            # Monthly partitions
    DataExportType.CUR_2_0: 'YYYY-MM',              # Monthly partitions  
    DataExportType.COH: 'YYYY-MM-DD',               # Daily partitions
    DataExportType.CARBON_EMISSION: 'YYYY-MM'       # Monthly partitions
}


@dataclass
class DataConfig:
    """Configuration for AWS Data Export access"""
    
    # Core S3 Configuration
    s3_bucket: str
    s3_data_prefix: str
    data_export_type: DataExportType
    table_name: str = "CUR"
    
    # Date filtering
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    
    # Local data configuration
    local_data_path: Optional[str] = None
    prefer_local_data: bool = True
    
    # API Data Configuration
    enable_pricing_api: bool = False
    enable_savings_plans_api: bool = False
    api_cache_max_age_days: int = 1
    pricing_api_regions: Optional[List[str]] = None
    pricing_api_instance_types: Optional[List[str]] = None
    savings_plans_include_rates: bool = True
    
    # AWS Authentication
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_profile: Optional[str] = None
    role_arn: Optional[str] = None
    external_id: Optional[str] = None
    expiration: Optional[str] = None
    
    def __post_init__(self):
        """Validate and normalize configuration"""
        # Normalize data export type
        if isinstance(self.data_export_type, str):
            try:
                self.data_export_type = DataExportType(self.data_export_type)
            except ValueError:
                valid_types = [e.value for e in DataExportType]
                raise ValueError(f"Invalid data_export_type '{self.data_export_type}'. Must be one of: {valid_types}")
        
        # Clean S3 prefix
        self.s3_data_prefix = self.s3_data_prefix.rstrip('/')
        
        # Set local data path if provided
        if self.local_data_path:
            import os
            self.local_data_path = os.path.abspath(self.local_data_path)
    
    @property
    def partition_format(self) -> str:
        """Get partition format for this data export type"""
        return DATA_EXPORT_PARTITION_FORMATS[self.data_export_type]
    
    @property
    def date_format(self) -> str:
        """Get expected date format for this data export type"""
        return DATA_EXPORT_DATE_FORMATS[self.data_export_type]
    
    @property
    def local_bucket_path(self) -> Optional[str]:
        """Get local path that mirrors S3 bucket structure"""
        if not self.local_data_path:
            return None
        import os
        return os.path.join(self.local_data_path, self.s3_bucket, self.s3_data_prefix)
    
    def get_aws_credentials(self) -> Dict[str, Any]:
        """Get AWS credentials as dictionary"""
        creds = {}
        if self.aws_region:
            creds['aws_region'] = self.aws_region
        if self.aws_access_key_id:
            creds['aws_access_key_id'] = self.aws_access_key_id
        if self.aws_secret_access_key:
            creds['aws_secret_access_key'] = self.aws_secret_access_key
        if self.aws_session_token:
            creds['aws_session_token'] = self.aws_session_token
        if self.aws_profile:
            creds['aws_profile'] = self.aws_profile
        if self.role_arn:
            creds['role_arn'] = self.role_arn
        if self.external_id:
            creds['external_id'] = self.external_id
        return creds
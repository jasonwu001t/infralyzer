"""
DE Polars client - Clean API for AWS Data Exports analysis with DuckDB SQL engine
"""
import polars as pl
import duckdb
from typing import Optional, List, Dict, Any
from datetime import datetime
from .auth import check_credential_expiration, get_boto3_client, get_storage_options

# AWS Data Export type partition format mapping
DATA_EXPORT_PARTITION_FORMATS = {
    'FOCUS1.0': 'billing_period',      # lowercase for FOCUS 1.0 (monthly: billing_period=YYYY-MM)
    'CUR2.0': 'BILLING_PERIOD',        # uppercase for CUR 2.0 (monthly: BILLING_PERIOD=YYYY-MM)
    'COH': 'date',                     # Cost Optimization Hub (daily: date=YYYY-MM-DD)
    'CARBON_EMISSION': 'BILLING_PERIOD'  # uppercase for Carbon Emissions (monthly: BILLING_PERIOD=YYYY-MM)
}

# Date format requirements by export type
DATA_EXPORT_DATE_FORMATS = {
    'FOCUS1.0': 'YYYY-MM',            # Monthly partitions
    'CUR2.0': 'YYYY-MM',              # Monthly partitions  
    'COH': 'YYYY-MM-DD',              # Daily partitions
    'CARBON_EMISSION': 'YYYY-MM'      # Monthly partitions
}

class DataExportsPolars:
    """
    Advanced SQL interface for querying AWS Data Exports powered by DuckDB.
    
    Features:
    - 🧠 DuckDB SQL Engine: Advanced SQL with window functions, CTEs, complex joins
    - 📊 Partition-aware Discovery: Automatically optimizes S3 data access
    - 🚀 High Performance: Columnar processing for large datasets
    - 🔒 AWS Integration: Secure credential management and S3 access
    
    Supports all AWS Data Exports products with automatic partition format detection
    and handles both Parquet and Gzip file formats:
    - AWS Cost and Usage Report (CUR 2.0) - uses BILLING_PERIOD=YYYY-MM (monthly)
    - FOCUS 1.0 - uses billing_period=YYYY-MM (monthly)  
    - Cost Optimization Hub (COH) - uses date=YYYY-MM-DD (daily)
    - Carbon emissions data - uses BILLING_PERIOD=YYYY-MM (monthly)
    
    File Format Support:
    - Parquet files (.parquet) - optimized columnar format
    - Gzip files (.gz) - compressed CSV format
    
    SQL Capabilities (via DuckDB):
    - Window functions (ROW_NUMBER, RANK, SUM OVER, etc.)
    - Common Table Expressions (WITH clauses)
    - Complex joins and subqueries
    - Advanced date/time functions (DATE_TRUNC, EXTRACT, etc.)
    - String functions and pattern matching
    - Analytical functions and aggregations
    
    Usage:
        # For FOCUS 1.0 data (lowercase partition format)
        focus_data = DataExportsPolars(
            s3_bucket='my-bucket', 
            s3_data_prefix='focus1/focus1/data',
            data_export_type='FOCUS1.0',
            table_name='FOCUS'
        )
        result = focus_data.query("SELECT * FROM FOCUS LIMIT 10")
        
        # For CUR 2.0 data (uppercase partition format)
        cur_data = DataExportsPolars(
            s3_bucket='my-bucket',
            s3_data_prefix='cur2/cur2/data', 
            data_export_type='CUR2.0'
        )
        result = cur_data.query("SELECT * FROM CUR LIMIT 10")
        
        # For Carbon emissions data (monthly partitions)
        carbon_data = DataExportsPolars(
            s3_bucket='my-bucket',
            s3_data_prefix='carbon/carbon/data',
            data_export_type='CARBON_EMISSION',
            table_name='CARBON',
            date_start='2025-07',              # Monthly format: YYYY-MM
            date_end='2025-07'
        )
        result = carbon_data.query("SELECT * FROM CARBON LIMIT 10")
        
        # For Cost Optimization Hub (daily partitions)
        coh_data = DataExportsPolars(
            s3_bucket='my-bucket',
            s3_data_prefix='coh/coh/data',
            data_export_type='COH',
            table_name='RECOMMENDATIONS',
            date_start='2025-07-15',           # Daily format: YYYY-MM-DD
            date_end='2025-07-20'
        )
        result = coh_data.query("SELECT * FROM RECOMMENDATIONS LIMIT 10")
    """
    
    def __init__(self, s3_bucket: str, s3_data_prefix: str, 
                 data_export_type: str,
                 table_name: str = "CUR",
                 date_start: Optional[str] = None, 
                 date_end: Optional[str] = None,
                 aws_region: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 aws_profile: Optional[str] = None,
                 role_arn: Optional[str] = None,
                 external_id: Optional[str] = None,
                 expiration: Optional[str] = None):
        """
        Initialize AWS Data Exports Polars interface.
        
        Args:
            s3_bucket: S3 bucket containing data export files
            s3_data_prefix: Full S3 prefix path to the data directory (e.g., 'focus1/focus1/data')
                           This should be the complete path up to where monthly folders like 
                           BILLING_PERIOD=2025-07 or billing_period=2025-07 are located
            data_export_type: Type of AWS Data Export. Options:
                             'FOCUS1.0' - FOCUS 1.0 data (uses billing_period=YYYY-MM, monthly partitions)
                             'CUR2.0' - Cost and Usage Report 2.0 (uses BILLING_PERIOD=YYYY-MM, monthly partitions)
                             'COH' - Cost Optimization Hub (uses date=YYYY-MM-DD, daily partitions)
                             'CARBON_EMISSION' - Carbon Emissions (uses BILLING_PERIOD=YYYY-MM, monthly partitions)
            table_name: Table name to use in SQL queries (default: "CUR")
                       Common options: "CUR", "FOCUS", "CARBON", "RECOMMENDATIONS"
            date_start: Start date. Format depends on data_export_type:
                       • FOCUS1.0, CUR2.0, CARBON_EMISSION: YYYY-MM format (e.g., '2025-07')
                       • COH: YYYY-MM-DD format (e.g., '2025-07-15')
                       If None, loads all data.
            date_end: End date. Format depends on data_export_type:
                     • FOCUS1.0, CUR2.0, CARBON_EMISSION: YYYY-MM format (e.g., '2025-07') 
                     • COH: YYYY-MM-DD format (e.g., '2025-07-20')
                     If None, loads all data.
            aws_region: AWS region (optional, auto-detected if not provided)
            aws_access_key_id: AWS access key ID (optional, uses environment if not provided)
            aws_secret_access_key: AWS secret access key (optional, uses environment if not provided)
            aws_session_token: AWS session token (optional, for temporary credentials/MFA)
            aws_profile: AWS profile name (optional, uses specific profile from ~/.aws/credentials)
            role_arn: IAM role ARN to assume (optional, for cross-account access)
            external_id: External ID for role assumption (optional, additional security)
            expiration: Expiration timestamp for temporary credentials (optional, ISO format or datetime)
        """
        # Validate data export type
        if data_export_type not in DATA_EXPORT_PARTITION_FORMATS:
            valid_types = list(DATA_EXPORT_PARTITION_FORMATS.keys())
            raise ValueError(f"Invalid data_export_type '{data_export_type}'. Must be one of: {valid_types}")
        
        self.s3_bucket = s3_bucket
        self.s3_data_prefix = s3_data_prefix.rstrip('/')  # Remove trailing slash for consistency
        self.data_export_type = data_export_type
        self.partition_format = DATA_EXPORT_PARTITION_FORMATS[data_export_type]
        self.table_name = table_name
        self.date_start = date_start
        self.date_end = date_end
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_profile = aws_profile
        self.role_arn = role_arn
        self.external_id = external_id
        self.expiration = expiration
        self._data = None
        self._schema_cache = None
        
        print(f"🎯 Data Export Type: {self.data_export_type}")
        
        # Show appropriate date format based on export type
        if self.data_export_type == 'COH':
            print(f"📁 Partition Format: {self.partition_format}=YYYY-MM-DD (daily)")
            print(f"📅 Date Format Required: YYYY-MM-DD (e.g., '2025-07-15')")
        else:
            print(f"📁 Partition Format: {self.partition_format}=YYYY-MM (monthly)")
            print(f"📅 Date Format Required: YYYY-MM (e.g., '2025-07')")
            
        print(f"📂 Data Path: s3://{self.s3_bucket}/{self.s3_data_prefix}/")
        
        # Validate and warn about credential expiration
        check_credential_expiration(self.expiration)
        
    def _get_boto3_client(self, service_name: str):
        """Create boto3 client with enhanced authentication support."""
        return get_boto3_client(
            service_name=service_name,
            aws_region=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            aws_profile=self.aws_profile,
            role_arn=self.role_arn,
            external_id=self.external_id
        )
        
    def _discover_data_files(self) -> List[str]:
        """Find data export files (parquet or gzip) in S3 using simplified partition-aware discovery."""
        s3 = self._get_boto3_client('s3')
        
        # Generate target partitions based on date filters
        target_partitions = self._get_target_partitions()
        
        if target_partitions:
            print(f"🎯 Partition-aware discovery: Looking for partitions {target_partitions}")
            if self.data_export_type == 'COH':
                print(f"📁 Using partition format: {self.partition_format}=YYYY-MM-DD (daily)")
            else:
                print(f"📁 Using partition format: {self.partition_format}=YYYY-MM (monthly)")
        else:
            print(f"📁 No date filters: Scanning all available partitions")
            if self.data_export_type == 'COH':
                print(f"📁 Using partition format: {self.partition_format}=YYYY-MM-DD (daily)")
            else:
                print(f"📁 Using partition format: {self.partition_format}=YYYY-MM (monthly)")
        
        data_files = []
        
        if target_partitions:
            # Scan specific monthly folders based on date filters
            for partition in target_partitions:
                partition_path = f"{self.s3_data_prefix}/{self.partition_format}={partition}/"
                print(f"   📂 Scanning: s3://{self.s3_bucket}/{partition_path}")
                
                files_found = self._scan_partition_directory(s3, partition_path)
                if files_found:
                    data_files.extend(files_found)
                    print(f"   ✅ Found {len(files_found)} files in partition {partition}")
                else:
                    print(f"   ❌ No files found in partition {partition}")
            
            # Strict enforcement: if user specified dates but no files found, raise error
            if not data_files:
                expected_paths = [f"s3://{self.s3_bucket}/{self.s3_data_prefix}/{self.partition_format}={partition}/" 
                                for partition in target_partitions]
                
                raise ValueError(
                    f"❌ PARTITION-AWARE DISCOVERY FAILED\n"
                    f"   Data Export Type: {self.data_export_type}\n"
                    f"   Requested partitions: {target_partitions}\n"
                    f"   Expected locations:\n" +
                    "\n".join(f"     • {path}" for path in expected_paths) +
                    f"\n\n💡 Possible solutions:\n"
                    f"   1. Check if data exists for the requested months\n"
                    f"   2. Verify s3_data_prefix: '{self.s3_data_prefix}' is correct\n"
                    f"   3. Verify data_export_type: '{self.data_export_type}' matches your data\n"
                    f"   4. Use different date range or remove date filters\n"
                    f"   5. Run list_available_partitions() to see what exists"
                )
        else:
            # No date filters: scan all available partitions
            print(f"   📁 Scanning all partitions: s3://{self.s3_bucket}/{self.s3_data_prefix}/")
            data_files = self._scan_all_partitions(s3, f"{self.s3_data_prefix}/")
            
            if not data_files:
                raise ValueError(
                    f"❌ NO DATA EXPORT FILES FOUND\n"
                    f"   Data Export Type: {self.data_export_type}\n"
                    f"   Expected location: s3://{self.s3_bucket}/{self.s3_data_prefix}/\n"
                    f"   Expected format: {self.partition_format}=YYYY-MM/\n\n"
                    f"💡 Solutions:\n"
                    f"   1. Check s3_data_prefix: '{self.s3_data_prefix}' is correct\n"
                    f"   2. Check data_export_type: '{self.data_export_type}' matches your data\n"
                    f"   3. Run list_available_partitions() to debug structure"
                )
        
        # Success message
        if target_partitions:
            print(f"🎉 SUCCESS: Found {len(data_files)} files in {len(target_partitions)} partition(s)")
            if self.data_export_type == 'COH':
                print(f"   📂 Structure: s3://{self.s3_bucket}/{self.s3_data_prefix}/{self.partition_format}=YYYY-MM-DD/")
            else:
                print(f"   📂 Structure: s3://{self.s3_bucket}/{self.s3_data_prefix}/{self.partition_format}=YYYY-MM/")
            print(f"   📊 Loaded partitions: {target_partitions}")
            print(f"   ⚡ Performance: Scanned only target partitions (not all data)!")
        else:
            print(f"📊 Discovered {len(data_files)} files across all available partitions")
            if self.data_export_type == 'COH':
                print(f"   📂 Structure: s3://{self.s3_bucket}/{self.s3_data_prefix}/{self.partition_format}=YYYY-MM-DD/")
            else:
                print(f"   📂 Structure: s3://{self.s3_bucket}/{self.s3_data_prefix}/{self.partition_format}=YYYY-MM/")
        
        return data_files
    
    def list_available_partitions(self) -> List[str]:
        """
        List all available partitions in the S3 location.
        Useful for debugging partition-aware discovery issues.
        
        Returns:
            List of available partition names (e.g., ['2025-01', '2025-07'])
        """
        s3 = self._get_boto3_client('s3')
        available_partitions = []
        
        print("🔍 Discovering available partitions...")
        print(f"   📂 Data Export Type: {self.data_export_type}")
        if self.data_export_type == 'COH':
            print(f"   📁 Expected format: {self.partition_format}=YYYY-MM-DD (daily)")
        else:
            print(f"   📁 Expected format: {self.partition_format}=YYYY-MM (monthly)")
        print(f"   📂 Checking: s3://{self.s3_bucket}/{self.s3_data_prefix}/")
        
        try:
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.s3_bucket,
                Prefix=f"{self.s3_data_prefix}/",
                Delimiter='/'
            )
            
            for page in pages:
                for prefix_info in page.get('CommonPrefixes', []):
                    prefix = prefix_info['Prefix']
                    # Look for our specific partition format
                    if f'{self.partition_format}=' in prefix:
                        # Extract partition name based on export type
                        import re
                        if self.data_export_type == 'COH':
                            # COH uses daily format: date=2025-07-15/ -> 2025-07-15
                            pattern = f'{re.escape(self.partition_format)}=(\\d{{4}}-\\d{{2}}-\\d{{2}})'
                        else:
                            # Other exports use monthly format: billing_period=2025-07/ -> 2025-07
                            pattern = f'{re.escape(self.partition_format)}=(\\d{{4}}-\\d{{2}})'
                        
                        match = re.search(pattern, prefix)
                        if match:
                            partition_name = match.group(1)
                            available_partitions.append(partition_name)
            
            if available_partitions:
                available_partitions = sorted(list(set(available_partitions)))
                print(f"   ✅ Found partitions: {available_partitions}")
            else:
                if self.data_export_type == 'COH':
                    print(f"   ❌ No {self.partition_format}=YYYY-MM-DD partitions found")
                else:
                    print(f"   ❌ No {self.partition_format}=YYYY-MM partitions found")
                
        except Exception as e:
            print(f"   ⚠️  Error accessing s3://{self.s3_bucket}/{self.s3_data_prefix}/: {e}")
        
        if available_partitions:
            print(f"\n📊 Summary: Found {len(available_partitions)} available partitions")
            print(f"   📅 Date range: {min(available_partitions)} to {max(available_partitions)}")
            print(f"   📁 All partitions: {available_partitions}")
        else:
            print(f"\n❌ No partitioned data structure found!")
            print(f"   💡 Your data might not follow AWS Data Exports partitioning")
            print(f"   💡 Expected structure: s3://bucket/prefix/data/BILLING_PERIOD=YYYY-MM/")
        
        return available_partitions
    
    def _get_target_partitions(self) -> List[str]:
        """Generate list of partitions based on data export type and date filters."""
        if not self.date_start and not self.date_end:
            return []  # No filters = scan all partitions
        
        from datetime import datetime, timedelta
        from dateutil.relativedelta import relativedelta
        
        partitions = []
        
        if self.data_export_type == 'COH':
            # COH uses daily partitions (date=YYYY-MM-DD)
            try:
                if self.date_start:
                    start_date = datetime.strptime(self.date_start, '%Y-%m-%d')
                else:
                    # Default to 30 days ago for COH
                    start_date = datetime.now() - timedelta(days=30)
                
                if self.date_end:
                    end_date = datetime.strptime(self.date_end, '%Y-%m-%d')
                else:
                    # Default to today for COH
                    end_date = datetime.now()
                
                # Generate daily partitions
                current = start_date
                while current <= end_date:
                    partitions.append(current.strftime('%Y-%m-%d'))
                    current += timedelta(days=1)
                    
            except ValueError as e:
                raise ValueError(
                    f"Invalid date format for COH export. "
                    f"COH requires YYYY-MM-DD format (e.g., '2025-07-15'). "
                    f"Error: {e}"
                )
        else:
            # Other exports use monthly partitions (YYYY-MM)
            try:
                if self.date_start:
                    start_date = datetime.strptime(self.date_start, '%Y-%m')
                else:
                    # Default to 2 years ago for monthly exports
                    start_date = datetime.now() - relativedelta(years=2)
                    start_date = start_date.replace(day=1)
                
                if self.date_end:
                    end_date = datetime.strptime(self.date_end, '%Y-%m')
                else:
                    # Default to current month for monthly exports
                    end_date = datetime.now().replace(day=1)
                
                # Generate monthly partitions
                current = start_date
                while current <= end_date:
                    partitions.append(current.strftime('%Y-%m'))
                    current += relativedelta(months=1)
                    
            except ValueError as e:
                export_types = [t for t in DATA_EXPORT_DATE_FORMATS.keys() if t != 'COH']
                raise ValueError(
                    f"Invalid date format for {self.data_export_type} export. "
                    f"{self.data_export_type} requires YYYY-MM format (e.g., '2025-07'). "
                    f"Error: {e}"
                )
        
        return partitions
    
    def _scan_partition_directory(self, s3_client, partition_prefix: str) -> List[str]:
        """Scan a specific partition directory for data files (parquet or gzip)."""
        data_files = []
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=partition_prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    # Support both parquet and gzip formats
                    if key.endswith('.parquet') or key.endswith('.gz'):
                        s3_url = f"s3://{self.s3_bucket}/{key}"
                        data_files.append(s3_url)
        except Exception as e:
            # Partition directory might not exist (e.g., future dates)
            print(f"⚠️  Partition directory {partition_prefix} not accessible: {e}")
        
        return data_files
    
    def _scan_all_partitions(self, s3_client, base_prefix: str) -> List[str]:
        """Scan all partition directories when no date filters are specified."""
        data_files = []
        
        try:
            # List all partition directories first
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.s3_bucket, 
                Prefix=base_prefix,
                Delimiter='/'
            )
            
            partition_prefixes = []
            for page in pages:
                # Look for partition directories with our specific format
                for prefix_info in page.get('CommonPrefixes', []):
                    prefix = prefix_info['Prefix']
                    if f'{self.partition_format}=' in prefix:
                        partition_prefixes.append(prefix)
            
            # Now scan each partition directory
            for partition_prefix in partition_prefixes:
                files_in_partition = self._scan_partition_directory(s3_client, partition_prefix)
                data_files.extend(files_in_partition)
                
                if files_in_partition:
                    # Extract partition name for logging
                    import re
                    if self.data_export_type == 'COH':
                        # COH uses daily format: date=2025-07-15/
                        pattern = f'{re.escape(self.partition_format)}=(\\d{{4}}-\\d{{2}}-\\d{{2}})'
                    else:
                        # Other exports use monthly format: billing_period=2025-07/
                        pattern = f'{re.escape(self.partition_format)}=(\\d{{4}}-\\d{{2}})'
                    
                    match = re.search(pattern, partition_prefix)
                    partition_name = match.group(1) if match else 'unknown'
                    print(f"✅ Found {len(files_in_partition)} files in partition {partition_name}")
        
        except Exception as e:
            print(f"⚠️  Error scanning partitions under {base_prefix}: {e}")
        
        return data_files
    

    
    def _detect_file_format(self, files: List[str]) -> str:
        """Detect the file format from the file list."""
        if not files:
            return 'unknown'
        
        # Check the first file to determine format
        first_file = files[0]
        if first_file.endswith('.parquet'):
            return 'parquet'
        elif first_file.endswith('.gz'):
            return 'gzip'
        else:
            return 'unknown'

    def _get_data(self) -> pl.LazyFrame:
        """Get data export data as Polars LazyFrame (cached)."""
        if self._data is None:
            files = self._discover_data_files()
            print(f"📊 Loading {len(files)} data export files...")
            
            # Configure storage options for S3 authentication
            storage_options = self._get_storage_options()
            
            # Detect file format and use appropriate scanner
            file_format = self._detect_file_format(files)
            
            if file_format == 'parquet':
                print(f"📁 Detected Parquet format - using optimized Parquet scanner")
                self._data = pl.scan_parquet(files, storage_options=storage_options)
            elif file_format == 'gzip':
                print(f"📁 Detected Gzip format - using CSV scanner with compression")
                # For gzip files, we assume they contain CSV data
                # Note: scan_csv can handle gzip compression automatically
                self._data = pl.scan_csv(
                    files, 
                    storage_options=storage_options,
                    separator=',',  # AWS Data Exports typically use comma separation
                    has_header=True,
                    try_parse_dates=True,
                    null_values=['', 'NULL', 'null']
                )
            else:
                raise ValueError(f"Unsupported file format detected. Found files: {files[:3]}...")
                
        return self._data
    
    def _get_storage_options(self) -> Dict[str, Any]:
        """Get storage options for S3 authentication in polars."""
        return get_storage_options(
            aws_region=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            role_arn=self.role_arn,
            external_id=self.external_id
        )
    
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure DuckDB connection with S3 support."""
        conn = duckdb.connect()
        
        # Install and load required extensions
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 credentials
        self._configure_duckdb_s3(conn)
        
        return conn
    
    def _configure_duckdb_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Configure DuckDB S3 settings with AWS credentials."""
        # Try to get credentials from multiple sources
        credentials = None
        region = None
        
        # First, try explicit credentials from constructor
        if self.aws_access_key_id and self.aws_secret_access_key:
            credentials = {
                'aws_access_key_id': self.aws_access_key_id,
                'aws_secret_access_key': self.aws_secret_access_key,
                'aws_session_token': self.aws_session_token
            }
            region = self.aws_region
            print("🔑 Using explicit credentials from constructor")
        else:
            # Fall back to boto3 credential resolution
            try:
                import boto3
                session = boto3.Session(
                    profile_name=self.aws_profile,
                    region_name=self.aws_region
                )
                
                # Get credentials from session
                session_credentials = session.get_credentials()
                if session_credentials:
                    credentials = {
                        'aws_access_key_id': session_credentials.access_key,
                        'aws_secret_access_key': session_credentials.secret_key,
                        'aws_session_token': session_credentials.token
                    }
                    region = session.region_name or self.aws_region
                    print(f"🔑 Using credentials from boto3 session (profile: {self.aws_profile or 'default'})")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not resolve AWS credentials: {e}")
                print("    DuckDB will try to use default credential chain...")
        
        # Configure DuckDB with resolved credentials
        if credentials and credentials.get('aws_access_key_id'):
            conn.execute(f"SET s3_access_key_id='{credentials['aws_access_key_id']}';")
            conn.execute(f"SET s3_secret_access_key='{credentials['aws_secret_access_key']}';")
            
            if credentials.get('aws_session_token'):
                conn.execute(f"SET s3_session_token='{credentials['aws_session_token']}';")
            
            print("✅ DuckDB S3 credentials configured")
        else:
            print("🔄 DuckDB will use default AWS credential chain")
        
        # Configure S3 region
        if region:
            conn.execute(f"SET s3_region='{region}';")
            print(f"🌍 DuckDB S3 region set to: {region}")
        
        # Use virtual-hosted-style S3 URLs
        conn.execute("SET s3_url_style='vhost';")
        
        print("🔌 DuckDB S3 connection configured")
    
    def _safe_pandas_to_polars_conversion(self, df) -> pl.DataFrame:
        """
        Safely convert pandas DataFrame to Polars DataFrame, handling problematic struct columns.
        """
        import pandas as pd
        import json
        
        try:
            # First, try direct conversion
            return pl.from_pandas(df)
        except Exception as e:
            if "StructArray must contain at least one field" in str(e):
                print("🔧 Handling empty struct columns...")
                
                # Create a copy to avoid modifying original
                df_clean = df.copy()
                
                # Convert problematic struct/object columns to JSON strings
                for col in df_clean.columns:
                    if df_clean[col].dtype == 'object':
                        try:
                            # Check if this column contains struct-like data
                            sample_val = df_clean[col].dropna().iloc[0] if not df_clean[col].dropna().empty else None
                            if isinstance(sample_val, dict) or (isinstance(sample_val, str) and sample_val.startswith('{')):
                                print(f"   🔧 Converting struct column '{col}' to JSON string")
                                df_clean[col] = df_clean[col].apply(
                                    lambda x: json.dumps(x) if x is not None and x != '' else None
                                )
                        except:
                            # If we can't determine the type, convert to string
                            print(f"   🔧 Converting problematic column '{col}' to string")
                            df_clean[col] = df_clean[col].astype(str).replace('nan', None)
                
                # Try conversion again
                return pl.from_pandas(df_clean)
            else:
                # Re-raise if it's a different error
                raise
    
    def _register_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register data files with DuckDB as a table."""
        files = self._discover_data_files()
        
        # Detect file format
        file_format = self._detect_file_format(files)
        
        # Prepare file list for DuckDB
        files_list = "', '".join(files)
        
        if file_format == 'parquet':
            print(f"📁 Registering {len(files)} Parquet files with DuckDB")
            # Create view from parquet files
            create_table_sql = f"""
            CREATE OR REPLACE VIEW {self.table_name} AS 
            SELECT * FROM read_parquet(['{files_list}'])
            """
        elif file_format == 'gzip':
            print(f"📁 Registering {len(files)} Gzip CSV files with DuckDB")
            # Create view from CSV files with gzip compression
            create_table_sql = f"""
            CREATE OR REPLACE VIEW {self.table_name} AS 
            SELECT * FROM read_csv(['{files_list}'], 
                compression='gzip',
                header=true,
                auto_detect=true,
                null_padding=true
            )
            """
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        conn.execute(create_table_sql)
        print(f"✅ Data registered as table '{self.table_name}' in DuckDB")
     
    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute SQL query on data export data using DuckDB SQL engine.
        
        Args:
            sql: SQL query string. Use the table name specified in constructor 
                 (default: "CUR", or your custom table_name).
                 Supports advanced SQL features like window functions, CTEs, 
                 complex joins, and more through DuckDB.
            
        Returns:
            Polars DataFrame with query results
            
        Examples:
            # Basic query
            result = data.query("SELECT COUNT(*) FROM CUR")
            
            # Advanced query with window functions
            result = data.query('''
                SELECT 
                    product_servicecode,
                    line_item_unblended_cost,
                    SUM(line_item_unblended_cost) OVER (
                        PARTITION BY product_servicecode 
                        ORDER BY line_item_usage_start_date
                    ) as running_total
                FROM CUR 
                WHERE line_item_unblended_cost > 0
                ORDER BY product_servicecode, line_item_usage_start_date
            ''')
            
            # CTE (Common Table Expression) query
            result = data.query('''
                WITH monthly_costs AS (
                    SELECT 
                        DATE_TRUNC('month', line_item_usage_start_date) as month,
                        product_servicecode,
                        SUM(line_item_unblended_cost) as monthly_cost
                    FROM CUR
                    GROUP BY 1, 2
                )
                SELECT * FROM monthly_costs 
                WHERE monthly_cost > 100
                ORDER BY month DESC, monthly_cost DESC
            ''')
        """
        print("🚀 Executing SQL query with DuckDB engine...")
        
        # Create DuckDB connection
        conn = self._get_duckdb_connection()
        
        try:
            # Register data with DuckDB
            self._register_data_with_duckdb(conn)
            
            # Execute query
            print(f"📊 Running query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            
            # Use Arrow format for better handling of complex data types
            try:
                # Try Arrow format first (best for complex data types)
                result_arrow = conn.execute(sql).fetch_arrow_table()
                polars_result = pl.from_arrow(result_arrow)
                print(f"✅ Query completed (Arrow): {polars_result.shape[0]} rows, {polars_result.shape[1]} columns")
                return polars_result
                
            except Exception as arrow_error:
                print(f"⚠️  Arrow conversion failed: {str(arrow_error)[:100]}...")
                print("🔄 Falling back to pandas conversion with struct handling...")
                
                # Fallback: pandas with struct column handling
                result_df = conn.execute(sql).fetchdf()
                
                # Handle problematic struct columns
                polars_result = self._safe_pandas_to_polars_conversion(result_df)
                print(f"✅ Query completed (pandas): {polars_result.shape[0]} rows, {polars_result.shape[1]} columns")
                return polars_result
            
        except Exception as e:
            print(f"❌ DuckDB query error: {str(e)}")
            raise
        finally:
            # Clean up connection
            conn.close()
    
    def schema(self) -> Dict[str, str]:
        """
        Get schema of data export data.
        
        Returns:
            Dictionary mapping column names to data types
        """
        if self._schema_cache is None:
            data = self._get_data()
            self._schema_cache = dict(data.schema)
        return self._schema_cache
    
    def catalog(self) -> Dict[str, Any]:
        """
        Get data statistics and metadata.
        
        Returns:
            Dictionary with file count, columns, and other metadata
        """
        files = self._discover_data_files()
        schema = self.schema()
        
        # Extract date range from file paths
        import re
        dates = []
        for file in files:
            match = re.search(r'BILLING_PERIOD=(\d{4}-\d{2})', file)
            if match:
                dates.append(match.group(1))
        
        date_range = f"{min(dates)} to {max(dates)}" if dates else "Unknown"
        
        # Identify common column patterns
        columns = list(schema.keys())
        cost_columns = [col for col in columns if 'cost' in col.lower() or 'amount' in col.lower()]
        common_columns = [col for col in columns if any(keyword in col.lower() 
                         for keyword in ['date', 'time', 'service', 'region', 'account'])]
        
        return {
            'total_files': len(files),
            'columns': columns,
            'column_count': len(columns),
            'date_range': date_range,
            'cost_columns': cost_columns,
            'common_columns': common_columns,
            'table_name': self.table_name,
            'data_type': self._detect_data_type()
        }
    
    def _detect_data_type(self) -> str:
        """Detect the type of AWS Data Export based on columns."""
        columns = list(self.schema().keys())
        column_str = ' '.join(columns).lower()
        
        if 'line_item' in column_str and 'product_' in column_str:
            return "AWS Cost and Usage Report (CUR 2.0)"
        elif 'focus' in column_str or 'billing_account' in column_str:
            return "FOCUS 1.0"
        elif 'carbon' in column_str or 'emission' in column_str:
            return "Carbon Emissions"
        elif 'recommendation' in column_str or 'saving' in column_str:
            return "Cost Optimization Recommendations"
        else:
            return "Unknown AWS Data Export"
    
    def sample(self, n: int = 10) -> pl.DataFrame:
        """
        Get sample rows from the data.
        
        Args:
            n: Number of rows to sample
            
        Returns:
            Polars DataFrame with sample data
        """
        data = self._get_data()
        return data.limit(n).collect()
    
    def info(self) -> None:
        """Print summary information about the dataset."""
        catalog = self.catalog()
        print(f"📊 AWS Data Exports Summary")
        print(f"   Data Type: {catalog['data_type']}")
        print(f"   Table Name: {catalog['table_name']}")
        print(f"   Files: {catalog['total_files']}")
        print(f"   Columns: {catalog['column_count']}")
        print(f"   Date Range: {catalog['date_range']}")
        print(f"   S3 Location: s3://{self.s3_bucket}/{self.s3_prefix}")
        
        if catalog['cost_columns']:
            print(f"   Cost Columns: {len(catalog['cost_columns'])}")
            print(f"   Sample Cost Columns: {catalog['cost_columns'][:3]}") 
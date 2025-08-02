"""
DuckDB Engine - Core SQL execution and data management
"""
import polars as pl
import duckdb
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

from .data_config import DataConfig
from ..auth import check_credential_expiration, get_boto3_client, get_storage_options
from ..data.pricing_api_manager import PricingApiManager
from ..data.savings_plan_api_manager import SavingsPlansApiManager


class DuckDBEngine:
    """
    Core DuckDB engine for executing SQL queries on AWS data exports.
    Handles both S3 and local data sources with automatic optimization.
    """
    
    def __init__(self, config: DataConfig):
        """
        Initialize DuckDB engine with data configuration.
        
        Args:
            config: DataConfig object with all necessary configuration
        """
        self.config = config
        self._data = None
        self._schema_cache = None
        
        # Check credential expiration if provided
        if config.expiration:
            check_credential_expiration(config.expiration)
        
        print(f"🎯 Data Export Type: {config.data_export_type.value}")
        
        # Show appropriate date format based on export type
        if config.data_export_type.value == 'COH':
            print(f"📁 Partition Format: {config.partition_format}=YYYY-MM-DD (daily)")
            print(f"📅 Date Format Required: YYYY-MM-DD (e.g., '2025-07-15')")
        else:
            print(f"📁 Partition Format: {config.partition_format}=YYYY-MM (monthly)")
            print(f"📅 Date Format Required: YYYY-MM (e.g., '2025-07')")
            
        print(f"📂 Data Path: s3://{config.s3_bucket}/{config.s3_data_prefix}")
        
        if config.local_data_path:
            print(f"💾 Local Cache: {config.local_bucket_path}")
            print(f"⚡ Prefer Local: {config.prefer_local_data}")
    
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        return get_boto3_client(service_name, **creds)
    
    def _get_storage_options(self) -> Dict[str, Any]:
        """Get storage options for S3 authentication in polars."""
        creds = self.config.get_aws_credentials()
        return get_storage_options(**creds)
    
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure DuckDB connection for S3 data access."""
        conn = duckdb.connect(":memory:")
        
        # Install and load required extensions
        try:
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
        except Exception as e:
            print(f"⚠️  Warning: Could not load httpfs extension: {e}")
        
        # Configure S3 credentials
        self._configure_duckdb_s3(conn)
        
        return conn
    
    def _configure_duckdb_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Configure DuckDB for S3 access with AWS credentials."""
        storage_options = self._get_storage_options()
        
        # Set AWS region
        if 'aws_region' in storage_options:
            conn.execute(f"SET s3_region='{storage_options['aws_region']}'")
        
        # Set AWS credentials if provided
        if 'aws_access_key_id' in storage_options:
            conn.execute(f"SET s3_access_key_id='{storage_options['aws_access_key_id']}'")
        
        if 'aws_secret_access_key' in storage_options:
            conn.execute(f"SET s3_secret_access_key='{storage_options['aws_secret_access_key']}'")
        
        if 'aws_session_token' in storage_options:
            conn.execute(f"SET s3_session_token='{storage_options['aws_session_token']}'")
    
    def _discover_data_files(self) -> List[str]:
        """Discover available data files in S3 based on configuration."""
        from ..data.s3_data_manager import S3DataManager
        s3_manager = S3DataManager(self.config)
        return s3_manager.discover_data_files()
    
    def _discover_local_data_files(self) -> List[str]:
        """Discover available local data files."""
        from ..data.local_data_manager import LocalDataManager
        local_manager = LocalDataManager(self.config)
        return local_manager.discover_data_files()
    
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        if not self.config.local_data_path or not self.config.local_bucket_path:
            return False
        
        try:
            local_files = self._discover_local_data_files()
            return len(local_files) > 0
        except Exception:
            return False
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """Download S3 data to local storage for cost optimization."""
        from ..data.data_downloader import DataDownloader
        downloader = DataDownloader(self.config)
        downloader.download_data_locally(overwrite=overwrite, show_progress=show_progress)
    
    def check_local_data_status(self) -> Dict[str, Any]:
        """Check status of local data cache."""
        from ..data.local_data_manager import LocalDataManager
        local_manager = LocalDataManager(self.config)
        return local_manager.check_data_status()
    
    def list_available_partitions(self) -> List[str]:
        """List all available data partitions."""
        from ..data.s3_data_manager import S3DataManager
        s3_manager = S3DataManager(self.config)
        return s3_manager.list_available_partitions()
    
    def _register_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register S3 data with DuckDB for SQL queries."""
        print("🔗 Registering S3 data with DuckDB...")
        
        # Get S3 file paths
        data_files = self._discover_data_files()
        
        if not data_files:
            raise ValueError("❌ No data files found in S3. Check your S3 bucket, prefix, and date filters.")
        
        print(f"📂 Found {len(data_files)} data files")
        
        # Create table from S3 files
        if len(data_files) == 1:
            s3_path = data_files[0]
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet('{s3_path}')")
        else:
            # Multiple files - use array syntax
            s3_paths = "['" + "', '".join(data_files) + "']"
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet({s3_paths})")
        
        print(f"✅ S3 data registered as table '{self.config.table_name}' in DuckDB")
    
    def _register_local_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register local data with DuckDB for SQL queries."""
        print("🔗 Registering local data with DuckDB...")
        
        # Get local file paths
        data_files = self._discover_local_data_files()
        
        if not data_files:
            raise ValueError("❌ No local data files found. Run download_data_locally() first.")
        
        print(f"📂 Found {len(data_files)} local data files")
        
        # Create table from local files
        if len(data_files) == 1:
            local_path = data_files[0]
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet('{local_path}')")
        else:
            # Multiple files - use array syntax
            local_paths = "['" + "', '".join(data_files) + "']"
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet({local_paths})")
        
        print(f"✅ Local data registered as table '{self.config.table_name}' in DuckDB")
    
    def _register_api_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register API data (Pricing and Savings Plans) with DuckDB as separate tables."""
        if not (self.config.enable_pricing_api or self.config.enable_savings_plans_api):
            return
            
        print("🔗 Registering API data with DuckDB...")
        
        # Register Pricing API data
        if self.config.enable_pricing_api:
            try:
                pricing_manager = PricingApiManager(self.config)
                
                # Try to load from cache first
                pricing_df = pricing_manager.load_pricing_data_from_cache(
                    "ec2_pricing", 
                    self.config.api_cache_max_age_days
                )
                
                if pricing_df is None:
                    print("📡 Fetching EC2 pricing data from AWS API...")
                    pricing_df = pricing_manager.get_ec2_pricing_data(
                        regions=self.config.pricing_api_regions,
                        instance_types=self.config.pricing_api_instance_types
                    )
                    
                    if not pricing_df.is_empty():
                        pricing_manager.save_pricing_data_to_cache(pricing_df, "ec2_pricing")
                
                if not pricing_df.is_empty():
                    # Save to temporary parquet for DuckDB
                    temp_pricing_path = self._save_temp_parquet(pricing_df, "pricing_data")
                    conn.execute(f"CREATE OR REPLACE TABLE aws_pricing AS SELECT * FROM read_parquet('{temp_pricing_path}')")
                    print("✅ AWS Pricing data registered as table 'aws_pricing'")
                    
                    # Also get RDS pricing if configured
                    rds_pricing_df = pricing_manager.load_pricing_data_from_cache(
                        "rds_pricing", 
                        self.config.api_cache_max_age_days
                    )
                    
                    if rds_pricing_df is None:
                        print("📡 Fetching RDS pricing data from AWS API...")
                        rds_pricing_df = pricing_manager.get_rds_pricing_data(
                            regions=self.config.pricing_api_regions
                        )
                        
                        if not rds_pricing_df.is_empty():
                            pricing_manager.save_pricing_data_to_cache(rds_pricing_df, "rds_pricing")
                    
                    if not rds_pricing_df.is_empty():
                        temp_rds_pricing_path = self._save_temp_parquet(rds_pricing_df, "rds_pricing_data")
                        conn.execute(f"CREATE OR REPLACE TABLE aws_rds_pricing AS SELECT * FROM read_parquet('{temp_rds_pricing_path}')")
                        print("✅ AWS RDS Pricing data registered as table 'aws_rds_pricing'")
                else:
                    print("⚠️  No pricing data available")
                    
            except Exception as e:
                print(f"❌ Error registering pricing data: {e}")
        
        # Register Savings Plans API data
        if self.config.enable_savings_plans_api:
            try:
                sp_manager = SavingsPlansApiManager(self.config)
                
                # Load Savings Plans data from cache or API
                sp_df = sp_manager.load_savings_plans_data_from_cache(
                    "savings_plans", 
                    self.config.api_cache_max_age_days
                )
                
                if sp_df is None:
                    print("📡 Fetching Savings Plans data from AWS API...")
                    sp_df = sp_manager.get_savings_plans_data(
                        states=['active', 'payment-pending']
                    )
                    
                    if not sp_df.is_empty():
                        sp_manager.save_savings_plans_data_to_cache(sp_df, "savings_plans")
                
                if not sp_df.is_empty():
                    # Create CUR-joinable format
                    joinable_df = sp_manager.create_cur_joinable_format(sp_df)
                    
                    # Save to temporary parquet for DuckDB
                    temp_sp_path = self._save_temp_parquet(joinable_df, "savings_plans_data")
                    conn.execute(f"CREATE OR REPLACE TABLE aws_savings_plans AS SELECT * FROM read_parquet('{temp_sp_path}')")
                    print("✅ AWS Savings Plans data registered as table 'aws_savings_plans'")
                    
                    # Get rates data if enabled
                    if self.config.savings_plans_include_rates:
                        rates_df = sp_manager.load_savings_plans_data_from_cache(
                            "savings_plans_rates", 
                            self.config.api_cache_max_age_days
                        )
                        
                        if rates_df is None:
                            print("📡 Fetching Savings Plans rates data from AWS API...")
                            rates_df = sp_manager.get_all_savings_plan_rates_data(
                                service_codes=['AmazonEC2']
                            )
                            
                            if not rates_df.is_empty():
                                sp_manager.save_savings_plans_data_to_cache(rates_df, "savings_plans_rates")
                        
                        if not rates_df.is_empty():
                            temp_rates_path = self._save_temp_parquet(rates_df, "savings_plans_rates_data")
                            conn.execute(f"CREATE OR REPLACE TABLE aws_savings_plans_rates AS SELECT * FROM read_parquet('{temp_rates_path}')")
                            print("✅ AWS Savings Plans rates data registered as table 'aws_savings_plans_rates'")
                else:
                    print("⚠️  No Savings Plans data available")
                    
            except Exception as e:
                print(f"❌ Error registering Savings Plans data: {e}")
    
    def _save_temp_parquet(self, df: pl.DataFrame, name: str) -> str:
        """Save DataFrame to temporary parquet file for DuckDB registration."""
        import tempfile
        temp_dir = Path(tempfile.gettempdir()) / "de_polars_temp"
        temp_dir.mkdir(exist_ok=True)
        
        temp_file = temp_dir / f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(temp_file)
        
        return str(temp_file)
    
    def _safe_pandas_to_polars_conversion(self, df) -> pl.DataFrame:
        """Safely convert pandas DataFrame to Polars with struct column handling."""
        try:
            return pl.from_pandas(df)
        except Exception as e:
            print(f"⚠️  Direct conversion failed: {str(e)[:100]}...")
            print("🔧 Attempting to handle problematic columns...")
            
            # Identify problematic columns (likely struct/complex types)
            problem_columns = []
            for col in df.columns:
                try:
                    # Test conversion of individual column
                    pl.from_pandas(df[[col]])
                except Exception:
                    problem_columns.append(col)
            
            if problem_columns:
                print(f"🔧 Converting {len(problem_columns)} problematic columns to string...")
                for col in problem_columns:
                    df[col] = df[col].astype(str)
            
            # Try conversion again
            return pl.from_pandas(df)
    
    def query(self, sql: str, force_s3: bool = False) -> pl.DataFrame:
        """
        Execute SQL query on data export data using DuckDB SQL engine.
        
        Args:
            sql: SQL query string. Use the table name specified in configuration.
            force_s3: If True, force querying from S3 even if local data is available.
        
        Returns:
            Polars DataFrame with query results
        """
        # Determine data source
        use_local_data = (
            not force_s3 and 
            self.config.prefer_local_data and 
            self.config.local_data_path and 
            self.has_local_data()
        )
        
        if use_local_data:
            print("🚀 Executing SQL query with DuckDB engine using LOCAL DATA...")
            print("   💾 Data source: Local files (no S3 costs)")
        else:
            print("🚀 Executing SQL query with DuckDB engine using S3 DATA...")
            if force_s3:
                print("   ☁️  Data source: S3 (forced)")
            elif not self.config.local_data_path:
                print("   ☁️  Data source: S3 (local data not configured)")
            elif not self.config.prefer_local_data:
                print("   ☁️  Data source: S3 (prefer_local_data=False)")
            elif not self.has_local_data():
                print("   ☁️  Data source: S3 (no local data found)")
        
        # Create DuckDB connection
        if use_local_data:
            # For local data, we don't need S3 credentials
            conn = duckdb.connect(":memory:")
        else:
            # For S3 data, we need full S3 configuration
            conn = self._get_duckdb_connection()
        
        try:
            # Register data with DuckDB
            if use_local_data:
                self._register_local_data_with_duckdb(conn)
            else:
                self._register_data_with_duckdb(conn)
            
            # Register API data tables (Pricing and Savings Plans)
            self._register_api_data_with_duckdb(conn)
            
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
        """Get schema information for the data."""
        if self._schema_cache:
            return self._schema_cache
        
        # Use a simple query to get schema
        try:
            result = self.query("SELECT * FROM " + self.config.table_name + " LIMIT 0")
            self._schema_cache = dict(zip(result.columns, [str(dt) for dt in result.dtypes]))
            return self._schema_cache
        except Exception as e:
            print(f"❌ Could not get schema: {e}")
            return {}
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        catalog_info = {
            "table_name": self.config.table_name,
            "data_export_type": self.config.data_export_type.value,
            "partition_format": self.config.partition_format,
            "s3_location": f"s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}",
            "local_location": self.config.local_bucket_path,
            "has_local_data": self.has_local_data(),
            "schema": self.schema(),
            "date_range": {
                "start": self.config.date_start,
                "end": self.config.date_end,
                "format": self.config.date_format
            }
        }
        
        return catalog_info
    
    def sample(self, n: int = 10) -> pl.DataFrame:
        """Get a sample of the data."""
        return self.query(f"SELECT * FROM {self.config.table_name} LIMIT {n}")
    
    def info(self) -> None:
        """Print information about the data source and configuration."""
        print("\n" + "="*60)
        print("📊 DE-Polars Data Source Information")
        print("="*60)
        
        # Configuration info
        print(f"🎯 Export Type: {self.config.data_export_type.value}")
        print(f"📁 Table Name: {self.config.table_name}")
        print(f"🌐 S3 Location: s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}")
        
        if self.config.local_data_path:
            print(f"💾 Local Cache: {self.config.local_bucket_path}")
            print(f"📂 Has Local Data: {'✅ Yes' if self.has_local_data() else '❌ No'}")
        
        # Date filtering
        if self.config.date_start or self.config.date_end:
            print(f"📅 Date Filter: {self.config.date_start or 'earliest'} to {self.config.date_end or 'latest'}")
        
        # Schema info
        schema = self.schema()
        if schema:
            print(f"📋 Columns: {len(schema)} columns")
            
        print("="*60)
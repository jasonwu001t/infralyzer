"""
DuckDB Engine - Core SQL execution and data management without Polars conversion
"""
import duckdb
import pandas as pd
import pyarrow as pa
import os
import io
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

from .base_engine import BaseQueryEngine, QueryResultFormat
from .data_config import DataConfig
from ..auth import check_credential_expiration, get_boto3_client, get_storage_options
from ..data.aws_pricing_manager import AWSPricingManager


class DuckDBEngine(BaseQueryEngine):
    """
    DuckDB-based query engine for executing SQL queries on AWS data exports.
    Handles both S3 and local data sources with automatic optimization.
    Returns native formats without unnecessary conversions.
    """
    
    def __init__(self, config: DataConfig):
        """Initialize DuckDB engine with data configuration."""
        super().__init__(config)
        self._data = None
        
        # Check credential expiration if provided
        if config.expiration:
            check_credential_expiration(config.expiration)
    
    @property
    def engine_name(self) -> str:
        return "DuckDB"
    
    @property
    def supports_s3_direct(self) -> bool:
        return True
    
    @property
    def supports_local_data(self) -> bool:
        return True
    
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        return get_boto3_client(service_name, **creds)
    
    def _get_storage_options(self) -> Dict[str, str]:
        """Get storage options for S3 access."""
        creds = self.config.get_aws_credentials()
        return get_storage_options(**creds)
    
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure a DuckDB connection with S3 support."""
        conn = duckdb.connect(":memory:")
        
        # Load S3 extension
        try:
            conn.execute("LOAD httpfs")
        except Exception as e:
            print(f"Warning: Could not load httpfs extension: {e}")
        
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
    
    def _register_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register S3 data with DuckDB for SQL queries."""
        print("Registering S3 data with DuckDB...")
        
        # Get S3 file paths
        data_files = self._discover_data_files()
        
        if not data_files:
            raise ValueError("No data files found in S3. Check your S3 bucket, prefix, and date filters.")
        
        print(f"Found {len(data_files)} data files")
        
        # Create table from S3 files
        if len(data_files) == 1:
            s3_path = data_files[0]
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet('{s3_path}')")
        else:
            # Multiple files - use array syntax
            s3_paths = "['" + "', '".join(data_files) + "']"
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet({s3_paths})")
        
        print(f"S3 data registered as table '{self.config.table_name}' in DuckDB")
    
    def _register_local_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register local data with DuckDB for SQL queries."""
        print("Registering local data with DuckDB...")
        
        # Get local file paths
        data_files = self._discover_local_data_files()
        
        if not data_files:
            raise ValueError("No local data files found. Run download_data_locally() first.")
        
        print(f"Found {len(data_files)} local data files")
        
        # Create table from local files
        if len(data_files) == 1:
            local_path = data_files[0]
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet('{local_path}')")
        else:
            # Multiple files - use array syntax
            local_paths = "['" + "', '".join(data_files) + "']"
            conn.execute(f"CREATE OR REPLACE TABLE {self.config.table_name} AS SELECT * FROM read_parquet({local_paths})")
        
        print(f"Local data registered as table '{self.config.table_name}' in DuckDB")
    
    def _register_api_data_with_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Register API data (pricing, savings plans) as tables in DuckDB."""
        try:
            # Initialize pricing manager
            pricing_manager = AWSPricingManager()
            
            # Get pricing data as DataFrames
            pricing_df = pricing_manager.get_ec2_pricing_dataframe()
            savings_plans_df = pricing_manager.get_savings_plans_dataframe()
            
            if not pricing_df.empty:
                # Register pricing data as a DuckDB table
                conn.register('aws_pricing', pricing_df)
                print("AWS Pricing data registered as 'aws_pricing' table")
            
            if not savings_plans_df.empty:
                # Register savings plans data as a DuckDB table  
                conn.register('aws_savings_plans', savings_plans_df)
                print("AWS Savings Plans data registered as 'aws_savings_plans' table")
                
        except Exception as e:
            print(f"Warning: Could not register API data tables: {e}")
    
    def query(self, 
              sql: str, 
              format: QueryResultFormat = QueryResultFormat.DATAFRAME,
              force_s3: bool = False) -> Union[List[Dict[str, Any]], pd.DataFrame, str, pa.Table]:
        """
        Execute SQL query and return results in specified format - NO POLARS CONVERSION.
        
        Args:
            sql: SQL query to execute
            format: Desired output format
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            Query results in the specified format (native, no conversion overhead)
        """
        # Determine data source
        use_local_data = (
            not force_s3 and 
            self.config.prefer_local_data and 
            self.has_local_data()
        )
        
        # Create appropriate DuckDB connection
        if use_local_data:
            print("Executing SQL query with DuckDB engine using LOCAL DATA...")
            print("Data source: Local files")
            conn = duckdb.connect(":memory:")
        else:
            if force_s3:
                print("Executing SQL query with DuckDB engine using S3 DATA...")
                print("Data source: S3 (forced)")
            elif not self.config.local_data_path:
                print("Data source: S3 (local data not configured)")
            elif not self.config.prefer_local_data:
                print("Data source: S3 (prefer_local_data=False)")
            else:
                print("Data source: S3 (no local data found)")
            
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
            print(f"Running query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            
            # Return results in requested format - NO CONVERSION OVERHEAD
            if format == QueryResultFormat.RECORDS:
                # Direct to list of dictionaries
                result_df = conn.execute(sql).fetchdf()
                result = result_df.to_dict('records')
                print(f"Query completed (Records): {len(result)} rows, {len(result[0]) if result else 0} columns")
                return result
                
            elif format == QueryResultFormat.DATAFRAME:
                # Direct pandas DataFrame
                result_df = conn.execute(sql).fetchdf()
                print(f"Query completed (DataFrame): {result_df.shape[0]} rows, {result_df.shape[1]} columns")
                return result_df
                
            elif format == QueryResultFormat.CSV:
                # Direct CSV string
                result_df = conn.execute(sql).fetchdf()
                csv_buffer = io.StringIO()
                result_df.to_csv(csv_buffer, index=False)
                result = csv_buffer.getvalue()
                print(f"Query completed (CSV): {result_df.shape[0]} rows")
                return result
                
            elif format == QueryResultFormat.ARROW:
                # Direct Arrow table
                result_arrow = conn.execute(sql).fetch_arrow_table()
                print(f"Query completed (Arrow): {len(result_arrow)} rows, {len(result_arrow.column_names)} columns")
                return result_arrow
                
            elif format == QueryResultFormat.RAW:
                # Raw DuckDB result
                result = conn.execute(sql).fetchall()
                print(f"Query completed (Raw): {len(result)} rows")
                return result
                
            else:
                raise ValueError(f"Unsupported format: {format}")
            
        except Exception as e:
            print(f"DuckDB query error: {str(e)}")
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
            result = self.query(f"SELECT * FROM {self.config.table_name} LIMIT 0", format=QueryResultFormat.DATAFRAME)
            self._schema_cache = dict(zip(result.columns, [str(dt) for dt in result.dtypes]))
            return self._schema_cache
        except Exception as e:
            print(f"Could not get schema: {e}")
            return {}
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        return {
            "engine": self.engine_name,
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
            },
            "supports_s3_direct": self.supports_s3_direct,
            "supports_local_data": self.supports_local_data
        }


# Register DuckDB engine with the factory
from .base_engine import QueryEngineFactory
QueryEngineFactory.register_engine("duckdb", DuckDBEngine)
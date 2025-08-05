"""
AWS Athena Engine - Query S3 data using AWS Athena service
"""
import boto3
import pandas as pd
import pyarrow as pa
import io
import time
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

from .base_engine import BaseQueryEngine, QueryResultFormat
from .data_config import DataConfig
from ..auth import check_credential_expiration


class AthenaEngine(BaseQueryEngine):
    """
    AWS Athena-based query engine for executing SQL queries on S3 data.
    Uses AWS Athena service for serverless SQL queries with automatic scaling.
    """
    
    def __init__(self, config: DataConfig, 
                 database_name: str = "default",
                 output_bucket: Optional[str] = None,
                 workgroup: str = "primary"):
        """
        Initialize Athena engine with data configuration.
        
        Args:
            config: DataConfig object with all necessary configuration
            database_name: Athena database name (default: "default")
            output_bucket: S3 bucket for query results (default: use config.s3_bucket)
            workgroup: Athena workgroup (default: "primary")
        """
        super().__init__(config)
        self.database_name = database_name
        self.output_bucket = output_bucket or config.s3_bucket
        self.workgroup = workgroup
        self._athena_client = None
        self._s3_client = None
        self._glue_client = None
        
        # Check credential expiration if provided
        if config.expiration:
            check_credential_expiration(config.expiration)
    
    @property
    def engine_name(self) -> str:
        return "AWS Athena"
    
    @property
    def supports_s3_direct(self) -> bool:
        return True  # Athena is designed for S3 querying
    
    @property
    def supports_local_data(self) -> bool:
        return False  # Athena can't query local files
    
    def _get_athena_client(self):
        """Get AWS Athena client."""
        if self._athena_client is None:
            session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region or 'us-east-1',
                profile_name=self.config.aws_profile
            )
            self._athena_client = session.client('athena')
        return self._athena_client
    
    def _get_s3_client(self):
        """Get AWS S3 client."""
        if self._s3_client is None:
            session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region or 'us-east-1',
                profile_name=self.config.aws_profile
            )
            self._s3_client = session.client('s3')
        return self._s3_client
    
    def _get_glue_client(self):
        """Get AWS Glue client."""
        if self._glue_client is None:
            session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region or 'us-east-1',
                profile_name=self.config.aws_profile
            )
            self._glue_client = session.client('glue')
        return self._glue_client
    
    def has_local_data(self) -> bool:
        """Athena doesn't support local data."""
        return False
    
    def _check_table_exists(self) -> bool:
        """Check if table exists in Glue catalog."""
        try:
            glue_client = self._get_glue_client()
            glue_client.get_table(
                DatabaseName=self.database_name,
                Name=self.config.table_name
            )
            return True
        except glue_client.exceptions.EntityNotFoundException:
            return False
        except Exception:
            return False
    
    def _create_glue_crawler(self) -> str:
        """Create Glue Crawler for automatic schema discovery."""
        crawler_name = f"{self.config.table_name}_crawler"
        
        # S3 target path
        s3_path = f"s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}/"
        
        crawler_config = {
            'Name': crawler_name,
            'Role': f'arn:aws:iam::{self._get_account_id()}:role/service-role/AWSGlueServiceRole-DefaultRole',
            'DatabaseName': self.database_name,
            'Description': f'Auto-generated crawler for {self.config.table_name} CUR data',
            'Targets': {
                'S3Targets': [
                    {
                        'Path': s3_path,
                        'Exclusions': []
                    }
                ]
            },
            'TablePrefix': '',
            'SchemaChangePolicy': {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            'RecrawlPolicy': {
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            'LineageConfiguration': {
                'CrawlerLineageSettings': 'DISABLE'
            }
        }
        
        try:
            glue_client = self._get_glue_client()
            
            # Delete existing crawler if it exists
            try:
                glue_client.delete_crawler(Name=crawler_name)
                print(f"Deleted existing crawler: {crawler_name}")
                time.sleep(2)  # Wait for deletion
            except glue_client.exceptions.EntityNotFoundException:
                pass
            
            # Create new crawler
            glue_client.create_crawler(**crawler_config)
            print(f"Created Glue Crawler: {crawler_name}")
            return crawler_name
            
        except Exception as e:
            # Fallback: try with a simpler role assumption
            try:
                # Use a more generic role path that might exist
                crawler_config['Role'] = 'arn:aws:iam::*:role/AWSGlueServiceRole*'
                glue_client.create_crawler(**crawler_config)
                return crawler_name
            except Exception as e2:
                print(f"Warning: Could not create Glue Crawler: {e}")
                print(f"Fallback failed: {e2}")
                return None
    
    def _get_account_id(self) -> str:
        """Get AWS Account ID."""
        try:
            sts_client = boto3.client('sts',
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region or 'us-east-1'
            )
            return sts_client.get_caller_identity()['Account']
        except:
            return "123456789012"  # Fallback
    
    def _run_crawler_and_wait(self, crawler_name: str, max_wait: int = 300) -> bool:
        """Run Glue Crawler and wait for completion."""
        if not crawler_name:
            return False
            
        try:
            glue_client = self._get_glue_client()
            
            # Start crawler
            glue_client.start_crawler(Name=crawler_name)
            print(f"Started Glue Crawler: {crawler_name}")
            
            # Wait for completion
            start_time = time.time()
            while time.time() - start_time < max_wait:
                response = glue_client.get_crawler(Name=crawler_name)
                state = response['Crawler']['State']
                
                if state == 'READY':
                    print(f"Glue Crawler completed successfully")
                    return True
                elif state in ['STOPPING', 'STOPPED']:
                    print(f"Glue Crawler stopped: {state}")
                    return False
                    
                print(f"Crawler state: {state} (waiting...)")
                time.sleep(10)
            
            print(f"Crawler timed out after {max_wait} seconds")
            return False
            
        except Exception as e:
            print(f"Error running crawler: {e}")
            return False
    
    def _create_table_if_not_exists(self) -> None:
        """Create Athena table using Glue Crawler for schema discovery."""
        
        # Check if table already exists
        if self._check_table_exists():
            print(f"Athena table '{self.config.table_name}' already exists (discovered by Glue)")
            return
        
        print(f"Table '{self.config.table_name}' not found. Using Glue Crawler for schema discovery...")
        
        # Create and run Glue Crawler
        crawler_name = self._create_glue_crawler()
        if crawler_name:
            success = self._run_crawler_and_wait(crawler_name)
            if success:
                print(f"Athena table '{self.config.table_name}' created with full schema via Glue Crawler")
            else:
                print(f"Warning: Glue Crawler failed. Table may not be available.")
        else:
            print(f"Warning: Could not create Glue Crawler. Using manual table creation...")
            # Fallback to simplified manual creation if Glue fails
            self._create_simple_table_fallback()
    
    def _create_simple_table_fallback(self):
        """Fallback method: create table with basic schema if Glue fails."""
        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.config.table_name} (
            line_item_unblended_cost double,
            product_servicecode string,
            line_item_usage_start_date string,
            line_item_resource_id string
        )
        STORED AS PARQUET
        LOCATION 's3://{self.config.s3_bucket}/{self.config.s3_data_prefix}/'
        """
        
        try:
            self._execute_athena_query(create_table_sql)
            print(f"Fallback: Created simple Athena table '{self.config.table_name}'")
        except Exception as e:
            print(f"Warning: Fallback table creation failed: {e}")
    
    def _execute_athena_query(self, sql: str, wait_for_completion: bool = True) -> str:
        """Execute query in Athena and return execution ID."""
        athena_client = self._get_athena_client()
        
        # Configure query execution
        query_config = {
            'QueryString': sql,
            'QueryExecutionContext': {
                'Database': self.database_name
            },
            'ResultConfiguration': {
                'OutputLocation': f's3://{self.output_bucket}/athena-results/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            },
            'WorkGroup': self.workgroup
        }
        
        # Start query execution
        response = athena_client.start_query_execution(**query_config)
        execution_id = response['QueryExecutionId']
        
        if wait_for_completion:
            self._wait_for_query_completion(execution_id)
        
        return execution_id
    
    def _wait_for_query_completion(self, execution_id: str, max_wait_time: int = 300) -> None:
        """Wait for Athena query to complete."""
        athena_client = self._get_athena_client()
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            response = athena_client.get_query_execution(QueryExecutionId=execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                return
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_msg}")
            
            time.sleep(2)  # Wait 2 seconds before checking again
        
        raise Exception(f"Athena query timed out after {max_wait_time} seconds")
    
    def _get_query_results(self, execution_id: str, format: QueryResultFormat) -> Any:
        """Get results from completed Athena query."""
        athena_client = self._get_athena_client()
        s3_client = self._get_s3_client()
        
        # Get query execution details
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
        output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        
        # Parse S3 URL
        output_location = output_location.replace('s3://', '')
        bucket, key = output_location.split('/', 1)
        
        if format == QueryResultFormat.RECORDS:
            # Use Athena's get_query_results for JSON-like records
            results = []
            paginator = athena_client.get_paginator('get_query_results')
            
            for page in paginator.paginate(QueryExecutionId=execution_id):
                rows = page['ResultSet']['Rows']
                
                if not results:  # First page - extract headers
                    headers = [col['VarCharValue'] for col in rows[0]['Data']]
                    rows = rows[1:]  # Skip header row
                
                for row in rows:
                    record = {}
                    for i, cell in enumerate(row['Data']):
                        value = cell.get('VarCharValue', '')
                        # Try to convert to appropriate type
                        try:
                            if '.' in value:
                                value = float(value)
                            elif value.isdigit():
                                value = int(value)
                        except ValueError:
                            pass  # Keep as string
                        record[headers[i]] = value
                    results.append(record)
            
            return results
        
        elif format == QueryResultFormat.DATAFRAME:
            # Download CSV and load as DataFrame
            csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = csv_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
            return df
        
        elif format == QueryResultFormat.CSV:
            # Download CSV directly
            csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
            return csv_obj['Body'].read().decode('utf-8')
        
        elif format == QueryResultFormat.RAW:
            # Return execution ID for further processing
            return execution_id
        
        else:
            raise ValueError(f"Unsupported format for Athena: {format}")
    
    def query(self, 
              sql: str, 
              format: QueryResultFormat = QueryResultFormat.DATAFRAME,
              force_s3: bool = False) -> Union[List[Dict[str, Any]], pd.DataFrame, str, Any]:
        """
        Execute SQL query using AWS Athena.
        
        Args:
            sql: SQL query to execute
            format: Desired output format
            force_s3: Ignored for Athena (always uses S3)
            
        Returns:
            Query results in the specified format
        """
        print(f"Executing query with AWS Athena: {sql[:100]}{'...' if len(sql) > 100 else ''}")
        
        # Ensure table exists
        self._create_table_if_not_exists()
        
        # Execute query
        execution_id = self._execute_athena_query(sql)
        
        # Get results
        results = self._get_query_results(execution_id, format)
        
        if format == QueryResultFormat.RECORDS:
            print(f"Athena query completed: {len(results)} rows")
        elif format == QueryResultFormat.DATAFRAME:
            print(f"Athena query completed: {results.shape[0]} rows, {results.shape[1]} columns")
        else:
            print("Athena query completed")
        
        return results
    
    def schema(self) -> Dict[str, str]:
        """Get schema information from Athena."""
        if self._schema_cache:
            return self._schema_cache
        
        try:
            # Query information schema
            schema_sql = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{self.config.table_name}' 
            AND table_schema = '{self.database_name}'
            """
            
            results = self.query(schema_sql, format=QueryResultFormat.RECORDS)
            self._schema_cache = {row['column_name']: row['data_type'] for row in results}
            return self._schema_cache
            
        except Exception as e:
            print(f"Could not get schema from Athena: {e}")
            return {}
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        return {
            "engine": self.engine_name,
            "table_name": self.config.table_name,
            "database_name": self.database_name,
            "workgroup": self.workgroup,
            "data_export_type": self.config.data_export_type.value,
            "partition_format": self.config.partition_format,
            "s3_location": f"s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}",
            "output_bucket": self.output_bucket,
            "has_local_data": False,
            "schema": self.schema(),
            "date_range": {
                "start": self.config.date_start,
                "end": self.config.date_end,
                "format": self.config.date_format
            },
            "supports_s3_direct": self.supports_s3_direct,
            "supports_local_data": self.supports_local_data
        }


# Register Athena engine with the factory
from .base_engine import QueryEngineFactory
QueryEngineFactory.register_engine("athena", AthenaEngine)
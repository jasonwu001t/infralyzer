"""
Enhanced Data Partitioner with SQL Query Library Support

Supports both simple examples and advanced analytics table creation from cur2_query_library.
Can create partitioned folders and parquet files based on SQL queries.
"""

import polars as pl
import tempfile
import os
import glob
from pathlib import Path
from typing import Optional, Dict, List, Union
from .auth import get_boto3_client, get_storage_options
from .client import DataExportsPolars


class DataPartitioner:
    """Enhanced data partitioner with SQL query library support."""
    
    def __init__(self, 
                 source_client: DataExportsPolars,
                 target_bucket: str,
                 target_prefix: str = "analytics-data",
                 query_library_path: str = "cur2_query_library",
                 aws_region: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 aws_profile: Optional[str] = None):
        """
        Initialize the enhanced data partitioner.
        
        Args:
            source_client: DataExportsPolars client for source data
            target_bucket: S3 bucket for storing analytics tables
            target_prefix: S3 prefix for analytics data
            query_library_path: Path to SQL query library
            aws_region: AWS region
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            aws_session_token: AWS session token
            aws_profile: AWS profile name
        """
        self.source_client = source_client
        self.target_bucket = target_bucket
        self.target_prefix = target_prefix.rstrip('/')
        self.query_library_path = Path(query_library_path)
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_profile = aws_profile
    
    def _get_s3_client(self):
        """Get boto3 S3 client using shared auth."""
        return get_boto3_client(
            service_name='s3',
            aws_region=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            aws_profile=self.aws_profile
        )
    
    def _save_to_s3(self, dataframe: pl.DataFrame, s3_key_path: str) -> str:
        """Save dataframe to S3 as parquet with full S3 key path."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # Write to parquet with snappy compression
            dataframe.write_parquet(tmp_path, compression='snappy')
            
            # Upload to S3
            s3_key = f"{self.target_prefix}/{s3_key_path}"
            s3_path = f"s3://{self.target_bucket}/{s3_key}"
            
            s3_client = self._get_s3_client()
            s3_client.upload_file(tmp_path, self.target_bucket, s3_key)
            
            # Calculate file size
            file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
            
            print(f"✅ Saved analytics table: {len(dataframe):,} rows, {file_size_mb:.1f}MB")
            print(f"   📂 S3 Path: {s3_path}")
            
            return s3_path
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def discover_sql_files(self) -> Dict[str, List[str]]:
        """Discover all SQL files in the query library organized by category."""
        categories = {}
        
        if not self.query_library_path.exists():
            print(f"❌ Query library not found: {self.query_library_path}")
            return categories
        
        # Find all SQL files recursively
        sql_files = glob.glob(str(self.query_library_path / "**" / "*.sql"), recursive=True)
        
        for sql_file in sql_files:
            # Get relative path from query library root
            rel_path = Path(sql_file).relative_to(self.query_library_path)
            category = str(rel_path.parent)
            filename = str(rel_path)
            
            if category not in categories:
                categories[category] = []
            categories[category].append(filename)
        
        return categories
    
    def load_sql_query(self, query_path: str) -> str:
        """Load SQL query from file."""
        full_path = self.query_library_path / query_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        with open(full_path, 'r') as file:
            return file.read()
    
    def extract_query_metadata(self, sql_content: str) -> Dict[str, str]:
        """Extract metadata from SQL file comments."""
        lines = sql_content.split('\n')
        metadata = {}
        
        for line in lines:
            line = line.strip()
            if line.startswith('-- Description:'):
                metadata['description'] = line.replace('-- Description:', '').strip()
            elif line.startswith('-- Partitioning:'):
                metadata['partitioning'] = line.replace('-- Partitioning:', '').strip()
            elif line.startswith('-- Output:'):
                metadata['output'] = line.replace('-- Output:', '').strip()
        
        return metadata
    
    def create_analytics_table(self, 
                              query_path: str, 
                              table_name: Optional[str] = None,
                              partitioned: bool = False,
                              partition_columns: Optional[List[str]] = None) -> str:
        """
        Create an analytics table from a SQL query file.
        
        Args:
            query_path: Relative path to SQL file (e.g., 'cost_analysis/cost_by_service.sql')
            table_name: Name for the analytics table (defaults to query filename)
            partitioned: Whether to create partitioned output
            partition_columns: Columns to partition by
        
        Returns:
            S3 path of created analytics table
        """
        print(f"🔨 Creating Analytics Table from: {query_path}")
        print("=" * 60)
        
        # Load and execute SQL query
        sql_content = self.load_sql_query(query_path)
        metadata = self.extract_query_metadata(sql_content)
        
        if 'description' in metadata:
            print(f"📝 Description: {metadata['description']}")
        
        # Default table name from filename
        if table_name is None:
            table_name = Path(query_path).stem
        
        print(f"📊 Table Name: {table_name}")
        print(f"📄 Query: {len(sql_content)} characters")
        print()
        
        # Execute query
        print("⚡ Executing query...")
        result_df = self.source_client.query(sql_content)
        print(f"✅ Query completed: {len(result_df):,} rows × {len(result_df.columns)} columns")
        print()
        
        # Create partitioned or single table
        if partitioned and partition_columns:
            return self._create_partitioned_table(result_df, table_name, partition_columns)
        else:
            # Single analytics table
            s3_key = f"{table_name}/{table_name}.parquet"
            return self._save_to_s3(result_df, s3_key)
    
    def _create_partitioned_table(self, 
                                 dataframe: pl.DataFrame, 
                                 table_name: str, 
                                 partition_columns: List[str]) -> List[str]:
        """Create partitioned analytics table based on specified columns."""
        print(f"🗂️ Creating partitioned table by: {', '.join(partition_columns)}")
        
        s3_paths = []
        
        # Group by partition columns
        for partition_values in dataframe.select(partition_columns).unique().iter_rows():
            # Create filter condition
            filter_condition = None
            partition_path_parts = []
            
            for col, value in zip(partition_columns, partition_values):
                condition = (pl.col(col) == value)
                filter_condition = condition if filter_condition is None else filter_condition & condition
                partition_path_parts.append(f"{col}={value}")
            
            # Filter data for this partition
            partition_data = dataframe.filter(filter_condition)
            
            # Create S3 key with partition structure
            partition_path = "/".join(partition_path_parts)
            s3_key = f"{table_name}/{partition_path}/{table_name}.parquet"
            
            # Save partition
            if len(partition_data) > 0:
                s3_path = self._save_to_s3(partition_data, s3_key)
                s3_paths.append(s3_path)
                print(f"   📁 Partition {partition_path}: {len(partition_data):,} rows")
        
        print(f"✅ Created {len(s3_paths)} partitions for {table_name}")
        return s3_paths
    
    def create_analytics_from_category(self, 
                                      category: str, 
                                      partitioned: bool = False) -> Dict[str, str]:
        """
        Create analytics tables for all queries in a category.
        
        Args:
            category: Category name (e.g., 'cost_analysis')
            partitioned: Whether to create partitioned tables
        
        Returns:
            Dict mapping query names to S3 paths
        """
        categories = self.discover_sql_files()
        
        if category not in categories:
            raise ValueError(f"Category not found: {category}. Available: {list(categories.keys())}")
        
        print(f"🏭 Creating Analytics Tables for Category: {category}")
        print(f"📁 Found {len(categories[category])} queries")
        print("=" * 80)
        print()
        
        analytics_tables = {}
        
        for query_file in sorted(categories[category]):
            table_name = Path(query_file).stem
            
            # Determine partitioning strategy based on query type
            partition_columns = self._get_default_partition_columns(category, table_name)
            
            try:
                s3_path = self.create_analytics_table(
                    query_file, 
                    table_name, 
                    partitioned and partition_columns is not None,
                    partition_columns
                )
                analytics_tables[table_name] = s3_path
                print()
                
            except Exception as e:
                print(f"❌ Failed to create table {table_name}: {str(e)}")
                print()
        
        print(f"🎉 Category Summary: {category}")
        print(f"   ✅ Successful: {len(analytics_tables)}")
        print(f"   ❌ Failed: {len(categories[category]) - len(analytics_tables)}")
        print()
        
        return analytics_tables
    
    def _get_default_partition_columns(self, category: str, table_name: str) -> Optional[List[str]]:
        """Get default partition columns based on category and table name."""
        partition_strategies = {
            'cost_analysis': {
                'cost_by_service': ['service'],
                'cost_by_region': ['region'],
                'cost_by_account': ['account_id']
            },
            'resource_analysis': {
                'top_resources_by_cost': ['service', 'cost_tier'],
                'unused_resources': ['service', 'utilization_status']
            },
            'optimization': {
                'ri_utilization': ['service', 'utilization_category']
            },
            'time_series': {
                'daily_cost_trends': ['year', 'month'],
                'monthly_trends': ['year']
            }
        }
        
        return partition_strategies.get(category, {}).get(table_name)
    
    def create_all_analytics_tables(self, partitioned: bool = False) -> Dict[str, Dict[str, str]]:
        """
        Create analytics tables for all categories.
        
        Args:
            partitioned: Whether to create partitioned tables
        
        Returns:
            Nested dict with category -> table_name -> s3_path
        """
        categories = self.discover_sql_files()
        
        print(f"🏭 Creating All Analytics Tables")
        print(f"📂 Found {len(categories)} categories")
        print(f"📄 Total queries: {sum(len(files) for files in categories.values())}")
        print("=" * 80)
        print()
        
        all_analytics = {}
        
        for category in sorted(categories.keys()):
            try:
                category_tables = self.create_analytics_from_category(category, partitioned)
                all_analytics[category] = category_tables
            except Exception as e:
                print(f"❌ Failed to process category {category}: {str(e)}")
                all_analytics[category] = {}
        
        # Overall summary
        total_tables = sum(len(cat_tables) for cat_tables in all_analytics.values())
        total_queries = sum(len(files) for files in categories.values())
        
        print(f"🎉 Overall Summary:")
        print(f"   📊 Total Analytics Tables Created: {total_tables}")
        print(f"   📄 Total Queries Processed: {total_queries}")
        print(f"   📂 Categories: {len(categories)}")
        print(f"   📈 Success Rate: {(total_tables/total_queries*100):.1f}%")
        
        return all_analytics
    
    def list_analytics_opportunities(self):
        """List all SQL queries and their potential analytics tables."""
        categories = self.discover_sql_files()
        
        print(f"📊 Analytics Table Opportunities")
        print("=" * 60)
        
        if not categories:
            print("❌ No SQL queries found in cur2_query_library!")
            return
        
        total_queries = 0
        for category, files in sorted(categories.items()):
            print(f"\n📁 {category}/ ({len(files)} tables)")
            
            for query_file in sorted(files):
                table_name = Path(query_file).stem
                partition_cols = self._get_default_partition_columns(category, table_name)
                
                print(f"   📊 {table_name}")
                print(f"      📄 Source: {query_file}")
                
                # Try to read description
                try:
                    sql_content = self.load_sql_query(query_file)
                    metadata = self.extract_query_metadata(sql_content)
                    if 'description' in metadata:
                        print(f"      💡 {metadata['description']}")
                    if partition_cols:
                        print(f"      🗂️ Default partitions: {', '.join(partition_cols)}")
                except:
                    pass
                
                total_queries += 1
        
        print(f"\n📈 Total Analytics Opportunities: {total_queries} tables across {len(categories)} categories")
    
    # Legacy simple examples (backward compatibility)
    def example_1_sql_partition(self):
        """Example 1: Use SQL to create a partition for high-cost items."""
        print("📊 Example 1: SQL-based partitioning (Legacy)")
        
        # Use SQL to filter data
        high_cost_df = self.source_client.query(f"""
            SELECT *
            FROM {self.source_client.table_name}
            WHERE line_item_unblended_cost > 0.01
            ORDER BY line_item_unblended_cost DESC
        """)
        
        # Save to S3
        s3_path = self._save_to_s3(high_cost_df, "legacy_examples/high_cost_items.parquet")
        return s3_path
    
    def example_2_python_partition(self):
        """Example 2: Use Python to create two partitions."""
        print("📊 Example 2: Python-based partitioning (Legacy)")
        
        # Get all data
        all_data = self.source_client.query(f"SELECT * FROM {self.source_client.table_name}")
        
        # Split into two dataframes using Python
        s3_data = all_data.filter(pl.col('product_servicecode') == 'AmazonS3')
        non_s3_data = all_data.filter(pl.col('product_servicecode') != 'AmazonS3')
        
        # Save both to S3
        s3_path1 = self._save_to_s3(s3_data, "legacy_examples/s3_only.parquet")
        s3_path2 = self._save_to_s3(non_s3_data, "legacy_examples/non_s3.parquet")
        
        return [s3_path1, s3_path2]


def simple_example():
    """Simple usage example with legacy functionality."""
    from de_polars import DataExportsPolars
    
    # Load source data
    source_data = DataExportsPolars(
        s3_bucket='billing-data-exports-focus',
        s3_data_prefix='focus1/focus1/data',
        data_export_type='FOCUS1.0',
        table_name='CUR'
    )
    
    # Create partitioner
    partitioner = DataPartitioner(
        source_client=source_data,
        target_bucket='billing-data-exports-focus',
        target_prefix='analytics-tables'
    )
    
    # Legacy examples
    print("🔄 Legacy Examples:")
    high_cost_path = partitioner.example_1_sql_partition()
    s3_paths = partitioner.example_2_python_partition()
    
    print(f"\n🎉 Legacy partitions created:")
    print(f"   High cost items: {high_cost_path}")
    print(f"   S3 data: {s3_paths[0]}")
    print(f"   Non-S3 data: {s3_paths[1]}")


def analytics_example():
    """Example using SQL query library to create analytics tables."""
    from de_polars import DataExportsPolars
    
    # Load source data
    source_data = DataExportsPolars(
        s3_bucket='billing-data-exports-focus',
        s3_data_prefix='focus1/focus1/data',
        data_export_type='FOCUS1.0',
        table_name='CUR',
        date_start='2025-07',
        date_end='2025-07'
    )
    
    # Create enhanced partitioner
    partitioner = DataPartitioner(
        source_client=source_data,
        target_bucket='billing-data-exports-focus',
        target_prefix='analytics-tables',
        query_library_path='cur2_query_library'
    )
    
    # List available analytics opportunities
    partitioner.list_analytics_opportunities()
    print()
    
    # Create analytics tables for cost analysis category
    print("🏭 Creating Cost Analysis Tables:")
    cost_tables = partitioner.create_analytics_from_category('cost_analysis', partitioned=True)
    
    print(f"\n🎉 Created analytics tables:")
    for table_name, s3_path in cost_tables.items():
        print(f"   📊 {table_name}: {s3_path}")


if __name__ == "__main__":
    print("Choose example:")
    print("1. Legacy simple example")
    print("2. Analytics table example")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        simple_example()
    elif choice == "2":
        analytics_example()
    else:
        print("Invalid choice. Running analytics example...")
        analytics_example() 
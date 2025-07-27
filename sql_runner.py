#!/usr/bin/env python3
"""
SQL Runner for CUR2 Query Library

This script uses the DataPartitioner and DataExportsPolars to run SQL files 
from the cur2_query_library and save results as parquet files in cur2_data/ 
maintaining the same directory structure.

Usage:
    python sql_runner.py

Example SQL file paths:
    - cur2_query_library/analytics/amazon_athena.sql -> cur2_data/analytics/amazon_athena.parquet
    - cur2_query_library/compute/lambda.sql -> cur2_data/compute/lambda.parquet
"""

from de_polars import DataExportsPolars
from de_polars.data_partitioner import DataPartitioner
import sys


def setup_data_client(s3_bucket: str, 
                     s3_data_prefix: str, 
                     data_export_type: str = 'CUR2.0',
                     table_name: str = 'CUR',
                     date_start: str = None,
                     date_end: str = None) -> DataExportsPolars:
    """
    Set up the DataExportsPolars client for querying AWS data exports.
    
    Args:
        s3_bucket: S3 bucket containing the data export files
        s3_data_prefix: S3 prefix path to the data directory
        data_export_type: Type of AWS Data Export ('CUR2.0', 'FOCUS1.0', etc.)
        table_name: Table name to use in SQL queries
        date_start: Start date filter (format depends on data_export_type)
        date_end: End date filter (format depends on data_export_type)
    
    Returns:
        Configured DataExportsPolars client
    """
    print("🔧 Setting up Data Export client...")
    
    client = DataExportsPolars(
        s3_bucket=s3_bucket,
        s3_data_prefix=s3_data_prefix,
        data_export_type=data_export_type,
        table_name=table_name,
        date_start=date_start,
        date_end=date_end
    )
    
    print("✅ Data Export client configured")
    return client


def main():
    """
    Main function to run SQL files and save results as parquet files.
    
    CONFIGURE THESE SETTINGS BELOW:
    """
    
    # =================================================================
    # CONFIGURATION - MODIFY THESE SETTINGS FOR YOUR DATA
    # =================================================================
    
    # AWS S3 settings for your data export
    S3_BUCKET = "billing-data-exports-cur"  # Your S3 bucket
    S3_DATA_PREFIX = "cur2/cur2/data"  # Your S3 data prefix
    DATA_EXPORT_TYPE = "CUR2.0"  # Data export type
    TABLE_NAME = "CUR"  # Table name used in SQL
    
    # Date filters (optional) - format depends on DATA_EXPORT_TYPE
    # For CUR2.0/FOCUS1.0: use 'YYYY-MM' format (e.g., '2025-07')
    # For COH: use 'YYYY-MM-DD' format (e.g., '2025-07-15')
    DATE_START = "2025-07"  # Filter by date range
    DATE_END = "2025-07"    # Filter by date range
    
    # SQL files to run - modify this list to include your desired files
    SQL_FILES_TO_RUN = [
        "analytics/amazon_athena.sql",
        "compute/lambda.sql",
        # Add more files here:
        # "analytics/sample.sql",
        # "container/ecs_daily_usage.sql",
        # "cost_optimization/ec2_spot_instance_avg_savings.sql",
    ]
    
    # Output directory for parquet files
    OUTPUT_DIR = "cur2_data"
    
    # =================================================================
    # END CONFIGURATION
    # =================================================================
    
    print("🚀 SQL Runner for CUR2 Query Library")
    print("=" * 60)
    print()
    
    # Validate configuration
    if not SQL_FILES_TO_RUN:
        print("❌ No SQL files specified to run")
        print("   Edit the SQL_FILES_TO_RUN list to include your desired files")
        sys.exit(1)
    
    # Display configuration
    print(f"📊 Configuration:")
    print(f"   S3 Bucket: {S3_BUCKET}")
    print(f"   S3 Prefix: {S3_DATA_PREFIX}")
    print(f"   Export Type: {DATA_EXPORT_TYPE}")
    print(f"   Table Name: {TABLE_NAME}")
    print(f"   Date Range: {DATE_START or 'All'} to {DATE_END or 'All'}")
    print(f"   Output Dir: {OUTPUT_DIR}")
    print(f"   SQL Files: {len(SQL_FILES_TO_RUN)} files")
    print()
    
    try:
        # Set up data client
        data_client = setup_data_client(
            s3_bucket=S3_BUCKET,
            s3_data_prefix=S3_DATA_PREFIX,
            data_export_type=DATA_EXPORT_TYPE,
            table_name=TABLE_NAME,
            date_start=DATE_START,
            date_end=DATE_END
        )
        print()
        
        # Set up data partitioner
        print("🔧 Setting up Data Partitioner...")
        partitioner = DataPartitioner(
            source_client=data_client,
            output_base_dir=OUTPUT_DIR,
            query_library_path="cur2_query_library"
        )
        print("✅ Data Partitioner configured")
        print()
        
        # List available SQL files for reference
        print("📋 Available SQL files in query library:")
        partitioner.list_available_sql_files()
        print()
        
        # Run the specified SQL files
        print(f"🚀 Running {len(SQL_FILES_TO_RUN)} SQL files...")
        print("=" * 60)
        
        results = partitioner.run_sql_files(SQL_FILES_TO_RUN)
        
        # Display final results
        print("🎉 FINAL RESULTS:")
        print("=" * 60)
        
        if results:
            print("✅ Successfully processed files:")
            for sql_file, parquet_path in results.items():
                print(f"   📊 {sql_file}")
                print(f"      ➜ {parquet_path}")
            
            print(f"\n📊 Summary:")
            print(f"   ✅ Successful: {len(results)} files")
            print(f"   📂 Output directory: {OUTPUT_DIR}/")
            print(f"   💾 Total parquet files created: {len(results)}")
            
        else:
            print("❌ No files were successfully processed")
            print("   Check the error messages above for details")
            
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("\n💡 Common issues:")
        print("   1. Check your AWS credentials are configured")
        print("   2. Verify S3 bucket and prefix are correct")
        print("   3. Ensure SQL files exist in cur2_query_library/")
        print("   4. Check data export type matches your data")
        sys.exit(1)


if __name__ == "__main__":
    # Check if user wants to list available files
    if len(sys.argv) > 1 and sys.argv[1] == "--list-files":
        print("📋 Available SQL Files in Query Library")
        print("=" * 60)
        
        try:
            # Create a simple partitioner just to list files
            partitioner = DataPartitioner(
                source_client=None,  # Not needed for file discovery
                output_base_dir="temp"
            )
            partitioner.list_available_sql_files()
            
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            print("   Make sure cur2_query_library/ directory exists")
    else:
        main() 
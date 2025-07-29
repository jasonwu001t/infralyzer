#!/usr/bin/env python3
"""
Simple script to test the de_polars package with sample.sql
"""

from de_polars import DataExportsPolars
import os
import polars as pl
from pathlib import Path


def read_sql_file(file_path):
    """Read SQL content from file."""
    with open(file_path, 'r') as f:
        return f.read().strip()

def save_result_as_parquet(result_df, sql_file_path):
    """Save the query result as parquet in the same directory as the SQL file."""
    sql_path = Path(sql_file_path)
    parquet_path = sql_path.with_suffix('.parquet')
    
    # Create directory if it doesn't exist
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save as parquet
    result_df.write_parquet(parquet_path)
    print(f"💾 Results saved to: {parquet_path}")
    return str(parquet_path)

def read_parquet_file(parquet_file_path):
    """Read parquet file using polars and return the dataframe."""
    if not os.path.exists(parquet_file_path):
        print(f"❌ Parquet file not found: {parquet_file_path}")
        return None
    
    try:
        df = pl.read_parquet(parquet_file_path)
        print(f"📖 Successfully read parquet file: {parquet_file_path}")
        print(f"📊 Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"❌ Error reading parquet file: {e}")
        return None

def main():
    # sql_file = "cur2_query_library/analytics/amazon_athena.sql"

    # Level 1 Independent Views
    # sql_file = "cur2_views/level_1_independent/account_map.sql"
    # sql_file = "cur2_views/level_1_independent/compute_savings_plan_eligible_spend.sql"
    # sql_file = "cur2_views/level_1_independent/ec2_running_cost.sql"
    # sql_file = "cur2_views/level_1_independent/hourly_view.sql"
    # sql_file = "cur2_views/level_1_independent/kpi_ebs_snap.sql"
    # sql_file = "cur2_views/level_1_independent/kpi_ebs_storage_all.sql"
    # sql_file = "cur2_views/level_1_independent/kpi_instance_mapping.sql"
    # sql_file = "cur2_views/level_1_independent/kpi_s3_storage_all.sql"
    # sql_file = "cur2_views/level_1_independent/resource_view.sql"
    # sql_file = "cur2_views/level_1_independent/ri_sp_mapping.sql"
    # sql_file = "cur2_views/level_1_independent/s3_view.sql"
    # sql_file = "cur2_views/level_1_independent/summary_view.sql"

    # # Level 2 Dependent Views
    sql_file = "cur2_views/level_2_dependent/kpi_instance_all.sql"

    # # Level 3 Final Views
    # sql_file = "cur2_views/level_3_final/kpi_tracker.sql"

    if not os.path.exists(sql_file):
        print(f"❌ SQL file not found: {sql_file}")
        return
    sql_query = read_sql_file(sql_file)

    try:
        print("🔌 Connecting to AWS Data Exports...")
        
        # Example configuration for CUR 2.0 data
        # MODIFY THESE VALUES for your actual setup:
        data_client = DataExportsPolars(
            s3_bucket='billing-data-exports-cur',          # Replace with your S3 bucket
            s3_data_prefix='cur2/cur2/data',          # Replace with your S3 prefix  
            data_export_type='CUR2.0',               # Data export type
            table_name='CUR',                        # Table name used in SQL
            date_start='2025-07',                    # Optional: filter by date range
            date_end='2025-07'                       # Optional: filter by date range
        )
        
        print(f"🚀 Executing query...")
        result = data_client.query(sql_query)
        
        print("✅ Query Results:")
        print(result)
        print(f"\n📊 Summary: {result.shape[0]} rows, {result.shape[1]} columns")

        # Save result as parquet
        parquet_path = save_result_as_parquet(result, sql_file)
        
        # Example: Read back the parquet file
        print(f"\n🔄 Testing parquet read functionality...")
        df_from_parquet = read_parquet_file(parquet_path)
        if df_from_parquet is not None:
            print("✅ Parquet file read successfully!")
            print(f"First 5 rows from parquet:")
            print(df_from_parquet.head())

        # print("\n📋 Dataset Info:")
        # data_client.info()
        
    except Exception as e:
        print(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    main()

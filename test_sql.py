#!/usr/bin/env python3
"""
Simple script to test the de_polars package with sample.sql
"""

from de_polars import DataExportsPolars
import os

def read_sql_file(file_path):
    """Read SQL content from file."""
    with open(file_path, 'r') as f:
        return f.read().strip()

def main():
    sql_file = "cur2_query_library/analytics/amazon_athena.sql"
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

        # print("\n📋 Dataset Info:")
        # data_client.info()
        
    except Exception as e:
        print(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    main()

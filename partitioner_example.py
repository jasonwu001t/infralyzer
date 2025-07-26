"""
Simple Partitioner Test - Two Examples

This demonstrates the simplified data partitioner with:
1. SQL-based partitioning 
2. Python-based partitioning
"""

from de_polars import DataExportsPolars
from de_polars.data_partitioner import DataPartitioner

def main():
    print("🧪 Simple Data Partitioner Test")
    print("=" * 40)
    
    # Load source data
    print("📊 Loading source data...")
    source_data = DataExportsPolars(
        s3_bucket='billing-data-exports-cur',
        s3_prefix='cur2',
        table_name='CUR',
        date_start='2025-07',
        date_end='2025-07'
    )
    
    # Show source data info
    total_rows = source_data.query("SELECT COUNT(*) FROM CUR").item(0, 0)
    print(f"✅ Source data: {total_rows:,} total rows")
    
    # Initialize partitioner
    partitioner = DataPartitioner(
        source_client=source_data,
        target_bucket='billing-data-exports-cur',
        target_prefix='test-partitions'
    )
    
    print("\n" + "="*40)
    
    # Example 1: SQL-based partitioning
    print("1️⃣ SQL Example: Filter high-cost items using SQL")
    high_cost_path = partitioner.example_1_sql_partition()
    
    print("\n" + "="*40)
    
    # Example 2: Python-based partitioning  
    print("2️⃣ Python Example: Split data using Python filtering")
    s3_paths = partitioner.example_2_python_partition()
    
    print("\n" + "="*40)
    print("🎉 Summary:")
    print(f"   SQL partition (high cost): {high_cost_path}")
    print(f"   Python partition (S3): {s3_paths[0]}")
    print(f"   Python partition (non-S3): {s3_paths[1]}")
    
    print(f"\n📁 All partitions saved to: s3://billing-data-exports-cur/test-partitions/")

if __name__ == "__main__":
    main() 
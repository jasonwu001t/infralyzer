#!/usr/bin/env python3
"""
Example script showing how to use CUR2 view partitioning.

This script demonstrates:
1. Loading CUR data from S3
2. Running all CUR2 views in dependency order
3. Saving results as partitioned parquet files
"""

import os
from de_polars.client import DataExportsPolars
from cur2_view_partitioning import CUR2ViewPartitioner


def main():
    """Example of running CUR2 view partitioning pipeline."""
    
    print("🚀 CUR2 View Partitioning Example")
    print("=" * 50)
    
    # Step 1: Initialize client and load CUR data
    print("📊 Step 1: Initialize DataExportsPolars client")
    try:
        # Initialize client (you may need to configure S3 credentials)
        client = DataExportsPolars()
        
        # Register your CUR data (replace with your actual S3 path)
        # client.register_s3_data("s3://your-bucket/path/to/cur2/")
        # Or register from local parquet files:
        # client.register_parquet_data("path/to/local/cur2.parquet")
        
        print("✅ Client initialized successfully")
        print("💡 Make sure your CUR table is registered as 'CUR'")
        
    except Exception as e:
        print(f"❌ Failed to initialize client: {e}")
        print("💡 Configure your AWS credentials and S3 paths")
        return
    
    # Step 2: Initialize view partitioner
    print("\n📂 Step 2: Initialize CUR2 view partitioner")
    partitioner = CUR2ViewPartitioner(
        source_client=client,
        views_base_dir="cur2_views",      # Where SQL files are located
        output_base_dir="cur2_view"       # Where parquet files will be saved
    )
    print("✅ Partitioner initialized")
    
    # Step 3: Run all views in dependency order
    print("\n🏭 Step 3: Process all views in dependency order")
    try:
        results = partitioner.run_all_views()
        
        print("\n🎉 SUCCESS! All views processed")
        print(f"📁 Results saved in: cur2_view/")
        print(f"📊 Total views processed: {len(partitioner.processed_views)}")
        
        # Show summary of what was created
        print("\n📋 Generated parquet files:")
        for level, files in results.items():
            print(f"  {level}:")
            for sql_file, parquet_path in files.items():
                view_name = sql_file.split('/')[-1].replace('.sql', '')
                print(f"    ✅ {view_name} → {parquet_path}")
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    main()
    
    print("\n" + "=" * 50)
    print("💡 Next steps:")
    print("1. Check the cur2_view/ directory for your parquet files")
    print("2. Use these files for analytics, dashboards, or further processing")
    print("3. You can now query individual views or combine them as needed")
    print("\n🔄 To re-run with updated data:")
    print("   python example_run_views.py") 
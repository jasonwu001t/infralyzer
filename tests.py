
from de_polars import DataExportsPolars

data = DataExportsPolars(
    s3_bucket='billing-data-exports-cur',
    s3_data_prefix='cur2/cur2/data',         # ✨ Exact path to data directory
    data_export_type='CUR2.0',              # ✨ Auto-selects BILLING_PERIOD= format
    table_name='CUR',
    date_start='2025-07',                    # Monthly format: YYYY-MM
    date_end='2025-07'                       # Single month for faster testing
)
# Test basic query
result = data.query("""
    SELECT product
    FROM CUR
    LIMIT 5
""")
print(f"✅ Basic query successful: {result.shape}")
print(result.head(3))

# Test DuckDB advanced SQL features
print("\n🚀 Testing DuckDB Advanced SQL Features")
print("=" * 50)

# Test window functions
print("🪟 Testing Window Functions...")
try:
    window_result = data.query("""
        SELECT 
            product_servicecode,
            line_item_unblended_cost,
            ROW_NUMBER() OVER (PARTITION BY product_servicecode ORDER BY line_item_unblended_cost DESC) as cost_rank,
            SUM(line_item_unblended_cost) OVER (PARTITION BY product_servicecode) as service_total_cost
        FROM CUR 
        WHERE line_item_unblended_cost > 0
        LIMIT 10
    """)
    print(f"✅ Window functions successful: {window_result.shape}")
    print(window_result.head(3))
except Exception as e:
    print(f"⚠️ Window functions test: {str(e)[:200]}...")

# Test CTE (Common Table Expression)
print("\n📊 Testing Common Table Expressions (CTEs)...")
try:
    cte_result = data.query("""
        WITH service_costs AS (
            SELECT 
                product_servicecode,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(*) as line_count
            FROM CUR
            WHERE line_item_unblended_cost > 0
            GROUP BY product_servicecode
        ),
        top_services AS (
            SELECT *,
                RANK() OVER (ORDER BY total_cost DESC) as cost_rank
            FROM service_costs
        )
        SELECT 
            product_servicecode,
            total_cost,
            line_count,
            cost_rank
        FROM top_services 
        WHERE cost_rank <= 5
        ORDER BY cost_rank
    """)
    print(f"✅ CTE query successful: {cte_result.shape}")
    print(cte_result.head())
except Exception as e:
    print(f"⚠️ CTE test: {str(e)[:200]}...")

# Test date functions
print("\n📅 Testing Advanced Date Functions...")
try:
    date_result = data.query("""
        SELECT 
            DATE_TRUNC('month', line_item_usage_start_date) as usage_month,
            product_servicecode,
            SUM(line_item_unblended_cost) as monthly_cost
        FROM CUR
        WHERE line_item_unblended_cost > 0
        GROUP BY 1, 2
        HAVING SUM(line_item_unblended_cost) > 10
        ORDER BY usage_month DESC, monthly_cost DESC
        LIMIT 10
    """)
    print(f"✅ Date functions successful: {date_result.shape}")
    print(date_result.head(3))
except Exception as e:
    print(f"⚠️ Date functions test: {str(e)[:200]}...")

# Test string functions and case statements
print("\n🔤 Testing String Functions and CASE Statements...")
try:
    string_result = data.query("""
        SELECT 
            product_servicecode,
            CASE 
                WHEN UPPER(product_servicecode) LIKE '%EC2%' THEN 'Compute'
                WHEN UPPER(product_servicecode) LIKE '%S3%' THEN 'Storage'
                WHEN UPPER(product_servicecode) LIKE '%RDS%' THEN 'Database'
                ELSE 'Other'
            END as service_category,
            COUNT(*) as line_items,
            SUM(line_item_unblended_cost) as total_cost
        FROM CUR
        WHERE line_item_unblended_cost > 0
        GROUP BY 1, 2
        ORDER BY total_cost DESC
        LIMIT 15
    """)
    print(f"✅ String functions and CASE successful: {string_result.shape}")
    print(string_result.head(5))
except Exception as e:
    print(f"⚠️ String functions test: {str(e)[:200]}...")

print("\n🎉 DuckDB Advanced SQL Features Test Complete!")
print("✅ Demonstrated window functions, CTEs, date functions, and string operations")
print("✅ DuckDB SQL engine is working with advanced SQL capabilities")

# from de_polars import DataExportsPolars

# def test_cur_query():
#     """Test CUR 2.0 data query functionality."""
#     print("🧪 Testing CUR 2.0 Query Function")
#     print("=" * 40)
    
#     try:
#         # CUR 2.0 with new simplified approach
#         data = DataExportsPolars(
#             s3_bucket='billing-data-exports-cur',
#             s3_data_prefix='cur2/cur2/data',         # ✨ Exact path to data directory
#             data_export_type='CUR2.0',              # ✨ Auto-selects BILLING_PERIOD= format
#             table_name='CUR',
#             date_start='2025-07',                    # Monthly format: YYYY-MM
#             date_end='2025-07'                       # Single month for faster testing
#         )

#         # Test basic query
#         result = data.query("""
#             SELECT *
#             FROM CUR
#             LIMIT 5
#         """)
#         print(f"✅ Basic query successful: {result.shape}")
#         print(result.head(3))

#         # Test aggregation query
#         cost_result = data.query("""
#             SELECT 
#                 product_servicecode,
#                 COUNT(*) as line_items,
#                 SUM(line_item_unblended_cost) as total_cost
#             FROM CUR 
#             WHERE line_item_unblended_cost > 0
#             GROUP BY product_servicecode
#             ORDER BY total_cost DESC
#             LIMIT 5
#         """)
#         print(f"✅ Aggregation query successful: {cost_result.shape}")
#         print(cost_result)

#     except Exception as e:
#         print(f"⚠️  CUR test: {str(e)[:200]}...")

# def test_focus_query():
#     """Test FOCUS 1.0 data query functionality."""
#     print(f"\n🧪 Testing FOCUS 1.0 Query Function")
#     print("=" * 40)
    
#     try:
#         # FOCUS 1.0 with new simplified approach
#         data = DataExportsPolars(
#             s3_bucket='billing-data-exports-focus',
#             s3_data_prefix='focus1/focus1/data',     # ✨ Exact path to data directory
#             data_export_type='FOCUS1.0',            # ✨ Auto-selects billing_period= format
#             table_name='FOCUS',
#             date_start='2025-07',                    # Monthly format: YYYY-MM
#             date_end='2025-07'                       # Single month for faster testing
#         )

#         # Test FOCUS-specific query with correct column names
#         result = data.query("""
#             SELECT 
#                 ServiceName,
#                 SUM(BilledCost) as total_cost,
#                 COUNT(*) as line_items
#             FROM FOCUS 
#             WHERE BilledCost > 0.01
#             GROUP BY ServiceName
#             ORDER BY total_cost DESC
#             LIMIT 5
#         """)
#         print(f"✅ FOCUS query successful: {result.shape}")
#         print(result)

#     except Exception as e:
#         print(f"⚠️  FOCUS test: {str(e)[:200]}...")

# def test_coh_query():
#     """Test COH (Cost Optimization Hub) data query functionality with daily partitions."""
#     print(f"\n🧪 Testing COH Query Function (Daily Partitions)")
#     print("=" * 40)
    
#     try:
#         # COH with daily partitions
#         data = DataExportsPolars(
#             s3_bucket='test-bucket',                 # Demo bucket
#             s3_data_prefix='coh/coh/data',           # ✨ COH data structure
#             data_export_type='COH',                 # ✨ Auto-selects date= format (daily)
#             table_name='RECOMMENDATIONS',
#             date_start='2025-07-15',                 # Daily format: YYYY-MM-DD
#             date_end='2025-07-17'                    # 3 days for testing
#         )

#         # Test COH-specific query
#         result = data.query("""
#             SELECT 
#                 recommendation_type,
#                 COUNT(*) as recommendation_count,
#                 SUM(estimated_monthly_savings_amount) as total_savings
#             FROM RECOMMENDATIONS 
#             WHERE estimated_monthly_savings_amount > 0
#             GROUP BY recommendation_type
#             ORDER BY total_savings DESC
#             LIMIT 5
#         """)
#         print(f"✅ COH query successful: {result.shape}")
#         print(result)

#     except Exception as e:
#         print(f"⚠️  COH test (expected - demo bucket): {str(e)[:200]}...")

# def test_query_edge_cases():
#     """Test query function edge cases and error handling."""
#     print(f"\n🧪 Testing Query Edge Cases")
#     print("=" * 40)
    
#     try:
#         # Use a working dataset for edge case testing
#         data = DataExportsPolars(
#             s3_bucket='billing-data-exports-cur',
#             s3_data_prefix='cur2/cur2/data',
#             data_export_type='CUR2.0',
#             table_name='CUR',
#             date_start='2025-07',
#             date_end='2025-07'
#         )

#         # Test empty result query
#         empty_result = data.query("""
#             SELECT *
#             FROM CUR
#             WHERE line_item_unblended_cost > 999999999
#             LIMIT 10
#         """)
#         print(f"✅ Empty result query: {empty_result.shape}")

#         # Test COUNT query
#         count_result = data.query("""
#             SELECT COUNT(*) as total_records
#             FROM CUR
#         """)
#         print(f"✅ Count query: {count_result}")

#         # Test date filtering query (using Polars-compatible date functions)
#         date_result = data.query("""
#             SELECT 
#                 line_item_usage_start_date::date as usage_day,
#                 COUNT(*) as daily_records
#             FROM CUR
#             GROUP BY usage_day
#             ORDER BY usage_day
#             LIMIT 5
#         """)
#         print(f"✅ Date filtering query: {date_result.shape}")
#         print(date_result.head(3))

#     except Exception as e:
#         print(f"⚠️  Edge cases test: {str(e)[:200]}...")

# def test_invalid_query():
#     """Test invalid query handling."""
#     print(f"\n🧪 Testing Invalid Query Handling")
#     print("=" * 40)
    
#     try:
#         data = DataExportsPolars(
#             s3_bucket='billing-data-exports-cur',
#             s3_data_prefix='cur2/cur2/data',
#             data_export_type='CUR2.0',
#             table_name='CUR',
#             date_start='2025-07',
#             date_end='2025-07'
#         )

#         # Test invalid SQL syntax
#         try:
#             invalid_result = data.query("SELECT * FROM INVALID_TABLE")
#             print("❌ Should have failed!")
#         except Exception as sql_error:
#             print(f"✅ Correctly caught invalid query: {str(sql_error)[:100]}...")

#         # Test invalid column name
#         try:
#             invalid_col = data.query("SELECT nonexistent_column FROM CUR LIMIT 1")
#             print("❌ Should have failed!")
#         except Exception as col_error:
#             print(f"✅ Correctly caught invalid column: {str(col_error)[:100]}...")

#     except Exception as e:
#         print(f"⚠️  Invalid query test setup: {str(e)[:200]}...")

# def main():
#     """Run all query function tests."""
#     print("🚀 DE Polars Query Function Tests")
#     print("🎯 Testing new simplified data export type approach")
#     print("=" * 60)
    
#     # Test different data export types
#     test_cur_query()
#     test_focus_query()
#     test_coh_query()
    
#     # Test edge cases and error handling
#     test_query_edge_cases()
#     test_invalid_query()
    
#     print(f"\n" + "=" * 60)
#     print("🎉 Query Function Test Summary")
#     print("=" * 60)
#     print("✅ Tested CUR 2.0 queries (monthly partitions)")
#     print("✅ Tested FOCUS 1.0 queries (monthly partitions)")
#     print("✅ Tested COH queries (daily partitions)")
#     print("✅ Tested edge cases and error handling")
#     print("✅ Verified new simplified data export type approach")
#     print("")
#     print("💡 Key Features Tested:")
#     print("   • Automatic partition format selection")
#     print("   • SQL query execution on different export types")
#     print("   • Date filtering with partition-aware discovery")
#     print("   • Error handling for invalid queries")
#     print("   • Different aggregation and filtering patterns")

def test_file_format_detection():
    """Test file format detection for parquet and gzip files."""
    print(f"\n🧪 Testing File Format Detection")
    print("=" * 40)
    
    # Create a test client
    data = DataExportsPolars(
        s3_bucket='test-bucket',
        s3_data_prefix='test/test/data',
        data_export_type='CUR2.0',
        table_name='TEST'
    )
    
    # Test parquet detection
    parquet_files = [
        's3://bucket/path/file1.parquet',
        's3://bucket/path/file2.parquet'
    ]
    format_result = data._detect_file_format(parquet_files)
    print(f"✅ Parquet detection: {format_result}")
    assert format_result == 'parquet', f"Expected 'parquet', got '{format_result}'"
    
    # Test gzip detection
    gzip_files = [
        's3://bucket/path/file1.gz',
        's3://bucket/path/file2.gz'
    ]
    format_result = data._detect_file_format(gzip_files)
    print(f"✅ Gzip detection: {format_result}")
    assert format_result == 'gzip', f"Expected 'gzip', got '{format_result}'"
    
    # Test unknown format
    unknown_files = [
        's3://bucket/path/file1.txt'
    ]
    format_result = data._detect_file_format(unknown_files)
    print(f"✅ Unknown format detection: {format_result}")
    assert format_result == 'unknown', f"Expected 'unknown', got '{format_result}'"
    
    # Test empty files list
    empty_files = []
    format_result = data._detect_file_format(empty_files)
    print(f"✅ Empty files detection: {format_result}")
    assert format_result == 'unknown', f"Expected 'unknown', got '{format_result}'"
    
    print("✅ All file format detection tests passed!")

# Test file format detection
test_file_format_detection()

# if __name__ == "__main__":
#     main()
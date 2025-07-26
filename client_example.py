"""
Simple example showing the DE Polars (Data Exports Polars) API usage with simplified data export type approach.

Supports ALL AWS Data Exports with automatic partition format detection:
- AWS Cost and Usage Report (CUR 2.0) - uses BILLING_PERIOD=YYYY-MM
- FOCUS 1.0 - uses billing_period=YYYY-MM  
- Cost Optimization Hub - uses BILLING_PERIOD=YYYY-MM
- Carbon Emissions Data - uses BILLING_PERIOD=YYYY-MM

✨ NEW SIMPLIFIED APPROACH:
- User provides exact s3_data_prefix (full path to data directory)
- User specifies data_export_type (FOCUS1.0, CUR2.0, COH, CARBON_EMISSION)
- Automatic partition format selection based on export type
- No more prefix guessing = faster and cheaper!

Requirements:
1. AWS credentials configured (environment variables, profiles, IAM roles, etc.)
2. Package installed: pip install -e . OR pip install git+https://github.com/jasonwu001t/de-polars.git

Authentication Options Available:
- Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) - Default method
- Manual credentials (aws_access_key_id, aws_secret_access_key)
- Temporary credentials with MFA/STS (includes session_token and expiration)
- AWS profiles from ~/.aws/credentials (aws_profile)
- Cross-account role assumption (role_arn, external_id)
"""

from de_polars import DataExportsPolars

def main():
    print("🚀 DE Polars (Data Exports Polars) - Simplified Approach")
    print("=" * 60)
    print("⚡ Demonstrating Data Export Type Auto-Detection:")
    print("📋 FOCUS1.0 → billing_period=YYYY-MM (lowercase)")
    print("📋 CUR2.0 → BILLING_PERIOD=YYYY-MM (uppercase)")
    print("")
    
    # ============================================================================
    # SIMPLIFIED PARTITION-AWARE DISCOVERY EXAMPLES
    # ============================================================================
    
    # Example 1: FOCUS 1.0 DATA (Lowercase partition format)
    print("🎯 Example 1: FOCUS 1.0 Data (Single Month)")
    print("=" * 50)
    print("📊 Scenario: Analyze July 2025 FOCUS costs")
    print("🔍 Auto-detects: billing_period=2025-07/ (lowercase)")
    print("⚡ Performance: Direct S3 navigation to exact partition!")
    
    try:
        focus_data = DataExportsPolars(
            s3_bucket='billing-data-exports-focus',
            s3_data_prefix='focus1/focus1/data',  # ✨ Exact path to data directory
            data_export_type='FOCUS1.0',         # ✨ Auto-selects billing_period= format
            table_name='FOCUS',
            date_start='2025-07',                 # Single month optimization
            date_end='2025-07'
        )
        
        # Query high-cost items
        result = focus_data.query("""
            SELECT 
                service_name,
                SUM(billed_cost) as total_cost
            FROM FOCUS 
            WHERE billed_cost > 0.01
            GROUP BY service_name
            ORDER BY total_cost DESC
            LIMIT 5
        """)
        
        print("✅ FOCUS Query Results:")
        print(result)
        print("")
        
    except Exception as e:
        print(f"⚠️  FOCUS Example: {str(e)[:150]}...")
        print("")
    
    # Example 2: CUR 2.0 DATA (Uppercase partition format)
    print("🎯 Example 2: CUR 2.0 Data (Multiple Months)")
    print("=" * 50)
    print("📊 Scenario: Analyze Q3 2025 costs (multiple partitions)")
    print("🔍 Auto-detects: BILLING_PERIOD=YYYY-MM/ (uppercase)")
    print("⚡ Performance: Scans only July-Sept partitions!")
    
    try:
        cur_data = DataExportsPolars(
            s3_bucket='billing-data-exports-cur',
            s3_data_prefix='cur2/cur2/data',     # ✨ Exact path to data directory
            data_export_type='CUR2.0',          # ✨ Auto-selects BILLING_PERIOD= format
            table_name='CUR',
            date_start='2025-07',                # Multi-month range
            date_end='2025-09'                   # July, August, September
        )
        
        # Query monthly costs trend
        result = cur_data.query("""
            SELECT 
                line_item_usage_start_date,
                COUNT(*) as line_items,
                SUM(line_item_unblended_cost) as monthly_cost
            FROM CUR 
            WHERE line_item_unblended_cost > 0
            GROUP BY line_item_usage_start_date
            ORDER BY line_item_usage_start_date
            LIMIT 10
        """)
        
        print("✅ CUR Query Results:")
        print(result)
        print("")
        
    except Exception as e:
        print(f"⚠️  CUR Example: {str(e)[:150]}...")
        print("")
    
    # Example 3: ALL DATA (No date filters)
    print("🎯 Example 3: All Available Data (No Date Filters)")
    print("=" * 50)
    print("📊 Scenario: Discovery mode - see what's available")
    print("🔍 Auto-detects: Scans all BILLING_PERIOD=YYYY-MM/ folders")
    print("⚡ Performance: Efficient partition discovery!")
    
    try:
        all_data = DataExportsPolars(
            s3_bucket='billing-data-exports-cur',
            s3_data_prefix='cur2/cur2/data',     # ✨ Exact path to data directory
            data_export_type='CUR2.0',          # ✨ Auto-selects BILLING_PERIOD= format
            table_name='CUR'
            # No date_start/date_end = scans all partitions
        )
        
        # Get basic statistics
        result = all_data.query("""
            SELECT 
                COUNT(*) as total_line_items,
                MIN(line_item_usage_start_date) as earliest_date,
                MAX(line_item_usage_start_date) as latest_date,
                SUM(line_item_unblended_cost) as total_cost
            FROM CUR
        """)
        
        print("✅ All Data Summary:")
        print(result)
        print("")
        
    except Exception as e:
        print(f"⚠️  All Data Example: {str(e)[:150]}...")
        print("")
    
    # Example 4: COH (Cost Optimization Hub) - Daily Partitions
    print("🎯 Example 4: COH Data (Daily Partitions)")
    print("=" * 50)
    print("📊 Scenario: Cost optimization recommendations for a week")
    print("🔍 Auto-detects: date=YYYY-MM-DD/ (daily partitions)")
    print("⚡ Performance: Daily granularity for detailed analysis!")
    
    try:
        coh_data = DataExportsPolars(
            s3_bucket='test-bucket',            # Would be your COH bucket
            s3_data_prefix='coh/coh/data',      # ✨ COH data directory structure
            data_export_type='COH',            # ✨ Auto-selects date= format (daily)
            table_name='RECOMMENDATIONS',
            date_start='2025-07-15',           # Daily format: YYYY-MM-DD
            date_end='2025-07-20'              # 6 days of recommendations
        )
        
        # Query optimization recommendations
        result = coh_data.query("""
            SELECT 
                recommendation_type,
                COUNT(*) as recommendation_count,
                SUM(estimated_monthly_savings_amount) as total_savings
            FROM RECOMMENDATIONS 
            WHERE estimated_monthly_savings_amount > 0
            GROUP BY recommendation_type
            ORDER BY total_savings DESC
            LIMIT 5
        """)
        
        print("✅ COH Query Results:")
        print(result)
        print("")
        
    except Exception as e:
        print(f"⚠️  COH Example (demo): {str(e)[:150]}...")
        print("💡 COH uses daily partitions with date=YYYY-MM-DD format")
        print("💡 Requires YYYY-MM-DD input format (e.g., '2025-07-15')")
        print("")
    
    # ============================================================================
    # DEBUG AND DISCOVERY TOOLS
    # ============================================================================
    
    print("🔍 Debug Tools")
    print("=" * 30)
    
    try:
        debug_client = DataExportsPolars(
            s3_bucket='billing-data-exports-cur',
            s3_data_prefix='cur2/cur2/data',
            data_export_type='CUR2.0'
        )
        
        # List available partitions for debugging
        partitions = debug_client.list_available_partitions()
        print(f"📅 Available partitions: {partitions}")
        
        # Show schema
        schema = debug_client.schema()
        print(f"📋 Schema preview: {len(schema)} columns")
        print(f"   Sample columns: {list(schema.keys())[:5]}...")
        
    except Exception as e:
        print(f"🔍 Debug tools: {e}")
    
    print("\n" + "=" * 60)
    print("✨ Summary: Simplified Approach Benefits")
    print("=" * 60)
    print("✅ NO MORE: Complex prefix guessing")
    print("✅ NO MORE: Multiple S3 API calls") 
    print("✅ ADDED: Data export type validation")
    print("✅ ADDED: Automatic partition format selection")
    print("✅ ADDED: COH daily partition support (date=YYYY-MM-DD)")
    print("✅ FASTER: Direct S3 navigation")
    print("✅ CHEAPER: Fewer S3 API calls")
    print("✅ SIMPLER: User provides exact data directory path")

def show_alternative_examples():
    """
    Commented examples for other data export types and authentication methods.
    
    Uncomment and modify as needed for your specific use case.
    """
    
    # ========================================================================
    # ALTERNATIVE DATA EXPORT TYPES
    # ========================================================================
    
    # Cost Optimization Hub (COH) - uses date= format (daily partitions)
    # coh_data = DataExportsPolars(
    #     s3_bucket='my-bucket',
    #     s3_data_prefix='coh/coh/data',
    #     data_export_type='COH',
    #     table_name='RECOMMENDATIONS',
    #     date_start='2025-07-15',        # Daily format: YYYY-MM-DD
    #     date_end='2025-07-20'           # Daily partition range
    # )
    
    # Carbon Emissions - uses BILLING_PERIOD= format
    # carbon_data = DataExportsPolars(
    #     s3_bucket='my-bucket',
    #     s3_data_prefix='carbon/carbon/data',
    #     data_export_type='CARBON_EMISSION',
    #     table_name='CARBON',
    #     date_start='2025-07',
    #     date_end='2025-07'
    # )
    
    # ========================================================================
    # ALTERNATIVE AUTHENTICATION METHODS
    # ========================================================================
    
    # Method 1: Manual AWS Credentials
    # manual_data = DataExportsPolars(
    #     s3_bucket='my-bucket',
    #     s3_data_prefix='cur2/cur2/data',
    #     data_export_type='CUR2.0',
    #     aws_access_key_id='AKIA...',
    #     aws_secret_access_key='...',
    #     aws_region='us-east-1'
    # )
    
    # Method 2: Temporary Credentials with MFA/STS
    # temp_data = DataExportsPolars(
    #     s3_bucket='my-bucket',
    #     s3_data_prefix='cur2/cur2/data', 
    #     data_export_type='CUR2.0',
    #     aws_access_key_id='ASIA...',
    #     aws_secret_access_key='...',
    #     aws_session_token='IQoJb3JpZ2luX2...',
    #     expiration='2025-01-20T10:30:00Z'
    # )
    
    # Method 3: AWS Profile
    # profile_data = DataExportsPolars(
    #     s3_bucket='my-bucket',
    #     s3_data_prefix='cur2/cur2/data',
    #     data_export_type='CUR2.0',
    #     aws_profile='my-profile'
    # )
    
    # Method 4: Cross-Account Role Assumption
    # role_data = DataExportsPolars(
    #     s3_bucket='cross-account-bucket',
    #     s3_data_prefix='cur2/cur2/data',
    #     data_export_type='CUR2.0',
    #     role_arn='arn:aws:iam::123456789012:role/CrossAccountRole',
    #     external_id='unique-external-id'
    # )
    
    pass

if __name__ == "__main__":
    main() 
    
    print("\n💡 See show_alternative_examples() function for:")
    print("   • Other data export types (COH, CARBON_EMISSION)")
    print("   • Alternative authentication methods")
    print("   • Cross-account access patterns") 
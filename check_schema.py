from de_polars import DataExportsPolars

print("🔍 Checking CUR Schema for Product Columns")
print("=" * 50)

# Initialize data client
data = DataExportsPolars(
    s3_bucket='billing-data-exports-cur',
    s3_data_prefix='cur2/cur2/data',
    data_export_type='CUR2.0',
    table_name='CUR',
    date_start='2025-07',
    date_end='2025-07'
)

# Get schema
print("📋 Getting schema information...")
schema = data.schema()

# Find product-related columns
product_columns = [col for col in schema.keys() if 'product' in col.lower()]
print(f"\n🏷️  Product-related columns found ({len(product_columns)}):")
for col in sorted(product_columns):
    print(f"   • {col}")

# Find service-related columns
service_columns = [col for col in schema.keys() if 'service' in col.lower()]
print(f"\n🔧 Service-related columns found ({len(service_columns)}):")
for col in sorted(service_columns):
    print(f"   • {col}")

# Find line_item columns that might help identify services
line_item_columns = [col for col in schema.keys() if col.startswith('line_item') and ('product' in col.lower() or 'service' in col.lower())]
print(f"\n📊 Line item columns with product/service info ({len(line_item_columns)}):")
for col in sorted(line_item_columns):
    print(f"   • {col}")

# Sample some data to see what values are in these columns
print(f"\n📄 Sample data to understand column content:")
sample_query = """
SELECT 
    line_item_product_code,
    product_product_family,
    product_servicename
FROM CUR 
WHERE line_item_product_code IS NOT NULL 
LIMIT 10
"""

try:
    sample = data.query(sample_query)
    print(sample)
except Exception as e:
    print(f"Error getting sample: {e}")
    
    # Try alternative columns
    print(f"\n🔄 Trying alternative sample...")
    alt_sample_query = """
    SELECT DISTINCT
        line_item_product_code,
        product_product_family
    FROM CUR 
    WHERE line_item_product_code LIKE '%Glue%' OR line_item_product_code LIKE '%glue%'
    LIMIT 5
    """
    
    try:
        alt_sample = data.query(alt_sample_query)
        print(alt_sample)
    except Exception as e2:
        print(f"Alt sample also failed: {e2}")

print(f"\n✅ Schema analysis complete!") 
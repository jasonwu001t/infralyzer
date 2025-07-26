from de_polars import DataExportsPolars

data = DataExportsPolars(
    s3_bucket='billing-data-exports-focus',
    s3_data_prefix='focus1/focus1/data',         # ✨ Exact path to data directory
    data_export_type='FOCUS1.0',              # ✨ Auto-selects BILLING_PERIOD= format
    table_name='CUR',
    date_start='2025-04',                    # Monthly format: YYYY-MM
    date_end='2025-07'                       # Single month for faster testing
)
# Test basic query
result = data.query("""
    SELECT *
    FROM CUR
    LIMIT 5
""")
print(f"✅ Basic query successful: {result.shape}")
print(result.head(3))

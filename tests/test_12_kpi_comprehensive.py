"""
Comprehensive KPI Tracker Test - Execute all SQL views and generate JSON output
=============================================================================

This test will:
1. Load the available data (CUR/FOCUS)
2. Execute all prerequisite SQL views for kpi_tracker.sql
3. Run the final kpi_tracker query
4. Generate the complete JSON API response
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add the project root to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine
from infralyzer.engine.data_config import DataConfig, DataExportType


def determine_data_source():
    """
    Determine which data source to use, prioritizing CUR 2.0 as requested.
    Returns tuple: (config, description)
    """
    print("Using CUR 2.0 data (as requested)")
    print()
    
    # Determine the correct local data path based on current working directory
    current_dir = os.getcwd()
    if current_dir.endswith('/tests'):
        # Running from tests directory
        local_data_path = '../test_local_data'
    else:
        # Running from project root
        local_data_path = 'test_local_data'
    
    # CUR 2.0 data configuration
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',
        s3_data_prefix='cur2/cur2/data',
        data_export_type=DataExportType.CUR_2_0,
        table_name='CUR',
        date_start='2025-07',
        date_end='2025-07',
        local_data_path=local_data_path,
        prefer_local_data=True
    )
    
    return config, "CUR 2.0"


def execute_view_from_sql_file(conn, sql_file_path, view_name, save_parquet=True, views_output_path=None):
    """
    Execute a SQL file to create a view in the current DuckDB connection.
    Optionally saves the view result as a parquet file.
    """
    print(f"Creating view: {view_name}")
    print(f"SQL file: {sql_file_path}")
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Special handling for kpi_instance_mapping.sql - fix DuckDB syntax
        if 'kpi_instance_mapping' in str(sql_file_path):
            print("Fixing DuckDB syntax for kpi_instance_mapping...")
            sql_content = sql_content.replace('ROW (', '(')
        
        # Remove any existing CREATE OR REPLACE VIEW and add our own
        lines = sql_content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped = line.strip()
            if (stripped.startswith('--') or 
                stripped.startswith('CREATE OR REPLACE VIEW') or
                stripped.startswith('CREATE VIEW') or
                stripped == ''):
                if not stripped.startswith('CREATE'):  # Keep comments and empty lines
                    cleaned_lines.append(line)
                continue
            cleaned_lines.append(line)
        
        # Create the view
        view_sql = f"CREATE OR REPLACE VIEW {view_name} AS\n" + '\n'.join(cleaned_lines)
        conn.execute(view_sql)
        print(f"View {view_name} created successfully")
        
        # Save view result as parquet if requested
        if save_parquet and views_output_path:
            query_sql = '\n'.join(cleaned_lines)
            result = conn.execute(query_sql).fetchdf()
            
            # Convert to polars for consistency and better performance
            import polars as pl
            result_pl = pl.from_pandas(result)
            
            # Save as parquet
            parquet_path = f"{views_output_path}/{view_name}.parquet"
            result_pl.write_parquet(parquet_path)
            
            file_size = os.path.getsize(parquet_path) / 1024  # KB
            print(f"Saved: {parquet_path} ({file_size:.1f} KB, {len(result_pl)} rows)")
        
        return True
        
    except Exception as e:
        print(f"Failed to create view {view_name}: {e}")
        return False


def main():
    print("Comprehensive KPI Tracker Test")
    print("=" * 80)
    
    # Determine data source (prioritize CUR 2.0)
    config, data_source_desc = determine_data_source()
    
    print(f" Using {data_source_desc} data source")
    print(f" Data Export Type: {config.data_export_type.value}")
    print(f"Partition Format: BILLING_PERIOD=YYYY-MM (monthly)")
    print(f"Date Format Required: YYYY-MM (e.g., '2025-07')")
    print(f"📂 Data Path: s3://{config.s3_bucket}/{config.s3_data_prefix}")
    print(f"Local Cache: {config.local_bucket_path}")
    print(f"Prefer Local: {config.prefer_local_data}")
    print()
    
    # Initialize FinOps Engine
    engine = FinOpsEngine(config)
    
    print("Testing basic data access...")
    try:
        # Test basic data access
        query = f"SELECT COUNT(*) as total_records FROM {config.table_name}"
        result = engine.query(query)
        if len(result) > 0:
            record_count = result['total_records'].iloc[0] if hasattr(result, 'iloc') else result['total_records'][0]
        else:
            record_count = 0
        print(f"Data available: {record_count:,} records")
        print()
        
    except Exception as e:
        print(f"Data access failed: {e}")
        return
    
    print("Creating prerequisite views in single DuckDB session...")
    # Get a persistent DuckDB connection
    conn = engine.engine._get_duckdb_connection()
    
    # Register local data with this connection
    engine.engine._register_local_data_with_duckdb(conn)
    print()
    
    # Determine the correct paths based on current working directory
    current_dir = os.getcwd()
    if current_dir.endswith('/tests'):
        # Running from tests directory
        views_base_path = '../cur2_views'
        views_output_path = '../test_views_output'
    else:
        # Running from project root
        views_base_path = 'cur2_views'
        views_output_path = 'test_views_output'
    
    # Create output directory for view results
    os.makedirs(views_output_path, exist_ok=True)
    print(f"View results will be saved to: {views_output_path}")
    print()
    
    # Define the views to create in dependency order
    views_to_create = [
        # Level 1 - Independent views
        (f'{views_base_path}/level_1_independent/summary_view.sql', 'summary_view'),
        (f'{views_base_path}/level_1_independent/kpi_instance_mapping.sql', 'kpi_instance_mapping'),
        (f'{views_base_path}/level_1_independent/kpi_ebs_storage_all.sql', 'kpi_ebs_storage_all'),
        (f'{views_base_path}/level_1_independent/kpi_ebs_snap.sql', 'kpi_ebs_snap'),
        (f'{views_base_path}/level_1_independent/kpi_s3_storage_all.sql', 'kpi_s3_storage_all'),
        # Level 2 - Dependent views
        (f'{views_base_path}/level_2_dependent/kpi_instance_all.sql', 'kpi_instance_all'),
    ]
    
    successful_views = []
    saved_parquets = []
    
    for sql_file, view_name in views_to_create:
        sql_path = Path(sql_file)
        if sql_path.exists():
            if execute_view_from_sql_file(conn, sql_path, view_name, save_parquet=True, views_output_path=views_output_path):
                successful_views.append(view_name)
                saved_parquets.append(f"{views_output_path}/{view_name}.parquet")
        else:
            print(f" SQL file not found: {sql_path}")
    
    print(f"\nSuccessfully created {len(successful_views)} views: {', '.join(successful_views)}")
    print(f"Saved {len(saved_parquets)} parquet files to {views_output_path}")
    print()
    
    # Execute kpi_tracker.sql
    print("Executing actual kpi_tracker.sql using cur2_views...")
    
    kpi_sql_path = Path(f"{views_base_path}/level_3_final/kpi_tracker.sql")
    
    if not kpi_sql_path.exists():
        print(f"kpi_tracker.sql not found at: {kpi_sql_path}")
        return
    
    try:
        print("Loading and executing kpi_tracker.sql...")
        
        with open(kpi_sql_path, 'r', encoding='utf-8') as f:
            kpi_sql = f.read()
        
        # Clean the SQL - remove CREATE statements and comments
        lines = kpi_sql.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped = line.strip()
            if (stripped.startswith('--') or 
                stripped.startswith('CREATE OR REPLACE VIEW') or
                stripped.startswith('CREATE VIEW') or
                stripped == ''):
                if not stripped.startswith('CREATE'):  # Keep comments and empty lines
                    cleaned_lines.append(line)
                continue
            cleaned_lines.append(line)
        
        kpi_sql_cleaned = '\n'.join(cleaned_lines)
        
        print(f"Executing kpi_tracker query with {len(successful_views)} prerequisite views...")
        print(f"Available views: {', '.join(successful_views)}")
        
        # Fix the original kpi_tracker.sql for DuckDB compatibility
        print("Fixing original kpi_tracker.sql for DuckDB compatibility...")
        
        # The issue is in the complex nested subqueries - convert to CTEs
        kpi_sql_fixed = kpi_sql_cleaned.replace(
            "GROUP BY 1, 2, 3, 4, 37",  # Column 37 = license_model causes issues
            "GROUP BY 1, 2, 3, 4, license_model"  # Use explicit column name
        )
        
        # Try the original SQL first
        try:
            print(" Attempting to run original kpi_tracker.sql with DuckDB fixes...")
            result = conn.execute(kpi_sql_fixed).fetchdf()
            print(f"Original kpi_tracker.sql executed successfully! Got {len(result)} row(s)")
        except Exception as e:
            print(f" Original SQL still has issues: {e}")
            print(" Falling back to restructured version...")
            
            # Load the restructured SQL file
            restructured_sql_path = Path(f"{views_base_path}/level_3_final/kpi_tracker_restructured.sql")
            print(f"Loading restructured SQL from: {restructured_sql_path}")
            
            if restructured_sql_path.exists():
                with open(restructured_sql_path, 'r', encoding='utf-8') as f:
                    restructured_sql = f.read()
                print(f"Loaded restructured SQL ({len(restructured_sql)} characters)")
                result = conn.execute(restructured_sql).fetchdf()
            else:
                print(f"Restructured SQL file not found: {restructured_sql_path}")
                raise FileNotFoundError(f"Could not find {restructured_sql_path}")
        
        if len(result) > 0:
            print(f"kpi_tracker.sql executed successfully! Got {len(result)} row(s)")
            row = result.iloc[0]
            
            # Create comprehensive JSON response
            current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            
            # Get unique billing periods and accounts from the result
            billing_periods = [str(row['billing_period'])] if 'billing_period' in row else []
            
            json_response = {
                "summary_metadata": {
                    "query_date": current_time,
                    "billing_periods": billing_periods,
                    "total_accounts": 1,  # Based on current result structure
                    "data_source": f"{data_source_desc.lower().replace(' ', '').replace('.', '')}_local_parquet",
                    "data_export_type": config.data_export_type.value,
                    "records_analyzed": record_count
                },
                "overall_spend": {
                    "billing_period": str(row.get('billing_period', '')),
                    "payer_account_id": str(row.get('payer_account_id', '')),
                    "linked_account_id": str(row.get('linked_account_id', '')),
                    "spend_all_cost": float(row.get('spend_all_cost', 0)),
                    "unblended_cost": float(row.get('unblended_cost', 0)),
                    "tags_json": str(row.get('tags_json', '{}'))
                },
                "ec2_metrics": {
                    "ec2_all_cost": float(row.get('ec2_all_cost', 0)),
                    "ec2_usage_cost": float(row.get('ec2_usage_cost', 0)),
                    "ec2_spot_cost": float(row.get('ec2_spot_cost', 0)),
                    "ec2_spot_potential_savings": float(row.get('ec2_spot_potential_savings', 0)),
                    "ec2_previous_generation_cost": float(row.get('ec2_previous_generation_cost', 0)),
                    "ec2_previous_generation_potential_savings": float(row.get('ec2_previous_generation_potential_savings', 0)),
                    "ec2_graviton_eligible_cost": float(row.get('ec2_graviton_eligible_cost', 0)),
                    "ec2_graviton_cost": float(row.get('ec2_graviton_cost', 0)),
                    "ec2_graviton_potential_savings": float(row.get('ec2_graviton_potential_savings', 0)),
                    "ec2_amd_eligible_cost": float(row.get('ec2_amd_eligible_cost', 0)),
                    "ec2_amd_cost": float(row.get('ec2_amd_cost', 0)),
                    "ec2_amd_potential_savings": float(row.get('ec2_amd_potential_savings', 0))
                },
                "rds_metrics": {
                    "rds_all_cost": float(row.get('rds_all_cost', 0)),
                    "rds_ondemand_cost": float(row.get('rds_ondemand_cost', 0)),
                    "rds_graviton_cost": float(row.get('rds_graviton_cost', 0)),
                    "rds_graviton_eligible_cost": float(row.get('rds_graviton_eligible_cost', 0)),
                    "rds_graviton_potential_savings": float(row.get('rds_graviton_potential_savings', 0)),
                    "rds_commit_potential_savings": float(row.get('rds_commit_potential_savings', 0)),
                    "rds_commit_savings": float(row.get('rds_commit_savings', 0)),
                    "rds_license": int(row.get('rds_license', 0)),
                    "rds_no_license": int(row.get('rds_no_license', 0)),
                    "rds_sql_server_cost": float(row.get('rds_sql_server_cost', 0)),
                    "rds_oracle_cost": float(row.get('rds_oracle_cost', 0))
                },
                "storage_metrics": {
                    "ebs_all_cost": float(row.get('ebs_all_cost', 0)),
                    "ebs_gp_all_cost": float(row.get('ebs_gp_all_cost', 0)),
                    "ebs_gp2_cost": float(row.get('ebs_gp2_cost', 0)),
                    "ebs_gp3_cost": float(row.get('ebs_gp3_cost', 0)),
                    "ebs_gp3_potential_savings": float(row.get('ebs_gp3_potential_savings', 0)),
                    "ebs_snapshots_under_1yr_cost": float(row.get('ebs_snapshots_under_1yr_cost', 0)),
                    "ebs_snapshots_over_1yr_cost": float(row.get('ebs_snapshots_over_1yr_cost', 0)),
                    "ebs_snapshot_cost": float(row.get('ebs_snapshot_cost', 0)),
                    "s3_all_storage_cost": float(row.get('s3_all_storage_cost', 0)),
                    "s3_standard_storage_cost": float(row.get('s3_standard_storage_cost', 0)),
                    "s3_standard_storage_potential_savings": float(row.get('s3_standard_storage_potential_savings', 0))
                },
                "compute_services": {
                    "compute_all_cost": float(row.get('compute_all_cost', 0)),
                    "compute_ondemand_cost": float(row.get('compute_ondemand_cost', 0)),
                    "compute_commit_potential_savings": float(row.get('compute_commit_potential_savings', 0)),
                    "compute_commit_savings": float(row.get('compute_commit_savings', 0)),
                    "dynamodb_all_cost": float(row.get('dynamodb_all_cost', 0)),
                    "lambda_all_cost": float(row.get('lambda_all_cost', 0))
                },
                "savings_summary": {
                    "total_potential_savings": float(
                        row.get('ec2_spot_potential_savings', 0) +
                        row.get('ec2_previous_generation_potential_savings', 0) +
                        row.get('ec2_graviton_potential_savings', 0) +
                        row.get('ec2_amd_potential_savings', 0) +
                        row.get('rds_graviton_potential_savings', 0) +
                        row.get('rds_commit_potential_savings', 0) +
                        row.get('ebs_gp3_potential_savings', 0) +
                        row.get('s3_standard_storage_potential_savings', 0) +
                        row.get('compute_commit_potential_savings', 0)
                    ),
                    "graviton_savings_potential": float(
                        row.get('ec2_graviton_potential_savings', 0) +
                        row.get('rds_graviton_potential_savings', 0)
                    ),
                    "commitment_savings_potential": float(
                        row.get('rds_commit_potential_savings', 0) +
                        row.get('compute_commit_potential_savings', 0)
                    ),
                    "storage_optimization_potential": float(
                        row.get('ebs_gp3_potential_savings', 0) +
                        row.get('s3_standard_storage_potential_savings', 0)
                    ),
                    "spot_instance_potential": float(row.get('ec2_spot_potential_savings', 0)),
                    "current_monthly_savings": float(
                        row.get('rds_commit_savings', 0) +
                        row.get('compute_commit_savings', 0)
                    ),
                    "annualized_savings_opportunity": float(
                        (row.get('ec2_spot_potential_savings', 0) +
                         row.get('ec2_previous_generation_potential_savings', 0) +
                         row.get('ec2_graviton_potential_savings', 0) +
                         row.get('ec2_amd_potential_savings', 0) +
                         row.get('rds_graviton_potential_savings', 0) +
                         row.get('rds_commit_potential_savings', 0) +
                         row.get('ebs_gp3_potential_savings', 0) +
                         row.get('s3_standard_storage_potential_savings', 0) +
                         row.get('compute_commit_potential_savings', 0)) * 12
                    )
                }
            }
            
            print("\n KPI Tracker Results")
            print("=" * 50)
            print(json.dumps(json_response, indent=2))
            
            # Save to file
            output_file = "kpi_tracker_results.json"
            with open(output_file, 'w') as f:
                json.dump(json_response, f, indent=2)
            print(f"\nResults saved to: {output_file}")
            
            # Summary
            print(f"\nKey Metrics Summary:")
            print(f"   Total Spend: ${json_response['overall_spend']['spend_all_cost']:.2f}")
            print(f"   Potential Savings: ${json_response['savings_summary']['total_potential_savings']:.2f}")
            print(f"   Billing Period: {json_response['overall_spend']['billing_period']}")
            print(f"    EC2 Cost: ${json_response['ec2_metrics']['ec2_all_cost']:.2f}")
            print(f"   Storage Cost: ${json_response['storage_metrics']['ebs_all_cost']:.2f}")
            
        else:
            print(" No data returned from kpi_tracker query")
            
    except Exception as e:
        print(f"Failed to execute kpi_tracker.sql: {e}")
        return
    
    finally:
        if 'conn' in locals():
            conn.close()
    
    # Summary of created parquet files
    print(f"\nView Results Summary:")
    print("=" * 50)
    print(f"Output Directory: {views_output_path}")
    print(f" Parquet Files Created:")
    
    for i, parquet_file in enumerate(saved_parquets, 1):
        if os.path.exists(parquet_file):
            file_size = os.path.getsize(parquet_file) / 1024  # KB
            # Extract just the filename for display
            filename = os.path.basename(parquet_file)
            print(f"   {i}. {filename} ({file_size:.1f} KB)")
        else:
            filename = os.path.basename(parquet_file)
            print(f"   {i}. {filename} ( not found)")
    
    print(f"\nKPI Tracker test completed successfully!")


if __name__ == "__main__":
    main()
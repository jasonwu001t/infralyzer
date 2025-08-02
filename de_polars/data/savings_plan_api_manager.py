"""
AWS SavingsPlans API Data Manager - Handle SavingsPlans API data for cost analysis
"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import polars as pl
from pathlib import Path

from ..engine.data_config import DataConfig
from ..auth import get_boto3_client


class SavingsPlansApiManager:
    """Manages AWS SavingsPlans API data collection and conversion to table format."""
    
    def __init__(self, config: DataConfig):
        """Initialize savings plans API manager with configuration."""
        self.config = config
        
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        return get_boto3_client(service_name, **creds)
    
    def get_savings_plans_data(self, 
                              savings_plan_ids: Optional[List[str]] = None,
                              states: Optional[List[str]] = None,
                              plan_types: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get Savings Plans data from AWS SavingsPlans API.
        
        Args:
            savings_plan_ids: List of specific Savings Plan IDs to retrieve
            states: List of plan states ('payment-pending', 'payment-failed', 'active', 'retired')
            plan_types: List of plan types ('Compute', 'EC2Instance')
        
        Returns:
            Polars DataFrame with Savings Plans data
        """
        savings_plans_client = self._get_boto3_client('savingsplans')
        
        print("🔍 Fetching Savings Plans data...")
        
        # Build request parameters
        request_params = {}
        
        if savings_plan_ids:
            request_params['savingsPlanIds'] = savings_plan_ids
            
        if states:
            request_params['states'] = states
            
        # Add filters for plan types if specified
        filters = []
        if plan_types:
            filters.append({
                'name': 'savings-plan-type',
                'values': plan_types
            })
            
        if filters:
            request_params['filters'] = filters
        
        all_savings_plans = []
        
        try:
            # Get results (describe_savings_plans supports pagination manually)
            response = savings_plans_client.describe_savings_plans(**request_params)
            savings_plans = response.get('savingsPlans', [])
            
            for plan in savings_plans:
                # Convert plan data to standardized format
                plan_record = {
                    'savings_plan_id': plan.get('savingsPlanId', ''),
                    'savings_plan_arn': plan.get('savingsPlanArn', ''),
                    'offering_id': plan.get('offeringId', ''),
                    'description': plan.get('description', ''),
                    'start_date': plan.get('start', ''),
                    'end_date': plan.get('end', ''),
                    'state': plan.get('state', ''),
                    'region': plan.get('region', ''),
                    'ec2_instance_family': plan.get('ec2InstanceFamily', ''),
                    'savings_plan_type': plan.get('savingsPlanType', ''),
                    'payment_option': plan.get('paymentOption', ''),
                    'product_types': ','.join(plan.get('productTypes', [])),
                    'currency': plan.get('currency', ''),
                    'commitment_amount_hourly': float(plan.get('commitment', '0')),
                    'upfront_payment_amount': float(plan.get('upfrontPaymentAmount', '0')),
                    'recurring_payment_amount': float(plan.get('recurringPaymentAmount', '0')),
                    'term_duration_seconds': plan.get('termDurationInSeconds', 0),
                    'term_duration_years': round(plan.get('termDurationInSeconds', 0) / (365.25 * 24 * 3600), 1),
                    'tags': json.dumps(plan.get('tags', {})),
                    'updated_at': datetime.now().isoformat()
                }
                
                all_savings_plans.append(plan_record)
                    
        except Exception as e:
            print(f"❌ Error fetching Savings Plans data: {e}")
            return pl.DataFrame()
        
        if not all_savings_plans:
            print("⚠️  No Savings Plans found")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_savings_plans)
        print(f"✅ Retrieved {len(df)} Savings Plans")
        
        return df
    
    def get_savings_plan_rates_data(self, 
                                   savings_plan_id: str,
                                   service_codes: Optional[List[str]] = None,
                                   usage_types: Optional[List[str]] = None,
                                   operations: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get Savings Plan rates data for a specific plan.
        
        Args:
            savings_plan_id: The ID of the Savings Plan
            service_codes: List of service codes to filter (e.g., ['AmazonEC2', 'AmazonECS'])
            usage_types: List of usage types to filter
            operations: List of operations to filter
        
        Returns:
            Polars DataFrame with Savings Plan rates data
        """
        savings_plans_client = self._get_boto3_client('savingsplans')
        
        print(f"🔍 Fetching Savings Plan rates for {savings_plan_id}...")
        
        # Build filters
        filters = []
        
        if service_codes:
            filters.append({
                'name': 'serviceCode',
                'values': service_codes
            })
            
        if usage_types:
            filters.append({
                'name': 'usageType',
                'values': usage_types
            })
            
        if operations:
            filters.append({
                'name': 'operation',
                'values': operations
            })
        
        request_params = {
            'savingsPlanId': savings_plan_id
        }
        
        if filters:
            request_params['filters'] = filters
        
        all_rates = []
        
        try:
            paginator = savings_plans_client.get_paginator('describe_savings_plan_rates')
            
            for page in paginator.paginate(**request_params):
                rates = page.get('searchResults', [])
                
                for rate in rates:
                    # Extract properties as separate columns
                    properties = {prop['name']: prop['value'] for prop in rate.get('properties', [])}
                    
                    rate_record = {
                        'savings_plan_id': savings_plan_id,
                        'rate': float(rate.get('rate', '0')),
                        'currency': rate.get('currency', ''),
                        'unit': rate.get('unit', ''),
                        'product_type': rate.get('productType', ''),
                        'service_code': rate.get('serviceCode', ''),
                        'usage_type': rate.get('usageType', ''),
                        'operation': rate.get('operation', ''),
                        # Property fields
                        'region': properties.get('region', ''),
                        'instance_type': properties.get('instanceType', ''),
                        'instance_family': properties.get('instanceFamily', ''),
                        'product_description': properties.get('productDescription', ''),
                        'tenancy': properties.get('tenancy', ''),
                        # Additional fields for joining
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    all_rates.append(rate_record)
                    
        except Exception as e:
            print(f"❌ Error fetching Savings Plan rates for {savings_plan_id}: {e}")
            return pl.DataFrame()
        
        if not all_rates:
            print(f"⚠️  No rates found for Savings Plan {savings_plan_id}")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_rates)
        print(f"✅ Retrieved {len(df)} rate records for {savings_plan_id}")
        
        return df
    
    def get_all_savings_plan_rates_data(self, 
                                       plan_types: Optional[List[str]] = None,
                                       service_codes: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get rates data for all active Savings Plans.
        
        Args:
            plan_types: List of plan types to include ('Compute', 'EC2Instance')
            service_codes: List of service codes to filter rates
        
        Returns:
            Polars DataFrame with combined rates data from all plans
        """
        # First get all active Savings Plans
        active_plans_df = self.get_savings_plans_data(
            states=['active'],
            plan_types=plan_types
        )
        
        if active_plans_df.is_empty():
            print("⚠️  No active Savings Plans found")
            return pl.DataFrame()
        
        print(f"📊 Getting rates for {len(active_plans_df)} active Savings Plans...")
        
        all_rates_data = []
        
        for plan_id in active_plans_df['savings_plan_id'].to_list():
            rates_df = self.get_savings_plan_rates_data(
                savings_plan_id=plan_id,
                service_codes=service_codes
            )
            
            if not rates_df.is_empty():
                all_rates_data.append(rates_df)
        
        if not all_rates_data:
            print("⚠️  No rates data found for any Savings Plans")
            return pl.DataFrame()
        
        # Combine all rates data
        combined_df = pl.concat(all_rates_data)
        
        print(f"✅ Combined rates data: {len(combined_df)} total rate records")
        
        return combined_df
    
    def get_savings_plan_offering_rates_data(self, 
                                            service_codes: Optional[List[str]] = None,
                                            usage_types: Optional[List[str]] = None,
                                            operations: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get Savings Plan offering rates data - available rates for purchase.
        
        Args:
            service_codes: List of service codes to filter rates (e.g., ['AmazonEC2'])
            usage_types: List of usage types to filter rates
            operations: List of operations to filter rates
        
        Returns:
            Polars DataFrame with offering rates data
        """
        print("🔍 Fetching Savings Plan offering rates...")
        
        savings_plans_client = self._get_boto3_client('savingsplans')
        
        request_params = {}
        
        if service_codes:
            request_params['serviceCodes'] = service_codes
        if usage_types:
            request_params['usageTypes'] = usage_types
        if operations:
            request_params['operations'] = operations
        
        all_rates = []
        
        try:
            # Call the API directly (does not support pagination)
            response = savings_plans_client.describe_savings_plans_offering_rates(**request_params)
            rates = response.get('searchResults', [])
            
            for rate in rates:
                # Parse usage type to extract instance information
                usage_type = rate.get('usageType', '')
                region = ''
                instance_type = ''
                
                # Parse usage type like "BoxUsage:c5d.2xlarge" or "APN1-DedicatedUsage:c6i.large"
                if ':' in usage_type:
                    parts = usage_type.split(':')
                    if len(parts) >= 2:
                        instance_type = parts[1]
                    
                    # Extract region from prefix (like APN1, NYC1)
                    prefix = parts[0]
                    if '-' in prefix:
                        region_code = prefix.split('-')[0]
                        # Map common region codes
                        region_map = {
                            'APN1': 'ap-northeast-1',
                            'USE1': 'us-east-1', 
                            'USW2': 'us-west-2',
                            'EUW1': 'eu-west-1',
                            'NYC1': 'us-east-1',  # NYC typically maps to us-east-1
                        }
                        region = region_map.get(region_code, 'us-east-1')  # Default to us-east-1
                    else:
                        region = 'us-east-1'  # Default region
                
                rate_record = {
                    'rate': float(rate.get('rate', '0')),
                    'currency': rate.get('currency', 'USD'),
                    'unit': rate.get('unit', 'Hrs'),
                    'product_type': rate.get('productType', ''),
                    'service_code': rate.get('serviceCode', ''),
                    'usage_type': usage_type,
                    'operation': rate.get('operation', ''),
                    'region': region,
                    'instance_type': instance_type,
                    'savings_plan_offering_type': rate.get('savingsPlanOfferingType', ''),
                    'updated_at': datetime.now().isoformat()
                }
                
                all_rates.append(rate_record)
                    
        except Exception as e:
            print(f"❌ Error fetching Savings Plan offering rates: {e}")
            return pl.DataFrame()
        
        if not all_rates:
            print("⚠️  No offering rates found")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_rates)
        print(f"✅ Retrieved {len(df)} offering rate records")
        
        return df
    
    def get_savings_plan_offerings_data(self, 
                                       plan_types: Optional[List[str]] = None,
                                       payment_options: Optional[List[str]] = None,
                                       durations: Optional[List[int]] = None) -> pl.DataFrame:
        """
        Get Savings Plan offerings data.
        
        Args:
            plan_types: List of plan types ('Compute', 'EC2Instance')
            payment_options: List of payment options ('All Upfront', 'Partial Upfront', 'No Upfront')
            durations: List of durations in seconds (e.g., [31536000] for 1 year)
        
        Returns:
            Polars DataFrame with Savings Plan offerings data
        """
        savings_plans_client = self._get_boto3_client('savingsplans')
        
        print("🔍 Fetching Savings Plan offerings...")
        
        request_params = {}
        
        if plan_types:
            request_params['planTypes'] = plan_types
            
        if payment_options:
            request_params['paymentOptions'] = payment_options
            
        if durations:
            request_params['durations'] = durations
        
        all_offerings = []
        
        try:
            paginator = savings_plans_client.get_paginator('describe_savings_plans_offerings')
            
            for page in paginator.paginate(**request_params):
                offerings = page.get('searchResults', [])
                
                for offering in offerings:
                    # Extract properties
                    properties = {prop['name']: prop['value'] for prop in offering.get('properties', [])}
                    
                    offering_record = {
                        'offering_id': offering.get('offeringId', ''),
                        'product_types': ','.join(offering.get('productTypes', [])),
                        'plan_type': offering.get('planType', ''),
                        'description': offering.get('description', ''),
                        'payment_option': offering.get('paymentOption', ''),
                        'duration_seconds': offering.get('durationSeconds', 0),
                        'duration_years': round(offering.get('durationSeconds', 0) / (365.25 * 24 * 3600), 1),
                        'currency': offering.get('currency', ''),
                        'service_code': offering.get('serviceCode', ''),
                        'usage_type': offering.get('usageType', ''),
                        'operation': offering.get('operation', ''),
                        # Property fields
                        'region': properties.get('region', ''),
                        'instance_family': properties.get('instanceFamily', ''),
                        'updated_at': datetime.now().isoformat()
                    }
                    
                    all_offerings.append(offering_record)
                    
        except Exception as e:
            print(f"❌ Error fetching Savings Plan offerings: {e}")
            return pl.DataFrame()
        
        if not all_offerings:
            print("⚠️  No Savings Plan offerings found")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_offerings)
        print(f"✅ Retrieved {len(df)} Savings Plan offerings")
        
        return df
    
    def save_savings_plans_data_to_cache(self, df: pl.DataFrame, cache_name: str) -> str:
        """Save Savings Plans data to local cache for faster access."""
        if not self.config.local_data_path:
            print("⚠️  No local cache path configured")
            return ""
            
        cache_dir = Path(self.config.local_data_path) / "savings_plans_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        cache_file = cache_dir / f"{cache_name}_{datetime.now().strftime('%Y%m%d')}.parquet"
        
        df.write_parquet(cache_file)
        print(f"💾 Saved Savings Plans data to cache: {cache_file}")
        
        return str(cache_file)
    
    def load_savings_plans_data_from_cache(self, cache_name: str, max_age_days: int = 1) -> Optional[pl.DataFrame]:
        """Load Savings Plans data from local cache if available and recent."""
        if not self.config.local_data_path:
            return None
            
        cache_dir = Path(self.config.local_data_path) / "savings_plans_cache"
        if not cache_dir.exists():
            return None
            
        # Look for recent cache files
        cache_pattern = f"{cache_name}_*.parquet"
        cache_files = list(cache_dir.glob(cache_pattern))
        
        if not cache_files:
            return None
            
        # Get the most recent file
        latest_cache = max(cache_files, key=lambda f: f.stat().st_mtime)
        
        # Check if cache is recent enough
        file_age = (datetime.now().timestamp() - latest_cache.stat().st_mtime) / 86400  # days
        
        if file_age > max_age_days:
            print(f"📅 Cache file {latest_cache.name} is {file_age:.1f} days old, refreshing...")
            return None
            
        try:
            df = pl.read_parquet(latest_cache)
            print(f"🚀 Loaded Savings Plans data from cache: {latest_cache.name}")
            return df
        except Exception as e:
            print(f"❌ Error loading cache file: {e}")
            return None
    
    def create_cur_joinable_format(self, 
                                  savings_plans_df: pl.DataFrame,
                                  rates_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
        """
        Create a format that's optimized for joining with CUR2.0 data.
        
        Args:
            savings_plans_df: DataFrame with Savings Plans information
            rates_df: Optional DataFrame with rates data
        
        Returns:
            Polars DataFrame formatted for CUR2.0 joins
        """
        print("🔗 Creating CUR2.0-joinable format...")
        
        # Start with basic Savings Plans data
        joinable_df = savings_plans_df.select([
            'savings_plan_id',
            'savings_plan_arn', 
            'savings_plan_type',
            'payment_option',
            'commitment_amount_hourly',
            'term_duration_years',
            'state',
            'region',
            'ec2_instance_family'
        ])
        
        # If rates data is provided, join it
        if rates_df is not None and not rates_df.is_empty():
            # Create join keys that match CUR2.0 patterns
            rates_for_join = rates_df.with_columns([
                # Create normalized columns for joining
                pl.col('region').alias('savings_plan_region'),
                pl.col('instance_type').alias('savings_plan_instance_type'),
                pl.col('product_type').alias('savings_plan_product_type'),
                pl.col('usage_type').alias('savings_plan_usage_type'),
                pl.col('operation').alias('savings_plan_operation'),
                pl.col('rate').alias('savings_plan_rate')
            ]).select([
                'savings_plan_id',
                'savings_plan_region',
                'savings_plan_instance_type', 
                'savings_plan_product_type',
                'savings_plan_usage_type',
                'savings_plan_operation',
                'savings_plan_rate',
                'service_code'
            ])
            
            # Join with main Savings Plans data
            joinable_df = joinable_df.join(
                rates_for_join,
                on='savings_plan_id',
                how='left'
            )
        
        print(f"✅ Created joinable format: {len(joinable_df)} rows")
        
        return joinable_df
    
    def get_simple_savings_plan_rate(self, instance_type: str, region: str, 
                                   savings_plan_id: Optional[str] = None) -> Optional[float]:
        """
        Simple function to get savings plan offering rate for specific instance attributes.
        Gets available rates for purchase (not from already purchased plans).
        
        Args:
            instance_type: EC2 instance type (e.g., 't3.micro')
            region: AWS region code (e.g., 'us-east-1')
            savings_plan_id: Deprecated - not used for offering rates
        
        Returns:
            Hourly rate in USD from available offerings, or None if not found
        """
        try:
            # Try to get offering rates from cache first
            cache_key = "savings_plans_offering_rates"
            rates_df = self.load_savings_plans_data_from_cache(cache_key, max_age_days=1)
            
            if rates_df is None:
                # Fetch available offering rates (not purchased plans)
                rates_df = self.get_savings_plan_offering_rates_data(
                    service_codes=['AmazonEC2']
                )
                
                if not rates_df.is_empty():
                    self.save_savings_plans_data_to_cache(rates_df, cache_key)
            
            if rates_df is None or rates_df.is_empty():
                return None
            
            # Filter for exact match
            filters = [
                (pl.col("instance_type") == instance_type),
                (pl.col("region") == region)
            ]
            
            # Note: savings_plan_id is not applicable for offering rates
            # These are available rates for purchase, not from specific purchased plans
            
            filtered = rates_df.filter(pl.all_horizontal(filters))
            
            if filtered.is_empty():
                return None
            
            # If multiple rates, return the lowest (best for customer)
            min_rate = filtered.select("rate").min().to_series()[0]
            return float(min_rate)
            
        except Exception as e:
            print(f"❌ Error getting savings plan rate: {e}")
            return None
    
    def compare_savings_vs_ondemand(self, region: str, instance_type: str, 
                                   on_demand_price: float) -> Dict[str, Any]:
        """
        Compare savings plan rate vs on-demand price for an instance.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            on_demand_price: Current on-demand hourly price
        
        Returns:
            Dictionary with comparison data
        """
        sp_rate = self.get_simple_savings_plan_rate(instance_type, region)
        
        result = {
            "region": region,
            "instance_type": instance_type,
            "on_demand_hourly": on_demand_price,
            "on_demand_monthly": on_demand_price * 24 * 30,
            "savings_plan_hourly": sp_rate,
            "savings_plan_monthly": sp_rate * 24 * 30 if sp_rate else None,
            "has_savings_plan": sp_rate is not None
        }
        
        if sp_rate:
            result.update({
                "hourly_savings": on_demand_price - sp_rate,
                "monthly_savings": (on_demand_price - sp_rate) * 24 * 30,
                "annual_savings": (on_demand_price - sp_rate) * 24 * 365,
                "savings_percentage": ((on_demand_price - sp_rate) / on_demand_price) * 100
            })
        
        return result
    
    def get_best_savings_plan_opportunities(self, regions: List[str], 
                                          instance_types: List[str]) -> pl.DataFrame:
        """
        Find best savings plan opportunities for given instances.
        
        Args:
            regions: List of AWS regions to check
            instance_types: List of instance types to analyze
        
        Returns:
            DataFrame with savings opportunities ranked by potential savings
        """
        opportunities = []
        
        # Get all rates data
        rates_df = self.get_all_savings_plan_rates_data(service_codes=['AmazonEC2'])
        
        if rates_df.is_empty():
            return pl.DataFrame()
        
        for region in regions:
            for instance_type in instance_types:
                # Filter rates for this combination
                filtered = rates_df.filter(
                    (pl.col("region") == region) &
                    (pl.col("instance_type") == instance_type)
                )
                
                if not filtered.is_empty():
                    # Get the best (lowest) rate
                    best_rate = filtered.select("rate").min().to_series()[0]
                    savings_plan_id = filtered.filter(pl.col("rate") == best_rate).select("savings_plan_id").to_series()[0]
                    
                    opportunities.append({
                        "region": region,
                        "instance_type": instance_type,
                        "savings_plan_id": savings_plan_id,
                        "savings_plan_rate": float(best_rate),
                        "potential_monthly_cost": float(best_rate) * 24 * 30,
                        "potential_annual_cost": float(best_rate) * 24 * 365
                    })
        
        return pl.DataFrame(opportunities) if opportunities else pl.DataFrame()
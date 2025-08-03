"""
AWS Pricing Manager - Unified handler for all AWS pricing models
Covers: On-Demand, Reserved Instances, Spot Instances, and Savings Plans
Includes instance metadata and bulk pricing operations for frontend applications
"""
import json
from typing import List, Dict, Any, Optional, Literal
from datetime import datetime, timedelta
import polars as pl
import concurrent.futures
import threading

from ..engine.data_config import DataConfig
from ..auth import get_boto3_client


class AWSPricingManager:
    """Unified AWS pricing manager for all pricing models."""
    
    def __init__(self, config: DataConfig):
        """Initialize AWS pricing manager with configuration."""
        self.config = config
        # AWS Pricing API is only available in us-east-1
        self._pricing_region = 'us-east-1'
        self._instance_metadata_cache = {}
        self._cache_lock = threading.Lock()
        
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        # Override region for pricing API
        if service_name == 'pricing':
            creds['aws_region'] = self._pricing_region
        return get_boto3_client(service_name, **creds)
    
    def _get_region_display_name(self, region_code: str) -> str:
        """Convert region code to display name used by Pricing API."""
        region_mappings = {
            'us-east-1': 'US East (N. Virginia)',
            'us-east-2': 'US East (Ohio)',
            'us-west-1': 'US West (N. California)',
            'us-west-2': 'US West (Oregon)',
            'eu-west-1': 'Europe (Ireland)',
            'eu-west-2': 'Europe (London)',
            'eu-west-3': 'Europe (Paris)',
            'eu-central-1': 'Europe (Frankfurt)',
            'eu-north-1': 'Europe (Stockholm)',
            'eu-south-1': 'Europe (Milan)',
            'ap-northeast-1': 'Asia Pacific (Tokyo)',
            'ap-northeast-2': 'Asia Pacific (Seoul)',
            'ap-northeast-3': 'Asia Pacific (Osaka)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'ap-southeast-2': 'Asia Pacific (Sydney)',
            'ap-south-1': 'Asia Pacific (Mumbai)',
            'ap-east-1': 'Asia Pacific (Hong Kong)',
            'ca-central-1': 'Canada (Central)',
            'sa-east-1': 'South America (Sao Paulo)',
            'me-south-1': 'Middle East (Bahrain)',
            'af-south-1': 'Africa (Cape Town)'
        }
        return region_mappings.get(region_code, region_code)
    
    # =============================================================================
    # INSTANCE METADATA
    # =============================================================================
    
    def get_instance_metadata(self, instance_type: str) -> Optional[Dict[str, Any]]:
        """
        Get instance metadata (vCPUs, memory, storage, network).
        Uses pricing API to get comprehensive instance details.
        
        Args:
            instance_type: EC2 instance type (e.g., 't3.micro')
        
        Returns:
            Dictionary with instance specifications or None if not found
        """
        # Check cache first
        with self._cache_lock:
            if instance_type in self._instance_metadata_cache:
                return self._instance_metadata_cache[instance_type]
        
        try:
            pricing_client = self._get_boto3_client('pricing')
            
            # Use a common region to get metadata
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': 'US East (N. Virginia)'}
            ]
            
            response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters, MaxResults=1)
            
            if response['PriceList']:
                product_data = json.loads(response['PriceList'][0])
                attributes = product_data.get('product', {}).get('attributes', {})
                
                metadata = {
                    'instance_type': instance_type,
                    'vcpu': attributes.get('vcpu', 'Unknown'),
                    'memory': attributes.get('memory', 'Unknown'),
                    'storage': attributes.get('storage', 'EBS only'),
                    'network_performance': attributes.get('networkPerformance', 'Unknown'),
                    'instance_family': attributes.get('instanceFamily', 'Unknown'),
                    'processor_features': attributes.get('processorFeatures', 'Unknown'),
                    'physical_processor': attributes.get('physicalProcessor', 'Unknown'),
                    'clock_speed': attributes.get('clockSpeed', 'Unknown'),
                    'enhanced_networking': attributes.get('enhancedNetworkingSupported', 'Unknown'),
                    'instance_sku': attributes.get('sku', 'Unknown')
                }
                
                # Cache the result
                with self._cache_lock:
                    self._instance_metadata_cache[instance_type] = metadata
                
                return metadata
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting instance metadata for {instance_type}: {e}")
            return None
    
    def get_popular_instance_types(self) -> List[str]:
        """
        Get a list of popular/common EC2 instance types.
        
        Returns:
            List of instance type strings
        """
        return [
            # General Purpose
            't3.nano', 't3.micro', 't3.small', 't3.medium', 't3.large', 't3.xlarge', 't3.2xlarge',
            't3a.nano', 't3a.micro', 't3a.small', 't3a.medium', 't3a.large', 't3a.xlarge', 't3a.2xlarge',
            'm5.large', 'm5.xlarge', 'm5.2xlarge', 'm5.4xlarge', 'm5.8xlarge', 'm5.12xlarge', 'm5.16xlarge', 'm5.24xlarge',
            'm5a.large', 'm5a.xlarge', 'm5a.2xlarge', 'm5a.4xlarge', 'm5a.8xlarge', 'm5a.12xlarge', 'm5a.16xlarge', 'm5a.24xlarge',
            'm6i.large', 'm6i.xlarge', 'm6i.2xlarge', 'm6i.4xlarge', 'm6i.8xlarge', 'm6i.12xlarge', 'm6i.16xlarge', 'm6i.24xlarge', 'm6i.32xlarge',
            
            # Compute Optimized
            'c5.large', 'c5.xlarge', 'c5.2xlarge', 'c5.4xlarge', 'c5.9xlarge', 'c5.12xlarge', 'c5.18xlarge', 'c5.24xlarge',
            'c5n.large', 'c5n.xlarge', 'c5n.2xlarge', 'c5n.4xlarge', 'c5n.9xlarge', 'c5n.18xlarge',
            'c6i.large', 'c6i.xlarge', 'c6i.2xlarge', 'c6i.4xlarge', 'c6i.8xlarge', 'c6i.12xlarge', 'c6i.16xlarge', 'c6i.24xlarge', 'c6i.32xlarge',
            
            # Memory Optimized
            'r5.large', 'r5.xlarge', 'r5.2xlarge', 'r5.4xlarge', 'r5.8xlarge', 'r5.12xlarge', 'r5.16xlarge', 'r5.24xlarge',
            'r5a.large', 'r5a.xlarge', 'r5a.2xlarge', 'r5a.4xlarge', 'r5a.8xlarge', 'r5a.12xlarge', 'r5a.16xlarge', 'r5a.24xlarge',
            'r6i.large', 'r6i.xlarge', 'r6i.2xlarge', 'r6i.4xlarge', 'r6i.8xlarge', 'r6i.12xlarge', 'r6i.16xlarge', 'r6i.24xlarge', 'r6i.32xlarge',
            
            # Storage Optimized
            'i3.large', 'i3.xlarge', 'i3.2xlarge', 'i3.4xlarge', 'i3.8xlarge', 'i3.16xlarge',
            'i4i.large', 'i4i.xlarge', 'i4i.2xlarge', 'i4i.4xlarge', 'i4i.8xlarge', 'i4i.16xlarge', 'i4i.32xlarge',
            
            # GPU Instances
            'p3.2xlarge', 'p3.8xlarge', 'p3.16xlarge',
            'g4dn.xlarge', 'g4dn.2xlarge', 'g4dn.4xlarge', 'g4dn.8xlarge', 'g4dn.12xlarge', 'g4dn.16xlarge'
        ]
    
    # =============================================================================
    # BULK OPERATIONS
    # =============================================================================
    
    def get_bulk_pricing_comparison(self, instance_types: List[str], region: str = 'us-east-1',
                                   operating_system: str = 'Linux', max_workers: int = 10) -> List[Dict[str, Any]]:
        """
        Get pricing comparison for multiple instances in parallel.
        
        Args:
            instance_types: List of EC2 instance types
            region: AWS region code
            operating_system: Operating system
            max_workers: Maximum number of concurrent workers
        
        Returns:
            List of pricing comparison dictionaries
        """
        print(f"🔍 Getting bulk pricing for {len(instance_types)} instances in {region}...")
        
        def get_instance_pricing(instance_type: str) -> Dict[str, Any]:
            try:
                # Get metadata
                metadata = self.get_instance_metadata(instance_type)
                
                # Get pricing comparison
                pricing = self.compare_all_pricing_options(region, instance_type, operating_system)
                
                # Combine metadata and pricing
                result = {
                    'instance_type': instance_type,
                    'metadata': metadata,
                    'pricing': pricing,
                    'status': 'success'
                }
                
                return result
                
            except Exception as e:
                return {
                    'instance_type': instance_type,
                    'metadata': None,
                    'pricing': None,
                    'status': 'error',
                    'error': str(e)
                }
        
        # Use ThreadPoolExecutor for parallel processing
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_instance = {
                executor.submit(get_instance_pricing, instance_type): instance_type 
                for instance_type in instance_types
            }
            
            for future in concurrent.futures.as_completed(future_to_instance):
                result = future.result()
                results.append(result)
        
        # Sort results by instance type for consistent ordering
        results.sort(key=lambda x: x['instance_type'])
        
        return results
    
    def get_pricing_matrix(self, instance_types: Optional[List[str]] = None, 
                          regions: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Generate a pricing matrix for multiple instances across regions.
        
        Args:
            instance_types: List of instance types (uses popular ones if None)
            regions: List of regions (uses common ones if None)
        
        Returns:
            DataFrame with pricing matrix
        """
        if not instance_types:
            instance_types = self.get_popular_instance_types()[:20]  # Limit for performance
        
        if not regions:
            regions = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']
        
        print(f"🔍 Building pricing matrix for {len(instance_types)} instances across {len(regions)} regions...")
        
        matrix_data = []
        
        for region in regions:
            bulk_pricing = self.get_bulk_pricing_comparison(instance_types, region)
            
            for item in bulk_pricing:
                if item['status'] == 'success' and item['pricing']:
                    pricing = item['pricing']
                    metadata = item['metadata'] or {}
                    
                    row = {
                        'region': region,
                        'instance_type': item['instance_type'],
                        'vcpu': metadata.get('vcpu', 'Unknown'),
                        'memory': metadata.get('memory', 'Unknown'),
                        'storage': metadata.get('storage', 'EBS only'),
                        'network_performance': metadata.get('network_performance', 'Unknown'),
                        'ondemand_hourly': pricing.get('ondemand', {}).get('hourly_price'),
                        'ondemand_monthly': pricing.get('ondemand', {}).get('monthly_price'),
                        'spot_hourly': pricing.get('spot', {}).get('hourly_price'),
                        'spot_savings_pct': pricing.get('spot', {}).get('savings_vs_ondemand_pct'),
                        'reserved_1yr_hourly': pricing.get('reserved_1yr', {}).get('hourly_price'),
                        'reserved_1yr_savings_pct': pricing.get('reserved_1yr', {}).get('savings_vs_ondemand_pct'),
                        'savings_plan_hourly': pricing.get('savings_plan', {}).get('hourly_price'),
                        'savings_plan_savings_pct': pricing.get('savings_plan', {}).get('savings_vs_ondemand_pct'),
                        'timestamp': datetime.now().isoformat()
                    }
                    matrix_data.append(row)
        
        return pl.DataFrame(matrix_data)
    
    # =============================================================================
    # ON-DEMAND PRICING
    # =============================================================================
    
    def get_ondemand_price(self, region: str, instance_type: str, 
                          operating_system: str = "Linux", tenancy: str = "Shared") -> Optional[float]:
        """
        Get on-demand price for specific instance.
        
        Args:
            region: AWS region code (e.g., 'us-east-1')
            instance_type: EC2 instance type (e.g., 't3.micro')
            operating_system: Operating system ('Linux', 'Windows', etc.)
            tenancy: Instance tenancy ('Shared', 'Dedicated', 'Host')
        
        Returns:
            Hourly price in USD, or None if not found
        """
        try:
            pricing_client = self._get_boto3_client('pricing')
            region_name = self._get_region_display_name(region)
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'OnDemand'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': operating_system},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': tenancy},
                {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
                {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': 'No License required'}
            ]
            
            response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters)
            
            for price_item in response['PriceList']:
                price_data = json.loads(price_item)
                terms = price_data.get('terms', {}).get('OnDemand', {})
                
                for term_data in terms.values():
                    price_dimensions = term_data.get('priceDimensions', {})
                    for dimension_data in price_dimensions.values():
                        price_per_unit = dimension_data.get('pricePerUnit', {})
                        usd_price = price_per_unit.get('USD', '0')
                        if usd_price and usd_price != '0':
                            return float(usd_price)
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting on-demand price: {e}")
            return None
    
    # =============================================================================
    # RESERVED INSTANCE PRICING
    # =============================================================================
    
    def get_reserved_instance_price(self, region: str, instance_type: str,
                                   term_length: Literal["1yr", "3yr"] = "1yr",
                                   payment_option: Literal["No Upfront", "Partial Upfront", "All Upfront"] = "No Upfront",
                                   operating_system: str = "Linux") -> Optional[Dict[str, float]]:
        """
        Get reserved instance pricing.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            term_length: '1yr' or '3yr'
            payment_option: Payment option
            operating_system: Operating system
        
        Returns:
            Dict with pricing info or None if not found
        """
        try:
            pricing_client = self._get_boto3_client('pricing')
            region_name = self._get_region_display_name(region)
            
            # Map term length
            lease_contract_length = "1yr" if term_length == "1yr" else "3yr"
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'Reserved'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': operating_system},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                {'Type': 'TERM_MATCH', 'Field': 'offeringClass', 'Value': 'standard'},
                {'Type': 'TERM_MATCH', 'Field': 'leaseContractLength', 'Value': lease_contract_length},
                {'Type': 'TERM_MATCH', 'Field': 'purchaseOption', 'Value': payment_option}
            ]
            
            response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters)
            
            for price_item in response['PriceList']:
                price_data = json.loads(price_item)
                terms = price_data.get('terms', {}).get('Reserved', {})
                
                for term_data in terms.values():
                    price_dimensions = term_data.get('priceDimensions', {})
                    
                    result = {
                        'term_length': term_length,
                        'payment_option': payment_option,
                        'upfront_cost': 0.0,
                        'hourly_cost': 0.0
                    }
                    
                    for dimension_data in price_dimensions.values():
                        price_per_unit = dimension_data.get('pricePerUnit', {})
                        usd_price = float(price_per_unit.get('USD', '0'))
                        unit = dimension_data.get('unit', '')
                        
                        if 'Quantity' in unit:  # Upfront cost
                            result['upfront_cost'] = usd_price
                        elif 'Hrs' in unit:  # Hourly cost
                            result['hourly_cost'] = usd_price
                    
                    return result
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting reserved instance price: {e}")
            return None
    
    # =============================================================================
    # SPOT PRICING
    # =============================================================================
    
    def get_current_spot_price(self, region: str, instance_type: str,
                              availability_zone: Optional[str] = None) -> Optional[float]:
        """
        Get current spot price for instance.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            availability_zone: Specific AZ (optional)
        
        Returns:
            Current spot price in USD per hour, or None if not found
        """
        try:
            # Use EC2 client in the target region (not pricing region)
            creds = self.config.get_aws_credentials()
            creds['aws_region'] = region
            ec2_client = get_boto3_client('ec2', **creds)
            
            # Build request parameters
            params = {
                'InstanceTypes': [instance_type],
                'ProductDescriptions': ['Linux/UNIX'],  # Most common
                'MaxResults': 10
            }
            
            if availability_zone:
                params['AvailabilityZone'] = availability_zone
            
            response = ec2_client.describe_spot_price_history(**params)
            
            # Get the most recent price
            if response['SpotPriceHistory']:
                latest_price = response['SpotPriceHistory'][0]  # Most recent first
                return float(latest_price['SpotPrice'])
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting spot price: {e}")
            return None
    
    def get_spot_price_history(self, region: str, instance_type: str,
                              days_back: int = 7) -> pl.DataFrame:
        """
        Get spot price history for analysis.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            days_back: Number of days of history to fetch
        
        Returns:
            DataFrame with spot price history
        """
        try:
            creds = self.config.get_aws_credentials()
            creds['aws_region'] = region
            ec2_client = get_boto3_client('ec2', **creds)
            
            start_time = datetime.utcnow() - timedelta(days=days_back)
            
            response = ec2_client.describe_spot_price_history(
                InstanceTypes=[instance_type],
                ProductDescriptions=['Linux/UNIX'],
                StartTime=start_time,
                MaxResults=1000
            )
            
            history_data = []
            for entry in response['SpotPriceHistory']:
                history_data.append({
                    'timestamp': entry['Timestamp'].isoformat(),
                    'availability_zone': entry['AvailabilityZone'],
                    'instance_type': entry['InstanceType'],
                    'product_description': entry['ProductDescription'],
                    'spot_price': float(entry['SpotPrice'])
                })
            
            return pl.DataFrame(history_data)
            
        except Exception as e:
            print(f"❌ Error getting spot price history: {e}")
            return pl.DataFrame()
    
    # =============================================================================
    # SAVINGS PLANS
    # =============================================================================
    
    def get_savings_plan_rate(self, instance_type: str, region: str) -> Optional[float]:
        """
        Get savings plan offering rate for specific instance.
        Gets available rates for purchase.
        
        Args:
            instance_type: EC2 instance type (e.g., 't3.micro')
            region: AWS region code (e.g., 'us-east-1')
        
        Returns:
            Hourly rate in USD from available offerings, or None if not found
        """
        try:
            savings_plans_client = self._get_boto3_client('savingsplans')
            
            response = savings_plans_client.describe_savings_plans_offering_rates(
                serviceCodes=['AmazonEC2']
            )
            
            rates = response.get('searchResults', [])
            
            for rate in rates:
                usage_type = rate.get('usageType', '')
                parsed_region = ''
                parsed_instance_type = ''
                
                # Parse usage type like "BoxUsage:c5d.2xlarge" or "APN1-DedicatedUsage:c6i.large"
                if ':' in usage_type:
                    parts = usage_type.split(':')
                    if len(parts) >= 2:
                        parsed_instance_type = parts[1]
                    
                    # Extract region from prefix
                    prefix = parts[0]
                    if '-' in prefix:
                        region_code = prefix.split('-')[0]
                        region_map = {
                            'APN1': 'ap-northeast-1',
                            'USE1': 'us-east-1', 
                            'USW2': 'us-west-2',
                            'EUW1': 'eu-west-1',
                            'NYC1': 'us-east-1',
                        }
                        parsed_region = region_map.get(region_code, 'us-east-1')
                    else:
                        parsed_region = 'us-east-1'
                
                # Check for match
                if parsed_instance_type == instance_type and parsed_region == region:
                    return float(rate.get('rate', '0'))
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting savings plan rate: {e}")
            return None
    
    # =============================================================================
    # COMPARISON FUNCTIONS
    # =============================================================================
    
    def compare_all_pricing_options(self, region: str, instance_type: str,
                                   operating_system: str = "Linux") -> Dict[str, Any]:
        """
        Compare all pricing options for an instance.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            operating_system: Operating system
        
        Returns:
            Dictionary with all pricing options
        """
        print(f"🔍 Comparing all pricing options for {instance_type} in {region}...")
        
        result = {
            'region': region,
            'instance_type': instance_type,
            'operating_system': operating_system,
            'timestamp': datetime.now().isoformat()
        }
        
        # On-Demand
        ondemand_price = self.get_ondemand_price(region, instance_type, operating_system)
        result['ondemand'] = {
            'hourly_price': ondemand_price,
            'monthly_price': ondemand_price * 24 * 30 if ondemand_price else None,
            'annual_price': ondemand_price * 24 * 365 if ondemand_price else None
        }
        
        # Spot
        spot_price = self.get_current_spot_price(region, instance_type)
        if spot_price and ondemand_price:
            spot_savings = ((ondemand_price - spot_price) / ondemand_price) * 100
        else:
            spot_savings = None
            
        result['spot'] = {
            'hourly_price': spot_price,
            'monthly_price': spot_price * 24 * 30 if spot_price else None,
            'annual_price': spot_price * 24 * 365 if spot_price else None,
            'savings_vs_ondemand_pct': spot_savings
        }
        
        # Reserved Instance (1-year, No Upfront)
        ri_1yr = self.get_reserved_instance_price(region, instance_type, "1yr", "No Upfront", operating_system)
        if ri_1yr and ondemand_price:
            ri_1yr_savings = ((ondemand_price - ri_1yr['hourly_cost']) / ondemand_price) * 100
        else:
            ri_1yr_savings = None
            
        result['reserved_1yr'] = {
            'hourly_price': ri_1yr['hourly_cost'] if ri_1yr else None,
            'upfront_cost': ri_1yr['upfront_cost'] if ri_1yr else None,
            'monthly_price': ri_1yr['hourly_cost'] * 24 * 30 if ri_1yr else None,
            'annual_price': ri_1yr['hourly_cost'] * 24 * 365 if ri_1yr else None,
            'savings_vs_ondemand_pct': ri_1yr_savings
        }
        
        # Savings Plans
        sp_rate = self.get_savings_plan_rate(instance_type, region)
        if sp_rate and ondemand_price:
            sp_savings = ((ondemand_price - sp_rate) / ondemand_price) * 100
        else:
            sp_savings = None
            
        result['savings_plan'] = {
            'hourly_price': sp_rate,
            'monthly_price': sp_rate * 24 * 30 if sp_rate else None,
            'annual_price': sp_rate * 24 * 365 if sp_rate else None,
            'savings_vs_ondemand_pct': sp_savings
        }
        
        return result
    
    def get_cheapest_option(self, region: str, instance_type: str,
                           operating_system: str = "Linux") -> Dict[str, Any]:
        """
        Find the cheapest pricing option for an instance.
        
        Args:
            region: AWS region code
            instance_type: EC2 instance type
            operating_system: Operating system
        
        Returns:
            Dictionary with cheapest option details
        """
        comparison = self.compare_all_pricing_options(region, instance_type, operating_system)
        
        options = []
        for option_name, option_data in comparison.items():
            if option_name in ['ondemand', 'spot', 'reserved_1yr', 'savings_plan']:
                hourly_price = option_data.get('hourly_price')
                if hourly_price is not None:
                    options.append({
                        'option': option_name,
                        'hourly_price': hourly_price,
                        'monthly_price': option_data.get('monthly_price'),
                        'annual_price': option_data.get('annual_price'),
                        'savings_pct': option_data.get('savings_vs_ondemand_pct', 0)
                    })
        
        if not options:
            return {'error': 'No pricing options available'}
        
        # Find cheapest by hourly price
        cheapest = min(options, key=lambda x: x['hourly_price'])
        
        return {
            'region': region,
            'instance_type': instance_type,
            'cheapest_option': cheapest['option'],
            'hourly_price': cheapest['hourly_price'],
            'monthly_price': cheapest['monthly_price'],
            'annual_price': cheapest['annual_price'],
            'savings_vs_ondemand_pct': cheapest['savings_pct'],
            'all_options': options
        }
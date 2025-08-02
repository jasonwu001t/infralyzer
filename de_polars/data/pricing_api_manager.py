"""
AWS Pricing API Data Manager - Handle AWS Pricing API data for cost analysis
"""
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import polars as pl
from pathlib import Path

from ..engine.data_config import DataConfig
from ..auth import get_boto3_client


class PricingApiManager:
    """Manages AWS Pricing API data collection and conversion to table format."""
    
    def __init__(self, config: DataConfig):
        """Initialize pricing API manager with configuration."""
        self.config = config
        # AWS Pricing API is only available in us-east-1
        self._pricing_region = 'us-east-1'
        
    def _get_boto3_client(self, service_name: str):
        """Get boto3 client using the configuration credentials"""
        creds = self.config.get_aws_credentials()
        # Override region for pricing API
        if service_name == 'pricing':
            creds['aws_region'] = self._pricing_region
        return get_boto3_client(service_name, **creds)
    
    def get_ec2_pricing_data(self, 
                            regions: Optional[List[str]] = None,
                            instance_types: Optional[List[str]] = None,
                            operating_systems: Optional[List[str]] = None,
                            tenancy: str = 'Shared',
                            term_type: str = 'OnDemand') -> pl.DataFrame:
        """
        Get EC2 pricing data from AWS Pricing API.
        
        Args:
            regions: List of region codes (e.g., ['us-east-1', 'eu-west-1'])
            instance_types: List of instance types (e.g., ['t3.micro', 'm5.large'])
            operating_systems: List of OS (e.g., ['Linux', 'Windows'])
            tenancy: Instance tenancy ('Shared', 'Dedicated', 'Host')
            term_type: Pricing term ('OnDemand', 'Reserved')
        
        Returns:
            Polars DataFrame with pricing data
        """
        pricing_client = self._get_boto3_client('pricing')
        
        # Default values if not provided
        if regions is None:
            regions = [self.config.aws_region] if self.config.aws_region else ['us-east-1']
        if operating_systems is None:
            operating_systems = ['Linux']
            
        print(f"🔍 Fetching EC2 pricing data for {len(regions)} region(s)...")
        
        all_pricing_data = []
        
        for region in regions:
            region_name = self._get_region_display_name(region)
            print(f"📍 Processing region: {region} ({region_name})")
            
            # Build filters for pricing API
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': term_type},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': tenancy},
                {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
                {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': 'No License required'}
            ]
            
            # Add instance type filter if specified
            if instance_types:
                for instance_type in instance_types:
                    for os in operating_systems:
                        specific_filters = filters + [
                            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                            {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': os}
                        ]
                        
                        pricing_data = self._fetch_pricing_data(pricing_client, specific_filters, 
                                                               region, instance_type, os)
                        all_pricing_data.extend(pricing_data)
            else:
                # Get all instance types for specified OS
                for os in operating_systems:
                    os_filters = filters + [
                        {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': os}
                    ]
                    
                    pricing_data = self._fetch_pricing_data(pricing_client, os_filters, 
                                                           region, None, os)
                    all_pricing_data.extend(pricing_data)
        
        # Convert to Polars DataFrame
        if not all_pricing_data:
            print("⚠️  No pricing data found")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_pricing_data)
        print(f"✅ Retrieved {len(df)} pricing records")
        
        return df
    
    def _fetch_pricing_data(self, pricing_client, filters: List[Dict], 
                           region_code: str, instance_type: Optional[str], 
                           operating_system: str) -> List[Dict]:
        """Fetch pricing data from AWS API and parse response."""
        pricing_records = []
        
        try:
            # Use pagination to get all results
            paginator = pricing_client.get_paginator('get_products')
            
            for page in paginator.paginate(ServiceCode='AmazonEC2', Filters=filters):
                for price_item in page['PriceList']:
                    price_data = json.loads(price_item)
                    
                    # Extract product attributes
                    product = price_data.get('product', {})
                    attributes = product.get('attributes', {})
                    
                    # Extract pricing terms
                    terms = price_data.get('terms', {})
                    on_demand_terms = terms.get('OnDemand', {})
                    
                    for term_key, term_data in on_demand_terms.items():
                        price_dimensions = term_data.get('priceDimensions', {})
                        
                        for dimension_key, dimension_data in price_dimensions.items():
                            price_per_unit = dimension_data.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD', '0')
                            
                            # Create standardized pricing record
                            record = {
                                'region_code': region_code,
                                'region_name': attributes.get('location', ''),
                                'instance_type': attributes.get('instanceType', ''),
                                'instance_family': attributes.get('instanceFamily', ''),
                                'operating_system': attributes.get('operatingSystem', ''),
                                'tenancy': attributes.get('tenancy', ''),
                                'vcpu': attributes.get('vcpu', ''),
                                'memory': attributes.get('memory', ''),
                                'storage': attributes.get('storage', ''),
                                'network_performance': attributes.get('networkPerformance', ''),
                                'processor_architecture': attributes.get('processorArchitecture', ''),
                                'processor_features': attributes.get('processorFeatures', ''),
                                'physical_processor': attributes.get('physicalProcessor', ''),
                                'clock_speed': attributes.get('clockSpeed', ''),
                                'enhanced_networking_supported': attributes.get('enhancedNetworkingSupported', ''),
                                'gpu': attributes.get('gpu', ''),
                                'price_per_hour_usd': float(usd_price) if usd_price and usd_price != '0' else 0.0,
                                'price_unit': dimension_data.get('unit', ''),
                                'price_description': dimension_data.get('description', ''),
                                'term_type': 'OnDemand',
                                'sku': product.get('sku', ''),
                                'updated_at': datetime.now().isoformat()
                            }
                            
                            pricing_records.append(record)
                            
        except Exception as e:
            print(f"❌ Error fetching pricing data for {region_code}: {e}")
            
        return pricing_records
    
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
    
    def get_rds_pricing_data(self, 
                            regions: Optional[List[str]] = None,
                            instance_classes: Optional[List[str]] = None,
                            database_engines: Optional[List[str]] = None,
                            deployment_option: str = 'Single-AZ') -> pl.DataFrame:
        """
        Get RDS pricing data from AWS Pricing API.
        
        Args:
            regions: List of region codes
            instance_classes: List of RDS instance classes (e.g., ['db.t3.micro'])
            database_engines: List of database engines (e.g., ['MySQL', 'PostgreSQL'])
            deployment_option: 'Single-AZ' or 'Multi-AZ'
        
        Returns:
            Polars DataFrame with RDS pricing data
        """
        pricing_client = self._get_boto3_client('pricing')
        
        # Default values
        if regions is None:
            regions = [self.config.aws_region] if self.config.aws_region else ['us-east-1']
        if database_engines is None:
            database_engines = ['MySQL']
            
        print(f"🔍 Fetching RDS pricing data for {len(regions)} region(s)...")
        
        all_pricing_data = []
        
        for region in regions:
            region_name = self._get_region_display_name(region)
            
            filters = [
                {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'OnDemand'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
                {'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': deployment_option}
            ]
            
            for engine in database_engines:
                engine_filters = filters + [
                    {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engine}
                ]
                
                if instance_classes:
                    for instance_class in instance_classes:
                        specific_filters = engine_filters + [
                            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class}
                        ]
                        pricing_data = self._fetch_rds_pricing_data(pricing_client, specific_filters, 
                                                                   region, instance_class, engine)
                        all_pricing_data.extend(pricing_data)
                else:
                    pricing_data = self._fetch_rds_pricing_data(pricing_client, engine_filters, 
                                                               region, None, engine)
                    all_pricing_data.extend(pricing_data)
        
        if not all_pricing_data:
            print("⚠️  No RDS pricing data found")
            return pl.DataFrame()
        
        df = pl.DataFrame(all_pricing_data)
        print(f"✅ Retrieved {len(df)} RDS pricing records")
        
        return df
    
    def _fetch_rds_pricing_data(self, pricing_client, filters: List[Dict], 
                               region_code: str, instance_class: Optional[str], 
                               database_engine: str) -> List[Dict]:
        """Fetch RDS pricing data from AWS API."""
        pricing_records = []
        
        try:
            paginator = pricing_client.get_paginator('get_products')
            
            for page in paginator.paginate(ServiceCode='AmazonRDS', Filters=filters):
                for price_item in page['PriceList']:
                    price_data = json.loads(price_item)
                    
                    product = price_data.get('product', {})
                    attributes = product.get('attributes', {})
                    
                    terms = price_data.get('terms', {})
                    on_demand_terms = terms.get('OnDemand', {})
                    
                    for term_key, term_data in on_demand_terms.items():
                        price_dimensions = term_data.get('priceDimensions', {})
                        
                        for dimension_key, dimension_data in price_dimensions.items():
                            price_per_unit = dimension_data.get('pricePerUnit', {})
                            usd_price = price_per_unit.get('USD', '0')
                            
                            record = {
                                'region_code': region_code,
                                'region_name': attributes.get('location', ''),
                                'instance_class': attributes.get('instanceType', ''),
                                'database_engine': attributes.get('databaseEngine', ''),
                                'database_edition': attributes.get('databaseEdition', ''),
                                'deployment_option': attributes.get('deploymentOption', ''),
                                'license_model': attributes.get('licenseModel', ''),
                                'vcpu': attributes.get('vcpu', ''),
                                'memory': attributes.get('memory', ''),
                                'storage_media': attributes.get('storageMedia', ''),
                                'network_performance': attributes.get('networkPerformance', ''),
                                'processor_architecture': attributes.get('processorArchitecture', ''),
                                'price_per_hour_usd': float(usd_price) if usd_price and usd_price != '0' else 0.0,
                                'price_unit': dimension_data.get('unit', ''),
                                'price_description': dimension_data.get('description', ''),
                                'term_type': 'OnDemand',
                                'sku': product.get('sku', ''),
                                'updated_at': datetime.now().isoformat()
                            }
                            
                            pricing_records.append(record)
                            
        except Exception as e:
            print(f"❌ Error fetching RDS pricing data for {region_code}: {e}")
            
        return pricing_records
    
    def save_pricing_data_to_cache(self, df: pl.DataFrame, cache_name: str) -> str:
        """Save pricing data to local cache for faster access."""
        if not self.config.local_data_path:
            print("⚠️  No local cache path configured")
            return ""
            
        cache_dir = Path(self.config.local_data_path) / "pricing_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        cache_file = cache_dir / f"{cache_name}_{datetime.now().strftime('%Y%m%d')}.parquet"
        
        df.write_parquet(cache_file)
        print(f"💾 Saved pricing data to cache: {cache_file}")
        
        return str(cache_file)
    
    def load_pricing_data_from_cache(self, cache_name: str, max_age_days: int = 1) -> Optional[pl.DataFrame]:
        """Load pricing data from local cache if available and recent."""
        if not self.config.local_data_path:
            return None
            
        cache_dir = Path(self.config.local_data_path) / "pricing_cache"
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
            print(f"🚀 Loaded pricing data from cache: {latest_cache.name}")
            return df
        except Exception as e:
            print(f"❌ Error loading cache file: {e}")
            return None
    
    def get_simple_price(self, region_code: str, instance_type: str, 
                        operating_system: str = "Linux", tenancy: str = "Shared") -> Optional[float]:
        """
        Simple function to get on-demand price for specific instance attributes.
        
        Args:
            region_code: AWS region code (e.g., 'us-east-1')
            instance_type: EC2 instance type (e.g., 't3.micro')
            operating_system: Operating system ('Linux', 'Windows', etc.)
            tenancy: Instance tenancy ('Shared', 'Dedicated', 'Host')
        
        Returns:
            Hourly price in USD, or None if not found
        """
        try:
            # Try to get from cache first
            cache_key = f"simple_pricing_{region_code}"
            pricing_df = self.load_pricing_data_from_cache(cache_key, max_age_days=1)
            
            if pricing_df is None:
                # Fetch from API for this specific region/instance
                pricing_df = self.get_ec2_pricing_data(
                    regions=[region_code],
                    instance_types=[instance_type],
                    operating_systems=[operating_system],
                    tenancy=tenancy
                )
                
                if not pricing_df.is_empty():
                    self.save_pricing_data_to_cache(pricing_df, cache_key)
            
            if pricing_df is None or pricing_df.is_empty():
                return None
            
            # Filter for exact match
            filtered = pricing_df.filter(
                (pl.col("region_code") == region_code) &
                (pl.col("instance_type") == instance_type) &
                (pl.col("operating_system") == operating_system) &
                (pl.col("tenancy") == tenancy)
            )
            
            if filtered.is_empty():
                return None
            
            # Return the price
            return float(filtered.select("price_per_hour_usd").to_series()[0])
            
        except Exception as e:
            print(f"❌ Error getting simple price: {e}")
            return None
    
    def compare_instance_pricing(self, region_code: str, instance_types: List[str], 
                               operating_system: str = "Linux") -> pl.DataFrame:
        """
        Compare pricing for multiple instance types in a region.
        
        Args:
            region_code: AWS region code
            instance_types: List of instance types to compare
            operating_system: Operating system
        
        Returns:
            DataFrame with pricing comparison
        """
        results = []
        
        for instance_type in instance_types:
            price = self.get_simple_price(region_code, instance_type, operating_system)
            
            if price is not None:
                results.append({
                    "region": region_code,
                    "instance_type": instance_type,
                    "operating_system": operating_system,
                    "hourly_price": price,
                    "monthly_price": price * 24 * 30,
                    "annual_price": price * 24 * 365
                })
        
        return pl.DataFrame(results) if results else pl.DataFrame()
#!/usr/bin/env python3
"""
Simple Price Lookup Example with Real AWS APIs

This example shows how to use the simplified pricing functions to get
real on-demand and savings plan prices from AWS APIs based on instance attributes.
"""

from de_polars.engine.data_config import DataConfig, DataExportType
from de_polars.data.pricing_api_manager import PricingApiManager
from de_polars.data.savings_plan_api_manager import SavingsPlansApiManager


def main():
    """Simple pricing example."""
    
    print("💰 Simple AWS Pricing Lookup Example - REAL APIs")
    print("=" * 55)
    print("⚠️  Note: This uses real AWS APIs and requires valid credentials")
    print("=" * 55)
    
    # Simple configuration
    config = DataConfig(
        s3_bucket="dummy-bucket",  # Not needed for API calls
        s3_data_prefix="dummy-prefix",
        data_export_type=DataExportType.CUR_2_0,
        aws_region="us-east-1",
        local_data_path="./cache"  # For caching API results
    )
    
    # Initialize managers
    pricing_manager = PricingApiManager(config)
    sp_manager = SavingsPlansApiManager(config)
    
    # Example 1: Get on-demand price for a specific instance
    print("\n🔍 Example 1: Get On-Demand Price")
    print("-" * 30)
    
    instance_type = "m5.large"
    region = "us-east-1"
    operating_system = "Linux"
    
    print(f"Getting price for {instance_type} in {region} ({operating_system})...")
    
    # Make a real API call to AWS Pricing API
    price = pricing_manager.get_simple_price(region, instance_type, operating_system)
    
    if price:
        print(f"💸 On-Demand Price: ${price:.4f}/hour")
        print(f"💸 Monthly Cost: ${price * 24 * 30:.2f}")
        print(f"💸 Annual Cost: ${price * 24 * 365:.2f}")
    else:
        print("❌ Price not found")
    
    # Example 2: Get savings plan rate
    print("\n🔍 Example 2: Get Savings Plan Rate")
    print("-" * 30)
    
    print(f"Getting savings plan rate for {instance_type} in {region}...")
    
    # Make a real API call to AWS SavingsPlans API
    sp_rate = sp_manager.get_simple_savings_plan_rate(instance_type, region)
    
    if sp_rate:
        print(f"💳 Savings Plan Rate: ${sp_rate:.4f}/hour")
        print(f"💳 Monthly Cost: ${sp_rate * 24 * 30:.2f}")
        print(f"💳 Annual Cost: ${sp_rate * 24 * 365:.2f}")
    else:
        print("❌ Savings plan rate not found")
    
    # Example 3: Compare pricing
    print("\n🔍 Example 3: Price Comparison")
    print("-" * 30)
    
    if price and sp_rate:
        comparison = sp_manager.compare_savings_vs_ondemand(region, instance_type, price)
        
        print(f"📊 Comparison for {comparison['instance_type']} in {comparison['region']}:")
        print(f"   On-Demand: ${comparison['on_demand_hourly']:.4f}/hour (${comparison['on_demand_monthly']:.2f}/month)")
        
        if comparison['has_savings_plan']:
            print(f"   Savings Plan: ${comparison['savings_plan_hourly']:.4f}/hour (${comparison['savings_plan_monthly']:.2f}/month)")
            print(f"   Monthly Savings: ${comparison['monthly_savings']:.2f}")
            print(f"   Annual Savings: ${comparison['annual_savings']:.2f}")
            print(f"   Savings Percentage: {comparison['savings_percentage']:.1f}%")
        else:
            print("   No savings plan available")
    
    # Example 4: Compare multiple instances
    print("\n🔍 Example 4: Compare Multiple Instances")
    print("-" * 30)
    
    # Reduce list for real API calls to avoid too many requests
    instance_types = ["t3.micro", "m5.large"]
    
    print(f"Comparing instances in {region} (using real AWS APIs):")
    print()
    
    print(f"{'Instance':<12} {'On-Demand':<12} {'Savings Plan':<12} {'Savings':<8} {'Monthly Savings'}")
    print("-" * 60)
    
    for instance in instance_types:
        print(f"🔍 Fetching prices for {instance}...")
        
        # Get real on-demand price
        od_price = pricing_manager.get_simple_price(region, instance, operating_system)
        
        # Get real savings plan rate
        sp_price = sp_manager.get_simple_savings_plan_rate(instance, region)
        
        if od_price:
            if sp_price:
                savings_pct = ((od_price - sp_price) / od_price) * 100
                monthly_savings = (od_price - sp_price) * 24 * 30
                
                print(f"{instance:<12} ${od_price:<11.4f} ${sp_price:<11.4f} {savings_pct:<7.1f}% ${monthly_savings:.2f}")
            else:
                print(f"{instance:<12} ${od_price:<11.4f} Not available  N/A       N/A")
        else:
            print(f"{instance:<12} Not available")
    
    # Example 5: Real-world usage pattern
    print("\n🔍 Example 5: Real Usage Pattern")
    print("-" * 30)
    
    print("To use with real AWS API calls:")
    print()
    print("# 1. Get on-demand pricing")
    print("pricing_manager = PricingApiManager(config)")
    print("price = pricing_manager.get_simple_price('us-east-1', 'm5.large', 'Linux')")
    print()
    print("# 2. Get savings plan rates")
    print("sp_manager = SavingsPlansApiManager(config)")
    print("sp_rate = sp_manager.get_simple_savings_plan_rate('m5.large', 'us-east-1')")
    print()
    print("# 3. Compare pricing")
    print("comparison = sp_manager.compare_savings_vs_ondemand('us-east-1', 'm5.large', price)")
    print()
    print("# 4. Compare multiple instances")
    print("comparison_df = pricing_manager.compare_instance_pricing('us-east-1', ['t3.micro', 'm5.large'])")
    
    print("\n✅ Simple pricing lookup examples completed!")
    print("\n💡 Key benefits:")
    print("   - Simple function calls based on instance attributes")
    print("   - Automatic caching for performance")
    print("   - Easy comparison between on-demand and savings plan pricing")
    print("   - No complex SQL queries needed for basic price lookups")


if __name__ == "__main__":
    main()
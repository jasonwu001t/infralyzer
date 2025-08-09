"""
FOCUS (FinOps Open Cost and Usage Specification) Schema Definition
================================================================

Based on FOCUS v1.0/v1.1/v1.2 specification from https://focus.finops.org/focus-columns/

This module provides the complete FOCUS column schema organized by categories
for use in the SQL Lab frontend and backend query processing.
"""

from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class FocusColumn:
    """Represents a single FOCUS column with metadata."""
    name: str
    data_type: str
    category: str
    description: str
    version: str = "v1.0"  # FOCUS version where column was introduced


class FocusSchema:
    """FOCUS specification column schema and utilities."""
    
    # FOCUS Column Groups with their columns and data types
    COLUMN_GROUPS = {
        "Account": {
            "description": "Account and billing account information",
            "color": "text-blue-600",
            "icon": "User",
            "columns": [
                {"name": "BillingAccountId", "type": "STRING", "description": "Unique identifier for the billing account"},
                {"name": "BillingAccountName", "type": "STRING", "description": "Display name for the billing account"},
                {"name": "BillingAccountType", "type": "STRING", "description": "Type of billing account"},
                {"name": "SubAccountId", "type": "STRING", "description": "Unique identifier for the sub account"},
                {"name": "SubAccountName", "type": "STRING", "description": "Display name for the sub account"},
                {"name": "SubAccountType", "type": "STRING", "description": "Type of sub account"},
            ]
        },
        "Billing": {
            "description": "Billing and cost information",
            "color": "text-green-600",
            "icon": "Receipt",
            "columns": [
                {"name": "BilledCost", "type": "DECIMAL", "description": "Cost billed for the line item"},
                {"name": "BillingCurrency", "type": "STRING", "description": "Currency used for billing"},
                {"name": "ConsumedQuantity", "type": "DECIMAL", "description": "Quantity of service consumed"},
                {"name": "ConsumedUnit", "type": "STRING", "description": "Unit of measurement for consumed quantity"},
                {"name": "ContractedCost", "type": "DECIMAL", "description": "Cost based on contracted rates"},
                {"name": "ContractedUnitPrice", "type": "DECIMAL", "description": "Unit price based on contracted rates"},
                {"name": "EffectiveCost", "type": "DECIMAL", "description": "Cost after applying discounts and credits"},
                {"name": "ListCost", "type": "DECIMAL", "description": "Cost at list price"},
                {"name": "ListUnitPrice", "type": "DECIMAL", "description": "Unit price at list rates"},
            ]
        },
        "Capacity Reservation": {
            "description": "Capacity reservation information",
            "color": "text-purple-600",
            "icon": "Shield",
            "columns": [
                {"name": "CapacityReservationId", "type": "STRING", "description": "Unique identifier for capacity reservation"},
                {"name": "CapacityReservationStatus", "type": "STRING", "description": "Status of the capacity reservation"},
            ]
        },
        "Charge": {
            "description": "Charge categorization and details",
            "color": "text-red-600",
            "icon": "DollarSign",
            "columns": [
                {"name": "ChargeCategory", "type": "STRING", "description": "Category of the charge"},
                {"name": "ChargeClass", "type": "STRING", "description": "Class of the charge"},
                {"name": "ChargeDescription", "type": "STRING", "description": "Description of the charge"},
                {"name": "ChargeFrequency", "type": "STRING", "description": "Frequency of the charge"},
            ]
        },
        "Charge Origination": {
            "description": "Invoice and provider information",
            "color": "text-indigo-600",
            "icon": "FileText",
            "columns": [
                {"name": "InvoiceId", "type": "STRING", "description": "Unique identifier for the invoice"},
                {"name": "InvoiceIssuer", "type": "STRING", "description": "Entity that issued the invoice"},
                {"name": "Provider", "type": "STRING", "description": "Cloud service provider"},
                {"name": "Publisher", "type": "STRING", "description": "Entity that published the resource"},
            ]
        },
        "Commitment Discount": {
            "description": "Commitment-based discounts and savings plans",
            "color": "text-yellow-600",
            "icon": "Percent",
            "columns": [
                {"name": "CommitmentDiscountCategory", "type": "STRING", "description": "Category of commitment discount"},
                {"name": "CommitmentDiscountId", "type": "STRING", "description": "Unique identifier for commitment discount"},
                {"name": "CommitmentDiscountName", "type": "STRING", "description": "Name of the commitment discount"},
                {"name": "CommitmentDiscountQuantity", "type": "DECIMAL", "description": "Quantity committed in the discount"},
                {"name": "CommitmentDiscountStatus", "type": "STRING", "description": "Status of the commitment discount"},
                {"name": "CommitmentDiscountType", "type": "STRING", "description": "Type of commitment discount"},
                {"name": "CommitmentDiscountUnit", "type": "STRING", "description": "Unit of the commitment discount"},
            ]
        },
        "Location": {
            "description": "Geographic location information",
            "color": "text-teal-600",
            "icon": "MapPin",
            "columns": [
                {"name": "AvailabilityZone", "type": "STRING", "description": "Availability zone where resource is deployed"},
                {"name": "RegionId", "type": "STRING", "description": "Unique identifier for the region"},
                {"name": "RegionName", "type": "STRING", "description": "Display name for the region"},
            ]
        },
        "Pricing": {
            "description": "Pricing and rate information",
            "color": "text-orange-600",
            "icon": "Calculator",
            "columns": [
                {"name": "PricingCategory", "type": "STRING", "description": "Category of pricing model"},
                {"name": "PricingCurrency", "type": "STRING", "description": "Currency used for pricing"},
                {"name": "PricingCurrencyContractedUnitPrice", "type": "DECIMAL", "description": "Contracted unit price in pricing currency"},
                {"name": "PricingCurrencyEffectiveCost", "type": "DECIMAL", "description": "Effective cost in pricing currency"},
                {"name": "PricingCurrencyListUnitPrice", "type": "DECIMAL", "description": "List unit price in pricing currency"},
                {"name": "PricingQuantity", "type": "DECIMAL", "description": "Quantity used for pricing"},
                {"name": "PricingUnit", "type": "STRING", "description": "Unit of measurement for pricing"},
            ]
        },
        "Resource": {
            "description": "Resource identification and tagging",
            "color": "text-pink-600",
            "icon": "Package",
            "columns": [
                {"name": "ResourceId", "type": "STRING", "description": "Unique identifier for the resource"},
                {"name": "ResourceName", "type": "STRING", "description": "Display name for the resource"},
                {"name": "ResourceType", "type": "STRING", "description": "Type of the resource"},
                {"name": "Tags", "type": "MAP", "description": "Key-value pairs of resource tags"},
            ]
        },
        "Service": {
            "description": "Service categorization",
            "color": "text-cyan-600",
            "icon": "Server",
            "columns": [
                {"name": "ServiceCategory", "type": "STRING", "description": "Category of the service"},
                {"name": "ServiceName", "type": "STRING", "description": "Name of the service"},
                {"name": "ServiceSubcategory", "type": "STRING", "description": "Subcategory of the service"},
            ]
        },
        "SKU": {
            "description": "Stock Keeping Unit details",
            "color": "text-lime-600",
            "icon": "Package2",
            "columns": [
                {"name": "SkuId", "type": "STRING", "description": "Unique identifier for the SKU"},
                {"name": "SkuMeter", "type": "STRING", "description": "Meter associated with the SKU"},
                {"name": "SkuPriceDetails", "type": "STRING", "description": "Detailed pricing information for the SKU"},
                {"name": "SkuPriceId", "type": "STRING", "description": "Unique identifier for SKU pricing"},
            ]
        },
        "Timeframe": {
            "description": "Time-based information",
            "color": "text-amber-600",
            "icon": "Clock",
            "columns": [
                {"name": "BillingPeriodEnd", "type": "TIMESTAMP", "description": "End date of the billing period"},
                {"name": "BillingPeriodStart", "type": "TIMESTAMP", "description": "Start date of the billing period"},
                {"name": "ChargePeriodEnd", "type": "TIMESTAMP", "description": "End date of the charge period"},
                {"name": "ChargePeriodStart", "type": "TIMESTAMP", "description": "Start date of the charge period"},
            ]
        }
    }
    
    @classmethod
    def get_all_columns(cls) -> List[Dict[str, Any]]:
        """Get all FOCUS columns as a flat list with metadata."""
        columns = []
        for group_name, group_data in cls.COLUMN_GROUPS.items():
            for column in group_data["columns"]:
                columns.append({
                    "name": column["name"],
                    "type": column["type"],
                    "category": group_name,
                    "description": column.get("description", ""),
                    "group_color": group_data["color"],
                    "group_icon": group_data["icon"]
                })
        return columns
    
    @classmethod
    def get_column_groups_for_frontend(cls) -> List[Dict[str, Any]]:
        """Get column groups formatted for frontend display."""
        groups = []
        for group_name, group_data in cls.COLUMN_GROUPS.items():
            groups.append({
                "name": group_name,
                "description": group_data["description"],
                "color": group_data["color"],
                "icon": group_data["icon"],
                "columns": [col["name"] for col in group_data["columns"]]
            })
        return groups
    
    @classmethod
    def get_schema_dict(cls) -> Dict[str, str]:
        """Get FOCUS schema as column_name -> data_type mapping."""
        schema = {}
        for group_data in cls.COLUMN_GROUPS.values():
            for column in group_data["columns"]:
                schema[column["name"]] = column["type"]
        return schema
    
    @classmethod
    def get_column_by_name(cls, column_name: str) -> Dict[str, Any]:
        """Get column metadata by name."""
        for group_name, group_data in cls.COLUMN_GROUPS.items():
            for column in group_data["columns"]:
                if column["name"].lower() == column_name.lower():
                    return {
                        "name": column["name"],
                        "type": column["type"],
                        "category": group_name,
                        "description": column.get("description", ""),
                        "group_color": group_data["color"],
                        "group_icon": group_data["icon"]
                    }
        return {}
    
    @classmethod
    def is_focus_column(cls, column_name: str) -> bool:
        """Check if a column name is a valid FOCUS column."""
        return bool(cls.get_column_by_name(column_name))
    
    @classmethod
    def get_sample_queries(cls) -> List[Dict[str, str]]:
        """Get sample FOCUS queries for different use cases."""
        return [
            {
                "name": "Total Cost by Service",
                "description": "Get total effective cost by service category",
                "query": """
SELECT 
    ServiceCategory,
    ServiceName,
    SUM(EffectiveCost) AS TotalCost,
    COUNT(*) AS RecordCount
FROM FOCUS 
WHERE BillingPeriodStart >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
GROUP BY ServiceCategory, ServiceName
ORDER BY TotalCost DESC
LIMIT 20;
                """.strip()
            },
            {
                "name": "Cost by Account and Region",
                "description": "Analyze costs by billing account and region",
                "query": """
SELECT 
    BillingAccountName,
    RegionName,
    SUM(EffectiveCost) AS TotalCost,
    SUM(ListCost) AS TotalListCost,
    SUM(ListCost - EffectiveCost) AS TotalSavings
FROM FOCUS 
WHERE BillingPeriodStart >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY BillingAccountName, RegionName
ORDER BY TotalCost DESC;
                """.strip()
            },
            {
                "name": "Commitment Discount Analysis",
                "description": "Analyze commitment-based discounts and savings",
                "query": """
SELECT 
    CommitmentDiscountType,
    CommitmentDiscountCategory,
    COUNT(DISTINCT CommitmentDiscountId) AS DiscountCount,
    SUM(EffectiveCost) AS TotalEffectiveCost,
    SUM(ListCost) AS TotalListCost,
    SUM(ListCost - EffectiveCost) AS CommitmentSavings
FROM FOCUS 
WHERE CommitmentDiscountId IS NOT NULL
    AND BillingPeriodStart >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
GROUP BY CommitmentDiscountType, CommitmentDiscountCategory
ORDER BY CommitmentSavings DESC;
                """.strip()
            },
            {
                "name": "Resource Utilization by Type",
                "description": "Analyze resource costs and utilization",
                "query": """
SELECT 
    ResourceType,
    COUNT(DISTINCT ResourceId) AS UniqueResources,
    SUM(ConsumedQuantity) AS TotalQuantity,
    ConsumedUnit,
    SUM(EffectiveCost) AS TotalCost,
    AVG(EffectiveCost) AS AvgCostPerResource
FROM FOCUS 
WHERE ResourceId IS NOT NULL
    AND BillingPeriodStart >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
GROUP BY ResourceType, ConsumedUnit
ORDER BY TotalCost DESC
LIMIT 15;
                """.strip()
            }
        ]


# Export commonly used constants
FOCUS_TABLE_NAME = "FOCUS"
FOCUS_VERSION = "v1.0"
FOCUS_SUPPORTED_VERSIONS = ["v1.0", "v1.1", "v1.2"]

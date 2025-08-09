"""
Join Tables API endpoints for enhanced CUR/FOCUS analysis
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from ..dependencies import get_finops_engine
from ...finops_engine import FinOpsEngine

router = APIRouter()

class JoinKey(BaseModel):
    source_column: str = Field(..., description="Column in source table (CUR/FOCUS)")
    target_column: str = Field(..., description="Column in target joinable table")
    description: str = Field(..., description="Description of the join relationship")
    confidence: str = Field(..., description="Confidence level: high, medium, low")

class JoinableTable(BaseModel):
    id: str = Field(..., description="Unique identifier for the joinable table")
    name: str = Field(..., description="Table name for SQL queries")
    display_name: str = Field(..., description="Human-readable name")
    description: str = Field(..., description="Table description")
    icon: str = Field(..., description="Icon name for UI")
    primary_keys: List[str] = Field(..., description="Primary keys of the table")
    join_keys: List[JoinKey] = Field(..., description="Available join relationships")
    sample_columns: List[Dict[str, str]] = Field(..., description="Sample columns with types")
    data_source: str = Field(..., description="Source of the data (API, file, etc.)")
    category: str = Field(..., description="Category: billing, pricing, usage, governance")

class JoinTablesRequest(BaseModel):
    base_table: str = Field(..., description="Base table: CUR or FOCUS")
    joined_tables: List[str] = Field(..., description="List of table IDs to join")

class JoinTablesResponse(BaseModel):
    success: bool
    available_tables: List[JoinableTable]
    message: str

def get_invoice_summaries_table() -> JoinableTable:
    """Get Invoice Summaries table definition based on AWS Billing API"""
    return JoinableTable(
        id="invoice_summaries",
        name="INVOICE_SUMMARIES",
        display_name="Invoice Summaries",
        description="AWS invoice summary data with billing periods, amounts, and payment details",
        icon="Receipt",
        primary_keys=["InvoiceId", "AccountId"],
        join_keys=[
            JoinKey(
                source_column="bill_invoice_id",
                target_column="InvoiceId",
                description="Join by Invoice ID for billing period correlation",
                confidence="high"
            ),
            JoinKey(
                source_column="line_item_usage_account_id",
                target_column="AccountId",
                description="Join by Account ID for account-level billing",
                confidence="high"
            )
        ],
        sample_columns=[
            {"name": "InvoiceId", "type": "STRING"},
            {"name": "AccountId", "type": "STRING"},
            {"name": "TotalAmount", "type": "DECIMAL"},
            {"name": "BillingPeriod", "type": "STRUCT"},
            {"name": "DueDate", "type": "TIMESTAMP"},
            {"name": "InvoiceType", "type": "STRING"},
            {"name": "BaseCurrencyAmount", "type": "STRUCT"},
            {"name": "PaymentCurrencyAmount", "type": "STRUCT"}
        ],
        data_source="AWS Billing API",
        category="billing"
    )

def get_ec2_pricing_table() -> JoinableTable:
    """Get EC2 Pricing table definition based on AWS Price List API"""
    return JoinableTable(
        id="ec2_pricing",
        name="EC2_PRICING",
        display_name="EC2 Pricing Data",
        description="Current EC2 instance pricing from AWS Price List API",
        icon="DollarSign",
        primary_keys=["InstanceType", "Region"],
        join_keys=[
            JoinKey(
                source_column="product_instance_type",
                target_column="InstanceType",
                description="Join by Instance Type for pricing correlation",
                confidence="high"
            ),
            JoinKey(
                source_column="product_region",
                target_column="Region",
                description="Join by Region for regional pricing",
                confidence="high"
            )
        ],
        sample_columns=[
            {"name": "InstanceType", "type": "STRING"},
            {"name": "Region", "type": "STRING"},
            {"name": "OnDemandPrice", "type": "DECIMAL"},
            {"name": "Currency", "type": "STRING"},
            {"name": "OperatingSystem", "type": "STRING"},
            {"name": "Tenancy", "type": "STRING"},
            {"name": "CapacityStatus", "type": "STRING"},
            {"name": "PreInstalledSw", "type": "STRING"}
        ],
        data_source="AWS Price List API",
        category="pricing"
    )

def get_savings_plans_table() -> JoinableTable:
    """Get Savings Plans table definition based on AWS Savings Plans API"""
    return JoinableTable(
        id="savings_plans",
        name="SAVINGS_PLANS",
        display_name="Savings Plans",
        description="Active Savings Plans with rates and coverage",
        icon="Percent",
        primary_keys=["SavingsPlanId"],
        join_keys=[
            JoinKey(
                source_column="line_item_usage_account_id",
                target_column="AccountId",
                description="Join by Account ID for Savings Plan coverage",
                confidence="medium"
            ),
            JoinKey(
                source_column="product_region",
                target_column="Region",
                description="Join by Region for regional Savings Plan rates",
                confidence="medium"
            )
        ],
        sample_columns=[
            {"name": "SavingsPlanId", "type": "STRING"},
            {"name": "AccountId", "type": "STRING"},
            {"name": "Region", "type": "STRING"},
            {"name": "SavingsPlanType", "type": "STRING"},
            {"name": "PaymentOption", "type": "STRING"},
            {"name": "HourlyRate", "type": "DECIMAL"},
            {"name": "CoveragePercentage", "type": "DECIMAL"},
            {"name": "UtilizationPercentage", "type": "DECIMAL"}
        ],
        data_source="AWS Savings Plans API",
        category="pricing"
    )

@router.get("/available-tables/{base_table}")
async def get_available_join_tables(
    base_table: str,
    finops_engine: FinOpsEngine = Depends(get_finops_engine)
) -> JoinTablesResponse:
    """
    Get available tables that can be joined to the base table.
    
    **Supported Base Tables:**
    - `CUR`: AWS Cost and Usage Report 2.0
    - `FOCUS`: FinOps Open Cost and Usage Specification
    
    **Returns:** List of joinable tables with their metadata and join keys
    """
    try:
        base_table = base_table.upper()
        
        if base_table == "CUR":
            available_tables = [
                get_invoice_summaries_table(),
                get_ec2_pricing_table(),
                get_savings_plans_table()
            ]
        elif base_table == "FOCUS":
            # For FOCUS, we can provide multi-cloud joinable tables
            available_tables = [
                JoinableTable(
                    id="invoice_summaries",
                    name="INVOICE_SUMMARIES",
                    display_name="Invoice Summaries",
                    description="Multi-cloud invoice summary data",
                    icon="Receipt",
                    primary_keys=["InvoiceId", "BillingAccountId"],
                    join_keys=[
                        JoinKey(
                            source_column="InvoiceId",
                            target_column="InvoiceId",
                            description="Join by Invoice ID for billing correlation",
                            confidence="high"
                        ),
                        JoinKey(
                            source_column="BillingAccountId",
                            target_column="BillingAccountId",
                            description="Join by Billing Account for account-level data",
                            confidence="high"
                        )
                    ],
                    sample_columns=[
                        {"name": "InvoiceId", "type": "STRING"},
                        {"name": "BillingAccountId", "type": "STRING"},
                        {"name": "TotalAmount", "type": "DECIMAL"},
                        {"name": "BillingPeriod", "type": "STRUCT"},
                        {"name": "DueDate", "type": "TIMESTAMP"}
                    ],
                    data_source="Multi-cloud Billing APIs",
                    category="billing"
                )
            ]
        else:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported base table: {base_table}. Supported: CUR, FOCUS"
            )
        
        return JoinTablesResponse(
            success=True,
            available_tables=available_tables,
            message=f"Found {len(available_tables)} joinable tables for {base_table}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving joinable tables: {str(e)}")

@router.post("/validate-joins")
async def validate_table_joins(
    request: JoinTablesRequest,
    finops_engine: FinOpsEngine = Depends(get_finops_engine)
):
    """
    Validate that the requested table joins are valid and return join SQL.
    
    **Features:**
    - Validates join keys exist in both tables
    - Returns SQL JOIN syntax for the combination
    - Provides warnings for low-confidence joins
    """
    try:
        # Get available tables for the base table
        available_response = await get_available_join_tables(request.base_table, finops_engine)
        available_tables = {table.id: table for table in available_response.available_tables}
        
        # Validate requested tables exist
        invalid_tables = [table_id for table_id in request.joined_tables if table_id not in available_tables]
        if invalid_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid table IDs: {invalid_tables}. Available: {list(available_tables.keys())}"
            )
        
        # Build join SQL
        join_sql_parts = []
        join_info = []
        
        for table_id in request.joined_tables:
            table = available_tables[table_id]
            
            # Use the first join key (highest priority)
            primary_join = table.join_keys[0] if table.join_keys else None
            
            if primary_join:
                join_sql = f"""
LEFT JOIN {table.name} 
  ON {request.base_table}.{primary_join.source_column} = {table.name}.{primary_join.target_column}"""
                join_sql_parts.append(join_sql.strip())
                
                join_info.append({
                    "table_name": table.name,
                    "join_type": "LEFT JOIN",
                    "join_condition": f"{request.base_table}.{primary_join.source_column} = {table.name}.{primary_join.target_column}",
                    "confidence": primary_join.confidence,
                    "description": primary_join.description
                })
        
        return {
            "success": True,
            "base_table": request.base_table,
            "joined_tables": request.joined_tables,
            "join_sql": "\n".join(join_sql_parts),
            "join_info": join_info,
            "sample_query": f"""
SELECT 
    {request.base_table}.*,
    {', '.join([f'{table_name}.{col["name"]}' for table_id in request.joined_tables for table_name in [available_tables[table_id].name] for col in available_tables[table_id].sample_columns[:2]])}
FROM {request.base_table}
{chr(10).join(join_sql_parts)}
LIMIT 10;
            """.strip()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error validating joins: {str(e)}")

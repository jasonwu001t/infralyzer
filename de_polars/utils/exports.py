"""
Data export and report generation utilities
"""
import polars as pl
from typing import Dict, Any, Optional, List, Union
import json
import csv
import io
from datetime import datetime
from pathlib import Path


class DataExporter:
    """Utility for exporting cost analytics data in various formats."""
    
    @staticmethod
    def export_to_json(data: Union[pl.DataFrame, Dict[str, Any]], 
                      file_path: Optional[str] = None,
                      indent: int = 2) -> Union[str, None]:
        """
        Export data to JSON format.
        
        Args:
            data: DataFrame or dictionary to export
            file_path: Optional file path to save to
            indent: JSON indentation level
            
        Returns:
            JSON string if no file_path, None if saved to file
        """
        if isinstance(data, pl.DataFrame):
            # Convert DataFrame to dictionary
            json_data = data.to_dicts()
        else:
            json_data = data
        
        json_str = json.dumps(json_data, indent=indent, default=str)
        
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_str)
            return None
        else:
            return json_str
    
    @staticmethod
    def export_to_csv(df: pl.DataFrame, 
                     file_path: Optional[str] = None,
                     include_headers: bool = True) -> Union[str, None]:
        """
        Export DataFrame to CSV format.
        
        Args:
            df: Polars DataFrame to export
            file_path: Optional file path to save to
            include_headers: Whether to include column headers
            
        Returns:
            CSV string if no file_path, None if saved to file
        """
        if file_path:
            df.write_csv(file_path, include_header=include_headers)
            return None
        else:
            # Return as string
            output = io.StringIO()
            df.write_csv(output, include_header=include_headers)
            return output.getvalue()
    
    @staticmethod
    def export_to_excel(df: pl.DataFrame, 
                       file_path: str,
                       sheet_name: str = "Sheet1",
                       include_headers: bool = True) -> None:
        """
        Export DataFrame to Excel format.
        
        Args:
            df: Polars DataFrame to export
            file_path: File path to save Excel file
            sheet_name: Name of the Excel sheet
            include_headers: Whether to include column headers
        """
        try:
            # Convert to pandas for Excel export (xlsxwriter dependency)
            pandas_df = df.to_pandas()
            pandas_df.to_excel(file_path, sheet_name=sheet_name, index=False, header=include_headers)
        except ImportError:
            raise ImportError("Excel export requires pandas and openpyxl/xlsxwriter. Install with: pip install pandas openpyxl")
    
    @staticmethod
    def export_summary_report(data: Dict[str, Any], 
                            format: str = "json",
                            file_path: Optional[str] = None) -> Union[str, None]:
        """
        Export formatted summary report.
        
        Args:
            data: Summary data dictionary
            format: Export format (json, txt, markdown)
            file_path: Optional file path to save to
            
        Returns:
            Formatted string if no file_path, None if saved to file
        """
        if format.lower() == "json":
            return DataExporter.export_to_json(data, file_path)
        
        elif format.lower() == "txt":
            report_text = DataExporter._format_text_report(data)
            
        elif format.lower() == "markdown":
            report_text = DataExporter._format_markdown_report(data)
            
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            return None
        else:
            return report_text
    
    @staticmethod
    def _format_text_report(data: Dict[str, Any]) -> str:
        """Format data as plain text report."""
        lines = []
        lines.append("FINOPS COST ANALYTICS REPORT")
        lines.append("=" * 40)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        def format_section(section_data, level=0):
            indent = "  " * level
            section_lines = []
            
            for key, value in section_data.items():
                if isinstance(value, dict):
                    section_lines.append(f"{indent}{key.replace('_', ' ').title()}:")
                    section_lines.extend(format_section(value, level + 1))
                elif isinstance(value, list):
                    section_lines.append(f"{indent}{key.replace('_', ' ').title()}:")
                    for item in value[:5]:  # Limit to 5 items
                        if isinstance(item, dict):
                            section_lines.extend(format_section(item, level + 1))
                        else:
                            section_lines.append(f"{indent}  - {item}")
                    if len(value) > 5:
                        section_lines.append(f"{indent}  ... and {len(value) - 5} more")
                else:
                    formatted_key = key.replace('_', ' ').title()
                    if isinstance(value, (int, float)) and 'cost' in key.lower():
                        formatted_value = f"${value:,.2f}"
                    elif isinstance(value, float) and 'percentage' in key.lower():
                        formatted_value = f"{value:.1f}%"
                    else:
                        formatted_value = str(value)
                    section_lines.append(f"{indent}{formatted_key}: {formatted_value}")
            
            return section_lines
        
        lines.extend(format_section(data))
        return "\n".join(lines)
    
    @staticmethod
    def _format_markdown_report(data: Dict[str, Any]) -> str:
        """Format data as Markdown report."""
        lines = []
        lines.append("# FinOps Cost Analytics Report")
        lines.append("")
        lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        def format_section(section_data, level=2):
            section_lines = []
            
            for key, value in section_data.items():
                if isinstance(value, dict):
                    section_lines.append(f"{'#' * level} {key.replace('_', ' ').title()}")
                    section_lines.append("")
                    section_lines.extend(format_section(value, level + 1))
                elif isinstance(value, list):
                    section_lines.append(f"{'#' * level} {key.replace('_', ' ').title()}")
                    section_lines.append("")
                    for item in value[:10]:  # Limit to 10 items
                        if isinstance(item, dict):
                            section_lines.extend(format_section(item, level + 1))
                        else:
                            section_lines.append(f"- {item}")
                    if len(value) > 10:
                        section_lines.append(f"- *... and {len(value) - 10} more items*")
                    section_lines.append("")
                else:
                    formatted_key = key.replace('_', ' ').title()
                    if isinstance(value, (int, float)) and 'cost' in key.lower():
                        formatted_value = f"${value:,.2f}"
                    elif isinstance(value, float) and 'percentage' in key.lower():
                        formatted_value = f"{value:.1f}%"
                    else:
                        formatted_value = str(value)
                    section_lines.append(f"**{formatted_key}:** {formatted_value}")
                    section_lines.append("")
            
            return section_lines
        
        lines.extend(format_section(data))
        return "\n".join(lines)


class ReportGenerator:
    """Utility for generating formatted cost analytics reports."""
    
    @staticmethod
    def generate_executive_summary(kpi_data: Dict[str, Any], 
                                 spend_data: Dict[str, Any],
                                 optimization_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate executive summary report.
        
        Args:
            kpi_data: KPI summary data
            spend_data: Spend analytics data
            optimization_data: Optimization recommendations
            
        Returns:
            Executive summary dictionary
        """
        # Extract key metrics
        total_spend = kpi_data.get('overall_spend', {}).get('spend_all_cost', 0)
        savings_potential = kpi_data.get('savings_summary', {}).get('total_potential_savings', 0)
        mom_change = spend_data.get('mom_change', 0)
        
        # Calculate key ratios
        savings_ratio = (savings_potential / total_spend * 100) if total_spend > 0 else 0
        
        summary = {
            "executive_summary": {
                "report_date": datetime.now().strftime('%Y-%m-%d'),
                "key_metrics": {
                    "current_monthly_spend": total_spend,
                    "optimization_potential": savings_potential,
                    "potential_savings_percentage": savings_ratio,
                    "month_over_month_change": mom_change
                },
                "highlights": ReportGenerator._generate_highlights(kpi_data, spend_data, optimization_data),
                "recommendations": ReportGenerator._generate_recommendations(savings_ratio, mom_change, optimization_data),
                "risk_assessment": ReportGenerator._assess_risks(mom_change, savings_ratio)
            }
        }
        
        return summary
    
    @staticmethod
    def _generate_highlights(kpi_data: Dict[str, Any], 
                           spend_data: Dict[str, Any],
                           optimization_data: Dict[str, Any]) -> List[str]:
        """Generate key highlights for executive summary."""
        highlights = []
        
        # Spend highlights
        total_spend = kpi_data.get('overall_spend', {}).get('spend_all_cost', 0)
        if total_spend > 0:
            highlights.append(f"Monthly cloud spend: ${total_spend:,.2f}")
        
        # Savings highlights
        savings_potential = kpi_data.get('savings_summary', {}).get('total_potential_savings', 0)
        if savings_potential > 0:
            highlights.append(f"Identified ${savings_potential:,.2f} in potential monthly savings")
        
        # Growth highlights
        mom_change = spend_data.get('mom_change', 0)
        if abs(mom_change) > 5:
            trend = "increased" if mom_change > 0 else "decreased"
            highlights.append(f"Spend {trend} {abs(mom_change):.1f}% from last month")
        
        # Optimization highlights
        idle_resources = optimization_data.get('idle_resources', [])
        if idle_resources:
            highlights.append(f"Found {len(idle_resources)} idle resources for review")
        
        return highlights
    
    @staticmethod
    def _generate_recommendations(savings_ratio: float, 
                                mom_change: float,
                                optimization_data: Dict[str, Any]) -> List[str]:
        """Generate executive recommendations."""
        recommendations = []
        
        if savings_ratio > 15:
            recommendations.append("HIGH PRIORITY: Significant cost optimization opportunities identified")
        
        if mom_change > 15:
            recommendations.append("URGENT: Investigate rapid cost growth causes")
        
        if savings_ratio > 10:
            recommendations.append("Implement cost optimization initiatives this quarter")
        
        if mom_change > 10:
            recommendations.append("Review recent infrastructure changes and scaling events")
        
        idle_resources = optimization_data.get('idle_resources', [])
        if len(idle_resources) > 10:
            recommendations.append("Clean up idle resources to reduce waste")
        
        if not recommendations:
            recommendations.append("Continue monitoring cost trends and optimization opportunities")
        
        return recommendations
    
    @staticmethod
    def _assess_risks(mom_change: float, savings_ratio: float) -> Dict[str, str]:
        """Assess financial risks based on trends."""
        risk_level = "LOW"
        risk_factors = []
        
        if mom_change > 20:
            risk_level = "HIGH"
            risk_factors.append("Rapid cost growth")
        elif mom_change > 10:
            risk_level = "MEDIUM"
            risk_factors.append("Moderate cost increase")
        
        if savings_ratio > 20:
            if risk_level == "LOW":
                risk_level = "MEDIUM"
            risk_factors.append("High optimization potential indicates inefficiency")
        
        return {
            "risk_level": risk_level,
            "risk_factors": risk_factors,
            "recommendation": ReportGenerator._get_risk_recommendation(risk_level)
        }
    
    @staticmethod
    def _get_risk_recommendation(risk_level: str) -> str:
        """Get recommendation based on risk level."""
        recommendations = {
            "LOW": "Continue current monitoring and optimization practices",
            "MEDIUM": "Increase monitoring frequency and implement cost controls",
            "HIGH": "Immediate action required - review and implement cost optimization measures"
        }
        return recommendations.get(risk_level, "Review cost management practices")
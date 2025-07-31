"""
Formatting utilities for cost analytics display and reporting
"""
import polars as pl
from typing import Union, Optional, Dict, Any
from datetime import datetime, date
from decimal import Decimal


class CurrencyFormatter:
    """Utility for formatting currency values across the platform."""
    
    @staticmethod
    def format_currency(amount: Union[float, int, Decimal], 
                       currency: str = "USD", 
                       precision: int = 2,
                       include_symbol: bool = True) -> str:
        """
        Format numeric value as currency.
        
        Args:
            amount: Numeric amount to format
            currency: Currency code (default: USD)
            precision: Decimal places (default: 2)
            include_symbol: Whether to include currency symbol
            
        Returns:
            Formatted currency string
        """
        if amount is None:
            return "N/A"
        
        try:
            # Convert to float for formatting
            amount = float(amount)
            
            # Format with commas and precision
            formatted = f"{amount:,.{precision}f}"
            
            if include_symbol:
                if currency == "USD":
                    return f"${formatted}"
                else:
                    return f"{formatted} {currency}"
            
            return formatted
            
        except (ValueError, TypeError):
            return "Invalid Amount"
    
    @staticmethod
    def format_large_currency(amount: Union[float, int], 
                             currency: str = "USD",
                             abbreviate: bool = True) -> str:
        """
        Format large currency amounts with abbreviations (K, M, B).
        
        Args:
            amount: Numeric amount to format
            currency: Currency code
            abbreviate: Whether to use abbreviations for large numbers
            
        Returns:
            Formatted currency string with abbreviations
        """
        if amount is None:
            return "N/A"
        
        try:
            amount = float(amount)
            
            if not abbreviate:
                return CurrencyFormatter.format_currency(amount, currency)
            
            # Determine abbreviation
            if abs(amount) >= 1_000_000_000:
                abbreviated = amount / 1_000_000_000
                suffix = "B"
            elif abs(amount) >= 1_000_000:
                abbreviated = amount / 1_000_000
                suffix = "M"
            elif abs(amount) >= 1_000:
                abbreviated = amount / 1_000
                suffix = "K"
            else:
                return CurrencyFormatter.format_currency(amount, currency)
            
            # Format with appropriate precision
            if abbreviated >= 100:
                precision = 0
            elif abbreviated >= 10:
                precision = 1
            else:
                precision = 2
            
            formatted = f"{abbreviated:.{precision}f}{suffix}"
            
            if currency == "USD":
                return f"${formatted}"
            else:
                return f"{formatted} {currency}"
                
        except (ValueError, TypeError):
            return "Invalid Amount"


class NumberFormatter:
    """Utility for formatting numeric values and percentages."""
    
    @staticmethod
    def format_percentage(value: Union[float, int], 
                         precision: int = 1,
                         include_sign: bool = True) -> str:
        """
        Format numeric value as percentage.
        
        Args:
            value: Numeric value to format (e.g., 15.5 for 15.5%)
            precision: Decimal places
            include_sign: Whether to include + for positive values
            
        Returns:
            Formatted percentage string
        """
        if value is None:
            return "N/A"
        
        try:
            value = float(value)
            formatted = f"{value:.{precision}f}%"
            
            if include_sign and value > 0:
                formatted = f"+{formatted}"
            
            return formatted
            
        except (ValueError, TypeError):
            return "Invalid Percentage"
    
    @staticmethod
    def format_number(value: Union[float, int], 
                     precision: int = 0,
                     thousands_separator: bool = True) -> str:
        """
        Format numeric value with optional thousands separator.
        
        Args:
            value: Numeric value to format
            precision: Decimal places
            thousands_separator: Whether to include comma separators
            
        Returns:
            Formatted number string
        """
        if value is None:
            return "N/A"
        
        try:
            value = float(value)
            
            if thousands_separator:
                return f"{value:,.{precision}f}"
            else:
                return f"{value:.{precision}f}"
                
        except (ValueError, TypeError):
            return "Invalid Number"
    
    @staticmethod
    def format_large_number(value: Union[float, int], 
                           abbreviate: bool = True) -> str:
        """
        Format large numbers with abbreviations.
        
        Args:
            value: Numeric value to format
            abbreviate: Whether to use abbreviations
            
        Returns:
            Formatted number string
        """
        if value is None:
            return "N/A"
        
        try:
            value = float(value)
            
            if not abbreviate:
                return NumberFormatter.format_number(value)
            
            if abs(value) >= 1_000_000_000:
                abbreviated = value / 1_000_000_000
                suffix = "B"
            elif abs(value) >= 1_000_000:
                abbreviated = value / 1_000_000
                suffix = "M"
            elif abs(value) >= 1_000:
                abbreviated = value / 1_000
                suffix = "K"
            else:
                return NumberFormatter.format_number(value)
            
            precision = 1 if abbreviated < 10 else 0
            return f"{abbreviated:.{precision}f}{suffix}"
            
        except (ValueError, TypeError):
            return "Invalid Number"


class DateFormatter:
    """Utility for formatting dates across different data export types."""
    
    # Date format mappings for different export types
    DATE_FORMATS = {
        'CUR2.0': '%Y-%m',
        'FOCUS1.0': '%Y-%m',
        'COH': '%Y-%m-%d',
        'CARBON_EMISSION': '%Y-%m'
    }
    
    @staticmethod
    def format_billing_period(date_value: Union[str, datetime, date],
                             export_type: str = 'CUR2.0',
                             display_format: str = 'YYYY-MM') -> str:
        """
        Format billing period based on export type.
        
        Args:
            date_value: Date value to format
            export_type: Data export type
            display_format: Desired display format
            
        Returns:
            Formatted date string
        """
        if date_value is None:
            return "N/A"
        
        try:
            # Convert to datetime if string
            if isinstance(date_value, str):
                # Try to parse common formats
                for fmt in ['%Y-%m-%d', '%Y-%m', '%Y-%m-%d %H:%M:%S']:
                    try:
                        date_value = datetime.strptime(date_value, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return date_value  # Return as-is if can't parse
            
            # Format based on display preference
            if display_format == 'YYYY-MM':
                return date_value.strftime('%Y-%m')
            elif display_format == 'YYYY-MM-DD':
                return date_value.strftime('%Y-%m-%d')
            elif display_format == 'Mon YYYY':
                return date_value.strftime('%b %Y')
            elif display_format == 'Month YYYY':
                return date_value.strftime('%B %Y')
            else:
                return date_value.strftime(display_format)
                
        except (ValueError, TypeError, AttributeError):
            return str(date_value) if date_value else "Invalid Date"
    
    @staticmethod
    def format_relative_date(date_value: Union[str, datetime, date]) -> str:
        """
        Format date as relative time (e.g., "2 months ago").
        
        Args:
            date_value: Date value to format
            
        Returns:
            Relative date string
        """
        if date_value is None:
            return "N/A"
        
        try:
            # Convert to datetime if needed
            if isinstance(date_value, str):
                date_value = datetime.strptime(date_value, '%Y-%m-%d')
            elif isinstance(date_value, date):
                date_value = datetime.combine(date_value, datetime.min.time())
            
            now = datetime.now()
            diff = now - date_value
            
            days = diff.days
            
            if days == 0:
                return "Today"
            elif days == 1:
                return "Yesterday"
            elif days < 7:
                return f"{days} days ago"
            elif days < 30:
                weeks = days // 7
                return f"{weeks} week{'s' if weeks > 1 else ''} ago"
            elif days < 365:
                months = days // 30
                return f"{months} month{'s' if months > 1 else ''} ago"
            else:
                years = days // 365
                return f"{years} year{'s' if years > 1 else ''} ago"
                
        except (ValueError, TypeError, AttributeError):
            return "Invalid Date"
    
    @staticmethod
    def get_date_range_description(start_date: Optional[str], 
                                 end_date: Optional[str],
                                 export_type: str = 'CUR2.0') -> str:
        """
        Generate human-readable date range description.
        
        Args:
            start_date: Start date string
            end_date: End date string  
            export_type: Data export type
            
        Returns:
            Date range description
        """
        if not start_date and not end_date:
            return "All available data"
        
        formatted_start = DateFormatter.format_billing_period(start_date, export_type, 'Mon YYYY') if start_date else "Beginning"
        formatted_end = DateFormatter.format_billing_period(end_date, export_type, 'Mon YYYY') if end_date else "Latest"
        
        if start_date and end_date:
            if start_date == end_date:
                return formatted_start
            else:
                return f"{formatted_start} to {formatted_end}"
        elif start_date:
            return f"From {formatted_start}"
        else:
            return f"Through {formatted_end}"
"""
Exception handling helpers for Infralyzer.
"""
import traceback
from typing import Optional, Dict, Any
from ..exceptions import InfralyzerError
from ..logging_config import get_logger


def handle_exception(
    exception: Exception,
    error_class: type = InfralyzerError,
    error_code: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    logger_name: Optional[str] = None
) -> InfralyzerError:
    """
    Convert a generic exception to an Infralyzer exception with proper logging.
    
    Args:
        exception: Original exception
        error_class: Infralyzer exception class to use
        error_code: Error code for tracking
        context: Additional context information
        logger_name: Logger name to use
        
    Returns:
        Properly formatted Infralyzer exception
    """
    logger = get_logger(logger_name or "infralyzer.exceptions")
    
    error_msg = str(exception)
    if not error_msg:
        error_msg = f"Unexpected {type(exception).__name__}"
    
    # Log the full traceback for debugging
    logger.debug(f"Exception details: {traceback.format_exc()}")
    
    # Create the new exception
    infralyzer_error = error_class(
        error_msg,
        error_code=error_code,
        context=context or {}
    )
    
    # Set the original exception as the cause
    infralyzer_error.__cause__ = exception
    
    return infralyzer_error


def log_and_raise(
    message: str,
    exception_class: type = InfralyzerError,
    error_code: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    logger_name: Optional[str] = None
) -> None:
    """
    Log an error and raise an exception.
    
    Args:
        message: Error message
        exception_class: Exception class to raise
        error_code: Error code for tracking
        context: Additional context information
        logger_name: Logger name to use
        
    Raises:
        InfralyzerError: The specified exception
    """
    logger = get_logger(logger_name or "infralyzer.exceptions")
    logger.error(message)
    
    raise exception_class(
        message,
        error_code=error_code,
        context=context or {}
    )


def safe_execute(
    func,
    *args,
    default_return=None,
    error_class: type = InfralyzerError,
    error_code: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    logger_name: Optional[str] = None,
    **kwargs
):
    """
    Safely execute a function with proper exception handling.
    
    Args:
        func: Function to execute
        *args: Arguments for the function
        default_return: Value to return if function fails
        error_class: Exception class to use for errors
        error_code: Error code for tracking
        context: Additional context information
        logger_name: Logger name to use
        **kwargs: Keyword arguments for the function
        
    Returns:
        Function result or default_return if function fails
        
    Raises:
        InfralyzerError: If function fails and default_return is None
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if default_return is not None:
            logger = get_logger(logger_name or "infralyzer.exceptions")
            logger.warning(f"Function {func.__name__} failed, returning default: {str(e)}")
            return default_return
        
        # Convert to Infralyzer exception and re-raise
        infralyzer_error = handle_exception(
            e,
            error_class=error_class,
            error_code=error_code,
            context=context,
            logger_name=logger_name
        )
        raise infralyzer_error
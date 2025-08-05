"""
Infralyzer Logging Configuration - Centralized logging setup.
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from .constants import LOG_LEVELS


class InfralyzerLogger:
    """Centralized logger for Infralyzer package."""
    
    _loggers = {}
    _configured = False
    
    @classmethod
    def get_logger(cls, name: str, level: str = "INFO") -> logging.Logger:
        """
        Get a configured logger instance.
        
        Args:
            name: Logger name (typically __name__)
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            
        Returns:
            Configured logger instance
        """
        if not cls._configured:
            cls._setup_logging()
        
        if name not in cls._loggers:
            logger = logging.getLogger(name)
            logger.setLevel(LOG_LEVELS.get(level.upper(), LOG_LEVELS["INFO"]))
            cls._loggers[name] = logger
            
        return cls._loggers[name]
    
    @classmethod
    def _setup_logging(cls):
        """Setup basic logging configuration."""
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Setup console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Configure root logger
        root_logger = logging.getLogger('infralyzer')
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        root_logger.propagate = False
        
        cls._configured = True
    
    @classmethod
    def setup_file_logging(cls, log_file: Optional[Path] = None, level: str = "INFO"):
        """
        Setup file logging in addition to console logging.
        
        Args:
            log_file: Path to log file (defaults to ./logs/infralyzer.log)
            level: Log level for file handler
        """
        if log_file is None:
            log_file = Path("logs/infralyzer.log")
        
        # Create logs directory if it doesn't exist
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(LOG_LEVELS.get(level.upper(), LOG_LEVELS["INFO"]))
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        # Add to root logger
        root_logger = logging.getLogger('infralyzer')
        root_logger.addHandler(file_handler)
    
    @classmethod
    def set_level(cls, level: str):
        """
        Set logging level for all infralyzer loggers.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        log_level = LOG_LEVELS.get(level.upper(), LOG_LEVELS["INFO"])
        
        # Update root logger
        root_logger = logging.getLogger('infralyzer')
        root_logger.setLevel(log_level)
        
        # Update existing loggers
        for logger in cls._loggers.values():
            logger.setLevel(log_level)


# Convenience function for getting logger
def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    return InfralyzerLogger.get_logger(name, level)
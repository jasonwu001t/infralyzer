"""
Performance monitoring and metrics utilities for Infralyzer.
"""
import time
import functools
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from ..logging_config import get_logger
from ..constants import DEFAULT_TIMEOUT_SECONDS


class PerformanceMonitor:
    """Monitor performance metrics for queries and operations."""
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {}
        self.logger = get_logger("infralyzer.PerformanceMonitor")
    
    def track_execution(self, operation_name: str):
        """
        Decorator to track execution time and success rate.
        
        Args:
            operation_name: Name of the operation being tracked
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                operation_key = f"{operation_name}_{func.__name__}"
                
                try:
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    # Track successful execution
                    self._record_metric(operation_key, execution_time, True)
                    self.logger.debug(f"{operation_key} completed in {execution_time:.3f}s")
                    
                    return result
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    
                    # Track failed execution
                    self._record_metric(operation_key, execution_time, False)
                    self.logger.warning(f"{operation_key} failed after {execution_time:.3f}s: {str(e)}")
                    
                    raise
            
            return wrapper
        return decorator
    
    def _record_metric(self, operation_key: str, execution_time: float, success: bool):
        """Record performance metric."""
        if operation_key not in self.metrics:
            self.metrics[operation_key] = {
                "total_executions": 0,
                "successful_executions": 0,
                "total_time": 0.0,
                "min_time": float('inf'),
                "max_time": 0.0,
                "last_execution": None
            }
        
        metrics = self.metrics[operation_key]
        metrics["total_executions"] += 1
        metrics["total_time"] += execution_time
        metrics["min_time"] = min(metrics["min_time"], execution_time)
        metrics["max_time"] = max(metrics["max_time"], execution_time)
        metrics["last_execution"] = datetime.now().isoformat()
        
        if success:
            metrics["successful_executions"] += 1
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary for all tracked operations.
        
        Returns:
            Dictionary with performance statistics
        """
        summary = {}
        
        for operation_key, metrics in self.metrics.items():
            if metrics["total_executions"] > 0:
                summary[operation_key] = {
                    "total_executions": metrics["total_executions"],
                    "success_rate": metrics["successful_executions"] / metrics["total_executions"],
                    "average_time": metrics["total_time"] / metrics["total_executions"],
                    "min_time": metrics["min_time"],
                    "max_time": metrics["max_time"],
                    "last_execution": metrics["last_execution"]
                }
        
        return summary
    
    def reset_metrics(self):
        """Reset all performance metrics."""
        self.metrics.clear()
        self.logger.info("Performance metrics reset")


class QueryTimeoutManager:
    """Manage query timeouts and cancellation."""
    
    def __init__(self, default_timeout: int = DEFAULT_TIMEOUT_SECONDS):
        self.default_timeout = default_timeout
        self.active_queries: Dict[str, datetime] = {}
        self.logger = get_logger("infralyzer.QueryTimeoutManager")
    
    def with_timeout(self, timeout: Optional[int] = None):
        """
        Decorator to enforce query timeout.
        
        Args:
            timeout: Timeout in seconds (uses default if None)
        """
        timeout = timeout or self.default_timeout
        
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                query_id = f"{func.__name__}_{int(time.time())}"
                self.active_queries[query_id] = datetime.now()
                
                try:
                    # Note: Actual timeout implementation would depend on the specific database driver
                    # For now, we just track the queries
                    self.logger.debug(f"Starting query {query_id} with {timeout}s timeout")
                    
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    if execution_time > timeout:
                        self.logger.warning(f"Query {query_id} exceeded timeout ({execution_time:.1f}s > {timeout}s)")
                    
                    return result
                    
                finally:
                    self.active_queries.pop(query_id, None)
            
            return wrapper
        return decorator
    
    def get_active_queries(self) -> Dict[str, Any]:
        """
        Get information about currently active queries.
        
        Returns:
            Dictionary with active query information
        """
        now = datetime.now()
        active_info = {}
        
        for query_id, start_time in self.active_queries.items():
            duration = (now - start_time).total_seconds()
            active_info[query_id] = {
                "start_time": start_time.isoformat(),
                "duration_seconds": duration,
                "is_long_running": duration > self.default_timeout
            }
        
        return active_info


# Global instances for easy access
performance_monitor = PerformanceMonitor()
timeout_manager = QueryTimeoutManager()


# Convenience decorators
def track_performance(operation_name: str):
    """Convenience decorator for performance tracking."""
    return performance_monitor.track_execution(operation_name)


def with_timeout(timeout: Optional[int] = None):
    """Convenience decorator for query timeout."""
    return timeout_manager.with_timeout(timeout)
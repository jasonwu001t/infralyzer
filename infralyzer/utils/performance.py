"""
Performance monitoring and optimization utilities
"""
import time
import functools
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
import threading


class QueryProfiler:
    """Utility for profiling SQL query performance."""
    
    def __init__(self):
        self.query_stats = {}
        self._lock = threading.Lock()
    
    def profile_query(self, query_name: str = None):
        """
        Decorator to profile SQL query execution time.
        
        Args:
            query_name: Optional name for the query
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                name = query_name or f"{func.__module__}.{func.__name__}"
                
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    self._record_query_stats(name, execution_time, success=True)
                    return result
                except Exception as e:
                    execution_time = time.time() - start_time
                    self._record_query_stats(name, execution_time, success=False, error=str(e))
                    raise
            
            return wrapper
        return decorator
    
    def _record_query_stats(self, query_name: str, execution_time: float, 
                           success: bool, error: Optional[str] = None):
        """Record query execution statistics."""
        with self._lock:
            if query_name not in self.query_stats:
                self.query_stats[query_name] = {
                    'total_executions': 0,
                    'successful_executions': 0,
                    'failed_executions': 0,
                    'total_time': 0.0,
                    'min_time': float('inf'),
                    'max_time': 0.0,
                    'last_execution': None,
                    'last_error': None
                }
            
            stats = self.query_stats[query_name]
            stats['total_executions'] += 1
            stats['total_time'] += execution_time
            stats['min_time'] = min(stats['min_time'], execution_time)
            stats['max_time'] = max(stats['max_time'], execution_time)
            stats['last_execution'] = datetime.now().isoformat()
            
            if success:
                stats['successful_executions'] += 1
                stats['last_error'] = None
            else:
                stats['failed_executions'] += 1
                stats['last_error'] = error
    
    def get_query_stats(self, query_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get query performance statistics.
        
        Args:
            query_name: Optional specific query name
            
        Returns:
            Query statistics
        """
        with self._lock:
            if query_name:
                if query_name in self.query_stats:
                    stats = self.query_stats[query_name].copy()
                    stats['avg_time'] = stats['total_time'] / stats['total_executions'] if stats['total_executions'] > 0 else 0
                    stats['success_rate'] = (stats['successful_executions'] / stats['total_executions'] * 100) if stats['total_executions'] > 0 else 0
                    return {query_name: stats}
                else:
                    return {}
            else:
                # Return all stats with calculated averages
                result = {}
                for name, stats in self.query_stats.items():
                    enhanced_stats = stats.copy()
                    enhanced_stats['avg_time'] = stats['total_time'] / stats['total_executions'] if stats['total_executions'] > 0 else 0
                    enhanced_stats['success_rate'] = (stats['successful_executions'] / stats['total_executions'] * 100) if stats['total_executions'] > 0 else 0
                    result[name] = enhanced_stats
                
                return result
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get overall performance summary."""
        with self._lock:
            if not self.query_stats:
                return {"message": "No query statistics available"}
            
            total_executions = sum(stats['total_executions'] for stats in self.query_stats.values())
            total_time = sum(stats['total_time'] for stats in self.query_stats.values())
            successful_executions = sum(stats['successful_executions'] for stats in self.query_stats.values())
            
            # Find slowest and fastest queries
            slowest_query = max(self.query_stats.items(), 
                              key=lambda x: x[1]['max_time'] if x[1]['total_executions'] > 0 else 0)
            fastest_avg_query = min(self.query_stats.items(),
                                  key=lambda x: x[1]['total_time'] / x[1]['total_executions'] if x[1]['total_executions'] > 0 else float('inf'))
            
            return {
                "total_queries": len(self.query_stats),
                "total_executions": total_executions,
                "total_time": round(total_time, 3),
                "avg_execution_time": round(total_time / total_executions, 3) if total_executions > 0 else 0,
                "success_rate": round((successful_executions / total_executions * 100), 1) if total_executions > 0 else 0,
                "slowest_query": {
                    "name": slowest_query[0],
                    "max_time": round(slowest_query[1]['max_time'], 3)
                },
                "fastest_avg_query": {
                    "name": fastest_avg_query[0],
                    "avg_time": round(fastest_avg_query[1]['total_time'] / fastest_avg_query[1]['total_executions'], 3) if fastest_avg_query[1]['total_executions'] > 0 else 0
                }
            }


class CacheManager:
    """Simple in-memory cache for query results."""
    
    def __init__(self, default_ttl: int = 300):  # 5 minutes default
        """
        Initialize cache manager.
        
        Args:
            default_ttl: Default time-to-live in seconds
        """
        self.cache = {}
        self.default_ttl = default_ttl
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get cached value.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if datetime.now() < expiry:
                    return value
                else:
                    # Remove expired entry
                    del self.cache[key]
            
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set cached value.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        ttl = ttl or self.default_ttl
        expiry = datetime.now() + timedelta(seconds=ttl)
        
        with self._lock:
            self.cache[key] = (value, expiry)
    
    def invalidate(self, key: str) -> bool:
        """
        Remove specific key from cache.
        
        Args:
            key: Cache key to remove
            
        Returns:
            True if key was found and removed
        """
        with self._lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear entire cache."""
        with self._lock:
            self.cache.clear()
    
    def cleanup_expired(self) -> int:
        """
        Remove expired entries from cache.
        
        Returns:
            Number of entries removed
        """
        now = datetime.now()
        expired_keys = []
        
        with self._lock:
            for key, (value, expiry) in self.cache.items():
                if now >= expiry:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.cache[key]
        
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_entries = len(self.cache)
            expired_count = 0
            now = datetime.now()
            
            for value, expiry in self.cache.values():
                if now >= expiry:
                    expired_count += 1
            
            return {
                "total_entries": total_entries,
                "active_entries": total_entries - expired_count,
                "expired_entries": expired_count,
                "default_ttl": self.default_ttl
            }
    
    def cache_result(self, ttl: Optional[int] = None):
        """
        Decorator to cache function results.
        
        Args:
            ttl: Time-to-live for cached result
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Create cache key from function name and arguments
                key = f"{func.__module__}.{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                
                # Try to get from cache first
                cached_result = self.get(key)
                if cached_result is not None:
                    return cached_result
                
                # Execute function and cache result
                result = func(*args, **kwargs)
                self.set(key, result, ttl)
                return result
            
            return wrapper
        return decorator


# Global instances for convenience
query_profiler = QueryProfiler()
cache_manager = CacheManager()
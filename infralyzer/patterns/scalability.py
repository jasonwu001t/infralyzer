"""
Scalability patterns and utilities for future growth of Infralyzer.
"""
from typing import Dict, Any, List, Optional, Protocol, TypeVar, Generic
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps, lru_cache
import weakref
from abc import ABC, abstractmethod

from ..logging_config import get_logger
from ..exceptions import InfralyzerError
from ..constants import DEFAULT_TIMEOUT_SECONDS


T = TypeVar('T')


class Cacheable(Protocol):
    """Protocol for cacheable operations."""
    
    def cache_key(self) -> str:
        """Generate a cache key for this operation."""
        ...


class ScalableAnalytics(ABC, Generic[T]):
    """
    Abstract base class for scalable analytics operations.
    
    Provides patterns for:
    - Async execution
    - Caching
    - Batch processing
    - Resource management
    """
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.logger = get_logger(f"infralyzer.{self.__class__.__name__}")
        self._cache = {}
        self._active_operations = weakref.WeakSet()
    
    @abstractmethod
    async def execute_async(self, operation: T) -> Any:
        """Execute operation asynchronously."""
        pass
    
    def execute_batch(self, operations: List[T], max_concurrent: int = None) -> List[Any]:
        """
        Execute multiple operations concurrently.
        
        Args:
            operations: List of operations to execute
            max_concurrent: Maximum concurrent operations
            
        Returns:
            List of results in order
        """
        max_concurrent = max_concurrent or self.max_workers
        
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit all operations
            future_to_operation = {
                executor.submit(self._execute_sync_wrapper, op): op 
                for op in operations
            }
            
            results = [None] * len(operations)
            
            # Collect results in order
            for future in as_completed(future_to_operation):
                operation = future_to_operation[future]
                operation_index = operations.index(operation)
                
                try:
                    results[operation_index] = future.result()
                except Exception as e:
                    self.logger.error(f"Operation failed: {str(e)}")
                    results[operation_index] = {"error": str(e)}
        
        return results
    
    def _execute_sync_wrapper(self, operation: T) -> Any:
        """Synchronous wrapper for async operations."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.execute_async(operation))
        finally:
            loop.close()
    
    def with_caching(self, ttl_seconds: int = 3600):
        """
        Decorator to add caching to operations.
        
        Args:
            ttl_seconds: Cache time-to-live in seconds
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                
                # Check cache
                if cache_key in self._cache:
                    cached_result, timestamp = self._cache[cache_key]
                    import time
                    if time.time() - timestamp < ttl_seconds:
                        self.logger.debug(f"Cache hit for {cache_key}")
                        return cached_result
                
                # Execute and cache
                result = func(*args, **kwargs)
                import time
                self._cache[cache_key] = (result, time.time())
                self.logger.debug(f"Cached result for {cache_key}")
                
                return result
            return wrapper
        return decorator


class QueryBatcher:
    """Batch and optimize SQL queries for better performance."""
    
    def __init__(self, batch_size: int = 10, flush_interval: float = 1.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.pending_queries = []
        self.logger = get_logger("infralyzer.QueryBatcher")
        self._last_flush = 0.0
    
    def add_query(self, sql: str, callback: callable = None) -> None:
        """
        Add a query to the batch.
        
        Args:
            sql: SQL query to execute
            callback: Optional callback for result handling
        """
        import time
        self.pending_queries.append({
            "sql": sql,
            "callback": callback,
            "timestamp": time.time()
        })
        
        # Auto-flush if batch is full or interval exceeded
        if (len(self.pending_queries) >= self.batch_size or 
            time.time() - self._last_flush > self.flush_interval):
            self.flush()
    
    def flush(self) -> List[Any]:
        """Execute all pending queries and return results."""
        if not self.pending_queries:
            return []
        
        queries = self.pending_queries.copy()
        self.pending_queries.clear()
        
        import time
        self._last_flush = time.time()
        
        self.logger.info(f"Executing batch of {len(queries)} queries")
        
        results = []
        for query_info in queries:
            try:
                # Simulate query execution (replace with actual engine call)
                result = {"sql": query_info["sql"], "status": "executed"}
                
                if query_info["callback"]:
                    query_info["callback"](result)
                
                results.append(result)
                
            except Exception as e:
                error_result = {"sql": query_info["sql"], "error": str(e)}
                results.append(error_result)
                self.logger.error(f"Query failed: {str(e)}")
        
        return results


class ResourcePool(Generic[T]):
    """
    Generic resource pool for managing expensive resources.
    
    Useful for database connections, API clients, etc.
    """
    
    def __init__(self, factory: callable, max_size: int = 10, min_size: int = 2):
        self.factory = factory
        self.max_size = max_size
        self.min_size = min_size
        self.pool = []
        self.in_use = set()
        self.logger = get_logger("infralyzer.ResourcePool")
        
        # Pre-populate with minimum resources
        for _ in range(min_size):
            self.pool.append(self.factory())
    
    def acquire(self) -> T:
        """Acquire a resource from the pool."""
        if self.pool:
            resource = self.pool.pop()
        elif len(self.in_use) < self.max_size:
            resource = self.factory()
            self.logger.debug("Created new resource")
        else:
            raise InfralyzerError("Resource pool exhausted")
        
        self.in_use.add(resource)
        return resource
    
    def release(self, resource: T) -> None:
        """Release a resource back to the pool."""
        if resource in self.in_use:
            self.in_use.remove(resource)
            if len(self.pool) < self.max_size:
                self.pool.append(resource)
                self.logger.debug("Resource returned to pool")
            else:
                # Pool is full, discard resource
                if hasattr(resource, 'close'):
                    resource.close()
                self.logger.debug("Resource discarded (pool full)")
    
    def __enter__(self):
        """Context manager entry."""
        self.resource = self.acquire()
        return self.resource
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release(self.resource)


class LoadBalancer:
    """Simple load balancer for distributing operations across engines."""
    
    def __init__(self, engines: List[Any]):
        self.engines = engines
        self.current_index = 0
        self.engine_stats = {i: {"requests": 0, "errors": 0} for i in range(len(engines))}
        self.logger = get_logger("infralyzer.LoadBalancer")
    
    def get_next_engine(self) -> Any:
        """Get the next engine using round-robin strategy."""
        engine = self.engines[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.engines)
        self.engine_stats[self.current_index - 1]["requests"] += 1
        return engine
    
    def get_best_engine(self) -> Any:
        """Get engine with lowest error rate."""
        best_index = 0
        best_ratio = float('inf')
        
        for i, stats in self.engine_stats.items():
            if stats["requests"] == 0:
                return self.engines[i]  # Prefer unused engines
            
            error_ratio = stats["errors"] / stats["requests"]
            if error_ratio < best_ratio:
                best_ratio = error_ratio
                best_index = i
        
        self.engine_stats[best_index]["requests"] += 1
        return self.engines[best_index]
    
    def record_error(self, engine: Any) -> None:
        """Record an error for an engine."""
        try:
            engine_index = self.engines.index(engine)
            self.engine_stats[engine_index]["errors"] += 1
        except ValueError:
            self.logger.warning("Attempted to record error for unknown engine")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        return {
            "engines": len(self.engines),
            "total_requests": sum(stats["requests"] for stats in self.engine_stats.values()),
            "total_errors": sum(stats["errors"] for stats in self.engine_stats.values()),
            "engine_stats": self.engine_stats
        }


class CircuitBreaker:
    """
    Circuit breaker pattern for fault tolerance.
    
    Prevents cascading failures by temporarily disabling failing operations.
    """
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.logger = get_logger("infralyzer.CircuitBreaker")
    
    def call(self, func: callable, *args, **kwargs):
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            InfralyzerError: If circuit is open
        """
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                self.logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise InfralyzerError("Circuit breaker is OPEN - operation blocked")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        
        import time
        return time.time() - self.last_failure_time > self.reset_timeout
    
    def _on_success(self) -> None:
        """Handle successful operation."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            self.logger.info("Circuit breaker reset to CLOSED state")
    
    def _on_failure(self) -> None:
        """Handle failed operation."""
        self.failure_count += 1
        import time
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


# Convenience decorators
def with_circuit_breaker(failure_threshold: int = 5, reset_timeout: float = 60.0):
    """Decorator to add circuit breaker protection to functions."""
    breaker = CircuitBreaker(failure_threshold, reset_timeout)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator


def with_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to add retry logic to functions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    
                    import time
                    time.sleep(current_delay)
                    current_delay *= backoff
                    
                    logger = get_logger("infralyzer.retry")
                    logger.warning(f"Attempt {attempt} failed, retrying in {current_delay}s: {str(e)}")
        
        return wrapper
    return decorator
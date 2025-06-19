"""
Performance monitoring and optimization utilities.
"""
import time
import functools
import psutil
import streamlit as st
from typing import Callable, Any, Dict
import logging
from contextlib import contextmanager
import pandas as pd
from datetime import datetime

from backend.logs.logger_setup import setup_logger

logger = setup_logger('performance', 'performance.log')

class PerformanceMonitor:
    """Monitor and track performance metrics."""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = time.time()
    
    def log_metric(self, name: str, value: float, unit: str = "seconds"):
        """Log a performance metric."""
        self.metrics[name] = {
            'value': value,
            'unit': unit,
            'timestamp': datetime.now()
        }
        logger.info(f"Performance metric - {name}: {value:.3f} {unit}")
    
    def get_system_metrics(self) -> Dict:
        """Get current system performance metrics."""
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'memory_available_gb': psutil.virtual_memory().available / (1024**3),
            'disk_usage_percent': psutil.disk_usage('/').percent
        }
    
    def display_metrics_sidebar(self):
        """Display performance metrics in Streamlit sidebar."""
        with st.sidebar:
            st.subheader("⚡ Performance Metrics")
            
            system_metrics = self.get_system_metrics()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("CPU", f"{system_metrics['cpu_percent']:.1f}%")
                st.metric("Memory", f"{system_metrics['memory_percent']:.1f}%")
            
            with col2:
                st.metric("Available RAM", f"{system_metrics['memory_available_gb']:.1f}GB")
                st.metric("Disk Usage", f"{system_metrics['disk_usage_percent']:.1f}%")
            
            # Show recent performance metrics
            if self.metrics:
                st.subheader("Recent Operations")
                for name, metric in list(self.metrics.items())[-5:]:
                    st.text(f"{name}: {metric['value']:.2f}s")

def timing_decorator(func_name: str = None):
    """Decorator to measure function execution time."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            name = func_name or func.__name__
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                # Log performance
                logger.info(f"Function '{name}' executed in {execution_time:.3f} seconds")
                
                # Store in session state for display
                if 'performance_metrics' not in st.session_state:
                    st.session_state.performance_metrics = {}
                st.session_state.performance_metrics[name] = execution_time
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Function '{name}' failed after {execution_time:.3f} seconds: {e}")
                raise
                
        return wrapper
    return decorator

@contextmanager
def performance_context(operation_name: str):
    """Context manager for measuring operation performance."""
    start_time = time.time()
    start_memory = psutil.virtual_memory().used / (1024**2)  # MB
    
    logger.info(f"Starting operation: {operation_name}")
    
    try:
        yield
        
    finally:
        end_time = time.time()
        end_memory = psutil.virtual_memory().used / (1024**2)  # MB
        
        execution_time = end_time - start_time
        memory_delta = end_memory - start_memory
        
        logger.info(f"Operation '{operation_name}' completed:")
        logger.info(f"  - Execution time: {execution_time:.3f} seconds")
        logger.info(f"  - Memory change: {memory_delta:+.1f} MB")

def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage."""
    if df.empty:
        return df
    
    original_memory = df.memory_usage(deep=True).sum() / (1024**2)  # MB
    
    # Optimize numeric columns
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Optimize object columns
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
            df[col] = df[col].astype('category')
    
    optimized_memory = df.memory_usage(deep=True).sum() / (1024**2)  # MB
    memory_reduction = ((original_memory - optimized_memory) / original_memory) * 100
    
    logger.info(f"DataFrame optimized: {original_memory:.1f}MB → {optimized_memory:.1f}MB "
               f"({memory_reduction:.1f}% reduction)")
    
    return df

def batch_process_data(data_list: list, batch_size: int = 100, 
                      process_func: Callable = None) -> list:
    """Process data in batches for better memory management."""
    results = []
    total_batches = len(data_list) // batch_size + (1 if len(data_list) % batch_size else 0)
    
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} items)")
        
        if process_func:
            batch_result = process_func(batch)
            results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
        else:
            results.extend(batch)
    
    return results

class DataLoadingOptimizer:
    """Optimize data loading operations."""
    
    @staticmethod
    def suggest_optimizations(df: pd.DataFrame, operation: str) -> Dict[str, str]:
        """Suggest optimizations based on data characteristics."""
        suggestions = {}
        
        if df.empty:
            return {"warning": "DataFrame is empty"}
        
        # Memory usage suggestions
        memory_mb = df.memory_usage(deep=True).sum() / (1024**2)
        if memory_mb > 100:
            suggestions["memory"] = f"Large dataset ({memory_mb:.1f}MB). Consider chunked processing."
        
        # Date filtering suggestions
        if 'time' in df.columns or 'date' in df.columns:
            suggestions["indexing"] = "Consider setting datetime column as index for faster filtering."
        
        # Categorical data suggestions
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            if df[col].nunique() / len(df) < 0.1:
                suggestions[f"categorical_{col}"] = f"Convert '{col}' to categorical for memory savings."
        
        # Operation-specific suggestions
        if operation == "filtering":
            suggestions["filtering"] = "Use vectorized operations instead of iterative filtering."
        elif operation == "aggregation":
            suggestions["aggregation"] = "Consider using groupby with appropriate aggregation functions."
        
        return suggestions

def create_performance_dashboard():
    """Create a performance monitoring dashboard."""
    st.subheader("🚀 Performance Dashboard")
    
    # System metrics
    col1, col2, col3, col4 = st.columns(4)
    
    system_metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'memory_available_gb': psutil.virtual_memory().available / (1024**3),
        'disk_usage_percent': psutil.disk_usage('/').percent
    }
    
    with col1:
        st.metric("CPU Usage", f"{system_metrics['cpu_percent']:.1f}%")
    with col2:
        st.metric("Memory Usage", f"{system_metrics['memory_percent']:.1f}%")
    with col3:
        st.metric("Available RAM", f"{system_metrics['memory_available_gb']:.1f}GB")
    with col4:
        st.metric("Disk Usage", f"{system_metrics['disk_usage_percent']:.1f}%")
    
    # Performance metrics from session state
    if 'performance_metrics' in st.session_state:
        st.subheader("Recent Operation Times")
        metrics_df = pd.DataFrame([
            {"Operation": name, "Time (seconds)": time_val}
            for name, time_val in st.session_state.performance_metrics.items()
        ])
        st.dataframe(metrics_df, use_container_width=True)
    
    # Cache information
    st.subheader("Cache Status")
    cache_info = st.cache_data.get_stats()
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Cache Hits", cache_info[0].cache_hits if cache_info else 0)
    with col2:
        st.metric("Cache Misses", cache_info[0].cache_misses if cache_info else 0)

# Global performance monitor instance
performance_monitor = PerformanceMonitor()

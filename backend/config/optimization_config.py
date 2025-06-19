"""
Configuration for optimization features.
This module controls which optimizations are enabled.
"""

import os
from typing import Dict, Any

# Optimization feature flags
OPTIMIZATION_CONFIG = {
    # Enable optimized data validation
    'enable_data_validation': True,
    
    # Enable smart caching with reduced TTL
    'enable_smart_caching': True,
    
    # Enable pre-query data availability checks
    'enable_availability_checks': True,
    
    # Enable optimized database queries
    'enable_query_optimization': True,
    
    # Enable intelligent error handling (info vs warning vs error)
    'enable_smart_logging': True,
    
    # Enable request validation before processing
    'enable_request_validation': True,
    
    # Cache TTL settings (in seconds)
    'cache_ttl': {
        'plants': 3600,      # 1 hour
        'generation': 1800,  # 30 minutes
        'consumption': 1800, # 30 minutes
        'settlement': 1800,  # 30 minutes
        'tod': 3600         # 1 hour
    },
    
    # Retry settings
    'retry_config': {
        'max_retries': 2,
        'retry_delay': 0.5
    },
    
    # Validation settings
    'validation_config': {
        'max_date_range_days': 365,
        'future_date_tolerance_days': 1,
        'enable_plant_existence_check': True,
        'enable_data_availability_check': True
    },
    
    # Logging settings
    'logging_config': {
        'log_data_availability_summary': True,
        'log_performance_metrics': True,
        'suppress_future_date_warnings': True,
        'suppress_no_data_warnings': True
    }
}

def get_optimization_config() -> Dict[str, Any]:
    """
    Get the current optimization configuration.
    
    Returns:
        Dictionary with optimization settings
    """
    return OPTIMIZATION_CONFIG.copy()

def is_optimization_enabled(feature: str) -> bool:
    """
    Check if a specific optimization feature is enabled.
    
    Args:
        feature: Name of the optimization feature
        
    Returns:
        True if enabled, False otherwise
    """
    return OPTIMIZATION_CONFIG.get(feature, False)

def get_cache_ttl(data_type: str) -> int:
    """
    Get cache TTL for a specific data type.
    
    Args:
        data_type: Type of data (plants, generation, consumption, etc.)
        
    Returns:
        TTL in seconds
    """
    return OPTIMIZATION_CONFIG.get('cache_ttl', {}).get(data_type, 1800)

def get_retry_config() -> Dict[str, Any]:
    """
    Get retry configuration.
    
    Returns:
        Dictionary with retry settings
    """
    return OPTIMIZATION_CONFIG.get('retry_config', {'max_retries': 2, 'retry_delay': 0.5})

def get_validation_config() -> Dict[str, Any]:
    """
    Get validation configuration.
    
    Returns:
        Dictionary with validation settings
    """
    return OPTIMIZATION_CONFIG.get('validation_config', {})

def get_logging_config() -> Dict[str, Any]:
    """
    Get logging configuration.
    
    Returns:
        Dictionary with logging settings
    """
    return OPTIMIZATION_CONFIG.get('logging_config', {})

def update_optimization_config(updates: Dict[str, Any]):
    """
    Update optimization configuration.
    
    Args:
        updates: Dictionary with configuration updates
    """
    global OPTIMIZATION_CONFIG
    OPTIMIZATION_CONFIG.update(updates)

# Environment-based configuration overrides
def load_environment_config():
    """Load configuration overrides from environment variables."""
    
    # Check if we're in development mode
    if os.getenv('DEVELOPMENT_MODE', 'false').lower() == 'true':
        OPTIMIZATION_CONFIG.update({
            'enable_smart_logging': True,
            'logging_config': {
                'log_data_availability_summary': True,
                'log_performance_metrics': True,
                'suppress_future_date_warnings': False,  # Show all warnings in dev
                'suppress_no_data_warnings': False
            }
        })
    
    # Check if we're in production mode
    if os.getenv('PRODUCTION_MODE', 'false').lower() == 'true':
        OPTIMIZATION_CONFIG.update({
            'enable_smart_logging': True,
            'logging_config': {
                'log_data_availability_summary': False,
                'log_performance_metrics': False,
                'suppress_future_date_warnings': True,  # Suppress in production
                'suppress_no_data_warnings': True
            }
        })

# Load environment configuration on import
load_environment_config()
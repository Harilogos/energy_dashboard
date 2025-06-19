"""
Optimized database data manager with improved error handling and performance.
This module provides clean, efficient data access with proper validation.
"""

import pandas as pd
import streamlit as st
import traceback
from functools import wraps
from datetime import datetime, timedelta

from backend.data.db_data_optimized import (
    get_generation_data_optimized,
    get_consumption_data_optimized,
    get_settlement_data_optimized,
    get_settlement_tod_data_optimized,
    get_plants_optimized,
    get_consumption_units_for_plant_optimized
)
from backend.data.data_validator import (
    DataAvailabilityChecker, 
    validate_date_range, 
    log_data_availability_summary,
    get_recommended_date_range
)
from backend.utils.client_mapping import (
    get_client_name_from_plant_name,
    get_plant_id_from_plant_name,
    validate_client_plant_mapping
)
from backend.logs.logger_setup import setup_logger
from backend.config.tod_config import get_tod_slots

# Configure logging
logger = setup_logger('db_data_manager_optimized', 'db_data_manager.log')

def smart_cache_and_retry(ttl=3600, max_retries=2):
    """Combined caching and retry decorator with optimized parameters"""
    def decorator(func):
        @st.cache_data(ttl=ttl)
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(0.5)
            logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {last_exception}")
            return pd.DataFrame()  # Return empty DataFrame instead of raising
        return wrapper
    return decorator

class OptimizedDataManager:
    """Centralized data manager with validation and optimization"""
    
    def __init__(self):
        self.plants_cache = None
        self.cache_timestamp = None
        self.cache_ttl = 3600  # 1 hour
    
    def get_plants(self):
        """Get plants with intelligent caching"""
        try:
            current_time = datetime.now()
            
            # Check if cache is still valid
            if (self.plants_cache is not None and 
                self.cache_timestamp is not None and 
                (current_time - self.cache_timestamp).seconds < self.cache_ttl):
                return self.plants_cache
            
            # Refresh cache
            self.plants_cache = get_plants_optimized()
            self.cache_timestamp = current_time
            
            return self.plants_cache
            
        except Exception as e:
            logger.error(f"Failed to get plants: {e}")
            return {}
    
    def get_plant_id(self, plant_name):
        """Get plant ID with validation"""
        try:
            if isinstance(plant_name, dict):
                return plant_name.get('plant_id')
            
            plant_id = get_plant_id_from_plant_name(plant_name)
            if not plant_id:
                logger.warning(f"Could not find plant_id for {plant_name}")
                return None
            
            return plant_id
            
        except Exception as e:
            logger.error(f"Failed to get plant ID for {plant_name}: {e}")
            return None
    
    def validate_request(self, plant_name, start_date, end_date=None):
        """Validate data request before processing"""
        try:
            if end_date is None:
                end_date = start_date
            
            # Convert dates to strings if they're date objects
            if hasattr(start_date, 'strftime'):
                start_str = start_date.strftime('%Y-%m-%d')
            else:
                start_str = start_date
            
            if hasattr(end_date, 'strftime'):
                end_str = end_date.strftime('%Y-%m-%d')
            else:
                end_str = end_date
            
            # Validate date range
            is_valid, error_msg = validate_date_range(start_str, end_str)
            if not is_valid:
                logger.error(f"Invalid request for {plant_name}: {error_msg}")
                return False, error_msg
            
            # Get plant ID
            plant_id = self.get_plant_id(plant_name)
            if not plant_id:
                return False, f"Plant {plant_name} not found"
            
            # Check data availability
            with DataAvailabilityChecker() as checker:
                availability = checker.check_data_availability(plant_id, start_str, end_str)
                
                if availability.get('future_date', False):
                    rec_start, rec_end = get_recommended_date_range(plant_id)
                    if rec_start and rec_end:
                        return False, f"Requested dates are in the future. Try: {rec_start} to {rec_end}"
                    else:
                        return False, "Requested dates are in the future and no historical data available"
                
                if not availability.get('plant_exists', False):
                    return False, f"Plant {plant_name} does not exist in database"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False, str(e)

# Global instance
data_manager = OptimizedDataManager()

@smart_cache_and_retry()
def get_generation_consumption_comparison_optimized(plant_name, date):
    """
    Optimized generation vs consumption comparison with validation.
    """
    try:
        # Validate request
        is_valid, error_msg = data_manager.validate_request(plant_name, date)
        if not is_valid:
            logger.info(f"Skipping comparison for {plant_name} on {date}: {error_msg}")
            return pd.DataFrame(), pd.DataFrame()
        
        plant_id = data_manager.get_plant_id(plant_name)
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else date
        
        # Check if we have settlement data first (preferred)
        settlement_df = get_settlement_data_optimized(plant_id, date_str, date_str)
        
        if not settlement_df.empty:
            # Use settlement data
            generation_df = settlement_df[['datetime', 'allocated_generation']].copy()
            generation_df = generation_df.rename(columns={'datetime': 'time', 'allocated_generation': 'Generation'})
            
            consumption_df = settlement_df[['datetime', 'consumption']].copy()
            consumption_df = consumption_df.rename(columns={'datetime': 'time', 'consumption': 'Consumption'})
            
            logger.info(f"Retrieved comparison data from settlement table for {plant_name}")
            return generation_df, consumption_df
        
        # Fallback to separate tables
        logger.info(f"Using fallback method for {plant_name} - settlement data not available")
        
        # Get generation data
        generation_df = get_generation_data_optimized(plant_id, date_str, date_str)
        if not generation_df.empty:
            generation_df = generation_df.rename(columns={'datetime': 'time', 'generation': 'Generation'})
        
        # Get consumption data
        client_name = get_client_name_from_plant_name(plant_name)
        consumption_df = pd.DataFrame()
        
        if client_name:
            consumption_df = get_consumption_data_optimized(client_name, date_str, date_str)
            if not consumption_df.empty:
                consumption_df = consumption_df.rename(columns={'datetime': 'time', 'consumption': 'Consumption'})
        
        return generation_df, consumption_df
        
    except Exception as e:
        logger.error(f"Failed to get generation-consumption comparison: {e}")
        return pd.DataFrame(), pd.DataFrame()

@smart_cache_and_retry()
def get_daily_generation_consumption_comparison_optimized(selected_plant, start_date, end_date):
    """
    Optimized daily comparison with validation.
    """
    try:
        # Validate request
        is_valid, error_msg = data_manager.validate_request(selected_plant, start_date, end_date)
        if not is_valid:
            logger.info(f"Skipping daily comparison for {selected_plant}: {error_msg}")
            return pd.DataFrame()
        
        plant_id = data_manager.get_plant_id(selected_plant)
        start_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date
        end_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date
        
        # Try settlement data first
        settlement_df = get_settlement_data_optimized(plant_id, start_str, end_str)
        
        if not settlement_df.empty:
            # Aggregate by date
            settlement_df['date'] = settlement_df['datetime'].dt.date
            daily_df = settlement_df.groupby('date').agg({
                'allocated_generation': 'sum',
                'consumption': 'sum'
            }).reset_index()
            
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            daily_df = daily_df.rename(columns={
                'allocated_generation': 'generation_kwh',
                'consumption': 'consumption_kwh'
            })
            
            logger.info(f"Retrieved daily comparison from settlement data for {selected_plant}")
            return daily_df
        
        # Fallback method
        logger.info(f"Using fallback method for daily comparison: {selected_plant}")
        
        generation_df = get_generation_data_optimized(plant_id, start_str, end_str)
        client_name = get_client_name_from_plant_name(selected_plant)
        
        if not client_name:
            logger.warning(f"No client found for plant {selected_plant}")
            return pd.DataFrame()
        
        consumption_df = get_consumption_data_optimized(client_name, start_str, end_str)
        
        if generation_df.empty or consumption_df.empty:
            logger.info(f"Insufficient data for daily comparison: {selected_plant}")
            return pd.DataFrame()
        
        # Aggregate to daily
        generation_df['date'] = generation_df['datetime'].dt.date
        consumption_df['date'] = consumption_df['datetime'].dt.date
        
        gen_daily = generation_df.groupby('date')['generation'].sum().reset_index()
        cons_daily = consumption_df.groupby('date')['consumption'].sum().reset_index()
        
        # Merge
        merged_df = pd.merge(gen_daily, cons_daily, on='date', how='outer').fillna(0)
        merged_df['date'] = pd.to_datetime(merged_df['date'])
        merged_df = merged_df.rename(columns={
            'generation': 'generation_kwh',
            'consumption': 'consumption_kwh'
        })
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Failed to get daily comparison: {e}")
        return pd.DataFrame()

@smart_cache_and_retry()
def get_generation_only_data_optimized(plant_name, start_date, end_date=None):
    """
    Optimized generation-only data with validation.
    """
    try:
        if end_date is None:
            end_date = start_date
        
        # Validate request
        is_valid, error_msg = data_manager.validate_request(plant_name, start_date, end_date)
        if not is_valid:
            logger.info(f"Skipping generation query for {plant_name}: {error_msg}")
            return pd.DataFrame()
        
        plant_id = data_manager.get_plant_id(plant_name)
        start_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date
        end_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date
        
        # Get generation data
        generation_df = get_generation_data_optimized(plant_id, start_str, end_str)
        
        if generation_df.empty:
            return pd.DataFrame()
        
        # Format based on date range
        if start_str == end_str:
            # Single day
            generation_df = generation_df.rename(columns={'datetime': 'time', 'generation': 'generation_kwh'})
            generation_df['hour'] = generation_df['time'].dt.hour
        else:
            # Multi-day - aggregate by date
            generation_df['date'] = generation_df['datetime'].dt.date
            daily_df = generation_df.groupby('date')['generation'].sum().reset_index()
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            generation_df = daily_df.rename(columns={'date': 'time', 'generation': 'generation_kwh'})
        
        logger.info(f"Retrieved generation data for {plant_name}")
        return generation_df
        
    except Exception as e:
        logger.error(f"Failed to get generation data: {e}")
        return pd.DataFrame()

@smart_cache_and_retry()
def get_consumption_data_optimized_wrapper(plant_name, start_date, end_date=None):
    """
    Optimized consumption data wrapper with validation.
    """
    try:
        if end_date is None:
            end_date = start_date
        
        # Validate request
        is_valid, error_msg = data_manager.validate_request(plant_name, start_date, end_date)
        if not is_valid:
            logger.info(f"Skipping consumption query for {plant_name}: {error_msg}")
            return pd.DataFrame()
        
        client_name = get_client_name_from_plant_name(plant_name)
        if not client_name:
            logger.warning(f"No client found for plant {plant_name}")
            return pd.DataFrame()
        
        start_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date
        end_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date
        
        consumption_df = get_consumption_data_optimized(client_name, start_str, end_str)
        
        if not consumption_df.empty:
            consumption_df = consumption_df.rename(columns={'datetime': 'time', 'consumption': 'Consumption'})
        
        return consumption_df
        
    except Exception as e:
        logger.error(f"Failed to get consumption data: {e}")
        return pd.DataFrame()

@smart_cache_and_retry()
def get_tod_binned_data_optimized(plant_name, start_date, end_date=None):
    """
    Optimized ToD binned data with validation.
    """
    try:
        if end_date is None:
            end_date = start_date
        
        # Validate request
        is_valid, error_msg = data_manager.validate_request(plant_name, start_date, end_date)
        if not is_valid:
            logger.info(f"Skipping ToD query for {plant_name}: {error_msg}")
            return pd.DataFrame()
        
        plant_id = data_manager.get_plant_id(plant_name)
        start_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else start_date
        end_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else end_date
        
        # Determine plant type
        plants = data_manager.get_plants()
        plant_type = None
        for client_name, client_plants in plants.items():
            for ptype in ['solar', 'wind']:
                for plant in client_plants.get(ptype, []):
                    if plant.get('name') == plant_name:
                        plant_type = ptype
                        break
                if plant_type:
                    break
            if plant_type:
                break
        
        # Get ToD data
        tod_df = get_settlement_tod_data_optimized(plant_id, start_str, end_str, plant_type)
        
        if tod_df.empty:
            return pd.DataFrame()
        
        # Rename columns for compatibility
        rename_dict = {
            'allocated_generation_total': 'generation_kwh',
            'consumption_total': 'consumption_kwh',
            'surplus_total': 'surplus'
        }
        
        tod_df = tod_df.rename(columns=rename_dict)
        
        # Ensure surplus column exists
        if 'generation_kwh' in tod_df.columns and 'consumption_kwh' in tod_df.columns and 'surplus' not in tod_df.columns:
            tod_df['surplus'] = tod_df['generation_kwh'] - tod_df['consumption_kwh']
        
        logger.info(f"Retrieved ToD data for {plant_name}: {len(tod_df)} ToD bins")
        return tod_df
        
    except Exception as e:
        logger.error(f"Failed to get ToD data: {e}")
        return pd.DataFrame()

# Backward compatibility functions
def get_plants():
    """Backward compatibility wrapper"""
    return data_manager.get_plants()

def get_generation_consumption_comparison(plant_name, date):
    """Backward compatibility wrapper"""
    return get_generation_consumption_comparison_optimized(plant_name, date)

def get_daily_generation_consumption_comparison(selected_plant, start_date, end_date):
    """Backward compatibility wrapper"""
    return get_daily_generation_consumption_comparison_optimized(selected_plant, start_date, end_date)

def get_generation_only_data(plant_name, start_date, end_date=None):
    """Backward compatibility wrapper"""
    return get_generation_only_data_optimized(plant_name, start_date, end_date)

def get_consumption_data_from_csv(plant_name, start_date, end_date=None):
    """Backward compatibility wrapper"""
    return get_consumption_data_optimized_wrapper(plant_name, start_date, end_date)

def get_tod_binned_data(plant_name, start_date, end_date=None):
    """Backward compatibility wrapper"""
    return get_tod_binned_data_optimized(plant_name, start_date, end_date)

def get_plant_id(plant_name):
    """Backward compatibility wrapper"""
    return data_manager.get_plant_id(plant_name)

def is_solar_plant(plant_name):
    """Check if plant is solar type"""
    try:
        plants = data_manager.get_plants()
        for client_name, client_plants in plants.items():
            for plant in client_plants.get('solar', []):
                if plant.get('name') == plant_name:
                    return True
        return False
    except Exception as e:
        logger.error(f"Failed to check plant type: {e}")
        return False
"""
Database-based data manager with optimized performance and error handling.
This module provides clean, efficient data access with proper validation.
"""

import pandas as pd
import streamlit as st
import traceback
from functools import wraps

# Import optimized functions
from backend.data.db_data_manager_optimized import (
    get_generation_consumption_comparison_optimized,
    get_daily_generation_consumption_comparison_optimized,
    get_generation_only_data_optimized,
    get_consumption_data_optimized_wrapper,
    get_tod_binned_data_optimized,
    data_manager
)

# Import original functions for backward compatibility
from backend.data.db_data import (
    get_generation_data_db,
    get_consumption_data_db,
    get_consumption_data_by_client,
    get_settlement_data_db,
    get_plants_from_db,
    get_daily_aggregated_generation_db,
    get_daily_aggregated_consumption_db,
    get_tod_aggregated_data_db,
    get_combined_plants_data_db,
    get_plant_id_from_name,
    get_consumption_unit_from_plant,
    get_monthly_before_banking_settlement_data_db,
    get_monthly_energy_metrics_data_db,
    # New SettlementData-based functions
    get_settlement_generation_consumption_data,
    get_settlement_generation_data,
    get_settlement_consumption_data,
    get_settlement_combined_client_data,
    get_settlement_tod_aggregated_data
)
from backend.data.db_data_clean import (
    get_monthly_energy_metrics_data_db as get_monthly_energy_metrics_data_clean_db
)
from backend.utils.client_mapping import (
    get_client_name_from_plant_name,
    get_plant_id_from_plant_name,
    validate_client_plant_mapping
)
from backend.logs.logger_setup import setup_logger
from backend.config.tod_config import get_tod_slots, get_tod_slot

# Configure logging
logger = setup_logger('db_data_manager', 'db_data_manager.log')

def retry_on_exception(max_retries=3, retry_delay=1):
    """Decorator to retry a function on exception"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(retry_delay)
            logger.error(f"Function {func.__name__} failed after {max_retries} attempts")
            raise last_exception
        return wrapper
    return decorator

# Plant management functions - using optimized version
def get_plants():
    """
    Get all available plants from database using optimized method.
    
    Returns:
        Dictionary with plant information organized by client and type
    """
    try:
        return data_manager.get_plants()
    except Exception as e:
        logger.error(f"Failed to get plants: {e}")
        return {}

def get_plant_display_name(plant_obj):
    """
    Get display name for a plant object.
    
    Args:
        plant_obj: Plant object with 'name' attribute or string
        
    Returns:
        Display name for the plant
    """
    if isinstance(plant_obj, dict):
        return plant_obj.get('name', str(plant_obj))
    elif hasattr(plant_obj, 'name'):
        return plant_obj.name
    else:
        return str(plant_obj)

def get_plant_id(plant_name):
    """
    Get plant ID from plant name using optimized method.
    
    Args:
        plant_name: Name of the plant or plant object with 'plant_id' key
        
    Returns:
        Plant ID if found, None otherwise
    """
    try:
        return data_manager.get_plant_id(plant_name)
    except Exception as e:
        logger.error(f"Failed to get plant ID for {plant_name}: {e}")
        return None

def is_solar_plant(plant_name):
    """
    Check if a plant is a solar plant.
    
    Args:
        plant_name: Name of the plant or plant object
        
    Returns:
        True if solar plant, False otherwise
    """
    try:
        # Handle case where plant_name is actually a plant object/dict
        if isinstance(plant_name, dict):
            actual_plant_name = plant_name.get('name', str(plant_name))
        else:
            actual_plant_name = plant_name
        
        plants = get_plants()
        for client_name, client_plants in plants.items():
            for plant in client_plants.get('solar', []):
                if plant.get('name') == actual_plant_name:
                    return True
        return False
    except Exception as e:
        logger.error(f"Failed to check if {plant_name} is solar: {e}")
        return False

# Generation data functions - using optimized versions
def get_generation_consumption_comparison(plant_name, date):
    """
    Get generation vs consumption comparison using optimized method.
    
    Args:
        plant_name: Name of the plant
        date: Date for comparison
        
    Returns:
        Tuple of (generation_df, consumption_df)
    """
    try:
        return get_generation_consumption_comparison_optimized(plant_name, date)
    except Exception as e:
        logger.error(f"Failed to get generation-consumption comparison: {e}")
        return pd.DataFrame(), pd.DataFrame()

def get_daily_generation_consumption_comparison(selected_plant, start_date, end_date):
    """
    Get daily aggregated generation vs consumption comparison using optimized method.
    
    Args:
        selected_plant: Name of the selected plant
        start_date: Start date
        end_date: End date
        
    Returns:
        DataFrame with daily comparison data
    """
    try:
        return get_daily_generation_consumption_comparison_optimized(selected_plant, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to get daily generation-consumption comparison: {e}")
        return pd.DataFrame()

def get_generation_only_data(plant_name, start_date, end_date=None):
    """
    Get generation-only data using optimized method.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional, defaults to start_date)
        
    Returns:
        DataFrame with generation data
    """
    try:
        return get_generation_only_data_optimized(plant_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to get generation data: {e}")
        return pd.DataFrame()

# Consumption data functions - using optimized versions
def get_consumption_data_from_csv(plant_name, start_date, end_date=None):
    """
    Get consumption data using optimized method.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional)
        
    Returns:
        DataFrame with consumption data (columns: time, Consumption)
    """
    try:
        return get_consumption_data_optimized_wrapper(plant_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to get consumption data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_daily_consumption_data(plant_name, start_date, end_date):
    """
    Get daily aggregated consumption data.
    Uses new SettlementData computed datetime approach for better performance.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date
        
    Returns:
        DataFrame with daily consumption data
    """
    try:
        plant_id = get_plant_id(plant_name)
        if not plant_id:
            logger.warning(f"Could not find plant_id for {plant_name}")
            return pd.DataFrame()
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Get plant type for filtering
        plant_type = None
        if is_solar_plant(plant_name):
            plant_type = 'solar'
        else:
            # Assume wind if not solar
            plant_type = 'wind'
        
        # Use new SettlementData computed datetime approach
        settlement_df = get_settlement_consumption_data(plant_id, start_str, end_str, plant_type)
        
        if not settlement_df.empty:
            # Aggregate consumption data by date
            settlement_df['date'] = settlement_df['datetime'].dt.date
            daily_df = settlement_df.groupby('date').agg({
                'total_consumption': 'sum'
            }).reset_index()
            
            # Convert date to datetime for consistency
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            daily_df = daily_df.rename(columns={'date': 'time', 'total_consumption': 'Consumption'})
            
            logger.info(f"Retrieved daily consumption data using new SettlementData approach for {plant_name} from {start_str} to {end_str}")
            return daily_df
        else:
            logger.warning(f"No settlement data found for {plant_name}, falling back to consumption table")
            
            # Fallback to original method if settlement data is not available
            cons_units = get_consumption_unit_from_plant(plant_name)
            if not cons_units:
                logger.warning(f"Could not find any consumption units for {plant_name}")
                return pd.DataFrame()
            
            # Get consumption data for all units and combine
            all_consumption_dfs = []
            for cons_unit in cons_units:
                unit_df = get_daily_aggregated_consumption_db(cons_unit, start_str, end_str)
                if not unit_df.empty:
                    all_consumption_dfs.append(unit_df)
            
            if all_consumption_dfs:
                # Combine all consumption dataframes
                consumption_df = pd.concat(all_consumption_dfs)
                # Aggregate by date to sum consumption across all units
                consumption_df = consumption_df.groupby('date')['consumption_kwh'].sum().reset_index()
            else:
                consumption_df = pd.DataFrame(columns=['date', 'consumption_kwh'])
            
            if not consumption_df.empty:
                consumption_df = consumption_df.rename(columns={'date': 'time', 'consumption': 'Consumption'})
            
            logger.info(f"Retrieved daily consumption data from consumption table for {plant_name} from {start_str} to {end_str}")
            return consumption_df
        
    except Exception as e:
        logger.error(f"Failed to get daily consumption data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

# Combined wind and solar functions
@st.cache_data(ttl=3600)
@retry_on_exception()
def get_combined_wind_solar_generation(client_name, start_date, end_date):
    """
    Get combined wind and solar generation data for a client.
    Uses new SettlementData computed datetime approach for better performance.
    
    Args:
        client_name: Name of the client
        start_date: Start date
        end_date: End date
        
    Returns:
        DataFrame with combined generation data
    """
    try:
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Use new SettlementData computed datetime approach
        combined_df = get_settlement_combined_client_data(client_name, start_str, end_str)
        
        if not combined_df.empty:
            # Check if single day or multi-day
            if start_date == end_date:
                # Single day - use datetime
                time_col = 'datetime'
            else:
                # Multi-day - aggregate by date
                combined_df['date'] = combined_df['datetime'].dt.date
                combined_df = combined_df.groupby(['date', 'type']).agg({
                    'total_generation': 'sum'
                }).reset_index()
                combined_df['date'] = pd.to_datetime(combined_df['date'])
                time_col = 'date'
            
            # Pivot to get separate columns for solar and wind
            pivot_df = combined_df.pivot_table(
                index=time_col, 
                columns='type', 
                values='total_generation', 
                fill_value=0
            ).reset_index()
            
            # Rename columns
            column_mapping = {time_col: 'time'}
            if 'solar' in pivot_df.columns:
                column_mapping['solar'] = 'Solar Generation'
            if 'wind' in pivot_df.columns:
                column_mapping['wind'] = 'Wind Generation'
            
            pivot_df = pivot_df.rename(columns=column_mapping)
            
            # Ensure both columns exist
            if 'Solar Generation' not in pivot_df.columns:
                pivot_df['Solar Generation'] = 0
            if 'Wind Generation' not in pivot_df.columns:
                pivot_df['Wind Generation'] = 0
            
            logger.info(f"Retrieved combined wind-solar data using new SettlementData approach for {client_name} from {start_str} to {end_str}")
            return pivot_df
        else:
            logger.warning(f"No settlement data found for client {client_name}, falling back to separate tables")
            
            # Fallback to original method if settlement data is not available
            # Get solar data
            solar_df = get_combined_plants_data_db(client_name, 'solar', start_str, end_str)
            
            # Get wind data
            wind_df = get_combined_plants_data_db(client_name, 'wind', start_str, end_str)
            
            if solar_df.empty and wind_df.empty:
                logger.warning(f"No combined data found for client {client_name}")
                return pd.DataFrame()
            
            # Check if single day or multi-day
            if start_date == end_date:
                # Single day - use datetime
                time_col = 'datetime'
            else:
                # Multi-day - aggregate by date
                if not solar_df.empty:
                    solar_df['date'] = solar_df['datetime'].dt.date
                    solar_df = solar_df.groupby('date')['generation'].sum().reset_index()
                    solar_df['date'] = pd.to_datetime(solar_df['date'])
                
                if not wind_df.empty:
                    wind_df['date'] = wind_df['datetime'].dt.date
                    wind_df = wind_df.groupby('date')['generation'].sum().reset_index()
                    wind_df['date'] = pd.to_datetime(wind_df['date'])
                
                time_col = 'date'
            
            # Merge solar and wind data
            if not solar_df.empty and not wind_df.empty:
                merged_df = pd.merge(solar_df, wind_df, on=time_col, how='outer', suffixes=('_solar', '_wind'))
                merged_df = merged_df.fillna(0)
                merged_df = merged_df.rename(columns={
                    'generation_solar': 'Solar Generation',
                    'generation_wind': 'Wind Generation',
                    time_col: 'time'
                })
            elif not solar_df.empty:
                merged_df = solar_df.copy()
                merged_df['Wind Generation'] = 0
                merged_df = merged_df.rename(columns={'generation': 'Solar Generation', time_col: 'time'})
            elif not wind_df.empty:
                merged_df = wind_df.copy()
                merged_df['Solar Generation'] = 0
                merged_df = merged_df.rename(columns={'generation': 'Wind Generation', time_col: 'time'})
            else:
                return pd.DataFrame()
            
            logger.info(f"Retrieved combined wind-solar data from separate tables for {client_name} from {start_str} to {end_str}")
            return merged_df
        
    except Exception as e:
        logger.error(f"Failed to get combined wind-solar data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

# Time-of-Day (ToD) functions - using optimized versions
def get_tod_binned_data(plant_name, start_date, end_date=None):
    """
    Get Time-of-Day binned data using optimized method.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional)
        
    Returns:
        DataFrame with ToD binned data
    """
    try:
        return get_tod_binned_data_optimized(plant_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to get ToD data: {e}")
        return pd.DataFrame()

# Power cost analysis functions
@st.cache_data(ttl=3600)
@retry_on_exception()
def calculate_power_cost_metrics(plant_name, start_date, end_date, grid_rate_per_kwh):
    """
    Calculate power cost metrics for a plant.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date
        grid_rate_per_kwh: Grid electricity rate per kWh
        
    Returns:
        DataFrame with cost analysis
    """
    try:
        plant_id = get_plant_id(plant_name)
        if not plant_id:
            logger.warning(f"Could not find plant_id for {plant_name}")
            return pd.DataFrame()
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Try to get settlement data first
        settlement_df = get_settlement_data_db(plant_id, start_str, end_str)
        
        # Add surplus column for backward compatibility if it doesn't exist
        if not settlement_df.empty and 'surplus' not in settlement_df.columns:
            settlement_df['surplus'] = settlement_df['allocated_generation'] - settlement_df['consumption']
        
        if settlement_df.empty:
            logger.warning(f"No settlement data found for {plant_name}, trying to calculate from generation and consumption data")
            
            # Fallback: Get generation and consumption data separately and combine them
            generation_df = get_generation_data_db(plant_id, start_str, end_str)
            
            # Get consumption unit for the plant
            cons_units = get_consumption_unit_from_plant(plant_name)
            if cons_units:
                # Get consumption data for all units and combine
                all_consumption_dfs = []
                for cons_unit in cons_units:
                    unit_df = get_consumption_data_db(cons_unit, start_str, end_str)
                    if not unit_df.empty:
                        all_consumption_dfs.append(unit_df)
                
                if all_consumption_dfs:
                    # Combine all consumption data
                    consumption_df = pd.concat(all_consumption_dfs, ignore_index=True)
                    # Group by datetime and sum consumption values
                    consumption_df = consumption_df.groupby('datetime').agg({
                        'consumption': 'sum'
                    }).reset_index()
                else:
                    consumption_df = pd.DataFrame()
            else:
                consumption_df = pd.DataFrame()
            
            if generation_df.empty and consumption_df.empty:
                logger.warning(f"No generation or consumption data found for {plant_name}")
                return pd.DataFrame()
            
            # Create a combined dataframe
            if not generation_df.empty and not consumption_df.empty:
                # Merge on datetime
                combined_df = pd.merge(
                    generation_df[['datetime', 'generation']],
                    consumption_df[['datetime', 'consumption']],
                    on='datetime',
                    how='outer'
                ).fillna(0)
            elif not generation_df.empty:
                combined_df = generation_df[['datetime', 'generation']].copy()
                combined_df['consumption'] = 0
            else:
                combined_df = consumption_df[['datetime', 'consumption']].copy()
                combined_df['generation'] = 0
            
            # Calculate surplus and deficit columns for consistency with new schema
            combined_df['surplus_demand'] = (combined_df['generation'] - combined_df['consumption']).clip(lower=0)
            combined_df['surplus_generation'] = (combined_df['generation'] - combined_df['consumption']).clip(lower=0)
            combined_df['deficit'] = (combined_df['consumption'] - combined_df['generation']).clip(lower=0)
            # For backward compatibility
            combined_df['surplus'] = combined_df['generation'] - combined_df['consumption']
            combined_df['surplus_deficit'] = combined_df['deficit']  # Map old column name to new
            settlement_df = combined_df
        
        # Calculate cost metrics
        settlement_df['grid_cost'] = settlement_df['consumption'] * grid_rate_per_kwh
        
        # Use allocated_generation if available, otherwise fall back to generation
        generation_col = 'allocated_generation' if 'allocated_generation' in settlement_df.columns else 'generation'
        settlement_df['actual_cost'] = (settlement_df['consumption'] - settlement_df[generation_col]).clip(lower=0) * grid_rate_per_kwh
        settlement_df['savings'] = settlement_df['grid_cost'] - settlement_df['actual_cost']
        settlement_df['savings_percentage'] = (settlement_df['savings'] / settlement_df['grid_cost'] * 100).fillna(0)
        
        # Create columns expected by display components
        settlement_df['consumption_kwh'] = settlement_df['consumption']
        settlement_df['generation_kwh'] = settlement_df[generation_col]
        settlement_df['net_consumption_kwh'] = (settlement_df['consumption'] - settlement_df[generation_col]).clip(lower=0)
        
        # Handle datetime column for both single day and multi-day scenarios
        if 'datetime' in settlement_df.columns:
            # Convert datetime to pandas datetime if it's not already
            settlement_df['datetime'] = pd.to_datetime(settlement_df['datetime'])
            
            # Determine if this is single day or multi-day data
            if start_date == end_date:
                # Single day: keep time information
                settlement_df['time'] = settlement_df['datetime']
                settlement_df['date'] = settlement_df['datetime'].dt.date
            else:
                # Multi-day: create both time and date columns
                settlement_df['time'] = settlement_df['datetime']
                settlement_df['date'] = settlement_df['datetime'].dt.date
        
        # Remove the original datetime column if it exists to avoid confusion
        if 'datetime' in settlement_df.columns:
            settlement_df = settlement_df.drop(columns=['datetime'])
        
        logger.info(f"Calculated power cost metrics for {plant_name} from {start_str} to {end_str}")
        return settlement_df
        
    except Exception as e:
        logger.error(f"Failed to calculate power cost metrics: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def get_power_cost_summary(cost_df):
    """
    Get summary metrics from power cost DataFrame.
    
    Args:
        cost_df: DataFrame with cost analysis
        
    Returns:
        Dictionary with summary metrics
    """
    try:
        if cost_df.empty:
            return {
                'total_grid_cost': 0,
                'total_actual_cost': 0,
                'total_savings': 0,
                'savings_percentage': 0
            }
        
        summary = {
            'total_grid_cost': cost_df['grid_cost'].sum(),
            'total_actual_cost': cost_df['actual_cost'].sum(),
            'total_savings': cost_df['savings'].sum(),
            'savings_percentage': (cost_df['savings'].sum() / cost_df['grid_cost'].sum() * 100) if cost_df['grid_cost'].sum() > 0 else 0
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to get power cost summary: {e}")
        return {
            'total_grid_cost': 0,
            'total_actual_cost': 0,
            'total_savings': 0,
            'savings_percentage': 0
        }

# Utility functions for data processing
def compare_generation_consumption(generation_df, consumption_df):
    """
    Compare generation and consumption data.
    This function can work with data from any source, including settlement_data table.
    
    Args:
        generation_df: DataFrame with generation data
        consumption_df: DataFrame with consumption data
        
    Returns:
        Merged DataFrame with comparison
    """
    try:
        if generation_df.empty or consumption_df.empty:
            return pd.DataFrame()
        
        # Merge on time column
        merged_df = pd.merge(generation_df, consumption_df, on='time', how='outer')
        merged_df = merged_df.fillna(0)
        
        # Calculate surplus/deficit
        merged_df['surplus'] = merged_df['Generation'] - merged_df['Consumption']
        
        # Add surplus_demand and surplus_deficit columns for compatibility with settlement_data
        merged_df['surplus_demand'] = merged_df['surplus'].clip(lower=0)
        merged_df['surplus_deficit'] = (-merged_df['surplus']).clip(lower=0)
        
        # Rename columns to match expected format
        merged_df = merged_df.rename(columns={'Generation': 'generation_kwh', 'Consumption': 'consumption_kwh'})
        
        # Add hour column for plotting if time column is datetime
        if 'hour' not in merged_df.columns and pd.api.types.is_datetime64_any_dtype(merged_df['time']):
            merged_df['hour'] = merged_df['time'].dt.hour
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Failed to compare generation and consumption: {e}")
        return pd.DataFrame()

def standardize_dataframe_columns(df):
    """
    Standardize DataFrame column names.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized columns
    """
    try:
        if df.empty:
            return df
        
        # Standard column mappings
        column_mappings = {
            'datetime': 'time',
            'Date': 'time',
            'generation': 'Generation',
            'consumption': 'Consumption',
            'surplus': 'Surplus'
        }
        
        # Apply mappings
        for old_col, new_col in column_mappings.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to standardize DataFrame columns: {e}")
        return df

# Additional utility functions for compatibility
def get_consumption_data_by_timeframe(plant_name, start_date, end_date=None):
    """
    Get consumption data by timeframe (wrapper for compatibility).
    """
    return get_consumption_data_from_csv(plant_name, start_date, end_date)

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_data_by_timeframe(plant_name, start_date, end_date=None):
    """
    Get settlement data directly from the settlement_data table.
    This is the preferred method for getting all data as it contains all necessary information.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional, defaults to start_date)
        
    Returns:
        DataFrame with all settlement data
    """
    try:
        if end_date is None:
            end_date = start_date
        
        plant_id = get_plant_id(plant_name)
        if not plant_id:
            logger.warning(f"Could not find plant_id for {plant_name}")
            return pd.DataFrame()
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Get data directly from settlement_data table
        settlement_df = get_settlement_data_db(plant_id, start_str, end_str)
        
        if settlement_df.empty:
            logger.warning(f"No settlement data found for {plant_name} from {start_str} to {end_str}")
            return pd.DataFrame()
        
        # Rename columns to match expected format
        settlement_df = settlement_df.rename(columns={
            'datetime': 'time',
            'generation': 'generation_kwh',
            'consumption': 'consumption_kwh'
        })
        
        # Add hour column for plotting if single day
        if start_date == end_date and 'time' in settlement_df.columns:
            settlement_df['hour'] = settlement_df['time'].dt.hour
        
        logger.info(f"Retrieved settlement data for {plant_name} from {start_str} to {end_str}")
        return settlement_df
        
    except Exception as e:
        logger.error(f"Failed to get settlement data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def get_generation_consumption_by_timeframe(plant_name, start_date, end_date=None):
    """
    Get generation and consumption data by timeframe.
    Primarily uses settlement_data table which contains all necessary data.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional)
        
    Returns:
        DataFrame with generation and consumption data
    """
    try:
        if end_date is None:
            end_date = start_date
        
        # First try to get data directly from settlement_data table
        settlement_df = get_settlement_data_by_timeframe(plant_name, start_date, end_date)
        
        if not settlement_df.empty:
            # If we have settlement data, use it directly
            logger.info(f"Using settlement data for {plant_name} from {start_date} to {end_date}")
            return settlement_df
        else:
            # Fallback to original method if settlement data is not available
            logger.warning(f"No settlement data found for {plant_name}, falling back to separate tables")
            
            if start_date == end_date:
                # Single day
                generation_df, consumption_df = get_generation_consumption_comparison(plant_name, start_date)
                return compare_generation_consumption(generation_df, consumption_df)
            else:
                # Multi-day
                return get_daily_generation_consumption_comparison(plant_name, start_date, end_date)
            
    except Exception as e:
        logger.error(f"Failed to get generation-consumption by timeframe: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

# New clean consumption functions following ToD pattern
@st.cache_data(ttl=3600)
@retry_on_exception()
def get_consumption_data_db_clean(plant_name, start_date, end_date=None):
    """
    Get consumption data from database using direct client-based approach.
    This function follows the ToD consumption pattern for consistency.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date (datetime object)
        end_date: End date (datetime object, optional)
        
    Returns:
        DataFrame with columns: datetime, consumption
    """
    try:
        if end_date is None:
            end_date = start_date
        
        # Convert dates to string format
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Get client name from plant name
        client_name = get_client_name_from_plant_name(plant_name)
        if not client_name:
            logger.warning(f"Could not find client name for plant {plant_name}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        # Get consumption data using client name
        consumption_df = get_consumption_data_by_client(client_name, start_str, end_str)
        
        if consumption_df.empty:
            logger.warning(f"No consumption data found for plant {plant_name} (client: {client_name})")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        logger.info(f"Retrieved {len(consumption_df)} consumption records for plant {plant_name}")
        return consumption_df
        
    except Exception as e:
        logger.error(f"Failed to get consumption data for {plant_name}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'consumption'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_daily_consumption_data_db_clean(plant_name, start_date, end_date):
    """
    Get daily aggregated consumption data from database.
    This function follows the ToD consumption pattern for consistency.
    
    Args:
        plant_name: Name of the plant
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
        
    Returns:
        DataFrame with columns: datetime, consumption
    """
    try:
        # Convert dates to string format
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Get client name from plant name
        client_name = get_client_name_from_plant_name(plant_name)
        if not client_name:
            logger.warning(f"Could not find client name for plant {plant_name}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        # Get consumption data using client name for the date range
        consumption_df = get_consumption_data_by_client(client_name, start_str, end_str)
        
        if consumption_df.empty:
            logger.warning(f"No consumption data found for plant {plant_name} (client: {client_name})")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        logger.info(f"Retrieved {len(consumption_df)} consumption records for plant {plant_name} from {start_str} to {end_str}")
        return consumption_df
        
    except Exception as e:
        logger.error(f"Failed to get daily consumption data for {plant_name}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'consumption'])

def get_banking_data(plant_name, start_date, end_date=None, banking_type="daily", tod_based=False):
    """
    Get banking data for a plant (placeholder function).
    
    Args:
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date (optional)
        banking_type: Type of banking data
        tod_based: Whether to use ToD-based logic
        
    Returns:
        DataFrame with banking data
    """
    try:
        # For now, return empty DataFrame as banking functionality needs to be implemented
        logger.warning("Banking functionality not yet implemented in database version")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Failed to get banking data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
@retry_on_exception()
def get_monthly_before_banking_settlement_data(plant_name: str) -> pd.DataFrame:
    """
    Get monthly BEFORE banking settlement data for ToD visualization.
    Shows initial surplus generation and demand values before any banking settlement process.
    
    Args:
        plant_name: Name of the plant
        
    Returns:
        DataFrame with monthly before banking settlement data by ToD slots
    """
    try:
        client_name = get_client_name_from_plant_name(plant_name)
        return get_monthly_before_banking_settlement_data_db(plant_name, client_name)
        
    except Exception as e:
        logger.error(f"Failed to get monthly banking settlement data for {plant_name}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def get_monthly_energy_metrics_data(plant_name: str) -> pd.DataFrame:
    """
    Get monthly energy metrics data from banking_settlement table for comparison visualization.
    
    Args:
        plant_name: Name of the plant
        
    Returns:
        DataFrame with monthly energy metrics data
    """
    try:
        client_name = get_client_name_from_plant_name(plant_name)
        return get_monthly_energy_metrics_data_clean_db(plant_name, client_name)
        
    except Exception as e:
        logger.error(f"Failed to get monthly energy metrics data for {plant_name}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()
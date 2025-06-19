"""
Optimized data functions for improved performance.
"""
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
import asyncio
from functools import lru_cache
import numpy as np

from backend.data.data_manager import data_manager
from backend.utils.performance_utils import timing_decorator, performance_context, optimize_dataframe
from backend.logs.logger_setup import setup_logger
from backend.config.app_config import CONFIG

logger = setup_logger('optimized_data', 'optimized_data.log')

class OptimizedDataFunctions:
    """Optimized versions of data fetching and processing functions."""
    
    def __init__(self):
        self.csv_cache = None
        self.plant_mapping = None
    
    @timing_decorator("load_consumption_csv")
    def get_consumption_csv_cached(self) -> pd.DataFrame:
        """Load and cache the consumption CSV file."""
        if self.csv_cache is None:
            csv_path = CONFIG["data"].get("consumption_csv_path", 
                                        "Data/csv/Consumption data Cloud nine - processed_data.csv")
            
            with performance_context("Loading consumption CSV"):
                self.csv_cache = data_manager.load_csv_optimized(csv_path)
                self.csv_cache = optimize_dataframe(self.csv_cache)
            
            logger.info(f"Consumption CSV loaded and cached: {self.csv_cache.shape}")
        
        return self.csv_cache
    
    @timing_decorator("get_plant_mapping")
    def get_plant_mapping_cached(self) -> Dict[str, str]:
        """Get cached plant name to ID mapping."""
        if self.plant_mapping is None:
            self.plant_mapping = data_manager.get_plant_mapping()
        return self.plant_mapping
    
    @timing_decorator("filter_consumption_data")
    def get_consumption_data_optimized(self, plant_name: str, start_date, end_date) -> pd.DataFrame:
        """
        Optimized consumption data retrieval with caching and vectorized operations.
        """
        try:
            # Get cached CSV data
            df = self.get_consumption_csv_cached()
            
            if df.empty:
                logger.warning("No consumption data available")
                return pd.DataFrame()
            
            # Get plant ID
            plant_mapping = self.get_plant_mapping_cached()
            plant_id = plant_mapping.get(plant_name, plant_name)
            
            # Use optimized filtering
            with performance_context(f"Filtering consumption data for {plant_name}"):
                filtered_df = data_manager.filter_data_optimized(df, plant_id, start_date, end_date)
            
            # Preprocess the data
            if not filtered_df.empty:
                filtered_df = data_manager.preprocess_consumption_data(filtered_df)
            
            logger.info(f"Retrieved consumption data: {len(filtered_df)} rows for {plant_name}")
            return filtered_df
            
        except Exception as e:
            logger.error(f"Error getting optimized consumption data: {e}")
            return pd.DataFrame()
    
    @timing_decorator("batch_api_calls")
    def fetch_multiple_plants_data(self, plant_requests: List[Dict]) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple plants concurrently.
        
        Args:
            plant_requests: List of dicts with keys: plant_name, start_date, end_date, data_type
        """
        results = {}
        
        if not CONFIG["data"].get("enable_concurrent_processing", True):
            # Sequential fallback
            for request in plant_requests:
                key = f"{request['plant_name']}_{request['data_type']}"
                results[key] = self._fetch_single_plant_data(request)
            return results
        
        # Concurrent processing
        max_workers = CONFIG["data"].get("max_concurrent_requests", 4)
        
        with performance_context("Concurrent API calls"):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_key = {}
                
                for request in plant_requests:
                    key = f"{request['plant_name']}_{request['data_type']}"
                    future = executor.submit(self._fetch_single_plant_data, request)
                    future_to_key[future] = key
                
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        results[key] = future.result()
                        logger.info(f"Completed API call for {key}")
                    except Exception as e:
                        logger.error(f"API call failed for {key}: {e}")
                        results[key] = pd.DataFrame()
        
        return results
    
    def _fetch_single_plant_data(self, request: Dict) -> pd.DataFrame:
        """
        Fetch data for a single plant (placeholder for actual API integration).
        """
        # This would integrate with your existing API functions
        # For now, return empty DataFrame as placeholder
        plant_name = request['plant_name']
        data_type = request['data_type']
        
        logger.info(f"Fetching {data_type} data for {plant_name}")
        
        # Placeholder - integrate with actual API calls
        return pd.DataFrame()
    
    @timing_decorator("aggregate_hourly_data")
    def aggregate_hourly_to_daily(self, df: pd.DataFrame, value_column: str = 'energy_kwh') -> pd.DataFrame:
        """
        Optimized aggregation from hourly to daily data.
        """
        try:
            if df.empty or value_column not in df.columns:
                return pd.DataFrame()
            
            # Ensure we have date column
            if 'date' not in df.columns and 'time' in df.columns:
                df['date'] = pd.to_datetime(df['time']).dt.date
            
            # Vectorized aggregation
            daily_df = df.groupby('date', as_index=False).agg({
                value_column: 'sum',
                'time': 'first'  # Keep first timestamp for reference
            }).rename(columns={value_column: f'daily_{value_column}'})
            
            logger.info(f"Aggregated {len(df)} hourly records to {len(daily_df)} daily records")
            return daily_df
            
        except Exception as e:
            logger.error(f"Error in hourly to daily aggregation: {e}")
            return pd.DataFrame()
    
    @timing_decorator("tod_binning")
    def bin_data_to_tod_optimized(self, df: pd.DataFrame, tod_slots: List[Dict]) -> pd.DataFrame:
        """
        Optimized Time-of-Day binning using vectorized operations.
        """
        try:
            if df.empty or 'hour' not in df.columns:
                return pd.DataFrame()
            
            # Create hour to ToD mapping for vectorized operation
            hour_to_tod = {}
            for slot in tod_slots:
                start_hour = slot['start_hour']
                end_hour = slot['end_hour']
                slot_name = slot['name']
                
                if end_hour > start_hour:
                    hours = list(range(start_hour, end_hour))
                else:  # Crosses midnight
                    hours = list(range(start_hour, 24)) + list(range(0, end_hour))
                
                for hour in hours:
                    hour_to_tod[hour] = slot_name
            
            # Vectorized mapping
            df['tod_bin'] = df['hour'].map(hour_to_tod)
            
            # Remove unmapped hours
            df = df.dropna(subset=['tod_bin'])
            
            # Aggregate by ToD bin
            agg_columns = {}
            if 'generation_kwh' in df.columns:
                agg_columns['generation_kwh'] = 'sum'
            if 'energy_kwh' in df.columns:
                agg_columns['energy_kwh'] = 'sum'
            if 'consumption_kwh' in df.columns:
                agg_columns['consumption_kwh'] = 'sum'
            
            if not agg_columns:
                logger.warning("No aggregatable columns found for ToD binning")
                return pd.DataFrame()
            
            result_df = df.groupby('tod_bin', as_index=False).agg(agg_columns)
            
            logger.info(f"ToD binning completed: {len(result_df)} bins from {len(df)} records")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in ToD binning: {e}")
            return pd.DataFrame()
    
    @timing_decorator("data_validation")
    def validate_and_clean_data(self, df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """
        Validate and clean data with optimized operations.
        """
        try:
            if df.empty:
                return df
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing required columns: {missing_columns}")
                return pd.DataFrame()
            
            # Remove rows with null values in required columns
            df_clean = df.dropna(subset=required_columns)
            
            # Remove duplicate rows
            initial_rows = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            
            if len(df_clean) < initial_rows:
                logger.info(f"Removed {initial_rows - len(df_clean)} duplicate rows")
            
            # Validate numeric columns
            numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                # Remove infinite values
                df_clean = df_clean[~np.isinf(df_clean[col])]
                
                # Remove extreme outliers (beyond 3 standard deviations)
                if len(df_clean) > 10:  # Only if we have enough data
                    mean_val = df_clean[col].mean()
                    std_val = df_clean[col].std()
                    if std_val > 0:
                        outlier_mask = np.abs(df_clean[col] - mean_val) <= 3 * std_val
                        df_clean = df_clean[outlier_mask]
            
            logger.info(f"Data validation completed: {len(df_clean)} valid rows from {len(df)} original rows")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error in data validation: {e}")
            return df
    
    def clear_cache(self):
        """Clear all cached data."""
        self.csv_cache = None
        self.plant_mapping = None
        data_manager.clear_cache()
        logger.info("All caches cleared")

# Global instance
optimized_data = OptimizedDataFunctions()

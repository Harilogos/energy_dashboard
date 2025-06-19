#!/usr/bin/env python3
"""
Debug script to investigate ToD data aggregation issue
"""

import pandas as pd
import sys
import os
from datetime import datetime, date

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.data.db_data import (
    get_settlement_data_db,
    get_tod_aggregated_data_db
)
from backend.data.db_data_manager import get_plant_id
from backend.logs.logger_setup import setup_logger

# Setup logging
logger = setup_logger('debug_tod', 'debug_tod.log')

def debug_tod_aggregation():
    """Debug ToD aggregation issue"""
    
    # Configuration
    PLANT_NAME = "Kids Clinic India Limited"
    START_DATE = date(2025, 3, 1)
    END_DATE = date(2025, 3, 31)
    
    plant_id = get_plant_id(PLANT_NAME)
    start_str = START_DATE.strftime('%Y-%m-%d')
    end_str = END_DATE.strftime('%Y-%m-%d')
    
    print(f"Debugging ToD aggregation for {PLANT_NAME}")
    print(f"Plant ID: {plant_id}")
    print(f"Date Range: {start_str} to {end_str}")
    print("=" * 60)
    
    # Get raw settlement data
    print("\n1. RAW SETTLEMENT DATA ANALYSIS:")
    settlement_df = get_settlement_data_db(plant_id, start_str, end_str)
    print(f"Settlement data shape: {settlement_df.shape}")
    print(f"Total allocated_generation (raw sum): {settlement_df['allocated_generation'].sum():.2f} kWh")
    
    # Manual correct aggregation
    datetime_agg = settlement_df.groupby('datetime').agg({
        'allocated_generation': 'first',  # Same for all cons_units
        'consumption': 'sum'  # Sum across cons_units
    }).reset_index()
    
    datetime_agg['date'] = datetime_agg['datetime'].dt.date
    daily_manual = datetime_agg.groupby('date').agg({
        'allocated_generation': 'sum',
        'consumption': 'sum'
    }).reset_index()
    
    manual_total_gen = daily_manual['allocated_generation'].sum()
    print(f"Correct aggregated generation: {manual_total_gen:.2f} kWh")
    
    # Get ToD aggregated data
    print("\n2. TOD AGGREGATED DATA ANALYSIS:")
    tod_df = get_tod_aggregated_data_db(plant_id, start_str, end_str)
    print(f"ToD data shape: {tod_df.shape}")
    print(f"ToD data columns: {tod_df.columns.tolist()}")
    print(f"Unique dates: {tod_df['date'].unique()}")
    print(f"Unique ToD bins: {tod_df['tod_bin'].unique()}")
    
    # Check if we have allocated_generation_total column
    if 'allocated_generation_total' in tod_df.columns:
        total_gen_tod = tod_df['allocated_generation_total'].sum()
        print(f"Sum of allocated_generation_total: {total_gen_tod:.2f} kWh")
        
        # Show breakdown by date and tod_bin
        print("\nToD data breakdown:")
        for _, row in tod_df.iterrows():
            print(f"  Date: {row['date']}, ToD: {row['tod_bin']}, Gen: {row['allocated_generation_total']:.2f}")
    
    # Check for multi-day aggregation issue
    print("\n3. MULTI-DAY AGGREGATION CHECK:")
    start_date_obj = pd.to_datetime(start_str).date()
    
    # Check if we have total aggregation data (rows with date = start_date that represent totals)
    potential_total_agg = tod_df[tod_df['date'] == start_date_obj].copy()
    daily_data = tod_df[tod_df['date'] != start_date_obj]
    
    print(f"Rows with start_date ({start_date_obj}): {len(potential_total_agg)}")
    print(f"Rows with other dates: {len(daily_data)}")
    
    if not potential_total_agg.empty:
        print("Data for start_date (potential total aggregation):")
        for _, row in potential_total_agg.iterrows():
            if 'allocated_generation_total' in row:
                print(f"  ToD: {row['tod_bin']}, Gen: {row['allocated_generation_total']:.2f}")
    
    if not daily_data.empty:
        print("Sample daily data:")
        for _, row in daily_data.head(5).iterrows():
            if 'allocated_generation_total' in row:
                print(f"  Date: {row['date']}, ToD: {row['tod_bin']}, Gen: {row['allocated_generation_total']:.2f}")

if __name__ == "__main__":
    debug_tod_aggregation()
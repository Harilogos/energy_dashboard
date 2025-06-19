"""
Temporary file to hold the banking settlement function until we clean up db_data.py
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Optional, Dict, List
import traceback
from functools import wraps

from sqlalchemy import and_, func

from db.db_setup import SessionLocal
from db.models import TblPlants, TblGeneration, TblConsumption, ConsumptionMapping, SettlementData, BankingSettlement
from backend.logs.logger_setup import setup_logger

# Configure logging
logger = setup_logger('db_data', 'db_data.log')

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

def get_db_session():
    """Get database session"""
    return SessionLocal()

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_monthly_before_banking_settlement_data_db(plant_name: str, client_name: str = None) -> pd.DataFrame:
    """
    Get monthly aggregated BEFORE banking settlement data for ToD visualization.
    Shows initial surplus generation and demand values before any banking settlement process.
    
    Args:
        plant_name: Name of the plant
        client_name: Optional client name for filtering
    
    Returns:
        DataFrame with monthly banking settlement data by ToD slots
    """
    try:
        session = get_db_session()
        
        # Build query with NULL date handling
        query = session.query(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'N/A').label('month'),
            BankingSettlement.slot_name,
            BankingSettlement.slot_time,
            func.sum(BankingSettlement.surplus_generation_sum).label('total_generation'),
            func.sum(BankingSettlement.surplus_demand_sum).label('total_consumption')
        )
        
        # Apply filters
        if plant_name != "Combined View":
            query = query.filter(BankingSettlement.plant_name == plant_name)
        
        if client_name:
            query = query.filter(BankingSettlement.client_name == client_name)
        
        # Group by month and slot
        query = query.group_by(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'N/A'),
            BankingSettlement.slot_name,
            BankingSettlement.slot_time
        ).order_by(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'N/A'),
            BankingSettlement.slot_name
        )
        
        results = query.all()
        session.close()
        
        if not results:
            logger.warning(f"No banking settlement data found for plant: {plant_name}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        data = []
        for result in results:
            data.append({
                'month': result.month,
                'slot_name': result.slot_name,
                'slot_time': result.slot_time,
                'total_generation': float(result.total_generation or 0),
                'total_consumption': float(result.total_consumption or 0)
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Retrieved {len(df)} banking settlement records for plant: {plant_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get monthly banking settlement data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


@st.cache_data(ttl=3600)
@retry_on_exception()
def get_monthly_energy_metrics_data_db(plant_name: str, client_name: str = None) -> pd.DataFrame:
    """
    Get monthly energy metrics data from banking_settlement table for comparison visualization.
    
    Args:
        plant_name: Name of the plant
        client_name: Optional client name for filtering
    
    Returns:
        DataFrame with monthly energy metrics data
    """
    try:
        session = get_db_session()
        
        # Build query to get aggregated monthly data
        query = session.query(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'Unknown').label('month'),
            # Settled without banking = matched_settled_sum (initial amount that could be directly matched and settled)
            func.sum(func.coalesce(BankingSettlement.matched_settled_sum, 0)).label('settled_without_banking'),
            # Settled with banking = matched_settled_sum + intra_settlement + inter_settlement
            func.sum(
                func.coalesce(BankingSettlement.matched_settled_sum, 0) +
                func.coalesce(BankingSettlement.intra_settlement, 0) +
                func.coalesce(BankingSettlement.inter_settlement, 0)
            ).label('settled_with_banking'),
            # Grid consumption with banking = surplus_demand_sum_after_inter (final demand after banking)
            func.sum(func.coalesce(BankingSettlement.surplus_demand_sum_after_inter, 0)).label('grid_consumption_with_banking'),
            # Grid consumption without banking = surplus_demand_sum (initial demand before banking)
            func.sum(func.coalesce(BankingSettlement.surplus_demand_sum, 0)).label('grid_consumption_without_banking')
        )
        
        # Apply filters
        if plant_name != "Combined View":
            query = query.filter(BankingSettlement.plant_name == plant_name)
        
        if client_name:
            query = query.filter(BankingSettlement.client_name == client_name)
        
        # Group by month and order by date  
        query = query.group_by(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'Unknown')
        ).order_by(
            func.coalesce(func.date_format(BankingSettlement.date, '%Y-%m'), 'Unknown')
        )
        
        results = query.all()
        session.close()
        
        if not results:
            logger.warning(f"No banking settlement data found for plant: {plant_name}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        data = []
        for result in results:
            # Skip records with invalid month data
            if not result.month or result.month == 'Unknown':
                continue
                
            settled_without = float(result.settled_without_banking or 0)
            settled_with = float(result.settled_with_banking or 0)
            grid_with = float(result.grid_consumption_with_banking or 0)
            grid_without = float(result.grid_consumption_without_banking or 0)
            
            data.append({
                'month': result.month,
                'settled_without_banking': settled_without,
                'settled_with_banking': settled_with,
                'grid_consumption_with_banking': grid_with,
                'grid_consumption_without_banking': grid_without,
                'banking_savings': settled_without - settled_with,
                'grid_reduction': grid_without - grid_with,
                'total_savings': (settled_without - settled_with) + (grid_without - grid_with)
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Retrieved {len(df)} months of energy metrics data for plant: {plant_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get monthly energy metrics data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()
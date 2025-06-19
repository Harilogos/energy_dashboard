"""
Database-based data fetching functions for the Energy Generation Dashboard.
All data is stored in 15-minute intervals in the database.
"""

import pandas as pd
import streamlit as st
from datetime import datetime, date, time
from typing import Optional, Dict, List
import traceback
from functools import wraps

from sqlalchemy import and_, func, String, case, or_

from db.db_setup import SessionLocal
from db.models import TblPlants, TblGeneration, TblConsumption, ConsumptionMapping, SettlementData, BankingSettlement
from backend.logs.logger_setup import setup_logger
from backend.config.tod_config import get_tod_slots

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
def get_generation_data_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get generation data from database for a plant and date range.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: datetime, generation
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query generation data
        query = session.query(
            TblGeneration.datetime,
            TblGeneration.generation
        ).filter(
            and_(
                TblGeneration.plant_id == plant_id,
                TblGeneration.date >= start_dt,
                TblGeneration.date <= end_dt
            )
        ).order_by(TblGeneration.datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No generation data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['datetime', 'generation'])
        
        df = pd.DataFrame(result, columns=['datetime', 'generation'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} generation records for plant {plant_id}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get generation data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'generation'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_consumption_data_by_client(client_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get consumption data from database for a client and date range.
    Uses only client_name, datetime, consumption columns as specified.
    
    Args:
        client_name: Client name identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: datetime, consumption
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query consumption data using only client_name, datetime, consumption
        query = session.query(
            TblConsumption.datetime,
            TblConsumption.consumption
        ).filter(
            and_(
                TblConsumption.client_name == client_name,
                TblConsumption.date >= start_dt,
                TblConsumption.date <= end_dt
            )
        ).order_by(TblConsumption.datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No consumption data found for client {client_name} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        df = pd.DataFrame(result, columns=['datetime', 'consumption'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['consumption'] = pd.to_numeric(df['consumption'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} consumption records for client {client_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get consumption data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'consumption'])

# Keep the old function for backward compatibility but mark as deprecated
@st.cache_data(ttl=3600)
@retry_on_exception()
def get_consumption_data_db(cons_unit: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    DEPRECATED: Get consumption data from database for a consumption unit and date range.
    Use get_consumption_data_by_client() instead.
    
    Args:
        cons_unit: Consumption unit identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: datetime, consumption
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query consumption data
        query = session.query(
            TblConsumption.datetime,
            TblConsumption.consumption
        ).filter(
            and_(
                TblConsumption.cons_unit == cons_unit,
                TblConsumption.date >= start_dt,
                TblConsumption.date <= end_dt
            )
        ).order_by(TblConsumption.datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No consumption data found for unit {cons_unit} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        df = pd.DataFrame(result, columns=['datetime', 'consumption'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['consumption'] = pd.to_numeric(df['consumption'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} consumption records for unit {cons_unit}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get consumption data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'consumption'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_data_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get settlement data from database for a plant and date range.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: datetime, cons_unit, allocated_generation, consumption, surplus_demand, deficit
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query settlement data - include cons_unit to distinguish between multiple consumption units
        query = session.query(
            SettlementData.datetime,
            SettlementData.cons_unit,
            SettlementData.allocated_generation,
            SettlementData.consumption,
            SettlementData.deficit,
            SettlementData.surplus_demand,
            SettlementData.surplus_generation,
            SettlementData.settled,
            SettlementData.slot_name,
            SettlementData.slot_time
        ).filter(
            and_(
                SettlementData.plant_id == plant_id,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        ).order_by(SettlementData.datetime, SettlementData.cons_unit)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
        
        df = pd.DataFrame(result, columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Convert numeric columns
        for col in ['allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement records for plant {plant_id} across {df['cons_unit'].nunique()} consumption units")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])

# New SettlementData-based functions using computed datetime approach
@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_generation_consumption_data(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """
    Get generation and consumption data from SettlementData table using computed datetime.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        plant_type: Plant type ('solar' or 'wind'), optional
        
    Returns:
        DataFrame with columns: datetime, total_generation, total_consumption
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Build query with computed datetime
        query = session.query(
            SettlementData.plant_id,
            SettlementData.type,
            computed_datetime.label('datetime'),
            func.sum(SettlementData.allocated_generation).label('total_generation'),
            func.sum(SettlementData.consumption).label('total_consumption')
        ).filter(
            and_(
                SettlementData.plant_id == plant_id,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        )
        
        # Add plant type filter if specified
        if plant_type:
            query = query.filter(SettlementData.type == plant_type)
        
        # Group by plant_id, type, and computed datetime
        query = query.group_by(
            SettlementData.plant_id, 
            SettlementData.type, 
            computed_datetime
        ).order_by(computed_datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_generation', 'total_consumption'])
        
        df = pd.DataFrame(result, columns=['plant_id', 'type', 'datetime', 'total_generation', 'total_consumption'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Convert numeric columns
        for col in ['total_generation', 'total_consumption']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement generation-consumption records for plant {plant_id}")
       
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement generation-consumption data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_generation', 'total_consumption'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_generation_data(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """
    Get generation data from SettlementData table using computed datetime.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        plant_type: Plant type ('solar' or 'wind'), optional
        
    Returns:
        DataFrame with columns: datetime, total_generation
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Build query with computed datetime
        query = session.query(
            SettlementData.plant_id,
            SettlementData.type,
            computed_datetime.label('datetime'),
            func.sum(SettlementData.allocated_generation).label('total_generation')
        ).filter(
            and_(
                SettlementData.plant_id == plant_id,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        )
        
        # Add plant type filter if specified
        if plant_type:
            query = query.filter(SettlementData.type == plant_type)
        
        # Group by plant_id, type, and computed datetime
        query = query.group_by(
            SettlementData.plant_id, 
            SettlementData.type, 
            computed_datetime
        ).order_by(computed_datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement generation data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_generation'])
        
        df = pd.DataFrame(result, columns=['plant_id', 'type', 'datetime', 'total_generation'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['total_generation'] = pd.to_numeric(df['total_generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement generation records for plant {plant_id}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement generation data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_generation'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_consumption_data(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """
    Get consumption data from SettlementData table using computed datetime.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        plant_type: Plant type ('solar' or 'wind'), optional
        
    Returns:
        DataFrame with columns: datetime, total_consumption
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Build query with computed datetime
        query = session.query(
            SettlementData.plant_id,
            SettlementData.type,
            computed_datetime.label('datetime'),
            func.sum(SettlementData.consumption).label('total_consumption')
        ).filter(
            and_(
                SettlementData.plant_id == plant_id,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        )
        
        # Add plant type filter if specified
        if plant_type:
            query = query.filter(SettlementData.type == plant_type)
        
        # Group by plant_id, type, and computed datetime
        query = query.group_by(
            SettlementData.plant_id, 
            SettlementData.type, 
            computed_datetime
        ).order_by(computed_datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement consumption data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_consumption'])
        
        df = pd.DataFrame(result, columns=['plant_id', 'type', 'datetime', 'total_consumption'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['total_consumption'] = pd.to_numeric(df['total_consumption'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement consumption records for plant {plant_id}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement consumption data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['plant_id', 'type', 'datetime', 'total_consumption'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_combined_client_data(client_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get combined generation data for all plants of a client from SettlementData table.
    
    Args:
        client_name: Client name
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: datetime, type, total_generation
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Build query with computed datetime, grouping by client and type
        query = session.query(
            SettlementData.client_name,
            SettlementData.type,
            computed_datetime.label('datetime'),
            func.sum(SettlementData.allocated_generation).label('total_generation')
        ).filter(
            and_(
                SettlementData.client_name == client_name,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        ).group_by(
            SettlementData.client_name,
            SettlementData.type, 
            computed_datetime
        ).order_by(computed_datetime, SettlementData.type)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement data found for client {client_name} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['client_name', 'type', 'datetime', 'total_generation'])
        
        df = pd.DataFrame(result, columns=['client_name', 'type', 'datetime', 'total_generation'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['total_generation'] = pd.to_numeric(df['total_generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement combined records for client {client_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement combined client data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['client_name', 'type', 'datetime', 'total_generation'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_settlement_tod_aggregated_data(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """
    Get Time-of-Day aggregated data from SettlementData table using computed datetime approach.
    This function uses the new computed datetime approach for better performance.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        plant_type: Plant type ('solar' or 'wind'), optional
        
    Returns:
        DataFrame with ToD analysis using computed datetime
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Get ToD slots configuration
        from backend.config.tod_config import get_tod_slots
        tod_slots = get_tod_slots()
        
        # Create a CASE statement for ToD bin assignment
        tod_case_conditions = []
        for slot_name, slot_info in tod_slots.items():
            start_hour = slot_info['start_hour']
            end_hour = slot_info['end_hour']
            
            if start_hour <= end_hour:
                # Normal case (doesn't cross midnight)
                condition = and_(
                    func.hour(computed_datetime) >= start_hour,
                    func.hour(computed_datetime) < end_hour
                )
            else:
                # Crosses midnight case
                condition = or_(
                    func.hour(computed_datetime) >= start_hour,
                    func.hour(computed_datetime) < end_hour
                )
            
            tod_case_conditions.append((condition, slot_name))
        
        # Create CASE statement for ToD bin
        tod_bin_case = case(*tod_case_conditions, else_='Unknown')
        
        # Build query with computed datetime and ToD binning
        query = session.query(
            SettlementData.plant_id,
            SettlementData.type,
            tod_bin_case.label('tod_bin'),
            func.sum(SettlementData.allocated_generation).label('total_generation'),
            func.sum(SettlementData.consumption).label('total_consumption'),
            func.sum(SettlementData.deficit).label('total_deficit'),
            func.sum(SettlementData.surplus_demand).label('total_surplus_demand'),
            func.count().label('interval_count')
        ).filter(
            and_(
                SettlementData.plant_id == plant_id,
                SettlementData.date >= start_dt,
                SettlementData.date <= end_dt
            )
        )
        
        # Add plant type filter if specified
        if plant_type:
            query = query.filter(SettlementData.type == plant_type)
        
        # Group by plant_id, type, and ToD bin
        query = query.group_by(
            SettlementData.plant_id, 
            SettlementData.type, 
            tod_bin_case
        ).order_by(tod_bin_case)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No settlement ToD data found for plant {plant_id} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['plant_id', 'type', 'tod_bin', 'total_generation', 'total_consumption', 'total_deficit', 'total_surplus_demand', 'interval_count'])
        
        df = pd.DataFrame(result, columns=['plant_id', 'type', 'tod_bin', 'total_generation', 'total_consumption', 'total_deficit', 'total_surplus_demand', 'interval_count'])
        
        # Convert numeric columns
        for col in ['total_generation', 'total_consumption', 'total_deficit', 'total_surplus_demand', 'interval_count']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Calculate surplus from generation and consumption
        df['total_surplus'] = df['total_generation'] - df['total_consumption']
        
        # Add normalized values (per 15-minute interval)
        for col in ['total_generation', 'total_consumption', 'total_surplus', 'total_deficit', 'total_surplus_demand']:
            df[f'{col.replace("total_", "")}_normalized'] = df[col] / df['interval_count']
        
        # Add date column for consistency with existing functions
        df['date'] = start_dt
        
        # Rename columns to match existing format
        df = df.rename(columns={
            'total_generation': 'allocated_generation_total',
            'total_consumption': 'consumption_total',
            'total_surplus': 'surplus_total',
            'total_deficit': 'deficit_total',
            'total_surplus_demand': 'surplus_demand_total'
        })
        
        logger.info(f"Retrieved ToD data using computed datetime for plant {plant_id} from {start_date} to {end_date}")
        logger.info(f"Total generation: {df['allocated_generation_total'].sum():.2f} kWh")
        logger.info(f"Total consumption: {df['consumption_total'].sum():.2f} kWh")
        logger.info(f"ToD bins: {df['tod_bin'].tolist()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement ToD data using computed datetime: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['plant_id', 'type', 'tod_bin', 'allocated_generation_total', 'consumption_total', 'surplus_total', 'deficit_total', 'surplus_demand_total', 'interval_count', 'date'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_plants_from_db() -> Dict:
    """
    Get all available plants from database.
    
    Returns:
        Dictionary with plant information organized by client and type
    """
    try:
        session = get_db_session()
        
        # Get unique plants from plants table
        plants_query = session.query(
            TblPlants.plant_id,
            TblPlants.plant_name,
            TblPlants.client_name,
            TblPlants.type
        ).distinct().all()
        
        session.close()
        
        # Organize plants by client and type
        plants_dict = {}
        
        for plant_id, plant_name, client_name, plant_type in plants_query:
            if client_name not in plants_dict:
                plants_dict[client_name] = {'solar': [], 'wind': []}
            
            plant_info = {
                'plant_id': plant_id,
                'name': plant_name or plant_id,
                'client': client_name
            }
            
            if plant_type in ['solar', 'wind']:
                plants_dict[client_name][plant_type].append(plant_info)
        
        logger.info(f"Retrieved {len(plants_query)} plants from database")
        return plants_dict
        
    except Exception as e:
        logger.error(f"Failed to get plants from DB: {e}")
        logger.error(traceback.format_exc())
        return {}

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_daily_aggregated_generation_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get daily aggregated generation data from database.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: date, generation
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query daily aggregated generation data
        query = session.query(
            TblGeneration.date,
            func.sum(TblGeneration.generation).label('generation')
        ).filter(
            and_(
                TblGeneration.plant_id == plant_id,
                TblGeneration.date >= start_dt,
                TblGeneration.date <= end_dt
            )
        ).group_by(TblGeneration.date).order_by(TblGeneration.date)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No daily generation data found for plant {plant_id}")
            return pd.DataFrame(columns=['date', 'generation'])
        
        df = pd.DataFrame(result, columns=['date', 'generation'])
        df['date'] = pd.to_datetime(df['date'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} daily generation records for plant {plant_id}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get daily generation data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['date', 'generation'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_daily_aggregated_consumption_db(cons_unit: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get daily aggregated consumption data from database.
    
    Args:
        cons_unit: Consumption unit identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with columns: date, consumption
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query daily aggregated consumption data
        query = session.query(
            TblConsumption.date,
            func.sum(TblConsumption.consumption).label('consumption')
        ).filter(
            and_(
                TblConsumption.cons_unit == cons_unit,
                TblConsumption.date >= start_dt,
                TblConsumption.date <= end_dt
            )
        ).group_by(TblConsumption.date).order_by(TblConsumption.date)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No daily consumption data found for unit {cons_unit}")
            return pd.DataFrame(columns=['date', 'consumption'])
        
        df = pd.DataFrame(result, columns=['date', 'consumption'])
        df['date'] = pd.to_datetime(df['date'])
        df['consumption'] = pd.to_numeric(df['consumption'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} daily consumption records for unit {cons_unit}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get daily consumption data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['date', 'consumption'])

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_tod_aggregated_data_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get Time-of-Day aggregated data from database.
    
    Args:
        plant_id: Plant identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with ToD analysis
    """
    try:
        # Get settlement data first
        settlement_df = get_settlement_data_db(plant_id, start_date, end_date)
        
        if settlement_df.empty:
            return pd.DataFrame()
        
        # Get ToD slots configuration
        tod_slots = get_tod_slots()
        
        def assign_tod_bin(hour):
            """Assign ToD bin based on hour"""
            for slot_name, slot_info in tod_slots.items():
                start_hour = slot_info['start_hour']
                end_hour = slot_info['end_hour']
                
                if start_hour <= end_hour:
                    if start_hour <= hour < end_hour:
                        return slot_name
                else:  # Crosses midnight
                    if hour >= start_hour or hour < end_hour:
                        return slot_name
            return 'Unknown'
        
        # Add ToD bin column
        settlement_df['hour'] = settlement_df['datetime'].dt.hour
        settlement_df['date'] = settlement_df['datetime'].dt.date
        settlement_df['tod_bin'] = settlement_df['hour'].apply(assign_tod_bin)
        
        # Add surplus column for backward compatibility
        settlement_df['surplus'] = settlement_df['allocated_generation'] - settlement_df['consumption']
        
        # Check if this is a multi-day analysis
        start_date_obj = pd.to_datetime(start_date).date()
        end_date_obj = pd.to_datetime(end_date).date()
        is_multi_day = start_date_obj != end_date_obj
        
        if is_multi_day:
            # For multi-day analysis, we need to provide both:
            # 1. Daily breakdown (for daily comparison charts)
            # 2. Total aggregation across all days (for ToD comparison charts)
            
            # First, create daily breakdown by date and ToD bin
            daily_tod_aggregated = settlement_df.groupby(['date', 'tod_bin']).agg({
                'allocated_generation': 'sum',
                'consumption': 'sum',
                'surplus': 'sum',
                'surplus_demand': 'sum',
                'deficit': 'sum'
            }).reset_index()
            
            # Count intervals per date and ToD bin for daily data
            daily_interval_counts = settlement_df.groupby(['date', 'tod_bin']).size().reset_index(name='interval_count')
            daily_tod_aggregated = daily_tod_aggregated.merge(daily_interval_counts, on=['date', 'tod_bin'])
            
            # For multi-day ToD comparison, aggregate across all days by ToD bin only
            # This gives us the total for each ToD bin across all selected days
            total_tod_aggregated = settlement_df.groupby('tod_bin').agg({
                'allocated_generation': 'sum',
                'consumption': 'sum',
                'surplus': 'sum',
                'surplus_demand': 'sum',
                'deficit': 'sum'
            }).reset_index()
            
            # Count total intervals per ToD bin across all days
            total_interval_counts = settlement_df.groupby('tod_bin').size().reset_index(name='total_interval_count')
            total_tod_aggregated = total_tod_aggregated.merge(total_interval_counts, on='tod_bin')
            
            # Add date column to total aggregated data to maintain consistency
            # Use the start_date as representative date for total aggregation
            total_tod_aggregated['date'] = start_date_obj
            total_tod_aggregated['interval_count'] = total_tod_aggregated['total_interval_count']
            
            # Log the totals for debugging
            logger.info(f"Multi-day ToD aggregation for plant {plant_id}:")
            logger.info(f"Daily breakdown: {len(daily_tod_aggregated)} records")
            logger.info(f"Total aggregation: {len(total_tod_aggregated)} records")
            logger.info(f"Total generation across all days and ToD bins: {total_tod_aggregated['allocated_generation'].sum():.2f} kWh")
            logger.info(f"Total consumption across all days and ToD bins: {total_tod_aggregated['consumption'].sum():.2f} kWh")
            
            # Combine both datasets - daily breakdown + total aggregation
            # The visualization layer can choose which one to use based on the plot type
            tod_aggregated = pd.concat([daily_tod_aggregated, total_tod_aggregated], ignore_index=True)
            
        else:
            # For single-day analysis, aggregate by ToD bin only
            tod_aggregated = settlement_df.groupby('tod_bin').agg({
                'allocated_generation': 'sum',
                'consumption': 'sum',
                'surplus': 'sum',
                'surplus_demand': 'sum',
                'deficit': 'sum'
            }).reset_index()
            
            # Calculate normalized values (per 15-minute interval)
            # Count intervals per ToD bin
            interval_counts = settlement_df.groupby('tod_bin').size().reset_index(name='interval_count')
            tod_aggregated = tod_aggregated.merge(interval_counts, on='tod_bin')
            
            # Add date column for consistency
            tod_aggregated['date'] = start_date_obj
        
        # Normalize to per-interval values - this represents average per 15-minute interval
        for col in ['allocated_generation', 'consumption', 'surplus', 'surplus_demand', 'deficit']:
            tod_aggregated[f'{col}_normalized'] = tod_aggregated[col] / tod_aggregated['interval_count']
        
        # Add total columns (non-normalized) for cases where we need actual totals
        for col in ['allocated_generation', 'consumption', 'surplus', 'surplus_demand', 'deficit']:
            tod_aggregated[f'{col}_total'] = tod_aggregated[col]
        
        logger.info(f"Generated ToD aggregated data for plant {plant_id}")
        return tod_aggregated
        
    except Exception as e:
        logger.error(f"Failed to get ToD aggregated data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

@st.cache_data(ttl=3600)
@retry_on_exception()
def get_combined_plants_data_db(client_name: str, plant_type: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get combined data for all plants of a specific type for a client.
    
    Args:
        client_name: Client name
        plant_type: Plant type ('solar' or 'wind')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with combined generation data
    """
    try:
        session = get_db_session()
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Query combined generation data for all plants of the type
        query = session.query(
            TblGeneration.datetime,
            func.sum(TblGeneration.generation).label('generation')
        ).filter(
            and_(
                TblGeneration.client_name == client_name,
                TblGeneration.type == plant_type,
                TblGeneration.date >= start_dt,
                TblGeneration.date <= end_dt
            )
        ).group_by(TblGeneration.datetime).order_by(TblGeneration.datetime)
        
        # Execute query and convert to DataFrame
        result = query.all()
        session.close()
        
        if not result:
            logger.warning(f"No combined {plant_type} data found for client {client_name}")
            return pd.DataFrame(columns=['datetime', 'generation'])
        
        df = pd.DataFrame(result, columns=['datetime', 'generation'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} combined {plant_type} records for client {client_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get combined plants data from DB: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['datetime', 'generation'])

def get_plant_id_from_name(plant_name) -> Optional[str]:
    """
    Get plant ID from plant name by querying the database.
    
    Args:
        plant_name: Plant name or plant object
        
    Returns:
        Plant ID if found, None otherwise
    """
    try:
        # Handle case where plant_name is actually a plant object/dict
        if isinstance(plant_name, dict):
            if 'plant_id' in plant_name:
                return plant_name['plant_id']
            elif 'name' in plant_name:
                actual_plant_name = plant_name['name']
            else:
                logger.warning(f"Invalid plant object: {plant_name}")
                return None
        else:
            actual_plant_name = plant_name
        
        session = get_db_session()
        
        # Query to find plant_id by plant_name
        result = session.query(TblGeneration.plant_id).filter(
            TblGeneration.plant_name == actual_plant_name
        ).first()
        
        session.close()
        
        if result:
            return result[0]
        else:
            logger.warning(f"Plant ID not found for plant name: {actual_plant_name}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to get plant ID from name: {e}")
        return None

def get_consumption_unit_from_plant(plant_name) -> List[str]:
    """
    Get consumption units associated with a plant.
    This is a mapping function that needs to be implemented based on your business logic.
    
    Args:
        plant_name: Plant name or plant object
        
    Returns:
        List of consumption unit identifiers
    """
    try:
        session = get_db_session()
        
        # Handle case where plant_name is actually a plant object/dict
        if isinstance(plant_name, dict):
            if 'plant_id' in plant_name:
                plant_id = plant_name['plant_id']
            elif 'name' in plant_name:
                plant_id = get_plant_id_from_name(plant_name['name'])
            else:
                logger.warning(f"Invalid plant object: {plant_name}")
                return []
        else:
            # Handle case where plant_name is a string
            plant_id = get_plant_id_from_name(plant_name)
        
        if not plant_id:
            logger.warning(f"Could not determine plant_id for: {plant_name}")
            return []
        
        # Query to find all consumption units associated with the plant
        results = session.query(SettlementData.cons_unit).filter(
            SettlementData.plant_id == plant_id
        ).distinct().all()
        
        session.close()
        
        if results:
            # Extract consumption units from results
            cons_units = [result[0] for result in results]
            logger.info(f"Found {len(cons_units)} consumption units for plant: {plant_name}")
            return cons_units
        else:
            logger.warning(f"No consumption units found for plant: {plant_name}")
            return []
            
    except Exception as e:
        logger.error(f"Failed to get consumption units from plant: {e}")
        return []


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
        DataFrame with monthly before banking settlement data by ToD slots
    """
    try:
        session = get_db_session()
        
        # Build query with proper date handling
        # Since date is stored as string in 'YYYY-MM' format, cast it properly
        query = session.query(
            func.coalesce(func.cast(BankingSettlement.date, String), 'N/A').label('month'),
            BankingSettlement.slot_name,
            BankingSettlement.slot_time,
            func.sum(func.coalesce(BankingSettlement.surplus_generation_sum, 0)).label('total_generation'),
            func.sum(func.coalesce(BankingSettlement.surplus_demand_sum, 0)).label('total_consumption'),
            func.sum(func.coalesce(BankingSettlement.matched_settled_sum, 0)).label('settled_units_with_banking'),
            func.sum(func.coalesce(BankingSettlement.intra_settlement, 0)).label('intra_settlement'),
            func.sum(func.coalesce(BankingSettlement.inter_settlement, 0)).label('inter_settlement'),
            func.sum(func.coalesce(BankingSettlement.surplus_generation_sum_after_inter, 0)).label('surplus_generation_after_banking'),
            func.sum(func.coalesce(BankingSettlement.surplus_demand_sum_after_inter, 0)).label('surplus_demand_after_banking')
        )
        
        # Apply filters
        if plant_name != "Combined View":
            query = query.filter(BankingSettlement.plant_name == plant_name)
        
        if client_name:
            query = query.filter(BankingSettlement.client_name == client_name)
        
        # Group by month and slot
        query = query.group_by(
            func.coalesce(func.cast(BankingSettlement.date, String), 'N/A'),
            BankingSettlement.slot_name,
            BankingSettlement.slot_time
        ).order_by(
            func.coalesce(func.cast(BankingSettlement.date, String), 'N/A'),
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
                'total_consumption': float(result.total_consumption or 0),
                'settled_units_with_banking': float(result.settled_units_with_banking or 0),
                'intra_settlement': float(result.intra_settlement or 0),
                'inter_settlement': float(result.inter_settlement or 0),
                'surplus_generation_after_banking': float(result.surplus_generation_after_banking or 0),
                'surplus_demand_after_banking': float(result.surplus_demand_after_banking or 0)
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Retrieved {len(df)} banking settlement records for plant: {plant_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get monthly banking settlement data: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


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
            func.date_format(BankingSettlement.date, '%Y-%m').label('month'),
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
            func.date_format(BankingSettlement.date, '%Y-%m')
        ).order_by(
            func.date_format(BankingSettlement.date, '%Y-%m')
        )
        
        results = query.all()
        session.close()
        
        if not results:
            logger.warning(f"No banking settlement data found for plant: {plant_name}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        data = []
        for result in results:
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

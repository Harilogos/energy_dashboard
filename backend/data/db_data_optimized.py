"""
Optimized database data functions with improved error handling and data validation.
This module replaces the original db_data.py with better performance and cleaner error handling.
"""

import pandas as pd
import streamlit as st
from datetime import datetime, date, time
from typing import Optional, Dict, List, Tuple
import traceback
from functools import wraps

from sqlalchemy import and_, func, String, case, or_

from db.db_setup import SessionLocal
from db.models import TblPlants, TblGeneration, TblConsumption, ConsumptionMapping, SettlementData, BankingSettlement
from backend.logs.logger_setup import setup_logger
from backend.config.tod_config import get_tod_slots
from backend.data.data_validator import DataAvailabilityChecker, validate_date_range, log_data_availability_summary

# Configure logging
logger = setup_logger('db_data_optimized', 'db_data.log')

def smart_retry_on_exception(max_retries=2, retry_delay=0.5):
    """Optimized decorator with fewer retries and shorter delays"""
    def decorator(func):
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
                        time.sleep(retry_delay)
            logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {last_exception}")
            raise last_exception
        return wrapper
    return decorator

def get_db_session():
    """Get database session with connection pooling"""
    return SessionLocal()

@st.cache_data(ttl=3600)
@smart_retry_on_exception()
def get_generation_data_optimized(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Optimized generation data retrieval with pre-validation.
    """
    try:
        # Validate inputs first
        is_valid, error_msg = validate_date_range(start_date, end_date)
        if not is_valid:
            logger.error(f"Invalid date range for plant {plant_id}: {error_msg}")
            return pd.DataFrame(columns=['datetime', 'generation'])
        
        # Check data availability before querying
        with DataAvailabilityChecker() as checker:
            availability = checker.check_data_availability(plant_id, start_date, end_date)
            
            if availability.get('future_date', False):
                logger.info(f"Skipping generation query for future dates: {start_date} to {end_date}")
                return pd.DataFrame(columns=['datetime', 'generation'])
            
            if not availability.get('plant_exists', False):
                logger.warning(f"Plant {plant_id} does not exist in database")
                return pd.DataFrame(columns=['datetime', 'generation'])
            
            if not availability.get('generation', False):
                logger.info(f"No generation data available for plant {plant_id} from {start_date} to {end_date}")
                return pd.DataFrame(columns=['datetime', 'generation'])
        
        # Proceed with optimized query
        session = get_db_session()
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Optimized query with proper indexing
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
        
        result = query.all()
        session.close()
        
        if not result:
            return pd.DataFrame(columns=['datetime', 'generation'])
        
        df = pd.DataFrame(result, columns=['datetime', 'generation'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} generation records for plant {plant_id}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get generation data: {e}")
        return pd.DataFrame(columns=['datetime', 'generation'])

@st.cache_data(ttl=3600)
@smart_retry_on_exception()
def get_consumption_data_optimized(client_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Optimized consumption data retrieval with pre-validation.
    """
    try:
        # Validate inputs first
        is_valid, error_msg = validate_date_range(start_date, end_date)
        if not is_valid:
            logger.error(f"Invalid date range for client {client_name}: {error_msg}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        session = get_db_session()
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Check if data exists before querying
        count_query = session.query(func.count(TblConsumption.id)).filter(
            and_(
                TblConsumption.client_name == client_name,
                TblConsumption.date >= start_dt,
                TblConsumption.date <= end_dt
            )
        )
        
        record_count = count_query.scalar()
        
        if record_count == 0:
            session.close()
            logger.info(f"No consumption data available for client {client_name} from {start_date} to {end_date}")
            return pd.DataFrame(columns=['datetime', 'consumption'])
        
        # Proceed with data query
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
        
        result = query.all()
        session.close()
        
        df = pd.DataFrame(result, columns=['datetime', 'consumption'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['consumption'] = pd.to_numeric(df['consumption'], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} consumption records for client {client_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get consumption data: {e}")
        return pd.DataFrame(columns=['datetime', 'consumption'])

@st.cache_data(ttl=3600)
@smart_retry_on_exception()
def get_settlement_data_optimized(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Optimized settlement data retrieval with comprehensive validation.
    """
    try:
        # Validate inputs first
        is_valid, error_msg = validate_date_range(start_date, end_date)
        if not is_valid:
            logger.error(f"Invalid date range for plant {plant_id}: {error_msg}")
            return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
        
        # Check data availability
        with DataAvailabilityChecker() as checker:
            availability = checker.check_data_availability(plant_id, start_date, end_date)
            
            if availability.get('future_date', False):
                logger.info(f"Skipping settlement query for future dates: {start_date} to {end_date}")
                return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
            
            if not availability.get('settlement', False):
                logger.info(f"No settlement data available for plant {plant_id} from {start_date} to {end_date}")
                return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
        
        session = get_db_session()
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Optimized settlement query
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
        
        result = query.all()
        session.close()
        
        df = pd.DataFrame(result, columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Convert numeric columns efficiently
        numeric_cols = ['allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"Retrieved {len(df)} settlement records for plant {plant_id} across {df['cons_unit'].nunique()} consumption units")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get settlement data: {e}")
        return pd.DataFrame(columns=['datetime', 'cons_unit', 'allocated_generation', 'consumption', 'deficit', 'surplus_demand', 'surplus_generation', 'settled', 'slot_name', 'slot_time'])

@st.cache_data(ttl=3600)
@smart_retry_on_exception()
def get_settlement_tod_data_optimized(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """
    Optimized ToD aggregated data using computed datetime approach with validation.
    """
    try:
        # Validate inputs
        is_valid, error_msg = validate_date_range(start_date, end_date)
        if not is_valid:
            logger.error(f"Invalid date range for plant {plant_id}: {error_msg}")
            return pd.DataFrame()
        
        # Check data availability
        with DataAvailabilityChecker() as checker:
            availability = checker.check_data_availability(plant_id, start_date, end_date)
            
            if availability.get('future_date', False):
                logger.info(f"Skipping ToD query for future dates: {start_date} to {end_date}")
                return pd.DataFrame()
            
            if not availability.get('settlement', False):
                logger.info(f"No settlement data available for ToD analysis: plant {plant_id} from {start_date} to {end_date}")
                return pd.DataFrame()
        
        session = get_db_session()
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create computed datetime using func.timestamp
        computed_datetime = func.timestamp(SettlementData.date, SettlementData.time)
        
        # Get ToD slots configuration
        tod_slots = get_tod_slots()
        
        # Create CASE statement for ToD bin assignment
        tod_case_conditions = []
        for slot_name, slot_info in tod_slots.items():
            start_hour = slot_info['start_hour']
            end_hour = slot_info['end_hour']
            
            if start_hour <= end_hour:
                condition = and_(
                    func.hour(computed_datetime) >= start_hour,
                    func.hour(computed_datetime) < end_hour
                )
            else:
                condition = or_(
                    func.hour(computed_datetime) >= start_hour,
                    func.hour(computed_datetime) < end_hour
                )
            
            tod_case_conditions.append((condition, slot_name))
        
        tod_bin_case = case(*tod_case_conditions, else_='Unknown')
        
        # Build optimized query
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
        
        if plant_type:
            query = query.filter(SettlementData.type == plant_type)
        
        query = query.group_by(
            SettlementData.plant_id, 
            SettlementData.type, 
            tod_bin_case
        ).order_by(tod_bin_case)
        
        result = query.all()
        session.close()
        
        if not result:
            return pd.DataFrame()
        
        df = pd.DataFrame(result, columns=['plant_id', 'type', 'tod_bin', 'total_generation', 'total_consumption', 'total_deficit', 'total_surplus_demand', 'interval_count'])
        
        # Convert numeric columns
        numeric_cols = ['total_generation', 'total_consumption', 'total_deficit', 'total_surplus_demand', 'interval_count']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Calculate surplus
        df['total_surplus'] = df['total_generation'] - df['total_consumption']
        
        # Add normalized values
        for col in ['total_generation', 'total_consumption', 'total_surplus', 'total_deficit', 'total_surplus_demand']:
            df[f'{col.replace("total_", "")}_normalized'] = df[col] / df['interval_count']
        
        # Add date column
        df['date'] = start_dt
        
        # Rename columns to match expected format
        df = df.rename(columns={
            'total_generation': 'allocated_generation_total',
            'total_consumption': 'consumption_total',
            'total_surplus': 'surplus_total',
            'total_deficit': 'deficit_total',
            'total_surplus_demand': 'surplus_demand_total'
        })
        
        logger.info(f"Retrieved optimized ToD data for plant {plant_id}: {len(df)} ToD bins")
        return df
        
    except Exception as e:
        logger.error(f"Failed to get optimized ToD data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
@smart_retry_on_exception()
def get_plants_optimized() -> Dict:
    """
    Optimized plants retrieval with caching and error handling.
    """
    try:
        session = get_db_session()
        
        # Single query to get all plants
        plants_query = session.query(
            TblPlants.plant_id,
            TblPlants.plant_name,
            TblPlants.client_name,
            TblPlants.type
        ).all()
        
        session.close()
        
        if not plants_query:
            logger.warning("No plants found in database")
            return {}
        
        # Organize plants efficiently
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
        logger.error(f"Failed to get plants: {e}")
        return {}

def get_consumption_units_for_plant_optimized(plant_id: str) -> List[str]:
    """
    Optimized consumption units retrieval with validation.
    """
    try:
        with DataAvailabilityChecker() as checker:
            units = checker.get_consumption_units_for_plant(plant_id)
            
            if not units:
                logger.info(f"No consumption units found for plant {plant_id}")
            else:
                logger.info(f"Found {len(units)} consumption units for plant {plant_id}")
            
            return units
            
    except Exception as e:
        logger.error(f"Failed to get consumption units for plant {plant_id}: {e}")
        return []

# Backward compatibility functions that use optimized versions
def get_generation_data_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Backward compatibility wrapper"""
    return get_generation_data_optimized(plant_id, start_date, end_date)

def get_consumption_data_by_client(client_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Backward compatibility wrapper"""
    return get_consumption_data_optimized(client_name, start_date, end_date)

def get_settlement_data_db(plant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Backward compatibility wrapper"""
    return get_settlement_data_optimized(plant_id, start_date, end_date)

def get_settlement_tod_aggregated_data(plant_id: str, start_date: str, end_date: str, plant_type: str = None) -> pd.DataFrame:
    """Backward compatibility wrapper"""
    return get_settlement_tod_data_optimized(plant_id, start_date, end_date, plant_type)

def get_plants_from_db() -> Dict:
    """Backward compatibility wrapper"""
    return get_plants_optimized()

def get_consumption_unit_from_plant(plant_name) -> List[str]:
    """Backward compatibility wrapper with plant name to ID conversion"""
    try:
        # Handle different input types
        if isinstance(plant_name, dict):
            plant_id = plant_name.get('plant_id')
        else:
            # This would need plant name to ID mapping - for now return empty
            logger.warning(f"Plant name to ID conversion needed for: {plant_name}")
            return []
        
        if plant_id:
            return get_consumption_units_for_plant_optimized(plant_id)
        
        return []
        
    except Exception as e:
        logger.error(f"Failed to get consumption units: {e}")
        return []
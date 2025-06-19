"""
Data validation utilities for the Energy Generation Dashboard.
Provides centralized data availability checks and validation logic.
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple
from sqlalchemy import and_, func

from db.db_setup import SessionLocal
from db.models import TblPlants, TblGeneration, TblConsumption, SettlementData
from backend.logs.logger_setup import setup_logger

# Configure logging
logger = setup_logger('data_validator', 'data_validation.log')

class DataAvailabilityChecker:
    """Centralized data availability checker to prevent unnecessary warnings."""
    
    def __init__(self):
        self.session = None
    
    def __enter__(self):
        self.session = SessionLocal()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
    
    def is_future_date(self, check_date: str) -> bool:
        """Check if the given date is in the future."""
        try:
            date_obj = datetime.strptime(check_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            return date_obj > today
        except Exception:
            return False
    
    def get_available_date_range(self, plant_id: str) -> Tuple[Optional[date], Optional[date]]:
        """Get the available date range for a plant."""
        try:
            # Check generation data
            gen_range = self.session.query(
                func.min(TblGeneration.date),
                func.max(TblGeneration.date)
            ).filter(TblGeneration.plant_id == plant_id).first()
            
            # Check settlement data
            settle_range = self.session.query(
                func.min(SettlementData.date),
                func.max(SettlementData.date)
            ).filter(SettlementData.plant_id == plant_id).first()
            
            # Use the most comprehensive range
            min_date = None
            max_date = None
            
            if gen_range and gen_range[0]:
                min_date = gen_range[0]
                max_date = gen_range[1]
            
            if settle_range and settle_range[0]:
                if min_date is None or settle_range[0] < min_date:
                    min_date = settle_range[0]
                if max_date is None or settle_range[1] > max_date:
                    max_date = settle_range[1]
            
            return min_date, max_date
            
        except Exception as e:
            logger.error(f"Failed to get date range for plant {plant_id}: {e}")
            return None, None
    
    def check_plant_exists(self, plant_id: str) -> bool:
        """Check if a plant exists in the database."""
        try:
            exists = self.session.query(TblPlants).filter(
                TblPlants.plant_id == plant_id
            ).first() is not None
            return exists
        except Exception as e:
            logger.error(f"Failed to check plant existence for {plant_id}: {e}")
            return False
    
    def check_data_availability(self, plant_id: str, start_date: str, end_date: str) -> Dict[str, bool]:
        """
        Check data availability for a plant and date range.
        
        Returns:
            Dict with availability status for different data types
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Check if dates are in the future
            if self.is_future_date(start_date) or self.is_future_date(end_date):
                logger.info(f"Requested dates {start_date} to {end_date} are in the future")
                return {
                    'generation': False,
                    'consumption': False,
                    'settlement': False,
                    'future_date': True
                }
            
            # Check generation data
            gen_count = self.session.query(TblGeneration).filter(
                and_(
                    TblGeneration.plant_id == plant_id,
                    TblGeneration.date >= start_dt,
                    TblGeneration.date <= end_dt
                )
            ).count()
            
            # Check settlement data
            settle_count = self.session.query(SettlementData).filter(
                and_(
                    SettlementData.plant_id == plant_id,
                    SettlementData.date >= start_dt,
                    SettlementData.date <= end_dt
                )
            ).count()
            
            # Check consumption data (by client)
            plant_info = self.session.query(TblPlants).filter(
                TblPlants.plant_id == plant_id
            ).first()
            
            cons_count = 0
            if plant_info and plant_info.client_name:
                cons_count = self.session.query(TblConsumption).filter(
                    and_(
                        TblConsumption.client_name == plant_info.client_name,
                        TblConsumption.date >= start_dt,
                        TblConsumption.date <= end_dt
                    )
                ).count()
            
            return {
                'generation': gen_count > 0,
                'consumption': cons_count > 0,
                'settlement': settle_count > 0,
                'future_date': False,
                'plant_exists': plant_info is not None
            }
            
        except Exception as e:
            logger.error(f"Failed to check data availability: {e}")
            return {
                'generation': False,
                'consumption': False,
                'settlement': False,
                'future_date': False,
                'plant_exists': False
            }
    
    def get_consumption_units_for_plant(self, plant_id: str) -> List[str]:
        """Get all consumption units associated with a plant."""
        try:
            units = self.session.query(SettlementData.cons_unit).filter(
                SettlementData.plant_id == plant_id
            ).distinct().all()
            
            return [unit[0] for unit in units if unit[0]]
            
        except Exception as e:
            logger.error(f"Failed to get consumption units for plant {plant_id}: {e}")
            return []

def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, str]:
    """
    Validate date range parameters.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start_dt > end_dt:
            return False, "Start date cannot be after end date"
        
        # Check if dates are too far in the future
        today = datetime.now().date()
        if start_dt > today + timedelta(days=1):
            return False, f"Start date {start_date} is too far in the future"
        
        # Check if date range is too large (more than 1 year)
        if (end_dt - start_dt).days > 365:
            return False, "Date range cannot exceed 365 days"
        
        return True, ""
        
    except ValueError as e:
        return False, f"Invalid date format: {e}"
    except Exception as e:
        return False, f"Date validation error: {e}"

def get_recommended_date_range(plant_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Get recommended date range for a plant based on available data.
    
    Returns:
        Tuple of (start_date, end_date) as strings, or (None, None) if no data
    """
    try:
        with DataAvailabilityChecker() as checker:
            min_date, max_date = checker.get_available_date_range(plant_id)
            
            if min_date and max_date:
                # Recommend last 30 days of available data
                recommended_start = max(min_date, max_date - timedelta(days=30))
                return recommended_start.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')
            
            return None, None
            
    except Exception as e:
        logger.error(f"Failed to get recommended date range: {e}")
        return None, None

def log_data_availability_summary(plant_id: str, start_date: str, end_date: str):
    """Log a summary of data availability for debugging purposes."""
    try:
        with DataAvailabilityChecker() as checker:
            availability = checker.check_data_availability(plant_id, start_date, end_date)
            
            logger.info(f"Data availability summary for plant {plant_id} ({start_date} to {end_date}):")
            logger.info(f"  - Plant exists: {availability.get('plant_exists', False)}")
            logger.info(f"  - Generation data: {availability.get('generation', False)}")
            logger.info(f"  - Consumption data: {availability.get('consumption', False)}")
            logger.info(f"  - Settlement data: {availability.get('settlement', False)}")
            logger.info(f"  - Future date: {availability.get('future_date', False)}")
            
            if not any([availability.get('generation'), availability.get('settlement')]):
                # Suggest alternative date range
                rec_start, rec_end = get_recommended_date_range(plant_id)
                if rec_start and rec_end:
                    logger.info(f"  - Recommended date range: {rec_start} to {rec_end}")
            
    except Exception as e:
        logger.error(f"Failed to log data availability summary: {e}")
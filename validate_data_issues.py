#!/usr/bin/env python3
"""
Data Validation Script for Generation vs Consumption Analysis
Validates data issues between regular and ToD plots for specific date range and plant.

Date Range: 2025/03/01 – 2025/03/31
Plant: KIDS CLINIC
"""

import pandas as pd
import sys
import os
from datetime import datetime, date
import traceback

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import required modules
from backend.data.db_data import (
    get_settlement_data_db,
    get_tod_aggregated_data_db,
    get_generation_data_db,
    get_consumption_data_by_client,
    get_db_session
)
from backend.data.db_data_manager import (
    get_daily_generation_consumption_comparison,
    get_tod_binned_data,
    get_plant_id
)
from backend.logs.logger_setup import setup_logger
from backend.config.tod_config import get_tod_slots

# Setup logging
logger = setup_logger('data_validation', 'data_validation.log')

class DataValidator:
    def __init__(self, plant_name, start_date, end_date):
        self.plant_name = plant_name
        self.start_date = start_date
        self.end_date = end_date
        self.start_str = start_date.strftime('%Y-%m-%d')
        self.end_str = end_date.strftime('%Y-%m-%d')
        self.plant_id = None
        self.results = {}
        
    def get_plant_info(self):
        """Get plant ID and basic info"""
        try:
            self.plant_id = get_plant_id(self.plant_name)
            if not self.plant_id:
                logger.error(f"Could not find plant_id for {self.plant_name}")
                return False
            
            logger.info(f"Plant: {self.plant_name}, Plant ID: {self.plant_id}")
            logger.info(f"Date Range: {self.start_str} to {self.end_str}")
            return True
        except Exception as e:
            logger.error(f"Error getting plant info: {e}")
            return False
    
    def validate_raw_settlement_data(self):
        """Validate raw settlement data from database"""
        try:
            logger.info("=== VALIDATING RAW SETTLEMENT DATA ===")
            
            settlement_df = get_settlement_data_db(self.plant_id, self.start_str, self.end_str)
            
            if settlement_df.empty:
                logger.error("No settlement data found!")
                self.results['raw_settlement'] = {'status': 'EMPTY', 'records': 0}
                return False
            
            # Basic statistics
            total_records = len(settlement_df)
            unique_dates = settlement_df['datetime'].dt.date.nunique()
            unique_cons_units = settlement_df['cons_unit'].nunique()
            date_range = f"{settlement_df['datetime'].dt.date.min()} to {settlement_df['datetime'].dt.date.max()}"
            
            logger.info(f"Total settlement records: {total_records}")
            logger.info(f"Unique dates: {unique_dates}")
            logger.info(f"Unique consumption units: {unique_cons_units}")
            logger.info(f"Actual date range in data: {date_range}")
            logger.info(f"Consumption units: {settlement_df['cons_unit'].unique().tolist()}")
            
            # Check for missing data by date
            expected_dates = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
            actual_dates = pd.to_datetime(settlement_df['datetime'].dt.date.unique())
            missing_dates = set(expected_dates.date) - set(actual_dates.date)
            
            if missing_dates:
                logger.warning(f"Missing dates: {sorted(missing_dates)}")
            else:
                logger.info("All expected dates are present")
            
            # Aggregation validation
            total_generation = settlement_df['allocated_generation'].sum()
            total_consumption = settlement_df['consumption'].sum()
            
            logger.info(f"Raw totals - Generation: {total_generation:.2f} kWh, Consumption: {total_consumption:.2f} kWh")
            
            # Check for duplicates or data quality issues
            duplicates = settlement_df.duplicated(subset=['datetime', 'cons_unit']).sum()
            if duplicates > 0:
                logger.warning(f"Found {duplicates} duplicate records")
            
            # Sample data for inspection
            logger.info("Sample records:")
            sample_df = settlement_df.head(10)[['datetime', 'cons_unit', 'allocated_generation', 'consumption']]
            for _, row in sample_df.iterrows():
                logger.info(f"  {row['datetime']} | {row['cons_unit']} | Gen: {row['allocated_generation']} | Cons: {row['consumption']}")
            
            self.results['raw_settlement'] = {
                'status': 'SUCCESS',
                'records': total_records,
                'unique_dates': unique_dates,
                'unique_cons_units': unique_cons_units,
                'total_generation': total_generation,
                'total_consumption': total_consumption,
                'missing_dates': list(missing_dates),
                'duplicates': duplicates
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating raw settlement data: {e}")
            logger.error(traceback.format_exc())
            self.results['raw_settlement'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def validate_regular_generation_consumption(self):
        """Validate regular generation vs consumption data"""
        try:
            logger.info("=== VALIDATING REGULAR GENERATION VS CONSUMPTION ===")
            
            daily_df = get_daily_generation_consumption_comparison(self.plant_name, self.start_date, self.end_date)
            
            if daily_df.empty:
                logger.error("No daily generation-consumption data found!")
                self.results['regular_gen_cons'] = {'status': 'EMPTY'}
                return False
            
            total_generation = daily_df['generation_kwh'].sum()
            total_consumption = daily_df['consumption_kwh'].sum()
            num_days = len(daily_df)
            
            logger.info(f"Regular plot totals - Generation: {total_generation:.2f} kWh, Consumption: {total_consumption:.2f} kWh")
            logger.info(f"Number of days: {num_days}")
            
            # Daily breakdown
            logger.info("Daily breakdown (first 10 days):")
            for _, row in daily_df.head(10).iterrows():
                logger.info(f"  {row['date'].strftime('%Y-%m-%d')}: Gen={row['generation_kwh']:.2f}, Cons={row['consumption_kwh']:.2f}")
            
            self.results['regular_gen_cons'] = {
                'status': 'SUCCESS',
                'total_generation': total_generation,
                'total_consumption': total_consumption,
                'num_days': num_days,
                'daily_data': daily_df.to_dict('records')
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating regular generation-consumption: {e}")
            logger.error(traceback.format_exc())
            self.results['regular_gen_cons'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def validate_tod_data(self):
        """Validate ToD generation vs consumption data"""
        try:
            logger.info("=== VALIDATING TOD GENERATION VS CONSUMPTION ===")
            
            # First check the raw ToD aggregated data
            tod_raw_df = get_tod_aggregated_data_db(self.plant_id, self.start_str, self.end_str)
            
            if tod_raw_df.empty:
                logger.error("No raw ToD aggregated data found!")
                self.results['tod_raw'] = {'status': 'EMPTY'}
                return False
            
            logger.info(f"Raw ToD data shape: {tod_raw_df.shape}")
            logger.info(f"Raw ToD columns: {tod_raw_df.columns.tolist()}")
            logger.info(f"Unique dates in raw ToD data: {tod_raw_df['date'].unique()}")
            logger.info(f"Unique ToD bins: {tod_raw_df['tod_bin'].unique()}")
            
            # Check totals in raw data
            if 'allocated_generation_total' in tod_raw_df.columns:
                raw_total_gen = tod_raw_df['allocated_generation_total'].sum()
                raw_total_cons = tod_raw_df['consumption_total'].sum()
                logger.info(f"Raw ToD totals - Generation: {raw_total_gen:.2f} kWh, Consumption: {raw_total_cons:.2f} kWh")
            
            # Now check the processed ToD data
            tod_processed_df = get_tod_binned_data(self.plant_name, self.start_date, self.end_date)
            
            if tod_processed_df.empty:
                logger.error("No processed ToD data found!")
                self.results['tod_processed'] = {'status': 'EMPTY'}
                return False
            
            logger.info(f"Processed ToD data shape: {tod_processed_df.shape}")
            logger.info(f"Processed ToD columns: {tod_processed_df.columns.tolist()}")
            
            if 'generation_kwh' in tod_processed_df.columns:
                processed_total_gen = tod_processed_df['generation_kwh'].sum()
                processed_total_cons = tod_processed_df['consumption_kwh'].sum()
                logger.info(f"Processed ToD totals - Generation: {processed_total_gen:.2f} kWh, Consumption: {processed_total_cons:.2f} kWh")
                
                # ToD breakdown
                logger.info("ToD breakdown:")
                for _, row in tod_processed_df.iterrows():
                    logger.info(f"  {row['tod_bin']}: Gen={row['generation_kwh']:.2f}, Cons={row['consumption_kwh']:.2f}")
                
                self.results['tod_processed'] = {
                    'status': 'SUCCESS',
                    'total_generation': processed_total_gen,
                    'total_consumption': processed_total_cons,
                    'tod_data': tod_processed_df.to_dict('records')
                }
            else:
                logger.error("Missing generation_kwh column in processed ToD data")
                self.results['tod_processed'] = {'status': 'ERROR', 'error': 'Missing generation_kwh column'}
            
            self.results['tod_raw'] = {
                'status': 'SUCCESS',
                'shape': tod_raw_df.shape,
                'columns': tod_raw_df.columns.tolist(),
                'unique_dates': tod_raw_df['date'].unique().tolist(),
                'unique_tod_bins': tod_raw_df['tod_bin'].unique().tolist()
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating ToD data: {e}")
            logger.error(traceback.format_exc())
            self.results['tod_processed'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def validate_tod_configuration(self):
        """Validate ToD configuration and time slots"""
        try:
            logger.info("=== VALIDATING TOD CONFIGURATION ===")
            
            tod_slots = get_tod_slots()
            logger.info(f"ToD slots configuration: {tod_slots}")
            
            # Validate time coverage
            all_hours = set()
            for slot_name, slot_info in tod_slots.items():
                start_hour = slot_info['start_hour']
                end_hour = slot_info['end_hour']
                
                if start_hour <= end_hour:
                    hours = list(range(start_hour, end_hour))
                else:  # Crosses midnight
                    hours = list(range(start_hour, 24)) + list(range(0, end_hour))
                
                all_hours.update(hours)
                logger.info(f"Slot '{slot_name}': {start_hour}:00 - {end_hour}:00 (hours: {hours})")
            
            missing_hours = set(range(24)) - all_hours
            if missing_hours:
                logger.warning(f"Hours not covered by ToD slots: {sorted(missing_hours)}")
            else:
                logger.info("All 24 hours are covered by ToD slots")
            
            self.results['tod_config'] = {
                'status': 'SUCCESS',
                'slots': tod_slots,
                'covered_hours': sorted(all_hours),
                'missing_hours': sorted(missing_hours)
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating ToD configuration: {e}")
            self.results['tod_config'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def compare_totals(self):
        """Compare totals between regular and ToD data"""
        try:
            logger.info("=== COMPARING TOTALS ===")
            
            if 'regular_gen_cons' not in self.results or 'tod_processed' not in self.results:
                logger.error("Cannot compare - missing data from previous validations")
                return False
            
            regular_data = self.results['regular_gen_cons']
            tod_data = self.results['tod_processed']
            
            if regular_data['status'] != 'SUCCESS' or tod_data['status'] != 'SUCCESS':
                logger.error("Cannot compare - one or both data sources failed")
                return False
            
            regular_gen = regular_data['total_generation']
            regular_cons = regular_data['total_consumption']
            tod_gen = tod_data['total_generation']
            tod_cons = tod_data['total_consumption']
            
            gen_diff = abs(regular_gen - tod_gen)
            cons_diff = abs(regular_cons - tod_cons)
            gen_diff_pct = (gen_diff / regular_gen * 100) if regular_gen > 0 else 0
            cons_diff_pct = (cons_diff / regular_cons * 100) if regular_cons > 0 else 0
            
            logger.info("COMPARISON RESULTS:")
            logger.info(f"Regular Generation: {regular_gen:.2f} kWh")
            logger.info(f"ToD Generation:     {tod_gen:.2f} kWh")
            logger.info(f"Generation Diff:    {gen_diff:.2f} kWh ({gen_diff_pct:.2f}%)")
            logger.info("")
            logger.info(f"Regular Consumption: {regular_cons:.2f} kWh")
            logger.info(f"ToD Consumption:     {tod_cons:.2f} kWh")
            logger.info(f"Consumption Diff:    {cons_diff:.2f} kWh ({cons_diff_pct:.2f}%)")
            
            # Determine if differences are significant
            threshold_pct = 1.0  # 1% threshold
            gen_significant = gen_diff_pct > threshold_pct
            cons_significant = cons_diff_pct > threshold_pct
            
            if gen_significant or cons_significant:
                logger.warning("SIGNIFICANT DIFFERENCES DETECTED!")
                if gen_significant:
                    logger.warning(f"Generation difference of {gen_diff_pct:.2f}% exceeds threshold of {threshold_pct}%")
                if cons_significant:
                    logger.warning(f"Consumption difference of {cons_diff_pct:.2f}% exceeds threshold of {threshold_pct}%")
            else:
                logger.info("Differences are within acceptable range")
            
            self.results['comparison'] = {
                'regular_generation': regular_gen,
                'tod_generation': tod_gen,
                'generation_diff': gen_diff,
                'generation_diff_pct': gen_diff_pct,
                'regular_consumption': regular_cons,
                'tod_consumption': tod_cons,
                'consumption_diff': cons_diff,
                'consumption_diff_pct': cons_diff_pct,
                'significant_differences': gen_significant or cons_significant
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error comparing totals: {e}")
            self.results['comparison'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def validate_data_consistency(self):
        """Validate data consistency across different aggregation methods"""
        try:
            logger.info("=== VALIDATING DATA CONSISTENCY ===")
            
            # Get raw settlement data and manually aggregate
            settlement_df = get_settlement_data_db(self.plant_id, self.start_str, self.end_str)
            
            if settlement_df.empty:
                logger.error("No settlement data for consistency check")
                return False
            
            # Manual aggregation - sum by datetime first, then by date
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
            manual_total_cons = daily_manual['consumption'].sum()
            
            logger.info(f"Manual aggregation totals - Generation: {manual_total_gen:.2f} kWh, Consumption: {manual_total_cons:.2f} kWh")
            
            # Compare with regular data
            if 'regular_gen_cons' in self.results and self.results['regular_gen_cons']['status'] == 'SUCCESS':
                regular_gen = self.results['regular_gen_cons']['total_generation']
                regular_cons = self.results['regular_gen_cons']['total_consumption']
                
                gen_match = abs(manual_total_gen - regular_gen) < 0.01
                cons_match = abs(manual_total_cons - regular_cons) < 0.01
                
                logger.info(f"Manual vs Regular Generation match: {gen_match}")
                logger.info(f"Manual vs Regular Consumption match: {cons_match}")
                
                if not gen_match:
                    logger.warning(f"Generation mismatch: Manual={manual_total_gen:.2f}, Regular={regular_gen:.2f}")
                if not cons_match:
                    logger.warning(f"Consumption mismatch: Manual={manual_total_cons:.2f}, Regular={regular_cons:.2f}")
            
            self.results['consistency'] = {
                'manual_generation': manual_total_gen,
                'manual_consumption': manual_total_cons,
                'daily_records': len(daily_manual)
            }
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating data consistency: {e}")
            self.results['consistency'] = {'status': 'ERROR', 'error': str(e)}
            return False
    
    def generate_summary_report(self):
        """Generate a summary report of all validations"""
        try:
            logger.info("=== SUMMARY REPORT ===")
            
            logger.info(f"Plant: {self.plant_name}")
            logger.info(f"Date Range: {self.start_str} to {self.end_str}")
            logger.info(f"Plant ID: {self.plant_id}")
            logger.info("")
            
            # Status summary
            for key, result in self.results.items():
                if isinstance(result, dict) and 'status' in result:
                    logger.info(f"{key.upper()}: {result['status']}")
                    if result['status'] == 'ERROR':
                        logger.info(f"  Error: {result.get('error', 'Unknown error')}")
            
            logger.info("")
            
            # Key findings
            if 'comparison' in self.results and 'significant_differences' in self.results['comparison']:
                if self.results['comparison']['significant_differences']:
                    logger.warning("KEY FINDING: Significant differences detected between regular and ToD plots!")
                    logger.warning(f"Generation difference: {self.results['comparison']['generation_diff_pct']:.2f}%")
                    logger.warning(f"Consumption difference: {self.results['comparison']['consumption_diff_pct']:.2f}%")
                else:
                    logger.info("KEY FINDING: Regular and ToD plots show consistent totals")
            
            # Recommendations
            logger.info("")
            logger.info("RECOMMENDATIONS:")
            
            if 'raw_settlement' in self.results:
                raw_data = self.results['raw_settlement']
                if raw_data.get('missing_dates'):
                    logger.info("- Investigate missing dates in settlement data")
                if raw_data.get('duplicates', 0) > 0:
                    logger.info("- Review and clean duplicate records")
            
            if 'comparison' in self.results and self.results['comparison'].get('significant_differences'):
                logger.info("- Review ToD aggregation logic")
                logger.info("- Check date filtering in ToD functions")
                logger.info("- Verify consumption unit aggregation")
            
            return True
            
        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return False
    
    def run_full_validation(self):
        """Run all validation checks"""
        logger.info("Starting full data validation...")
        
        if not self.get_plant_info():
            return False
        
        # Run all validations
        validations = [
            self.validate_raw_settlement_data,
            self.validate_regular_generation_consumption,
            self.validate_tod_data,
            self.validate_tod_configuration,
            self.compare_totals,
            self.validate_data_consistency
        ]
        
        for validation in validations:
            try:
                validation()
            except Exception as e:
                logger.error(f"Validation {validation.__name__} failed: {e}")
        
        # Generate summary
        self.generate_summary_report()
        
        return True

def main():
    """Main function to run the validation"""
    # Configuration
    PLANT_NAME = "Kids Clinic India Limited"
    START_DATE = date(2025, 3, 1)
    END_DATE = date(2025, 3, 31)
    
    print(f"Data Validation Script")
    print(f"Plant: {PLANT_NAME}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Check the log file 'data_validation.log' for detailed results")
    print("-" * 50)
    
    # Create validator and run
    validator = DataValidator(PLANT_NAME, START_DATE, END_DATE)
    success = validator.run_full_validation()
    
    if success:
        print("Validation completed successfully!")
        print("Check the log file for detailed analysis and recommendations.")
    else:
        print("Validation encountered errors!")
        print("Check the log file for error details.")
    
    return success

if __name__ == "__main__":
    main()
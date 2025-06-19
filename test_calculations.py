"""
Test script to verify Generation vs Consumption calculations
"""
import pandas as pd
from datetime import datetime

def test_calculations():
    """Test the calculation logic with sample data"""
    
    # Sample data from the logs
    sample_data = {
        'time': pd.date_range('2025-03-12 00:00:00', periods=96, freq='15min'),
        'generation_kwh': [0.0] * 32 + [25.0] * 32 + [0.0] * 32,  # Simplified: generation during day hours
        'consumption_kwh': [140.46] * 96  # Constant consumption as seen in logs
    }
    
    df = pd.DataFrame(sample_data)
    
    # Calculate totals
    total_generation = df['generation_kwh'].sum()
    total_consumption = df['consumption_kwh'].sum()
    
    print(f"Total Generation: {total_generation:.2f} kWh")
    print(f"Total Consumption: {total_consumption:.2f} kWh")
    
    # Calculate replacement percentage
    if total_consumption > 0:
        actual_consumption_met = min(total_generation, total_consumption)
        replacement_percentage = (actual_consumption_met / total_consumption * 100)
        raw_replacement = (total_generation / total_consumption * 100)
        
        print(f"Actual consumption met: {actual_consumption_met:.2f} kWh")
        print(f"Replacement percentage: {replacement_percentage:.2f}%")
        print(f"Raw replacement ratio: {raw_replacement:.2f}%")
    
    # Convert to MWh
    total_generation_mwh = total_generation / 1000
    total_consumption_mwh = total_consumption / 1000
    
    print(f"Total Generation: {total_generation_mwh:.2f} MWh")
    print(f"Total Consumption: {total_consumption_mwh:.2f} MWh")
    
    # Calculate generation after loss (2.8%)
    loss_percentage = 2.8
    total_generation_after_loss_mwh = total_generation_mwh * (1 - loss_percentage / 100)
    total_generation_after_loss = total_generation_after_loss_mwh * 1000
    
    print(f"Generation after {loss_percentage}% loss: {total_generation_after_loss_mwh:.2f} MWh")
    
    # Calculate lapsed units
    lapsed_units = max(0, total_generation_after_loss - total_consumption)
    lapsed_units_mwh = lapsed_units / 1000
    
    print(f"Lapsed units: {lapsed_units:.2f} kWh ({lapsed_units_mwh:.2f} MWh)")
    
    # Test with actual log values
    print("\n--- Testing with actual log values ---")
    actual_generation = 2466.03  # From log line 60
    actual_consumption = 17489.76  # From log line 60
    
    print(f"Actual Total Generation: {actual_generation:.2f} kWh")
    print(f"Actual Total Consumption: {actual_consumption:.2f} kWh")
    
    # Replacement percentage
    actual_consumption_met = min(actual_generation, actual_consumption)
    replacement_percentage = (actual_consumption_met / actual_consumption * 100)
    
    print(f"Replacement percentage: {replacement_percentage:.2f}%")
    
    # Convert to MWh
    gen_mwh = actual_generation / 1000
    cons_mwh = actual_consumption / 1000
    
    print(f"Generation: {gen_mwh:.2f} MWh")
    print(f"Consumption: {cons_mwh:.2f} MWh")
    
    # After loss
    gen_after_loss_mwh = gen_mwh * (1 - 2.8 / 100)
    gen_after_loss = gen_after_loss_mwh * 1000
    
    print(f"Generation after loss: {gen_after_loss_mwh:.2f} MWh")
    
    # Lapsed units
    lapsed = max(0, gen_after_loss - actual_consumption)
    lapsed_mwh = lapsed / 1000
    
    print(f"Lapsed units: {lapsed:.2f} kWh ({lapsed_mwh:.2f} MWh)")

if __name__ == "__main__":
    test_calculations()
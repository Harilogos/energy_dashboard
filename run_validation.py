#!/usr/bin/env python3
"""
Simple runner script for data validation
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import and run the validation
from validate_data_issues import main

if __name__ == "__main__":
    print("Running Data Validation for KIDS CLINIC (2025/03/01 – 2025/03/31)")
    print("=" * 60)
    
    try:
        success = main()
        if success:
            print("\n[SUCCESS] Validation completed successfully!")
        else:
            print("\n[ERROR] Validation completed with errors!")
    except Exception as e:
        print(f"\n[FAILED] Validation failed with exception: {e}")
        import traceback
        traceback.print_exc()
    
    print("\nCheck 'data_validation.log' for detailed results.")
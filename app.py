"""
Main Streamlit application entry point for the Energy Generation Dashboard.
"""
import streamlit as st
import traceback
from dotenv import load_dotenv

from backend.config.app_config import setup_page
from backend.logs.error_logger import setup_error_logging
from backend.logs.logger_setup import setup_logger
from frontend.components.ui_components import create_client_plant_filters, create_date_filters
from src.display_components import (
    display_consumption_view,
    display_generation_consumption_view, 
    display_daily_generation_consumption_view,
    display_daily_consumption_view,
    display_combined_wind_solar_view,
    display_tod_binned_view,
    display_daily_tod_binned_view, 
    display_tod_generation_view,
    display_tod_consumption_view,
    display_generation_only_view,
    display_monthly_before_banking_settlement_view,
    display_monthly_banking_settlement_view
)

logger = setup_logger('app', 'app.log')

# Load environment variables
load_dotenv()

def main():
    """Main application function"""
    # Setup page configuration
    setup_page()

    # Setup error logging to capture all errors in error.log
    setup_error_logging()


    # Add custom CSS without the header title
    st.markdown("""
    <style>
    .header-container {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .header-title {
        color: #1E88E5;
        font-size: 28px;
        font-weight: bold;
        margin-bottom: 5px;
    }
    .header-subtitle {
        color: #424242;
        font-size: 16px;
        margin-bottom: 10px;
    }
    .sidebar-header {
        font-size: 20px;
        font-weight: bold;
        color: #1E88E5;
        margin-bottom: 10px;
    }
    .stSelectbox label {
        font-weight: bold;
        color: #424242;
    }
    </style>
    """, unsafe_allow_html=True)

    try:
        # Create client and plant filters
        filters = create_client_plant_filters()
        selected_client = filters["selected_client"]
        selected_plant = filters["selected_plant"]
        has_solar = filters["has_solar"]
        has_wind = filters["has_wind"]

        # Date filters
        start_date, end_date = create_date_filters()

        # Check if single day is selected
        is_single_day = start_date == end_date

        # Create tabs for Summary, ToD, Power Cost Analysis, and Validations
        summary_tab, tod_tab, cost_tab = st.tabs(["Summary", "ToD", "Power Cost Analysis"])

        # Summary Tab Content
        with summary_tab:
            # Show Generation vs Consumption based on date selection (1st plot)
            st.header("Generation vs Consumption")
            if is_single_day:
                display_generation_consumption_view(selected_plant, start_date, section="summary")
            else:
                display_daily_generation_consumption_view(selected_plant, start_date, end_date, section="summary")

            # Show generation plots as 2nd plot based on client configuration and plant selection
            if has_solar and has_wind:
                # Client has both wind and solar plants
                st.header("Combined Wind and Solar Generation")
                display_combined_wind_solar_view(selected_client, start_date, end_date, section="summary")
            elif selected_plant != "Combined View":
                # Client has only one plant OR user selected a specific plant
                st.header("Generation")
                display_generation_only_view(selected_plant, start_date, end_date, section="summary")
            else:
                # This case shouldn't occur, but handle it gracefully
                st.info("Please select a specific plant to view the Generation plot.")

            # Show Consumption based on date selection (3rd plot)
            st.header("Consumption")
            if is_single_day:
                display_consumption_view(selected_plant, start_date, section="summary")
            else:
                display_daily_consumption_view(selected_plant, start_date, end_date, section="summary")

     

        # ToD Tab Content
        with tod_tab:
            # Show Monthly ToD Before Banking (as the first plot)
            st.header("Monthly ToD Before Banking")
            if selected_plant != "Combined View":
                display_monthly_before_banking_settlement_view(selected_plant, section="tod")
            else:
                st.info("Please select a specific plant to view the Monthly ToD Before Banking.")

            # Show Monthly Banking Settlement (as the second plot)
            st.header("Monthly Banking Settlement")
            if selected_plant != "Combined View":
                display_monthly_banking_settlement_view(selected_plant, section="tod")
            else:
                st.info("Please select a specific plant to view the Monthly Banking Settlement.")

            # Show ToD Generation vs Consumption with custom time bins
            st.header("ToD Generation vs Consumption")

            # Only show the ToD binned view if we have a specific plant selected (not "Combined View")
            if selected_plant != "Combined View":
                # For single day view
                if is_single_day:
                    display_tod_binned_view(selected_plant, start_date, end_date, section="tod")
                # For multi-day view, use the daily ToD binned plot
                else:
                    display_daily_tod_binned_view(selected_plant, start_date, end_date, section="tod")
            else:
                st.info("Please select a specific plant to view the ToD Generation vs Consumption comparison.")

            # Show ToD Generation
            st.header("ToD Generation")
            if selected_plant != "Combined View":
                display_tod_generation_view(selected_plant, start_date, end_date, section="tod")
            else:
                st.info("Please select a specific plant to view the ToD Generation.")

            # Show ToD Consumption
            st.header("ToD Consumption")
            if selected_plant != "Combined View":
                display_tod_consumption_view(selected_plant, start_date, end_date, section="tod")
            else:
                st.info("Please select a specific plant to view the ToD Consumption.")

        # Power Cost Analysis Tab Content
        with cost_tab:
            st.header("Power Cost Analysis")

            if selected_plant != "Combined View":
                from src.display_components import display_power_cost_analysis
                display_power_cost_analysis(selected_plant, start_date, end_date, is_single_day)
            else:
                st.info("Please select a specific plant to view the Power Cost Analysis.")

     



    except Exception as e:
        logger.error(f"Application error: {e}")
        logger.error(traceback.format_exc())
        st.error("An unexpected error occurred. Please try again later.")
        st.error(f"Error details: {str(e)}")
        st.info("If this problem persists, please contact support.")

if __name__ == "__main__":
    main()

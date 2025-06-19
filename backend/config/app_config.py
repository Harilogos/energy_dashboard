"""
Configuration settings for the Streamlit application.
"""
import streamlit as st
import yaml
from pathlib import Path
from typing import Dict, Any

from backend.logs.logger_setup import setup_logger

logger = setup_logger('config', 'config.log')

# Default configuration
DEFAULT_CONFIG = {
    "app": {
        "title": "Solar & Wind Dashboard",
        "layout": "wide",
        "sidebar_state": "expanded",
        "theme": {
            "primaryColor": "#1E88E5",
            "backgroundColor": "#FFFFFF",
            "secondaryBackgroundColor": "#F0F2F6",
            "textColor": "#262730",
            "font": "sans serif"
        }
    },
    "data": {
        "cache_ttl": 3600,  # Cache time-to-live in seconds
        "max_rows": 10000,
        "date_format": "%Y-%m-%d",
        "consumption_csv_path": "Data/csv/Consumption data Cloud nine - processed_data.csv",
        # Performance optimization settings
        "enable_csv_caching": True,
        "csv_cache_ttl": 7200,  # 2 hours for CSV data
        "enable_concurrent_processing": True,
        "max_concurrent_requests": 4,
        "chunk_size": 1000,  # For processing large datasets
        "enable_data_compression": True,
        # Smart API caching settings
        "enable_smart_caching": True,
        "api_cache_ttl": 21600,  # 6 hours for API data
        "bulk_fetch_enabled": True,
        "bulk_fetch_months": 6,  # Fetch 6 months of data in bulk
        "auto_cleanup_days": 30,  # Auto cleanup cache older than 30 days
        "preload_common_ranges": True,  # Preload commonly used date ranges
        # Data availability settings
        "energy_metrics_start_year": 2024,  # Year from which energy metrics data is available
        "energy_metrics_start_month": 12    # Month from which energy metrics data is available
    },
    "visualization": {
        "default_height": 6,
        "default_width": 12,
        "dpi": 100,
        "style": "whitegrid",
        "colors": {
            "primary": "#4285F4",      # Softer blue
            "secondary": "#FBBC05",    # Softer amber
            "success": "#34A853",      # Softer green
            "danger": "#EA4335",       # Softer red
            "warning": "#FF9800",      # Orange
            "tod_generation": "#5E35B1", # Softer purple for ToD Generation
            "consumption": "#00897B",    # Teal for Consumption (easier on eyes than orange)
            "comparison": "#3949AB",     # Indigo for comparison charts
            "average": "#757575"         # Gray for average lines
        }
    },
    "generation": {
        "loss_percentage": 2.8  # Transmission/distribution loss percentage
    },
    # ToD configuration moved to tod_config.py
}

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file or use defaults"""
    config_path = Path("config/app_config.yaml")

    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                
                return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return DEFAULT_CONFIG
    else:
        logger.info("Using default configuration")
        return DEFAULT_CONFIG

# Global configuration object
CONFIG = load_config()

# ToD functions moved to tod_config.py
# Import these functions directly from tod_config.py where needed

def setup_page() -> None:
    """Configure Streamlit page settings"""
    st.set_page_config(
        page_title=CONFIG["app"]["title"],
        layout=CONFIG["app"]["layout"],
        initial_sidebar_state=CONFIG["app"]["sidebar_state"]
    )

    # Apply custom theme if specified
    if "theme" in CONFIG["app"]:
        theme = CONFIG["app"]["theme"]
        st.markdown(f"""
        <style>
            .reportview-container .main .block-container{{
                max-width: {theme.get("maxWidth", "1200px")};
                padding-top: {theme.get("paddingTop", "2rem")};
                padding-right: {theme.get("paddingRight", "2rem")};
                padding-left: {theme.get("paddingLeft", "2rem")};
                padding-bottom: {theme.get("paddingBottom", "2rem")};
            }}
            .reportview-container .main {{
                color: {theme.get("textColor", "#262730")};
                background-color: {theme.get("backgroundColor", "#FFFFFF")};
            }}
        </style>
        """, unsafe_allow_html=True)

    # Title removed as requested

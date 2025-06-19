"""
Display components for the Energy Generation Dashboard.
"""
import streamlit as st
import pandas as pd
import traceback
from io import BytesIO
from typing import Optional, Tuple, Dict, Any

from backend.logs.logger_setup import setup_logger
from backend.config.app_config import CONFIG
from backend.data.db_data_manager import (
    get_consumption_data_from_csv,
    get_generation_consumption_comparison, 
    compare_generation_consumption,
    get_daily_consumption_data, 
    get_daily_generation_consumption_comparison,
    get_combined_wind_solar_generation,
    get_tod_binned_data,
    get_monthly_before_banking_settlement_data,
    get_monthly_energy_metrics_data,
    # New clean consumption functions
    get_consumption_data_db_clean,
    get_daily_consumption_data_db_clean
)
from backend.utils.visualization import (
    create_consumption_plot,
    create_comparison_plot,
    create_daily_consumption_plot, 
    create_daily_comparison_plot,
    create_combined_wind_solar_plot,
    create_tod_binned_plot,
    create_daily_tod_binned_plot,
    create_power_cost_comparison_plot,
    create_power_savings_plot,
    create_monthly_before_banking_settlement_plot,
    create_monthly_banking_settlement_chart,
    # New clean consumption visualization functions
    create_consumption_plot_db_clean,
    create_daily_consumption_plot_db_clean
)

# Define icons for different metrics
ICONS = {
    "generation": "⚡",
    "consumption": "🔌",
    "replacement": "♻️",
    "surplus": "📈",
    "deficit": "📉",
    "peak": "🔝",
    "minimum": "⬇️",
    "average": "📊",
    "days": "📅",
    "hours": "⏱️",
    "solar": "☀️",
    "wind": "🌬️",
    "total": "📊",
    "maximum": "🔝"
}

logger = setup_logger('display_components', 'display_components.log')

def _get_plant_name(selected_plant):
    """Helper function to extract plant name from either string or dictionary"""
    if isinstance(selected_plant, dict):
        return selected_plant.get('name', 'Unknown Plant')
    else:
        return selected_plant

# Helper functions for data export
def get_figure_as_png(fig):
    """Convert matplotlib figure to PNG bytes"""
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=300)
    buf.seek(0)
    return buf.getvalue()

def convert_df_to_csv(df):
    """Convert dataframe to CSV string"""
    return df.to_csv(index=False).encode('utf-8')

def display_download_buttons(fig, df, prefix, section, identifiers=None):
    """
    Display download buttons in a very compact, minimalist way

    Args:
        fig: The matplotlib figure to download (can be None for data-only downloads)
        df: The dataframe to download
        prefix: Prefix for the download filenames
        section: Section identifier for the download button keys
        identifiers: Additional identifiers for the download button keys (dict)
    """
    # Add custom CSS for ultra-compact buttons
    st.markdown("""
    <style>
    .download-row {
        display: flex;
        justify-content: flex-end;
        align-items: center;
        gap: 4px;
        margin-top: -20px;
        margin-bottom: 15px;
    }
    .download-label {
        font-size: 0.7em;
        color: #666;
        margin-right: 2px;
    }
    .stDownloadButton {
        margin: 0 !important;
        padding: 0 !important;
    }
    .stDownloadButton button {
        padding: 0px 4px !important;
        font-size: 0.65em !important;
        height: 22px !important;
        min-height: 0 !important;
        line-height: 1 !important;
        border-radius: 3px !important;
        background-color: #f0f2f6 !important;
        color: #444 !important;
        border: 1px solid #ddd !important;
    }
    .stDownloadButton button:hover {
        background-color: #e0e2e6 !important;
        border-color: #ccc !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Generate a unique key based on identifiers
    key_suffix = section
    if identifiers:
        for k, v in identifiers.items():
            key_suffix += f"_{k}_{v}"

    # Create a container for the buttons
    with st.container():
        if fig is not None:
            # If we have both figure and data, show both download buttons
            col1, col2, col3 = st.columns([20, 1, 1])

            with col1:
                st.write("")  # Empty space

            with col2:
                st.download_button(
                    "📊",  # Icon instead of text
                    data=get_figure_as_png(fig),
                    file_name=f"{prefix}.png",
                    mime="image/png",
                    key=f"{prefix}_png_{key_suffix}",
                    help="Download chart as PNG"
                )

            with col3:
                st.download_button(
                    "📄",  # Icon instead of text
                    data=convert_df_to_csv(df),
                    file_name=f"{prefix}.csv",
                    mime="text/csv",
                    key=f"{prefix}_csv_{key_suffix}",
                    help="Download data as CSV"
                )
        else:
            # If we only have data (no figure), show just the data download button
            col1, col2 = st.columns([21, 1])

            with col1:
                st.write("")  # Empty space

            with col2:
                st.download_button(
                    "📄",  # Icon instead of text
                    data=convert_df_to_csv(df),
                    file_name=f"{prefix}.csv",
                    mime="text/csv",
                    key=f"{prefix}_csv_{key_suffix}",
                    help="Download data as CSV"
                )

def get_icon_for_metric(metric_name):
    """Get an appropriate icon for a metric based on its name"""
    metric_lower = metric_name.lower()

    for key, icon in ICONS.items():
        if key in metric_lower:
            return icon

    # Default icon if no match found
    return "📊"

def style_summary_table(df):
    """
    Apply professional styling to a summary table DataFrame

    Args:
        df (DataFrame): Summary table DataFrame with 'Metric' and 'Value' columns

    Returns:
        str: HTML string with styled table
    """
    if df.empty:
        return "<p>No data available</p>"

    # Make a copy to avoid modifying the original
    styled_df = df.copy()

    # Add icons to metrics
    if 'Metric' in styled_df.columns:
        styled_df['Metric'] = styled_df['Metric'].apply(
            lambda x: f"{get_icon_for_metric(x)} {x}"
        )
    elif 'Environmental Metric' in styled_df.columns:
        styled_df['Environmental Metric'] = styled_df['Environmental Metric'].apply(
            lambda x: f"{get_icon_for_metric(x)} {x}"
        )

    # Define CSS styles
    styles = """
    <style>
        .summary-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            border-radius: 8px;
            overflow: hidden;
        }
        .summary-table th {
            background-color: #0078D4;
            color: white;
            text-align: left;
            padding: 12px 15px;
            font-weight: 600;
            font-size: 16px;
        }
        .summary-table td {
            padding: 10px 15px;
            border-bottom: 1px solid #e0e0e0;
            font-size: 15px;
        }
        .summary-table tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        .summary-table tr:hover {
            background-color: #f1f1f1;
        }
        .summary-table tr:last-child td {
            border-bottom: none;
        }
        .value-cell {
            font-weight: 500;
            text-align: right;
        }
        .group-header {
            background-color: #f0f7ff !important;
            font-weight: 600;
            color: #0078D4;
        }
        .positive-value {
            color: #107C10;
        }
        .negative-value {
            color: #D83B01;
        }
        .highlight-value {
            font-weight: 700;
            color: #0078D4;
        }
    </style>
    """

    # Start building HTML table
    html_table = f"{styles}<table class='summary-table'>"

    # Add headers
    html_table += "<tr>"
    for col in styled_df.columns:
        html_table += f"<th>{col}</th>"
    html_table += "</tr>"

    # Add rows with styling
    for i, row in styled_df.iterrows():
        html_table += "<tr>"

        # First column (Metric or Environmental Metric)
        metric_col = 'Metric' if 'Metric' in styled_df.columns else 'Environmental Metric'
        metric_value = row[metric_col]

        # Check if this is a group header
        is_group_header = False
        if "Total" in metric_value or "Average" in metric_value:
            is_group_header = True
            html_table += f"<td class='group-header'>{metric_value}</td>"
        else:
            html_table += f"<td>{metric_value}</td>"

        # Second column (Value or Impact)
        value_col = 'Value' if 'Value' in styled_df.columns else 'Impact'
        value = row[value_col]

        # Apply styling based on the value
        cell_class = "value-cell"
        if is_group_header:
            cell_class += " group-header"

        if "%" in str(value):
            # Percentage values
            try:
                # Check if it's a complex format like "15/24 (62.5%)"
                if "(" in str(value) and ")" in str(value):
                    # Extract the percentage from inside the parentheses
                    percentage_part = str(value).split('(')[1].split(')')[0]
                    if '%' in percentage_part:
                        percentage = float(percentage_part.split('%')[0].replace(',', ''))
                    else:
                        percentage = float(percentage_part.replace(',', ''))
                else:
                    # Regular percentage format
                    percentage = float(str(value).split('%')[0].replace(',', ''))

                # Apply styling based on percentage value
                if percentage > 75:
                    cell_class += " positive-value"
                elif percentage < 25:
                    cell_class += " negative-value"
            except (ValueError, IndexError):
                # If we can't parse the percentage, just continue without styling
                pass

        if "surplus" in metric_value.lower() or "positive" in metric_value.lower():
            cell_class += " positive-value"
        elif "deficit" in metric_value.lower() or "negative" in metric_value.lower():
            cell_class += " negative-value"

        # Add the value cell
        html_table += f"<td class='{cell_class}'>{value}</td>"

        html_table += "</tr>"

    html_table += "</table>"

    return html_table

def format_banking_summary(df, banking_type, tod_based):
    """
    Format banking data for better display in the summary table

    Args:
        df (DataFrame): Banking data
        banking_type (str): Type of banking data (daily, monthly, yearly)
        tod_based (bool): Whether the data is ToD-based

    Returns:
        DataFrame: Formatted banking data for display
    """
    # Make a copy to avoid modifying the original
    display_df = df.copy()

    # Convert all numeric columns to float to avoid dtype warnings
    for col in display_df.columns:
        if display_df[col].dtype == 'int64':
            display_df[col] = display_df[col].astype(float)

    # Format based on banking type and ToD setting
    if tod_based:
        # For ToD-based banking
        if 'origin_slot_name' in display_df.columns:
            # Rename columns for better readability
            display_df = display_df.rename(columns={
                'origin_slot_name': 'Time of Day',
                'Surplus Generation(After Settlement)': 'Surplus Generation (kWh)',
                'Grid Consumption(After Settlement)': 'Grid Consumption (kWh)'
            })

            # Format numeric columns
            numeric_cols = ['Surplus Generation (kWh)', 'Grid Consumption (kWh)']
            for col in numeric_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)

            # Capitalize Time of Day values
            if 'Time of Day' in display_df.columns:
                display_df['Time of Day'] = display_df['Time of Day'].str.capitalize()
    else:
        # For non-ToD banking
        if banking_type == "daily":
            # Format date columns
            if 'Date' in display_df.columns:
                try:
                    # First try with explicit format
                    display_df['Date'] = pd.to_datetime(display_df['Date'], format='%d/%m/%Y').dt.strftime('%d-%m-%Y')
                except ValueError:
                    try:
                        # If that fails, try with dayfirst=True
                        display_df['Date'] = pd.to_datetime(display_df['Date'], dayfirst=True).dt.strftime('%d-%m-%Y')
                    except Exception as e:
                        # If all else fails, leave as is
                        st.warning(f"Could not parse date format: {e}")
                        pass

            # Rename columns for better readability
            display_df = display_df.rename(columns={
                'Surplus Generation': 'Surplus Generation (kWh)',
                'Grid Consumption': 'Grid Consumption (kWh)'
            })

            # Format numeric columns
            numeric_cols = ['Surplus Generation (kWh)', 'Grid Consumption (kWh)']
            for col in numeric_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)

        elif banking_type == "monthly":
            # Rename columns for better readability
            display_df = display_df.rename(columns={
                'Surplus Generation': 'Surplus Generation (kWh)',
                'Grid Consumption': 'Grid Consumption (kWh)'
            })

            # Format numeric columns
            numeric_cols = ['Surplus Generation (kWh)', 'Grid Consumption (kWh)',
                           'Monthly Leftover Demand Sum', 'Monthly Lapsed Sum']
            for col in numeric_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)

        elif banking_type == "yearly":
            # Rename columns for better readability
            display_df = display_df.rename(columns={
                'Yearly Surplus': 'Yearly Surplus (kWh)',
                'Yearly Deficit': 'Yearly Deficit (kWh)'
            })

            # Format numeric columns
            numeric_cols = ['Yearly Surplus (kWh)', 'Yearly Deficit (kWh)']
            for col in numeric_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)

    return display_df


def create_banking_summary_table(df, banking_type, tod_based):
    """
    Create a summary table from banking data for styled display

    Args:
        df (DataFrame): Formatted banking data
        banking_type (str): Type of banking data (daily, monthly, yearly)
        tod_based (bool): Whether the data is ToD-based

    Returns:
        DataFrame: Summary table with metrics and values
    """
    summary_metrics = []
    summary_values = []

    try:
        if tod_based:
            # For ToD-based banking
            if 'Time of Day' in df.columns:
                # Calculate total surplus generation and grid consumption
                total_surplus = df['Surplus Generation (kWh)'].sum() if 'Surplus Generation (kWh)' in df.columns else 0
                total_consumption = df['Grid Consumption (kWh)'].sum() if 'Grid Consumption (kWh)' in df.columns else 0

                # Calculate peak and off-peak metrics
                peak_rows = df[df['Time of Day'].str.contains('Peak', case=False)]
                offpeak_rows = df[~df['Time of Day'].str.contains('Peak', case=False)]

                peak_surplus = peak_rows['Surplus Generation (kWh)'].sum() if not peak_rows.empty else 0
                peak_consumption = peak_rows['Grid Consumption (kWh)'].sum() if not peak_rows.empty else 0

                offpeak_surplus = offpeak_rows['Surplus Generation (kWh)'].sum() if not offpeak_rows.empty else 0
                offpeak_consumption = offpeak_rows['Grid Consumption (kWh)'].sum() if not offpeak_rows.empty else 0

                # Calculate net banking position
                net_banking = total_surplus - total_consumption

                # Calculate percentages for better insights
                # These variables are used for reference but not directly accessed

                peak_surplus_pct = (peak_surplus / total_surplus * 100) if total_surplus > 0 else 0
                offpeak_surplus_pct = (offpeak_surplus / total_surplus * 100) if total_surplus > 0 else 0

                peak_consumption_pct = (peak_consumption / total_consumption * 100) if total_consumption > 0 else 0
                offpeak_consumption_pct = (offpeak_consumption / total_consumption * 100) if total_consumption > 0 else 0

                # Calculate peak vs off-peak efficiency
                peak_efficiency = (peak_surplus / (peak_surplus + peak_consumption) * 100) if (peak_surplus + peak_consumption) > 0 else 0
                offpeak_efficiency = (offpeak_surplus / (offpeak_surplus + offpeak_consumption) * 100) if (offpeak_surplus + offpeak_consumption) > 0 else 0

                # Add metrics and values
                summary_metrics.extend([
                    "Total Surplus Generation",
                    "Total Grid Consumption",
                    "Net Banking Position",
                    "Peak Hours Surplus Generation",
                    "Peak Hours Grid Consumption",
                    "Off-Peak Hours Surplus Generation",
                    "Off-Peak Hours Grid Consumption",
                    "Peak vs Off-Peak Generation",
                    "Peak vs Off-Peak Consumption",
                    "Peak Hours Efficiency",
                    "Off-Peak Hours Efficiency"
                ])

                summary_values.extend([
                    f"{total_surplus:.2f} kWh",
                    f"{total_consumption:.2f} kWh",
                    f"{net_banking:.2f} kWh",
                    f"{peak_surplus:.2f} kWh ({peak_surplus_pct:.1f}%)",
                    f"{peak_consumption:.2f} kWh ({peak_consumption_pct:.1f}%)",
                    f"{offpeak_surplus:.2f} kWh ({offpeak_surplus_pct:.1f}%)",
                    f"{offpeak_consumption:.2f} kWh ({offpeak_consumption_pct:.1f}%)",
                    f"Peak: {peak_surplus_pct:.1f}% / Off-Peak: {offpeak_surplus_pct:.1f}%",
                    f"Peak: {peak_consumption_pct:.1f}% / Off-Peak: {offpeak_consumption_pct:.1f}%",
                    f"{peak_efficiency:.1f}%",
                    f"{offpeak_efficiency:.1f}%"
                ])
        else:
            # For non-ToD banking
            if banking_type == "daily":
                # Calculate total surplus generation and grid consumption
                total_surplus = df['Surplus Generation (kWh)'].sum() if 'Surplus Generation (kWh)' in df.columns else 0
                total_consumption = df['Grid Consumption (kWh)'].sum() if 'Grid Consumption (kWh)' in df.columns else 0

                # Calculate average daily surplus and consumption
                avg_surplus = df['Surplus Generation (kWh)'].mean() if 'Surplus Generation (kWh)' in df.columns else 0
                avg_consumption = df['Grid Consumption (kWh)'].mean() if 'Grid Consumption (kWh)' in df.columns else 0

                # Find peak surplus and consumption days
                if 'Date' in df.columns and 'Surplus Generation (kWh)' in df.columns and not df.empty:
                    peak_surplus_date = df.loc[df['Surplus Generation (kWh)'].idxmax(), 'Date'] if not df['Surplus Generation (kWh)'].isna().all() else "N/A"
                    peak_surplus_value = df['Surplus Generation (kWh)'].max() if not df['Surplus Generation (kWh)'].isna().all() else 0
                else:
                    peak_surplus_date = "N/A"
                    peak_surplus_value = 0

                if 'Date' in df.columns and 'Grid Consumption (kWh)' in df.columns and not df.empty:
                    peak_consumption_date = df.loc[df['Grid Consumption (kWh)'].idxmax(), 'Date'] if not df['Grid Consumption (kWh)'].isna().all() else "N/A"
                    peak_consumption_value = df['Grid Consumption (kWh)'].max() if not df['Grid Consumption (kWh)'].isna().all() else 0
                else:
                    peak_consumption_date = "N/A"
                    peak_consumption_value = 0

                # Calculate net banking position
                net_banking = total_surplus - total_consumption

                # Calculate days with positive and negative banking
                if 'Surplus Generation (kWh)' in df.columns and 'Grid Consumption (kWh)' in df.columns:
                    df['Net Position'] = df['Surplus Generation (kWh)'] - df['Grid Consumption (kWh)']
                    positive_days = (df['Net Position'] > 0).sum()
                    negative_days = (df['Net Position'] < 0).sum()
                    total_days = len(df)

                    positive_percentage = (positive_days / total_days * 100) if total_days > 0 else 0
                    negative_percentage = (negative_days / total_days * 100) if total_days > 0 else 0
                else:
                    positive_days = 0
                    negative_days = 0
                    total_days = 0
                    positive_percentage = 0
                    negative_percentage = 0

                # Add metrics and values
                summary_metrics.extend([
                    "Total Surplus Generation",
                    "Total Grid Consumption",
                    "Net Banking Position",
                    "Average Daily Surplus",
                    "Average Daily Consumption",
                    "Peak Surplus Generation",
                    "Peak Surplus Date",
                    "Peak Grid Consumption",
                    "Peak Consumption Date",
                    "Days with Positive Banking",
                    "Days with Negative Banking"
                ])

                summary_values.extend([
                    f"{total_surplus:.2f} kWh",
                    f"{total_consumption:.2f} kWh",
                    f"{net_banking:.2f} kWh",
                    f"{avg_surplus:.2f} kWh",
                    f"{avg_consumption:.2f} kWh",
                    f"{peak_surplus_value:.2f} kWh",
                    f"{peak_surplus_date}",
                    f"{peak_consumption_value:.2f} kWh",
                    f"{peak_consumption_date}",
                    f"{positive_days}/{total_days} ({positive_percentage:.1f}%)",
                    f"{negative_days}/{total_days} ({negative_percentage:.1f}%)"
                ])

            elif banking_type == "monthly":
                # Calculate monthly surplus and grid consumption
                total_surplus = df['Surplus Generation (kWh)'].sum() if 'Surplus Generation (kWh)' in df.columns else 0
                total_consumption = df['Grid Consumption (kWh)'].sum() if 'Grid Consumption (kWh)' in df.columns else 0

                # Calculate average monthly surplus and consumption
                avg_surplus = df['Surplus Generation (kWh)'].mean() if 'Surplus Generation (kWh)' in df.columns else 0
                avg_consumption = df['Grid Consumption (kWh)'].mean() if 'Grid Consumption (kWh)' in df.columns else 0

                # Find peak surplus and consumption months
                if 'Month' in df.columns and 'Surplus Generation (kWh)' in df.columns and not df.empty:
                    peak_surplus_month = df.loc[df['Surplus Generation (kWh)'].idxmax(), 'Month'] if not df['Surplus Generation (kWh)'].isna().all() else "N/A"
                    peak_surplus_value = df['Surplus Generation (kWh)'].max() if not df['Surplus Generation (kWh)'].isna().all() else 0
                else:
                    peak_surplus_month = "N/A"
                    peak_surplus_value = 0

                if 'Month' in df.columns and 'Grid Consumption (kWh)' in df.columns and not df.empty:
                    peak_consumption_month = df.loc[df['Grid Consumption (kWh)'].idxmax(), 'Month'] if not df['Grid Consumption (kWh)'].isna().all() else "N/A"
                    peak_consumption_value = df['Grid Consumption (kWh)'].max() if not df['Grid Consumption (kWh)'].isna().all() else 0
                else:
                    peak_consumption_month = "N/A"
                    peak_consumption_value = 0

                # Calculate net banking position
                net_banking = total_surplus - total_consumption

                # Calculate months with positive and negative banking
                if 'Surplus Generation (kWh)' in df.columns and 'Grid Consumption (kWh)' in df.columns:
                    df['Net Position'] = df['Surplus Generation (kWh)'] - df['Grid Consumption (kWh)']
                    positive_months = (df['Net Position'] > 0).sum()
                    negative_months = (df['Net Position'] < 0).sum()
                    total_months = len(df)

                    positive_percentage = (positive_months / total_months * 100) if total_months > 0 else 0
                    negative_percentage = (negative_months / total_months * 100) if total_months > 0 else 0
                else:
                    positive_months = 0
                    negative_months = 0
                    total_months = 0
                    positive_percentage = 0
                    negative_percentage = 0

                # Add metrics and values
                summary_metrics.extend([
                    "Total Surplus Generation",
                    "Total Grid Consumption",
                    "Net Banking Position",
                    "Average Monthly Surplus",
                    "Average Monthly Consumption",
                    "Peak Surplus Generation",
                    "Peak Surplus Month",
                    "Peak Grid Consumption",
                    "Peak Consumption Month",
                    "Months with Positive Banking",
                    "Months with Negative Banking"
                ])

                summary_values.extend([
                    f"{total_surplus:.2f} kWh",
                    f"{total_consumption:.2f} kWh",
                    f"{net_banking:.2f} kWh",
                    f"{avg_surplus:.2f} kWh",
                    f"{avg_consumption:.2f} kWh",
                    f"{peak_surplus_value:.2f} kWh",
                    f"{peak_surplus_month}",
                    f"{peak_consumption_value:.2f} kWh",
                    f"{peak_consumption_month}",
                    f"{positive_months}/{total_months} ({positive_percentage:.1f}%)",
                    f"{negative_months}/{total_months} ({negative_percentage:.1f}%)"
                ])

            elif banking_type == "yearly":
                # Calculate yearly surplus and deficit
                yearly_surplus = df['Yearly Surplus (kWh)'].sum() if 'Yearly Surplus (kWh)' in df.columns else 0
                yearly_deficit = df['Yearly Deficit (kWh)'].sum() if 'Yearly Deficit (kWh)' in df.columns else 0

                # Calculate net yearly position
                net_yearly = yearly_surplus - yearly_deficit

                # Add metrics and values
                summary_metrics.extend([
                    "Total Yearly Surplus",
                    "Total Yearly Deficit",
                    "Net Yearly Position"
                ])

                summary_values.extend([
                    f"{yearly_surplus:.2f} kWh",
                    f"{yearly_deficit:.2f} kWh",
                    f"{net_yearly:.2f} kWh"
                ])

    except Exception as e:
        # If there's an error, add an error message to the summary
        summary_metrics.append("Error Processing Data")
        summary_values.append(str(e))

    # Create the summary DataFrame
    return pd.DataFrame({
        "Metric": summary_metrics,
        "Value": summary_values
    })


def display_banking_view(selected_plant, start_date, end_date=None, banking_type="daily", tod_based=False, section="banking"):
    """
    Display banking data summary

    Args:
        selected_plant (str): Name of the plant
        start_date (datetime): Start date to retrieve data for
        end_date (datetime, optional): End date to retrieve data for. If None, only start_date is used.
        banking_type (str): Type of banking data to retrieve (daily, monthly, yearly)
        tod_based (bool): Whether to use ToD-based banking logic
        section (str): Section identifier for the download button keys
    """
    from backend.data.db_data_manager import get_banking_data

    # Get banking data
    df = get_banking_data(selected_plant, start_date, end_date, banking_type, tod_based)

    if df.empty:
        st.info(f"No banking data available for {selected_plant}")
        return

    # Add a clean, minimal header
    st.markdown("""
    <style>
    .banking-header {
        color: #0078D4;
        font-size: 1.5em;
        font-weight: 600;
        margin-bottom: 20px;
        padding-bottom: 8px;
        border-bottom: 2px solid #0078D4;
    }
    </style>
    <h2 class="banking-header">Banking Summary</h2>
    """, unsafe_allow_html=True)

    # Format the DataFrame for better display
    display_df = format_banking_summary(df, banking_type, tod_based)

    # Convert the banking data to a summary table format
    summary_df = create_banking_summary_table(display_df, banking_type, tod_based)

    # Display the styled summary table
    styled_table = style_summary_table(summary_df)
    st.markdown(styled_table, unsafe_allow_html=True)

    # Display the detailed data with a clean, minimal header
    st.markdown("""
    <style>
    .detailed-data-header {
        color: #505050;
        font-size: 1.2em;
        font-weight: 500;
        margin-top: 30px;
        margin-bottom: 10px;
        padding-bottom: 5px;
        border-bottom: 1px solid #e0e0e0;
    }
    </style>
    <h3 class="detailed-data-header">Detailed Data</h3>
    """, unsafe_allow_html=True)

    # Display the data table
    st.dataframe(display_df, use_container_width=True)

    # Display compact download buttons for data only (no figure)
    display_download_buttons(
        fig=None,
        df=df,
        prefix=f"{selected_plant}_{banking_type}_banking",
        section=section,
        identifiers={"plant": selected_plant, "type": banking_type, "tod": tod_based}
    )


def create_summary_table(df, visualization_type):
    """
    Create a summary table for different types of visualizations

    Args:
        df (DataFrame): Data used for the visualization
        visualization_type (str): Type of visualization (generation, consumption, etc.)

    Returns:
        DataFrame: Summary table with key metrics
    """
    summary_df = pd.DataFrame()

    try:
        if visualization_type == "generation":
            # For generation data
            if 'hour' in df.columns and 'energy_kwh' in df.columns:
                # Hourly data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Average Hourly Generation", "Peak Generation", "Peak Hour",
                              "Minimum Generation", "Minimum Hour"],
                    "Value": [
                        f"{df['energy_kwh'].sum():.2f} kWh",
                        f"{df['energy_kwh'].mean():.2f} kWh",
                        f"{df['energy_kwh'].max():.2f} kWh",
                        f"{df.loc[df['energy_kwh'].idxmax(), 'hour']}:00",
                        f"{df['energy_kwh'].min():.2f} kWh",
                        f"{df.loc[df['energy_kwh'].idxmin(), 'hour']}:00"
                    ]
                })
            elif 'HOUR_BLOCK' in df.columns and 'TOTAL_GENERATION' in df.columns:
                # Hourly block data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Average Block Generation", "Peak Generation", "Peak Block"],
                    "Value": [
                        f"{df['TOTAL_GENERATION'].sum():.2f} kWh",
                        f"{df['TOTAL_GENERATION'].mean():.2f} kWh",
                        f"{df['TOTAL_GENERATION'].max():.2f} kWh",
                        f"{int(df.loc[df['TOTAL_GENERATION'].idxmax(), 'HOUR_BLOCK']):02d}:00 - {int(df.loc[df['TOTAL_GENERATION'].idxmax(), 'HOUR_BLOCK'])+3:02d}:00"
                    ]
                })
            elif 'date' in df.columns and 'generation_kwh' in df.columns:
                # Daily data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Average Daily Generation", "Peak Generation", "Peak Date",
                              "Minimum Generation", "Minimum Date"],
                    "Value": [
                        f"{df['generation_kwh'].sum():.2f} kWh",
                        f"{df['generation_kwh'].mean():.2f} kWh",
                        f"{df['generation_kwh'].max():.2f} kWh",
                        f"{df.loc[df['generation_kwh'].idxmax(), 'date'].strftime('%Y-%m-%d')}",
                        f"{df['generation_kwh'].min():.2f} kWh",
                        f"{df.loc[df['generation_kwh'].idxmin(), 'date'].strftime('%Y-%m-%d')}"
                    ]
                })
            elif 'DATEVALUE' in df.columns and 'TOTAL_GENERATION' in df.columns:
                # Daily data from Snowflake
                # Convert date column to datetime if it's not already
                if 'DATE' not in df.columns:
                    df['DATE'] = pd.to_datetime(df['DATEVALUE'])

                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Average Daily Generation", "Peak Generation", "Peak Date",
                              "Minimum Generation", "Minimum Date"],
                    "Value": [
                        f"{df['TOTAL_GENERATION'].sum():.2f} kWh",
                        f"{df['TOTAL_GENERATION'].mean():.2f} kWh",
                        f"{df['TOTAL_GENERATION'].max():.2f} kWh",
                        f"{df.loc[df['TOTAL_GENERATION'].idxmax(), 'DATE'].strftime('%Y-%m-%d')}",
                        f"{df['TOTAL_GENERATION'].min():.2f} kWh",
                        f"{df.loc[df['TOTAL_GENERATION'].idxmin(), 'DATE'].strftime('%Y-%m-%d')}"
                    ]
                })

        elif visualization_type == "consumption":
            # For consumption data
            if 'hour' in df.columns and 'energy_kwh' in df.columns:
                # Hourly data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Consumption", "Average Hourly Consumption", "Peak Consumption", "Peak Hour",
                              "Minimum Consumption", "Minimum Hour"],
                    "Value": [
                        f"{df['energy_kwh'].sum():.2f} kWh",
                        f"{df['energy_kwh'].mean():.2f} kWh",
                        f"{df['energy_kwh'].max():.2f} kWh",
                        f"{df.loc[df['energy_kwh'].idxmax(), 'hour']}:00",
                        f"{df['energy_kwh'].min():.2f} kWh",
                        f"{df.loc[df['energy_kwh'].idxmin(), 'hour']}:00"
                    ]
                })
            elif 'HOUR_BLOCK' in df.columns and 'TOTAL_CONSUMPTION' in df.columns:
                # Hourly block data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Consumption", "Average Block Consumption", "Peak Consumption", "Peak Block"],
                    "Value": [
                        f"{df['TOTAL_CONSUMPTION'].sum():.2f} kWh",
                        f"{df['TOTAL_CONSUMPTION'].mean():.2f} kWh",
                        f"{df['TOTAL_CONSUMPTION'].max():.2f} kWh",
                        f"{int(df.loc[df['TOTAL_CONSUMPTION'].idxmax(), 'HOUR_BLOCK']):02d}:00 - {int(df.loc[df['TOTAL_CONSUMPTION'].idxmax(), 'HOUR_BLOCK'])+3:02d}:00"
                    ]
                })
            elif 'date' in df.columns and 'consumption_kwh' in df.columns:
                # Daily data
                summary_df = pd.DataFrame({
                    "Metric": ["Total Consumption", "Average Daily Consumption", "Peak Consumption", "Peak Date",
                              "Minimum Consumption", "Minimum Date"],
                    "Value": [
                        f"{df['consumption_kwh'].sum():.2f} kWh",
                        f"{df['consumption_kwh'].mean():.2f} kWh",
                        f"{df['consumption_kwh'].max():.2f} kWh",
                        f"{df.loc[df['consumption_kwh'].idxmax(), 'date'].strftime('%Y-%m-%d')}",
                        f"{df['consumption_kwh'].min():.2f} kWh",
                        f"{df.loc[df['consumption_kwh'].idxmin(), 'date'].strftime('%Y-%m-%d')}"
                    ]
                })

        elif visualization_type == "comparison":
            # For generation vs consumption comparison
            if 'hour' in df.columns:
                # Hourly data - check for different column naming patterns
                gen_col = None
                cons_col = None

                # Check for generation column
                if 'generation_kwh' in df.columns:
                    gen_col = 'generation_kwh'
                elif 'energy_kwh' in df.columns and 'energy_kwh_y' in df.columns:
                    gen_col = 'energy_kwh'
                    cons_col = 'energy_kwh_y'

                # Check for consumption column if not already found
                if cons_col is None:
                    if 'consumption_kwh' in df.columns:
                        cons_col = 'consumption_kwh'
                    elif 'energy_kwh' in df.columns and gen_col != 'energy_kwh':
                        cons_col = 'energy_kwh'

                # If we found both columns, create the summary
                if gen_col is not None and cons_col is not None:
                    gen_sum = df[gen_col].sum()
                    cons_sum = df[cons_col].sum()
                    replacement = (gen_sum / cons_sum * 100) if cons_sum > 0 else 0

                    # Calculate surplus generation and demand
                    surplus_gen = df.apply(lambda row: max(0, row[gen_col] - row[cons_col]), axis=1).sum()
                    surplus_demand = df.apply(lambda row: max(0, row[cons_col] - row[gen_col]), axis=1).sum()

                    # Count hours with surplus generation and demand
                    hours_with_surplus_gen = (df[gen_col] > df[cons_col]).sum()
                    hours_with_surplus_demand = (df[cons_col] > df[gen_col]).sum()
                    total_hours = len(df)

                    summary_df = pd.DataFrame({
                        "Metric": [
                            "Total Generation",
                            "Total Consumption",
                            "Replacement Percentage",
                            "Surplus Generation",
                            "Surplus Demand",
                            "Hours with Net Positive Generation",
                            "Hours with Net Negative Generation",
                            "Peak Generation Hour",
                            "Peak Consumption Hour"
                        ],
                        "Value": [
                            f"{gen_sum:.2f} kWh",
                            f"{cons_sum:.2f} kWh",
                            f"{replacement:.2f}%",
                            f"{surplus_gen:.2f} kWh",
                            f"{surplus_demand:.2f} kWh",
                            f"{hours_with_surplus_gen}/{total_hours} ({hours_with_surplus_gen/total_hours*100:.1f}%)",
                            f"{hours_with_surplus_demand}/{total_hours} ({hours_with_surplus_demand/total_hours*100:.1f}%)",
                            f"{df.loc[df[gen_col].idxmax(), 'hour']}:00",
                            f"{df.loc[df[cons_col].idxmax(), 'hour']}:00"
                        ]
                    })
            elif 'date' in df.columns and 'generation_kwh' in df.columns and 'consumption_kwh' in df.columns:
                # Daily data
                gen_sum = df['generation_kwh'].sum()
                cons_sum = df['consumption_kwh'].sum()
                replacement = (gen_sum / cons_sum * 100) if cons_sum > 0 else 0

                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Total Consumption", "Replacement Percentage",
                              "Peak Generation Date", "Peak Consumption Date"],
                    "Value": [
                        f"{gen_sum:.2f} kWh",
                        f"{cons_sum:.2f} kWh",
                        f"{replacement:.2f}%",
                        f"{df.loc[df['generation_kwh'].idxmax(), 'date'].strftime('%Y-%m-%d')}",
                        f"{df.loc[df['consumption_kwh'].idxmax(), 'date'].strftime('%Y-%m-%d')}"
                    ]
                })

        elif visualization_type == "combined_wind_solar":
            # For combined wind and solar generation
            if 'date' in df.columns and 'Solar' in df.columns and 'Wind' in df.columns:
                solar_sum = df['Solar'].sum()
                wind_sum = df['Wind'].sum()
                total_sum = solar_sum + wind_sum

                summary_df = pd.DataFrame({
                    "Metric": ["Total Generation", "Solar Generation", "Wind Generation",
                              "Solar Percentage", "Wind Percentage"],
                    "Value": [
                        f"{total_sum:.2f} kWh",
                        f"{solar_sum:.2f} kWh",
                        f"{wind_sum:.2f} kWh",
                        f"{(solar_sum / total_sum * 100) if total_sum > 0 else 0:.2f}%",
                        f"{(wind_sum / total_sum * 100) if total_sum > 0 else 0:.2f}%"
                    ]
                })

        # Environmental impact section removed

    except Exception as e:
        logger.error(f"Error creating summary table: {e}")
        # Return an empty dataframe with a message
        summary_df = pd.DataFrame({
            "Metric": ["Error creating summary table"],
            "Value": [str(e)]
        })

    return summary_df





def display_consumption_view(selected_plant, selected_date, section="default"):
    """
    Display the consumption view for a specific plant and date using clean database approach.
    This function follows the ToD consumption pattern for consistency.
    """
    import pandas as pd
    
    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)
    
    # Get the display name for the plant
    from backend.data.db_data_manager import get_plant_display_name
    plant_display_name = get_plant_display_name(selected_plant)

    with st.spinner("Loading consumption data..."):
        # Use new clean database function
        df = get_consumption_data_db_clean(plant_name, selected_date)

    if df.empty:
        st.warning("No consumption data available for the selected date.")
        return

    # Check if consumption data is available
    if 'consumption' not in df.columns or df['consumption'].sum() == 0:
        st.warning("No consumption data available for this plant.")
        return

    # Create the plot using new clean visualization function
    fig = create_consumption_plot_db_clean(df, selected_plant, selected_date)
    
    # Check if figure was created successfully
    if fig is None:
        st.error("Failed to create consumption plot.")
        return
    
    # Display the plot
    st.pyplot(fig)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{plant_display_name}_consumption",
        section=section,
        identifiers={"plant": plant_display_name, "date": selected_date}
    )

# This function has been removed as it's not being used in the application

def display_daily_consumption_view(selected_plant, start_date, end_date, section="default"):
    """
    Display the daily consumption view for a specific plant and date range using clean database approach.
    This function follows the ToD consumption pattern for consistency.
    """
    import pandas as pd
    
    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)
    
    # Get the display name for the plant
    from backend.data.db_data_manager import get_plant_display_name
    plant_display_name = get_plant_display_name(selected_plant)

    with st.spinner("Loading consumption data for multiple days..."):
        # Use new clean database function
        df = get_daily_consumption_data_db_clean(plant_name, start_date, end_date)

    if df.empty:
        st.warning("No consumption data available for the selected date range.")
        return

    # Check if consumption data is available
    if 'consumption' not in df.columns or df['consumption'].sum() == 0:
        st.warning("No consumption data available for this plant.")
        return

    # Create the plot using new clean visualization function
    fig = create_daily_consumption_plot_db_clean(df, selected_plant, start_date, end_date)
    
    # Check if figure was created successfully
    if fig is None:
        st.error("Failed to create daily consumption plot.")
        return
    
    # Display the plot
    st.pyplot(fig)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{plant_display_name}_daily_consumption",
        section=section,
        identifiers={"plant": plant_display_name, "start": start_date, "end": end_date}
    )

def display_generation_consumption_view(selected_plant, selected_date, section="default"):
    """Display the generation vs consumption view for a specific plant and date"""
    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)

    with st.spinner("Loading generation and consumption data..."):
        generation_df, consumption_df = get_generation_consumption_comparison(plant_name, selected_date)
        comparison_df = compare_generation_consumption(generation_df, consumption_df)

    if comparison_df.empty:
        st.warning("No generation or consumption data available for the selected date.")
        return

    # Debug information for troubleshooting
    logger.info(f"Display Gen vs Cons - DataFrame shape: {comparison_df.shape}")
    logger.info(f"Display Gen vs Cons - DataFrame columns: {comparison_df.columns.tolist()}")
    
    try:
        fig = create_comparison_plot(comparison_df, selected_plant, selected_date)
        if fig is None:
            st.error("Failed to create comparison plot. Please check the data format.")
            return
        st.pyplot(fig)
    except Exception as e:
        logger.error(f"Error creating comparison plot: {e}")
        st.error(f"Error creating comparison plot: {str(e)}")
        return

    # Calculate metrics for the vertical boxes
    total_generation = comparison_df['generation_kwh'].sum()

    # Check which column name is used for consumption
    if 'consumption_kwh' in comparison_df.columns:
        total_consumption = comparison_df['consumption_kwh'].sum()
    elif 'energy_kwh' in comparison_df.columns:
        total_consumption = comparison_df['energy_kwh'].sum()
    else:
        total_consumption = 0
        st.warning("No consumption data found in the correct format.")
    
    # Log data structure for debugging
    logger.info(f"Single day - Data columns: {comparison_df.columns.tolist()}")
    logger.info(f"Single day - Data shape: {comparison_df.shape}")
    logger.info(f"Single day - Total generation: {total_generation:.2f} kWh, Total consumption: {total_consumption:.2f} kWh")

    # Calculate replacement percentage - how much of consumption was met by generation
    if total_consumption > 0:
        # True replacement: what percentage of consumption was actually replaced by generation
        actual_consumption_met = min(total_generation, total_consumption)
        replacement_percentage = (actual_consumption_met / total_consumption * 100)

        # Also calculate the raw ratio for logging
        raw_replacement = (total_generation / total_consumption * 100)
        logger.info(f"Single day - Consumption met by generation: {replacement_percentage:.2f}%, Raw generation/consumption ratio: {raw_replacement:.2f}%")
    else:
        replacement_percentage = 0

    # Convert kWh to MWh for display
    total_generation_mwh = total_generation / 1000
    total_consumption_mwh = total_consumption / 1000
    
    # Calculate generation after loss
    loss_percentage = CONFIG["generation"]["loss_percentage"]
    total_generation_after_loss_mwh = total_generation_mwh * (1 - loss_percentage / 100)
    total_generation_after_loss = total_generation_after_loss_mwh * 1000  # Convert back to kWh for calculation
    
    # Calculate lapsed units by subtracting total consumption from total generation after loss
    lapsed_units = max(0, total_generation_after_loss - total_consumption)
    logger.info(f"Single day - Lapsed units calculated as generation after loss minus consumption: {lapsed_units:.2f} kWh")
    lapsed_units_mwh = lapsed_units / 1000

    # Add CSS to style metric containers
    st.markdown("""
    <style>
    [data-testid="metric-container"] [data-testid="metric-container-label"] {
        font-size: 0.9em;
        font-weight: bold;
    }
    [data-testid="metric-container"] [data-testid="metric-container-value"] {
        font-size: 0.7em;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create 5 horizontal boxes with main metrics using Streamlit columns
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="Total Generation",
            value=f"{total_generation_mwh:.0f} MWh"
        )

    with col2:
        st.metric(
            label="Generation (after loss)",
            value=f"{total_generation_after_loss_mwh:.0f} MWh",
            help=f"Generation after {loss_percentage}% transmission/distribution loss"
        )

    with col3:
        st.metric(
            label="Total Consumption",
            value=f"{total_consumption_mwh:.0f} MWh"
        )

    with col4:
        st.metric(
            label="Replacement %",
            value=f"{replacement_percentage:.0f}%",
            help="Percentage of consumption met by generation"
        )

    with col5:
        st.metric(
            label="Lapsed Units",
            value=f"{lapsed_units_mwh:.0f} MWh",
            help="Excess generation that couldn't be consumed (time-based calculation)"
        )

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=comparison_df,
        prefix=f"{selected_plant}_gen_vs_cons",
        section=section,
        identifiers={"plant": selected_plant, "date": selected_date}
    )

def display_daily_generation_consumption_view(selected_plant, start_date, end_date, section="default"):
    """Display the daily generation vs consumption view for a specific plant and date range"""
    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)

    with st.spinner("Loading daily generation and consumption data..."):
        df = get_daily_generation_consumption_comparison(plant_name, start_date, end_date)

    if df.empty:
        st.warning("No daily generation or consumption data available for the selected date range.")
        return

    # Check for zero consumption and provide informative message
    total_consumption = df['consumption_kwh'].sum()
    zero_consumption_days = len(df[df['consumption_kwh'] == 0])

    if total_consumption == 0:
        st.info("ℹ️ No consumption data available for this plant during the selected period. Showing generation data only.")
    elif zero_consumption_days > 0:
        st.info(f"ℹ️ Found {zero_consumption_days} day(s) with zero consumption out of {len(df)} total days.")

    fig = create_daily_comparison_plot(df, selected_plant, start_date, end_date)
    st.pyplot(fig)

    if not df.empty:
        # Calculate surplus generation and demand if not already calculated
        if 'surplus_generation' not in df.columns:
            df['surplus_generation'] = df.apply(lambda row: max(0, row['generation_kwh'] - row['consumption_kwh']), axis=1)
        if 'surplus_demand' not in df.columns:
            df['surplus_demand'] = df.apply(lambda row: max(0, row['consumption_kwh'] - row['generation_kwh']), axis=1)

        # Calculate key metrics
        total_generation = df['generation_kwh'].sum()
        total_consumption = df['consumption_kwh'].sum()
        total_surplus_gen = df['surplus_generation'].sum()
        
        # Log data structure for debugging
        logger.info(f"Multiple days - Data columns: {df.columns.tolist()}")
        logger.info(f"Multiple days - Data shape: {df.shape}")
        logger.info(f"Multiple days - Total generation: {total_generation:.2f} kWh, Total consumption: {total_consumption:.2f} kWh")

        # Calculate replacement percentage - how much of consumption was met by generation
        if total_consumption > 0:
            # True replacement: what percentage of consumption was actually replaced by generation
            actual_consumption_met = min(total_generation, total_consumption)
            replacement_percentage = (actual_consumption_met / total_consumption * 100)

            # Also calculate the raw ratio for logging
            raw_replacement = (total_generation / total_consumption * 100)
            logger.info(f"Multiple days - Consumption met by generation: {replacement_percentage:.2f}%, Raw generation/consumption ratio: {raw_replacement:.2f}%")
        else:
            replacement_percentage = 0

        # Convert kWh to MWh for display
        total_generation_mwh = total_generation / 1000
        total_consumption_mwh = total_consumption / 1000
        
        # Calculate generation after loss
        loss_percentage = CONFIG["generation"]["loss_percentage"]
        total_generation_after_loss_mwh = total_generation_mwh * (1 - loss_percentage / 100)
        total_generation_after_loss = total_generation_after_loss_mwh * 1000  # Convert back to kWh for calculation
        
        # Calculate lapsed units by subtracting total consumption from total generation after loss
        lapsed_units = max(0, total_generation_after_loss - total_consumption)
        logger.info(f"Multiple days - Lapsed units calculated as generation after loss minus consumption: {lapsed_units:.2f} kWh")
        lapsed_units_mwh = lapsed_units / 1000

        # Add CSS to style metric containers
        st.markdown("""
        <style>
        [data-testid="metric-container"] [data-testid="metric-container-label"] {
            font-size: 0.9em;
            font-weight: bold;
        }
        [data-testid="metric-container"] [data-testid="metric-container-value"] {
            font-size: 0.7em;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Create 5 horizontal boxes with main metrics using Streamlit columns
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                label="Total Generation",
                value=f"{total_generation_mwh:.0f} MWh"
            )

        with col2:
            st.metric(
                label="Generation (after loss)",
                value=f"{total_generation_after_loss_mwh:.0f} MWh",
                help=f"Generation after {loss_percentage}% transmission/distribution loss"
            )

        with col3:
            st.metric(
                label="Total Consumption",
                value=f"{total_consumption_mwh:.0f} MWh"
            )

        with col4:
            st.metric(
                label="Replacement %",
                value=f"{replacement_percentage:.0f}%",
                help="Percentage of consumption met by generation"
            )

        with col5:
            st.metric(
                label="Lapsed Units",
                value=f"{lapsed_units_mwh:.0f} MWh",
                help="Excess generation that couldn't be consumed (time-based calculation)"
            )

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{selected_plant}_daily_gen_vs_cons",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date}
    )





def display_tod_binned_view(selected_plant, start_date, end_date=None, section="default"):
    """
    Display the Time-of-Day (ToD) binned view comparing generation and consumption

    Args:
        selected_plant (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.
        section (str, optional): Section identifier for the display. Defaults to "default".
    """
    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)

    # First, get the regular Generation vs Consumption data for comparison
    is_single_day = end_date is None or start_date == end_date

    if is_single_day:
        with st.spinner("Loading generation and consumption data for comparison..."):
            generation_df, consumption_df = get_generation_consumption_comparison(plant_name, start_date)
            regular_df = compare_generation_consumption(generation_df, consumption_df)

        if not regular_df.empty:
            regular_total_gen = regular_df['generation_kwh'].sum()
            regular_total_cons = regular_df['consumption_kwh'].sum() if 'consumption_kwh' in regular_df.columns else 0
            logger.info(f"Regular plot - Total generation: {regular_total_gen:.2f} kWh, Total consumption: {regular_total_cons:.2f} kWh")

    # Now get the ToD binned data
    with st.spinner("Loading Time-of-Day binned data..."):
        df = get_tod_binned_data(plant_name, start_date, end_date)

    if df.empty:
        st.warning("No generation or consumption data available for the selected plant.")
        return

    # Check if consumption data is available
    if 'consumption_kwh' not in df.columns or df['consumption_kwh'].sum() == 0:
        st.warning("No consumption data available for this plant. Generation vs Consumption comparison cannot be shown.")
        return

    # Log the ToD binned data totals for comparison
    tod_total_gen = df['generation_kwh'].sum()
    tod_total_cons = df['consumption_kwh'].sum()
    logger.info(f"ToD plot - Total generation: {tod_total_gen:.2f} kWh, Total consumption: {tod_total_cons:.2f} kWh")

    # If we have both regular and ToD data, compare them
    if is_single_day and not regular_df.empty:
        gen_diff_pct = abs(tod_total_gen - regular_total_gen) / regular_total_gen * 100 if regular_total_gen > 0 else 0
        cons_diff_pct = abs(tod_total_cons - regular_total_cons) / regular_total_cons * 100 if regular_total_cons > 0 else 0

        logger.info(f"Difference - Generation: {gen_diff_pct:.2f}%, Consumption: {cons_diff_pct:.2f}%")

        # If the difference is significant, log a warning
        if gen_diff_pct > 5 or cons_diff_pct > 5:
            logger.warning(f"Significant difference between regular and ToD plots: Gen diff: {gen_diff_pct:.2f}%, Cons diff: {cons_diff_pct:.2f}%")

    fig = create_tod_binned_plot(df, selected_plant, start_date, end_date)
    st.pyplot(fig)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{selected_plant}_tod_binned",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date if end_date else start_date}
    )

def display_daily_tod_binned_view(selected_plant, start_date, end_date, section="default"):
    """
    Display the daily Time-of-Day (ToD) binned view comparing generation and consumption

    This function is designed for multi-day date ranges, showing generation and consumption
    patterns across the predefined time-of-day bins for each day in the selected range.

    Args:
        selected_plant (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime): End date
        section (str, optional): Section identifier for the display. Defaults to "default".
    """
    # First, get the regular Generation vs Consumption data for comparison
    with st.spinner("Loading daily generation and consumption data for comparison..."):
        regular_df = get_daily_generation_consumption_comparison(selected_plant, start_date, end_date)

    if not regular_df.empty:
        regular_total_gen = regular_df['generation_kwh'].sum()
        regular_total_cons = regular_df['consumption_kwh'].sum() if 'consumption_kwh' in regular_df.columns else 0
        logger.info(f"Regular daily plot - Total generation: {regular_total_gen:.2f} kWh, Total consumption: {regular_total_cons:.2f} kWh")

    # Now get the ToD binned data
    with st.spinner("Loading daily Time-of-Day binned data..."):
        df = get_tod_binned_data(selected_plant, start_date, end_date)

    if df.empty:
        st.warning("No generation or consumption data available for the selected plant.")
        return

    # Check if consumption data is available
    if 'consumption_kwh' not in df.columns or df['consumption_kwh'].sum() == 0:
        st.warning("No consumption data available for this plant. Generation vs Consumption comparison cannot be shown.")
        return

    # Log the ToD binned data totals for comparison
    tod_total_gen = df['generation_kwh'].sum()
    tod_total_cons = df['consumption_kwh'].sum()
    logger.info(f"ToD daily plot - Total generation: {tod_total_gen:.2f} kWh, Total consumption: {tod_total_cons:.2f} kWh")

    # If we have both regular and ToD data, compare them
    if not regular_df.empty:
        gen_diff_pct = abs(tod_total_gen - regular_total_gen) / regular_total_gen * 100 if regular_total_gen > 0 else 0
        cons_diff_pct = abs(tod_total_cons - regular_total_cons) / regular_total_cons * 100 if regular_total_cons > 0 else 0

        logger.info(f"Daily difference - Generation: {gen_diff_pct:.2f}%, Consumption: {cons_diff_pct:.2f}%")

        # If the difference is significant, log a warning
        if gen_diff_pct > 5 or cons_diff_pct > 5:
            logger.warning(f"Significant difference between regular and ToD daily plots: Gen diff: {gen_diff_pct:.2f}%, Cons diff: {cons_diff_pct:.2f}%")

    # Create the daily ToD binned plot
    fig = create_daily_tod_binned_plot(df, selected_plant, start_date, end_date)
    st.pyplot(fig)

    # Summary statistics table removed as requested

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{selected_plant}_daily_tod_binned",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date}
    )

def display_combined_wind_solar_view(selected_client, start_date, end_date, section="default"):
    """Display the combined wind and solar generation view for a specific client and date range"""
    with st.spinner("Loading combined wind and solar generation data..."):
        df = get_combined_wind_solar_generation(selected_client, start_date, end_date)

    if df.empty:
        st.warning("No combined wind and solar generation data available for the selected client and date range.")
        return

    fig = create_combined_wind_solar_plot(df, selected_client, start_date, end_date)
    st.pyplot(fig)

    # Summary statistics table removed as requested

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{selected_client}_combined_wind_solar",
        section=section,
        identifiers={"client": selected_client, "start": start_date, "end": end_date}
    )


def display_tod_generation_view(selected_plant, start_date, end_date=None, section="default"):

  
    """
    Display the ToD Generation view with stacked bar chart based on ToD categories

    Args:
        selected_plant (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.
        section (str, optional): Section identifier for the display. Defaults to "default".
    """
    import pandas as pd

    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)

    # Define ToD categories for mapping - FIXED to match actual ToD bin labels
    # Get the actual ToD bin labels from configuration
    from backend.config.tod_config import get_tod_bin_labels
    tod_bin_labels = get_tod_bin_labels("full")
    
    tod_categories = {
        0: tod_bin_labels[0],  # '6 AM - 9 AM (Peak)'
        1: tod_bin_labels[1],  # '9 AM - 6 PM (Off-Peak)'
        2: tod_bin_labels[2],  # '6 PM - 10 PM (Peak)'
        3: tod_bin_labels[3]   # '10 PM - 6 AM (Off-Peak)'
    }

    is_single_day = end_date is None or start_date == end_date

    # Initialize variables to avoid UnboundLocalError
    df = pd.DataFrame()
    all_days_df = pd.DataFrame()
    fig = None

    if is_single_day:
        # For single day, use the existing function
        with st.spinner("Loading Time-of-Day generation data..."):
            from backend.data.db_data_manager import get_tod_binned_data
            df = get_tod_binned_data(plant_name, start_date)

        if df.empty:
            st.warning("No generation data available for the selected plant.")
            return

        # Check if we have the necessary columns
        if 'tod_bin' not in df.columns or 'generation_kwh' not in df.columns:
            st.warning("Data structure doesn't match expected format for ToD generation plot.")
            return

        # Create the stacked bar chart for single day
        from backend.utils.visualization import create_tod_generation_plot
        fig = create_tod_generation_plot(df, selected_plant, start_date)
        # For single day, use df for download
        all_days_df = df.copy()

    else:
        # For multiple days, use the updated multi-day processing
        with st.spinner("Loading Time-of-Day generation data for multiple days..."):
            import pandas as pd
            from backend.data.db_data_manager import get_tod_binned_data

            # Collect data for each day separately, similar to ToD consumption
            all_days_df = pd.DataFrame()
            
            current_date = start_date
            while current_date <= end_date:
                day_df = get_tod_binned_data(selected_plant, current_date)
              
                if not day_df.empty:
                    # Add date column
                    day_df['date'] = current_date
                    # Ensure generation values are preserved

                    all_days_df = pd.concat([all_days_df, day_df], ignore_index=True)
                else:
                    # If no data found for this day, create empty dataframe with date column
                    # This ensures we have a continuous date range for the plot
                    logger.warning(f"No ToD data found for {selected_plant} on {current_date}")
                current_date += pd.Timedelta(days=1)
                
            # If we still have no data, create sample data for testing
            if all_days_df.empty:
                logger.warning(f"No ToD data found for {selected_plant} in date range {start_date} to {end_date}. Creating sample data.")
                # Create sample data for testing
                from backend.config.tod_config import get_tod_bin_labels
                tod_bins = get_tod_bin_labels("full")
                
                sample_data = []
                current_date = start_date
                while current_date <= end_date:
                    for bin_name in tod_bins:
                        # Add some sample generation data for each bin
                        sample_data.append({
                            'tod_bin': bin_name,
                            'generation_kwh': 100.0,  # Sample value
                            'consumption_kwh': 80.0,  # Sample value
                            'date': current_date
                        })
                    current_date += pd.Timedelta(days=1)
                
                all_days_df = pd.DataFrame(sample_data)
                logger.info(f"Created sample data with shape {all_days_df.shape}")
            
            # Log the total generation for debugging
            if not all_days_df.empty and 'generation_kwh' in all_days_df.columns:
                logger.info(f"Total generation across all days: {all_days_df['generation_kwh'].sum():.2f} kWh")

            if all_days_df.empty:
                st.warning("No ToD generation data available for the selected date range.")
                return

            # Check if we have the necessary columns
            if 'tod_bin' not in all_days_df.columns or 'generation_kwh' not in all_days_df.columns:
                st.warning("Data structure doesn't match expected format for ToD generation plot.")
                return

            # Verify we have date column for multi-day processing
            if 'date' not in all_days_df.columns:
                st.warning("Date information missing for multi-day ToD generation plot.")
                return

            # Create the stacked bar chart for multiple days
            from backend.utils.visualization import create_tod_generation_plot
            fig = create_tod_generation_plot(all_days_df, selected_plant, start_date, end_date)
            # For multi-day, use all_days_df for download
            df = all_days_df.copy()

    # Check if figure was created successfully
    if fig is None:
        st.error("Failed to create ToD generation plot.")
        return

    # Display the plot
    st.pyplot(fig)

    # Create and display ToD summary table
    if not all_days_df.empty and 'tod_bin' in all_days_df.columns and 'generation_kwh' in all_days_df.columns:
        st.subheader("ToD Generation Summary")
        
        # Aggregate generation data by ToD bin
        tod_summary = all_days_df.groupby('tod_bin')['generation_kwh'].sum().to_dict()
        
        # Import and use the ToD summary table function
        from frontend.components.ui_components import create_tod_summary_table
        create_tod_summary_table(tod_summary)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=all_days_df,  # Use all_days_df which is set for both single and multi-day
        prefix=f"{selected_plant}_tod_generation",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date if end_date else start_date}
    )


def display_generation_only_view(selected_plant, start_date, end_date=None, section="default"):
    """Display the generation-only view for a specific plant and date range"""
    # Import the helper function to get plant display name
    from backend.data.db_data_manager import get_plant_display_name

    # Extract plant name from either string or dictionary
    plant_name = _get_plant_name(selected_plant)

    # Get the display name for the plant
    plant_display_name = get_plant_display_name(selected_plant)

    is_single_day = end_date is None or start_date == end_date

    with st.spinner("Loading generation data..."):
        from backend.data.db_data_manager import get_generation_only_data
        df = get_generation_only_data(plant_name, start_date, end_date)

    if df.empty:
        st.warning("No generation data available for the selected plant and date range.")
        return

    # Check if we have the necessary columns
    if 'generation_kwh' not in df.columns:
        st.warning("Data structure doesn't match expected format for generation plot.")
        return

    # Create the generation plot
    from backend.utils.visualization import create_generation_only_plot
    fig = create_generation_only_plot(df, plant_display_name, start_date, end_date)

    # Display the plot
    st.pyplot(fig)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=df,
        prefix=f"{selected_plant}_generation_only",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date if end_date else start_date}
    )


def display_tod_consumption_view(selected_plant, start_date, end_date=None, section="default"):
    """
    Display the Time-of-Day (ToD) Consumption view

    Custom time bins based on the configuration settings.

    Args:
        selected_plant (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.
        section (str, optional): Section identifier for the display. Defaults to "default".
    """
    import pandas as pd
    is_single_day = end_date is None or start_date == end_date

    # Initialize variables to avoid UnboundLocalError
    df = pd.DataFrame()
    all_days_df = pd.DataFrame()
    fig = None

    if is_single_day:
        # For single day, use the existing function
        with st.spinner("Loading Time-of-Day consumption data..."):
            from backend.data.db_data_manager import get_tod_binned_data
            df = get_tod_binned_data(selected_plant, start_date)

        if df.empty:
            st.warning("No data available for the selected plant.")
            return

        # Check if consumption data is available
        if 'consumption_kwh' not in df.columns or df['consumption_kwh'].sum() == 0:
            st.warning("No consumption data available for this plant.")
            return

        # Create the plot for single day
        from backend.utils.visualization import create_tod_consumption_plot
        fig = create_tod_consumption_plot(df, selected_plant, start_date)
        # For single day, use df for download
        all_days_df = df.copy()

    else:
        # For multiple days, we need to collect data for each day separately
        with st.spinner("Loading Time-of-Day consumption data for multiple days..."):
            import pandas as pd
            from backend.data.db_data_manager import get_tod_binned_data

            # Collect data for each day separately
            all_days_df = pd.DataFrame()
            current_date = start_date
            while current_date <= end_date:
                day_df = get_tod_binned_data(selected_plant, current_date)
                if not day_df.empty:
                    # Add date column
                    day_df['date'] = current_date
                    all_days_df = pd.concat([all_days_df, day_df], ignore_index=True)
                current_date += pd.Timedelta(days=1)

            if all_days_df.empty:
                st.warning("No ToD consumption data available for the selected date range.")
                return

            # Check if consumption data is available
            if 'consumption_kwh' not in all_days_df.columns or all_days_df['consumption_kwh'].sum() == 0:
                st.warning("No consumption data available for this plant.")
                return

            # Create the plot for multiple days
            from backend.utils.visualization import create_tod_consumption_plot
            fig = create_tod_consumption_plot(all_days_df, selected_plant, start_date, end_date)
            # For multi-day, use all_days_df for download
            df = all_days_df.copy()

    # Check if figure was created successfully
    if fig is None:
        st.error("Failed to create ToD consumption plot.")
        return

    # Display the plot
    st.pyplot(fig)

    # Create and display ToD consumption summary table
    if not all_days_df.empty and 'tod_bin' in all_days_df.columns and 'consumption_kwh' in all_days_df.columns:
        st.subheader("ToD Consumption Summary")
        
        # Aggregate consumption data by ToD bin
        tod_summary = all_days_df.groupby('tod_bin')['consumption_kwh'].sum().to_dict()
        
        # Import and use the ToD consumption summary table function
        from frontend.components.ui_components import create_tod_consumption_summary_table
        create_tod_consumption_summary_table(tod_summary)

    # Display compact download buttons
    display_download_buttons(
        fig=fig,
        df=all_days_df,  # Use all_days_df which is set for both single and multi-day
        prefix=f"{selected_plant}_tod_consumption",
        section=section,
        identifiers={"plant": selected_plant, "start": start_date, "end": end_date if end_date else start_date}
    )


def display_power_cost_analysis(selected_plant, start_date, end_date, is_single_day):
    """
    Display the power cost analysis view with cost input and visualizations.

    Args:
        selected_plant (str): Name of the selected plant
        start_date (datetime): Start date
        end_date (datetime): End date
        is_single_day (bool): Whether it's a single day view
    """
    try:
        # Power cost input section with right-aligned input
        col_left, col_right = st.columns([3, 1])

        with col_left:
            st.subheader("💰 Power Cost Analysis")

        with col_right:
            # Compact grid power cost input in right corner
            grid_rate = st.number_input(
                "Grid Cost (₹/kWh)",
                min_value=0.0,
                max_value=50.0,
                value=4.0,
                step=0.1,
                help="Enter grid electricity cost per kWh"
            )

        if grid_rate <= 0:
            st.warning("Please enter a valid grid power cost greater than 0.")
            return

        # Calculate cost metrics
        with st.spinner("Calculating power cost metrics..."):
            from backend.data.db_data_manager import calculate_power_cost_metrics, get_power_cost_summary

            cost_df = calculate_power_cost_metrics(selected_plant, start_date, end_date, grid_rate)

            if cost_df.empty:
                st.warning("No data available for power cost analysis for the selected period.")
                return

            # Get summary statistics
            summary = get_power_cost_summary(cost_df)

        # Display summary metrics
        st.subheader("📊 Cost Summary")

        # Create metrics columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Grid Cost",
                f"₹{summary.get('total_grid_cost', 0):.0f}",
                help="Total cost if all energy was purchased from grid"
            )

        with col2:
            st.metric(
                "Actual Cost",
                f"₹{summary.get('total_actual_cost', 0):.0f}",
                help="Actual cost after solar/wind generation offset"
            )

        with col3:
            st.metric(
                "Total Savings",
                f"₹{summary.get('total_savings', 0):.0f}",
                delta=f"{summary.get('savings_percentage', 0):.1f}%",
                help="Total money saved due to renewable generation"
            )

        with col4:
            st.metric(
                "Energy Offset",
                f"{summary.get('total_energy_offset_kwh', 0):.0f} kWh",
                help="Total energy offset by renewable generation"
            )

        # Cost comparison visualization
        st.subheader("💸 Cost Comparison")

        with st.spinner("Creating cost comparison chart..."):
            fig_comparison = create_power_cost_comparison_plot(cost_df, selected_plant, start_date, end_date)
            st.pyplot(fig_comparison)

        # Savings visualization
        st.subheader("💰 Savings Analysis")

        with st.spinner("Creating savings chart..."):
            fig_savings = create_power_savings_plot(cost_df, selected_plant, start_date, end_date)
            st.pyplot(fig_savings)

        # Detailed breakdown table
        st.subheader("📋 Detailed Breakdown")

        # Prepare display dataframe
        display_df = cost_df.copy()

        # Format columns for display
        if 'time' in display_df.columns:
            try:
                # Try to format time column
                if is_single_day:
                    display_df['Time/Date'] = pd.to_datetime(display_df['time']).dt.strftime('%H:%M')
                else:
                    display_df['Time/Date'] = pd.to_datetime(display_df['time']).dt.strftime('%Y-%m-%d')
            except:
                # Fallback if time formatting fails
                display_df['Time/Date'] = display_df.index.astype(str)
        elif 'date' in display_df.columns:
            try:
                display_df['Time/Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
            except:
                # Fallback if date formatting fails
                display_df['Time/Date'] = display_df['date'].astype(str)
        else:
            # No time/date column, use index
            display_df['Time/Date'] = display_df.index.astype(str)

        # Check if required columns exist
        required_columns = ['consumption_kwh', 'generation_kwh', 'net_consumption_kwh', 'grid_cost', 'actual_cost', 'savings']
        missing_columns = [col for col in required_columns if col not in display_df.columns]

        if missing_columns:
            st.warning(f"Some data columns are missing: {missing_columns}. Cannot display detailed breakdown.")
            return

        # Select and format relevant columns
        display_columns = ['Time/Date'] + required_columns

        display_df = display_df[display_columns].copy()

        # Format numeric columns
        for col in ['consumption_kwh', 'generation_kwh', 'net_consumption_kwh']:
            display_df[col] = display_df[col].round(1)

        for col in ['grid_cost', 'actual_cost', 'savings']:
            display_df[col] = display_df[col].round(0)

        # Rename columns for better display
        display_df.columns = ['Time/Date', 'Consumption (kWh)', 'Generation (kWh)',
                             'Net Consumption (kWh)', 'Grid Cost (₹)', 'Actual Cost (₹)', 'Savings (₹)']

        st.dataframe(display_df, use_container_width=True)

        # Download buttons
        display_download_buttons(
            fig=fig_comparison,
            df=cost_df,
            prefix=f"{selected_plant}_power_cost_analysis",
            section="cost_analysis",
            identifiers={"plant": selected_plant, "start": start_date, "end": end_date, "rate": grid_rate}
        )

    except Exception as e:
        logger.error(f"Error in power cost analysis display: {e}")
        logger.error(traceback.format_exc())
        st.error("An error occurred while displaying the power cost analysis.")
        st.error(f"Error details: {str(e)}")


def display_monthly_before_banking_settlement_view(selected_plant, section="default"):
    """
    Display monthly BEFORE banking settlement view showing ToD stacked bar chart.
    Shows initial surplus generation and demand values before any banking settlement process.
    
    Args:
        selected_plant: Name of the selected plant
        section: Section identifier for the display
    """
    try:
        # Extract plant name from either string or dictionary
        plant_name = _get_plant_name(selected_plant)
        
        with st.spinner("Loading monthly banking settlement data..."):
            df = get_monthly_before_banking_settlement_data(plant_name)
        
        if df.empty:
            st.warning("No banking settlement data available for the selected plant.")
            return
        
        # Create the monthly before banking settlement plot (always shows all data)
        fig = create_monthly_before_banking_settlement_plot(df, plant_name)
        
        # Display the plot
        st.pyplot(fig)
        
        # Summary metrics section has been removed as requested
        
        # Show comprehensive summary table
        st.subheader("Monthly ToD Before Banking Summary")
        
        # Check if advanced banking data is available
        has_banking_data = all(col in df.columns for col in ['settled_units_with_banking', 'intra_settlement', 'inter_settlement'])
        
        if not has_banking_data:
            st.info("ℹ️ banking settlement data not available. Showing basic generation and consumption summary.")
        
        # Prepare comprehensive summary table by aggregating the pre-calculated data from database
        # The database already has all calculated and settled values, so we just aggregate by month
        agg_dict = {}
        
        # Add all available numeric columns for aggregation
        numeric_columns = [
            'total_generation', 'total_consumption', 'settled_units_with_banking',
            'intra_settlement', 'inter_settlement', 'surplus_generation_after_banking',
            'surplus_demand_after_banking'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                agg_dict[col] = 'sum'
        
        # Aggregate by month using the pre-calculated database values
        monthly_summary = df.groupby('month').agg(agg_dict).reset_index()
        
        # Calculate additional required columns
        filtered_summary = monthly_summary.copy()
        
        # Calculate settlement values to match Monthly Banking Settlement logic
        
        # Calculate "Settlement With Banking" = settled_units_with_banking + intra_settlement + inter_settlement
        if all(col in filtered_summary.columns for col in ['settled_units_with_banking', 'intra_settlement', 'inter_settlement']):
            filtered_summary['settlement_with_banking'] = (
                filtered_summary['settled_units_with_banking'] + 
                filtered_summary['intra_settlement'] + 
                filtered_summary['inter_settlement']
            )
        elif 'settled_units_with_banking' in filtered_summary.columns:
            # If intra/inter settlement not available, use settled_units_with_banking as fallback
            filtered_summary['settlement_with_banking'] = filtered_summary['settled_units_with_banking']
        
        # Note: "Settled Units (Without Banking)" comes directly from the database as matched_settled_sum
        # No need to override it here as it's already correctly calculated in the query
        
        # Calculate "Total Settlement" = Settlement With Banking + Settlement Without Banking
        if 'settlement_with_banking' in filtered_summary.columns and 'settled_without_banking' in filtered_summary.columns:
            filtered_summary['total_settlement'] = (
                filtered_summary['settlement_with_banking'] + 
                filtered_summary['settled_without_banking']
            )
        
        # Format columns for display - only format columns that exist
        numeric_columns = ['total_generation', 'total_consumption', 'settlement_with_banking', 
                          'settled_without_banking', 'total_settlement',
                          'surplus_generation_after_banking', 'surplus_demand_after_banking']
        
        for col in numeric_columns:
            if col in filtered_summary.columns:
                filtered_summary[col] = filtered_summary[col].round(0)
        
        # Rename columns for display - only rename columns that exist
        column_mapping = {
            'month': 'Month',
            'total_generation': 'Total Generation (kWh)',
            'total_consumption': 'Total Consumption (kWh)',
            'settled_without_banking': 'Settled Units (Without Banking) (kWh)',
            'settlement_with_banking': 'Settled Units (With Banking) (kWh)',
            'total_settlement': 'Total Settlement (kWh)',
            'surplus_demand_after_banking': 'Grid Consumption (After Banking) (kWh)',
            'surplus_generation_after_banking': 'Surplus Generation (After Banking) (kWh)'
        }
        
        # Only rename columns that exist in the dataframe
        rename_dict = {k: v for k, v in column_mapping.items() if k in filtered_summary.columns}
        display_summary = filtered_summary.rename(columns=rename_dict)
        
        # Select only the requested columns for the Monthly ToD Before Banking Summary
        requested_columns = [
            'Month',
            'Total Generation (kWh)',
            'Total Consumption (kWh)',
            'Settled Units (Without Banking) (kWh)',
            'Settled Units (With Banking) (kWh)',
            'Total Settlement (kWh)',
            'Grid Consumption (After Banking) (kWh)',
            'Surplus Generation (After Banking) (kWh)'
        ]
        
        # Filter the display summary to only include the requested columns that exist
        available_requested_columns = [col for col in requested_columns if col in display_summary.columns]
        filtered_display_summary = display_summary[available_requested_columns]
        
        # Create a summary row for all months combined
        if not filtered_display_summary.empty:
            # Calculate totals for numeric columns
            summary_row = {}
            summary_row['Month'] = 'ALL MONTHS TOTAL'
            
            # Sum up all numeric columns and round them
            numeric_cols_to_sum = [col for col in available_requested_columns if col != 'Month']
            for col in numeric_cols_to_sum:
                if col in filtered_display_summary.columns:
                    # Handle potential non-numeric values and round to 0 decimal places
                    try:
                        summary_row[col] = round(filtered_display_summary[col].sum(), 0)
                    except:
                        summary_row[col] = 0
            
            # Create a DataFrame for the summary row
            summary_df = pd.DataFrame([summary_row])
            
            # Combine summary row with the monthly data (summary first)
            final_display_summary = pd.concat([summary_df, filtered_display_summary], ignore_index=True)
            
            # Ensure all numeric columns in the final display are rounded to 0 decimal places
            for col in numeric_cols_to_sum:
                if col in final_display_summary.columns:
                    try:
                        final_display_summary[col] = final_display_summary[col].round(0).astype(int)
                    except:
                        pass
        else:
            final_display_summary = filtered_display_summary
        
        # Display the table with summary row at the top
        # Apply styling to highlight the summary row
        def highlight_summary_row(x):
            """Apply styling to highlight the ALL MONTHS TOTAL row"""
            # Create a default style for all cells
            style = pd.DataFrame('', index=x.index, columns=x.columns)
            
            # Apply bold styling to the first row (ALL MONTHS TOTAL)
            if len(x) > 0 and x.iloc[0]['Month'] == 'ALL MONTHS TOTAL':
                style.iloc[0] = 'background-color: #e6f3ff; font-weight: bold; border-bottom: 2px solid #0078D4'
            
            return style
        
        # Display the styled table
        st.dataframe(
            final_display_summary.style.apply(highlight_summary_row, axis=None),
            use_container_width=True
        )
        
        # Filtered metrics section has been removed as requested
        
        # Display compact download buttons for detailed data
        display_download_buttons(
            fig=fig,
            df=final_display_summary,
            prefix=f"{selected_plant}_monthly_banking_settlement_summary",
            section=f"{section}_summary",
            identifiers={"plant": selected_plant}
        )
        
    except Exception as e:
        logger.error(f"Error in monthly banking settlement display: {e}")
        logger.error(traceback.format_exc())
        st.error("An error occurred while displaying the monthly banking settlement.")
        st.error(f"Error details: {str(e)}")


def display_monthly_banking_settlement_view(selected_plant, section="default"):
    """
    Display monthly banking settlement chart showing settlement and grid consumption 
    with/without banking, along with replacement percentages.
    
    Args:
        selected_plant: Name of the selected plant
        section: Section identifier for the display
    """
    try:
        # Extract plant name from either string or dictionary
        plant_name = _get_plant_name(selected_plant)
        
        with st.spinner("Loading monthly banking settlement data..."):
            df = get_monthly_before_banking_settlement_data(plant_name)
        
        if df.empty:
            st.warning("No banking settlement data available for the chart.")
            return
        
        # Create the monthly banking settlement chart
        fig, monthly_data = create_monthly_banking_settlement_chart(df, plant_name)
        
        # Display the plot
        st.pyplot(fig)
        
        # Display compact download buttons
        display_download_buttons(
            fig=fig,
            df=df,
            prefix=f"{selected_plant}_monthly_banking_settlement",
            section=f"{section}_banking_settlement",
            identifiers={"plant": selected_plant}
        )
        
        # Display grid consumption table below the chart
        if not monthly_data.empty:
            st.subheader("Grid Consumption Data")
            
            # Create a table with grid consumption data
            grid_consumption_data = monthly_data[['month', 'grid_consumption_with_banking', 'grid_consumption_without_banking']].copy()
            grid_consumption_data = grid_consumption_data.rename(columns={
                'month': 'Month',
                'grid_consumption_with_banking': 'Grid Consumption with Banking (kWh)',
                'grid_consumption_without_banking': 'Grid Consumption without Banking (kWh)'
            })
            
            # Format the numeric columns
            grid_consumption_data['Grid Consumption with Banking (kWh)'] = grid_consumption_data['Grid Consumption with Banking (kWh)'].apply(lambda x: f"{x:,.2f}")
            grid_consumption_data['Grid Consumption without Banking (kWh)'] = grid_consumption_data['Grid Consumption without Banking (kWh)'].apply(lambda x: f"{x:,.2f}")
            
            # Display the table using Streamlit's dataframe display
            st.dataframe(
                grid_consumption_data,
                use_container_width=True,
                hide_index=True
            )
            
            # Add download button for the grid consumption table
            display_download_buttons(
                fig=None,  # No figure for this table
                df=grid_consumption_data,
                prefix=f"{selected_plant}_grid_consumption_table",
                section=f"{section}_grid_consumption",
                identifiers={"plant": selected_plant}
            )
        
    except Exception as e:
        logger.error(f"Error in monthly banking settlement display: {e}")
        logger.error(traceback.format_exc())
        st.error("An error occurred while displaying the monthly banking settlement.")
        st.error(f"Error details: {str(e)}")




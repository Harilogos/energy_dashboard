"""
Visualization functions for creating plots and charts.
"""
from typing import Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import seaborn as sns   
import pandas as pd
import numpy as np
import traceback
from backend.config.app_config import CONFIG
from backend.logs.logger_setup import setup_logger
from datetime import datetime, timedelta
from backend.data.db_data_manager import get_plant_display_name
from backend.config.tod_config import get_tod_bin_labels


logger = setup_logger('visualization', 'visualization.log')

# Set default visualization style
sns.set_theme(style=CONFIG["visualization"]["style"])

# Define color palette based on config
COLORS = CONFIG["visualization"].get("colors", {
    "primary": "#1E88E5",
    "secondary": "#FFC107",
    "success": "#4CAF50",
    "danger": "#F44336",
    "warning": "#FF9800"
})

def map_tod_bin_name(tod_bin):
    """
    Map database ToD bin names to expected visualization format.
    
    Args:
        tod_bin: ToD bin name from database (e.g., 'Peak', 'Off-Peak', etc.)
        
    Returns:
        str: Mapped ToD bin name in expected format
    """
    # Import ToD configuration to get bin labels
    from backend.config.tod_config import get_tod_bin_labels
    
    # Get bin labels in the full format for visualization
    tod_bin_labels = get_tod_bin_labels("full")
    
    
    # Mapping from database names to expected visualization names
    tod_mapping = {
        'Peak': tod_bin_labels[0],  # '6 AM - 9 AM (Peak)'
        'Peak_2': tod_bin_labels[2],  # '6 PM - 10 PM (Peak)'
        'Off-Peak': tod_bin_labels[1],  # '9 AM - 6 PM (Off-Peak)'
        'Off-Peak_3': tod_bin_labels[3],  # '10 PM - 6 AM (Off-Peak)',
        # Add fallback mappings for common variations
        'Morning Peak': tod_bin_labels[0],
        'Evening Peak': tod_bin_labels[2],
        'Daytime Off-Peak': tod_bin_labels[1],
        'Nighttime Off-Peak': tod_bin_labels[3],
        # Add mappings for compact format from tod_config
        '6-9AM (Peak)': tod_bin_labels[0],
        '18-22PM (Peak)': tod_bin_labels[2],
        '9-18PM (Off-Peak)': tod_bin_labels[1],
        '22-6AM (Off-Peak)': tod_bin_labels[3]
    }
    
    # Return mapped name if found, otherwise return original
    return tod_mapping.get(str(tod_bin), str(tod_bin))

def format_thousands(x: float, pos: int) -> str:
    """Format y-axis labels to show thousands with K suffix"""
    if x >= 1000:
        return f'{x/1000:.1f}K'
    return f'{x:.0f}'


def create_figure(width: Optional[float] = None, height: Optional[float] = None) -> plt.Figure:
    """Create a figure with the specified dimensions"""
    width = width or CONFIG["visualization"]["default_width"]
    height = height or CONFIG["visualization"]["default_height"]
    return plt.figure(figsize=(width, height), dpi=CONFIG["visualization"]["dpi"])


def add_bar_labels(bars, ax: plt.Axes, rotation: int = 90, color: str = 'white', 
                   fontsize: int = 9, position: str = 'center') -> None:
    """
    Add labels to bars in a bar plot.
    
    Args:
        bars: Bar objects from matplotlib
        ax: Matplotlib axes object
        rotation: Text rotation angle
        color: Text color
        fontsize: Font size for labels
        position: Label position ('center', 'top')
    """
    for bar in bars:
        height = bar.get_height()
        if position == 'center':
            y_pos = height / 2
            va = 'center'
        else:  # position == 'top'
            y_pos = height
            va = 'bottom'
            
        ax.text(
            bar.get_x() + bar.get_width() / 2.,
            y_pos,
            f'{height:.1f}',
            ha='center',
            va=va,
            fontsize=fontsize,
            rotation=rotation,
            color=color
        )


def add_bar_labels_annotate(bars, ax: plt.Axes, fontsize: int = 9) -> None:
    """
    Add labels to bars using annotate method.
    
    Args:
        bars: Bar objects from matplotlib
        ax: Matplotlib axes object
        fontsize: Font size for labels
    """
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                   xy=(bar.get_x() + bar.get_width() / 2, height),
                   xytext=(0, 3),  # 3 points vertical offset
                   textcoords="offset points",
                   ha='center', va='bottom', fontsize=fontsize)


def create_error_figure(error_message: str, figsize: Tuple[float, float] = (12, 6)) -> plt.Figure:
    """
    Create a figure displaying an error message.
    
    Args:
        error_message: Error message to display
        figsize: Figure size tuple
        
    Returns:
        Figure with error message
    """
    fig, ax = plt.subplots(figsize=figsize)
    ax.text(0.5, 0.5, f"Error creating plot: {error_message}",
            ha='center', va='center', fontsize=12)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    return fig


def create_hourly_block_consumption_plot(df: pd.DataFrame, plant_name: str, selected_date) -> plt.Figure:
    """
    Create a bar plot of hourly block consumption data (Time-of-Day Hourly Consumption Trend)

    Args:
        df (DataFrame): Hourly block consumption data
        plant_name (str): Name of the plant
        selected_date (datetime): Selected date for the plot

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Format the hour blocks for display
        df['HOUR_BLOCK_LABEL'] = df['HOUR_BLOCK'].apply(lambda x: f"{int(x):02d}:00 - {int(x)+3:02d}:00")

        # Plot the data with a softer color for ToD Consumption
        bars = sns.barplot(
            data=df,
            x='HOUR_BLOCK_LABEL',
            y='TOTAL_CONSUMPTION',
            color=COLORS.get("consumption", "#00897B"),  # Teal color for consumption
            alpha=0.8,  # Add transparency
            ax=ax
        )

        # Add data labels on top of bars
        add_bar_labels(bars.patches, ax, rotation=90, color='white', position='center')

        # Customize the plot
        date_str = selected_date.strftime('%Y-%m-%d')
        ax.set_title(f"ToD Consumption for {plant_name} on {date_str}", fontsize=16, pad=20)
        ax.set_ylabel("Total Consumption (kWh)", fontsize=12)
        ax.set_xlabel("Time Block", fontsize=12)

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')

        # Add grid for y-axis only
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)  # Lighter grid

        # Add average line
        if not df.empty:
            avg = df['TOTAL_CONSUMPTION'].mean()
            ax.axhline(
                y=avg,
                color=COLORS.get("average", "#757575"),  # Gray for average
                linestyle='--',
                linewidth=1.5,
                label=f'Average: {avg:.1f}'
            )
            ax.legend(loc='upper right', frameon=True, framealpha=0.9)

        # Add subtle watermark
        fig.text(0.99, 0.01, 'ToD Consumption',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)  # More subtle

        # Adjust layout
        plt.tight_layout()
        return fig

    except Exception as e:
        logger.error(f"Error creating hourly block consumption plot: {e}")
        return create_error_figure(str(e))



def create_consumption_plot(df, plant_name):
    """
    Create a line plot of consumption data with improved styling

    Args:
        df (DataFrame): Consumption data
        plant_name (str): Name of the plant

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Check if we have hourly data or daily data
        if 'hour' in df.columns and 'energy_kwh' in df.columns:
            # We have hourly data - create a bar chart
            # Create hour labels for x-axis
            df = df.copy()
            df['hour_label'] = df['hour'].apply(lambda x: f"{int(x):02d}:00")

            # Plot the data with a softer color for consumption
            bars = sns.barplot(
                data=df,
                x='hour_label',
                y='energy_kwh',
                color=COLORS.get("consumption", "#00897B"),  # Teal color (easier on eyes)
                alpha=0.8,  # Add transparency
                ax=ax
            )

            # Add data labels on top of bars
            for i, bar in enumerate(ax.patches):
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2.,
                    height / 2,
                    f'{height:.1f}',
                    ha='center',
                    va='center',
                    fontsize=9,
                    rotation=90,
                    color='white'
                )

            

            # Get the display name for the plant
            plant_display_name = get_plant_display_name(plant_name)

            ax.set_title(f"Consumption for {plant_display_name}", fontsize=16, pad=20)
            ax.set_ylabel("Consumption (kWh)", fontsize=12)
            ax.set_xlabel("Hour", fontsize=12)

            # Add average consumption line
            if not df.empty:
                avg = df['energy_kwh'].mean()
                ax.axhline(
                    y=avg,
                    color=COLORS.get("average", "#757575"),  # Gray for average
                    linestyle='--',
                    linewidth=1.5,
                    label=f'Average: {avg:.1f}'
                )
                ax.legend(loc='upper right', frameon=True, framealpha=0.9)
        else:
            # Handle the actual data format from get_consumption_data_from_csv
            # Data comes with columns: 'time' and 'Consumption'
            if 'time' in df.columns and 'Consumption' in df.columns:
                # Convert time column to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(df['time']):
                    df['time'] = pd.to_datetime(df['time'])
                
                # For single day data, create hourly plot
                if len(df) > 1 and (df['time'].max() - df['time'].min()).days == 0:
                    # Single day - create hourly plot
                    df_copy = df.copy()
                    df_copy['hour'] = df_copy['time'].dt.hour
                    
                    # Plot the data as a line plot for intraday consumption
                    sns.lineplot(
                        data=df_copy,
                        x='time',
                        y='Consumption',
                        marker='o',
                        markersize=4,
                        linewidth=2,
                        color=COLORS.get("consumption", "#00897B"),  # Teal color
                        alpha=0.9,
                        ax=ax
                    )
                    
                    # Format x-axis for hourly data
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
                    
                else:
                    # Multi-day data - create daily plot
                    sns.lineplot(
                        data=df,
                        x='time',
                        y='Consumption',
                        marker='o',
                        markersize=6,
                        linewidth=2,
                        color=COLORS.get("consumption", "#00897B"),  # Teal color
                        alpha=0.9,
                        ax=ax
                    )
                    
                    # Format x-axis for daily data
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                    if len(df) > 30:
                        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
                    else:
                        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                
                # Add annotations for max and min values
                if not df.empty and len(df) > 1:
                    max_cons = df['Consumption'].max()
                    max_time = df.loc[df['Consumption'].idxmax(), 'time']
                    min_cons = df['Consumption'].min()
                    min_time = df.loc[df['Consumption'].idxmin(), 'time']

                    # Only annotate if we have enough data points
                    if len(df) > 5:
                        ax.annotate(f'Max: {max_cons:.1f}',
                                    xy=(max_time, max_cons),
                                    xytext=(0, 15),
                                    textcoords='offset points',
                                    ha='center',
                                    va='bottom',
                                    fontsize=9,
                                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                                    arrowprops=dict(arrowstyle='->', connectionstyle="arc3,rad=.2"))

                        ax.annotate(f'Min: {min_cons:.1f}',
                                    xy=(min_time, min_cons),
                                    xytext=(0, -15),
                                    textcoords='offset points',
                                    ha='center',
                                    va='top',
                                    fontsize=9,
                                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                                    arrowprops=dict(arrowstyle='->', connectionstyle="arc3,rad=.2"))
            
            else:
                # Fallback: Check if we have a date column (legacy support)
                if 'date' not in df.columns:
                    # Create a dummy date column for demonstration
                    

                    # Create a date range for the last 7 days
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=6)
                    date_range = pd.date_range(start=start_date, end=end_date)

                    # Create a new DataFrame with dates and random consumption values
                    
                    consumption_values = np.random.randint(100, 500, size=len(date_range))
                    date_df = pd.DataFrame({
                        'DATE': date_range,
                        'CONSUMPTION': consumption_values
                    })

                    # Use this DataFrame for plotting
                    df = date_df

                # Plot the data
                sns.lineplot(
                    data=df,
                    x='DATE',
                    y='CONSUMPTION',
                    marker='o',
                    markersize=6,
                    linewidth=2,
                    color=COLORS.get("consumption", "#00897B"),  # Teal color
                    alpha=0.9,  # Slight transparency
                    ax=ax
                )

                # Add annotations for legacy data format
                if not df.empty and 'CONSUMPTION' in df.columns:
                    max_cons = df['CONSUMPTION'].max()
                    max_date = df.loc[df['CONSUMPTION'].idxmax(), 'DATE']
                    min_cons = df['CONSUMPTION'].min()
                    min_date = df.loc[df['CONSUMPTION'].idxmin(), 'DATE']

                    # Only annotate if we have enough data points
                    if len(df) > 5:
                        ax.annotate(f'Max: {max_cons:.1f}',
                                    xy=(max_date, max_cons),
                                    xytext=(0, 15),
                                    textcoords='offset points',
                                    ha='center',
                                    va='bottom',
                                    fontsize=9,
                                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                                    arrowprops=dict(arrowstyle='->', connectionstyle="arc3,rad=.2"))

                        ax.annotate(f'Min: {min_cons:.1f}',
                                    xy=(min_date, min_cons),
                                    xytext=(0, -15),
                                    textcoords='offset points',
                                    ha='center',
                                    va='top',
                                    fontsize=9,
                                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                                    arrowprops=dict(arrowstyle='->', connectionstyle="arc3,rad=.2"))


        

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Customize the plot
        ax.set_title(f"Consumption for {plant_display_name}", fontsize=16, pad=20)
        ax.set_ylabel("Consumption (kWh)", fontsize=12)
        
        # Set appropriate x-axis label based on data type
        if 'time' in df.columns and len(df) > 1 and (df['time'].max() - df['time'].min()).days == 0:
            ax.set_xlabel("Time", fontsize=12)
        else:
            ax.set_xlabel("Date", fontsize=12)

        # Add subtle watermark
        fig.text(0.99, 0.01, 'Consumption Data',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)  # More subtle

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')

        # Add grid
        ax.grid(True, linestyle='--', alpha=0.5)  # Lighter grid

        # Adjust layout
        plt.tight_layout()
        return fig

    except Exception as e:
        logger.error(f"Error creating consumption plot: {e}")
        return create_error_figure(str(e))


def create_comparison_plot(df, plant_name, date):
    """Create a plot comparing generation and consumption data with surplus calculations"""
    try:
        sns.set_theme(style="whitegrid")
        
        # Log input data for debugging
        logger.info(f"Input DataFrame for create_comparison_plot: {df.head()}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"DataFrame shape: {df.shape}")

        # Check if DataFrame is empty
        if df.empty:
            logger.error("DataFrame is empty")
            return create_error_figure("No data available for the selected date")

        # Sort by time column to ensure proper ordering (supports both 15-min and hourly data)
        time_col = None
        if 'time_interval' in df.columns:
            time_col = 'time_interval'
        elif 'hour' in df.columns:
            time_col = 'hour'
        elif 'time' in df.columns:
            time_col = 'time'
        elif 'datetime' in df.columns:
            time_col = 'datetime'
        
        if time_col:
            df = df.sort_values(time_col)
        else:
            logger.warning(f"No time column found in DataFrame. Available columns: {df.columns.tolist()}")

        # Determine which column names are used for generation and consumption
        generation_col = None
        consumption_col = None
        
        if 'generation_kwh' in df.columns:
            generation_col = 'generation_kwh'
        elif 'Generation' in df.columns:
            generation_col = 'Generation'
        elif 'generation' in df.columns:
            generation_col = 'generation'
        
        if 'consumption_kwh' in df.columns:
            consumption_col = 'consumption_kwh'
        elif 'Consumption' in df.columns:
            consumption_col = 'Consumption'
        elif 'energy_kwh' in df.columns:
            consumption_col = 'energy_kwh'
        elif 'consumption' in df.columns:
            consumption_col = 'consumption'
        
        if not generation_col or not consumption_col:
            logger.error(f"Required columns not found. Available: {df.columns.tolist()}")
            return create_error_figure("Required generation or consumption data columns not found")

        # Calculate surplus generation and demand
        df['surplus_generation'] = df.apply(lambda row: max(0, row[generation_col] - row[consumption_col]), axis=1)
        df['surplus_demand'] = df.apply(lambda row: max(0, row[consumption_col] - row[generation_col]), axis=1)

       
        # Create figure and axes
        fig, ax = plt.subplots(figsize=(14, 8))

        # Plot generation (solid green line)
        ax.plot(df[time_col], df[generation_col], color='green', linewidth=3,
                marker='o', markersize=6, label='Generation')

        # Plot consumption (dashed red line)
        ax.plot(df[time_col], df[consumption_col], color='red', linewidth=3,
                linestyle='--', marker='s', markersize=6, label='Consumption')

        # Add transparent fill between curves (only if we have more than one data point)
        if len(df) > 1:
            for i in range(len(df)-1):
                time_range = [df.iloc[i][time_col], df.iloc[i+1][time_col]]
                gen_vals = [df.iloc[i][generation_col], df.iloc[i+1][generation_col]]
                cons_vals = [df.iloc[i][consumption_col], df.iloc[i+1][consumption_col]]

                fill_color = 'green' if gen_vals[0] > cons_vals[0] else 'red'
                ax.fill_between(time_range, gen_vals, cons_vals, color=fill_color, alpha=0.2)

        # Axes labels and ticks - adjust based on data granularity
        if time_col == 'time':
            # For datetime data, format the x-axis properly
            # Format x-axis for datetime
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            
            # Set appropriate tick frequency for 15-minute data
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))  # Every 2 hours
            ax.xaxis.set_minor_locator(mdates.HourLocator())  # Every hour as minor ticks
            
            # Rotate labels for better readability
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            ax.set_xlabel("Time of Day")
        elif time_col == 'time_interval':
            # For 15-minute data, show every 2 hours (8 intervals)
            ax.set_xticks([i for i in range(0, 24, 2)])
            ax.set_xlabel("Time of Day")
        else:
            # For hourly data
            ax.set_xticks(range(0, 24))
            ax.set_xlabel("Hour of Day")

        ax.set_ylabel("Energy (kWh)")

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid for better readability
        ax.grid(True, linestyle='--', alpha=0.7)

        # Summary text box removed as requested
        ax.legend(loc='upper right')

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Title and layout
        plt.title(f"Energy Generation vs Consumption for {plant_display_name} on {date.strftime('%Y-%m-%d')}")
        plt.tight_layout()

        return fig

    except Exception as e:
        logger.error(f"Error creating comparison plot: {e}")
        return create_error_figure(f"Error creating comparison plot: {str(e)}")

def create_daily_consumption_plot(df, plant_name, start_date, end_date):
    """
    Create a line plot of consumption data with configurable granularity (default: 60-minute)
    
    Args:
        df (DataFrame): Consumption data
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # DEBUGGING: Log DataFrame structure
        logger.info(f"Daily Consumption Plot - DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Daily Consumption Plot - DataFrame shape: {df.shape}")
        logger.info(f"Daily Consumption Plot - Date range requested: {start_date} to {end_date}")
        if not df.empty and 'datetime' in df.columns:
            logger.info(f"Daily Consumption Plot - Data date range: {df['datetime'].min()} to {df['datetime'].max()}")
        elif not df.empty and 'time' in df.columns:
            logger.info(f"Daily Consumption Plot - Data time range: {df['time'].min()} to {df['time'].max()}")
        
        # Create a copy of the dataframe to avoid modifying the original
        plot_df = df.copy()
        
        # Standardize column names to lowercase
        plot_df.columns = [col.lower() for col in plot_df.columns]
        
        logger.info(f"Daily Consumption Plot - DataFrame columns after lowercase: {plot_df.columns.tolist()}")

        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Determine the consumption column name
        consumption_col = None
        for possible_col in ['consumption_kwh', 'consumption', 'energy_kwh']:
            if possible_col in plot_df.columns:
                consumption_col = possible_col
                break
                
        # If consumption column not found, check for 'Consumption' in original dataframe
        if consumption_col is None:
            logger.warning("Consumption column not found in lowercase columns, checking original columns")
            for col in df.columns:
                if col.lower() in ['consumption', 'consumption_kwh', 'energy_kwh']:
                    # Use the original column name
                    consumption_col = col
                    # Make sure it's available in plot_df
                    plot_df[col.lower()] = df[col]
                    consumption_col = col.lower()
                    break
        
        if consumption_col is None:
            logger.error(f"No consumption column found in DataFrame. Available columns: {plot_df.columns.tolist()}")
            return create_error_figure("No consumption data column found in the dataset")

        # Check if we have the datetime column for hourly plotting
        if 'datetime' in plot_df.columns:
            # For multiple days, resample to 60-minute intervals to ensure consistent granularity
            days_diff = (end_date - start_date).days
            if days_diff > 1:
                logger.info(f"Resampling data to 60-minute intervals for {days_diff} days")
                # Ensure datetime is datetime type
                plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
                # Set datetime as index for resampling
                plot_df = plot_df.set_index('datetime')
                # Resample to 60-minute intervals - use mean for consumption data to avoid inflating values
                plot_df = plot_df.resample('60T').mean().reset_index()
                # Remove any NaN values that might have been introduced
                plot_df = plot_df.dropna(subset=[consumption_col])
                logger.info(f"After resampling: DataFrame shape: {plot_df.shape}")
            else:
                # For single day, ensure datetime is datetime type
                plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
            # Sort by datetime to ensure chronological order
            plot_df = plot_df.sort_values('datetime')
            
            logger.info(f"Final plot data shape: {plot_df.shape}")
            logger.info(f"Date range in data: {plot_df['datetime'].min()} to {plot_df['datetime'].max()}")
            logger.info(f"Consumption column '{consumption_col}' - min: {plot_df[consumption_col].min()}, max: {plot_df[consumption_col].max()}")
            logger.info(f"Number of non-null consumption values: {plot_df[consumption_col].notna().sum()}")

            # Plot the data with hourly granularity
            sns.lineplot(
                data=plot_df,
                x='datetime',
                y=consumption_col,
                marker='o',
                markersize=4,
                linewidth=2,
                color=COLORS.get("consumption", "#00897B"),  # Teal color
                alpha=0.9,
                ax=ax
            )

            # Set appropriate x-axis tick frequency based on date range
            # Always show only dates on x-axis, no time component
            days_diff = (end_date - start_date).days
            if days_diff <= 1:
                # For single day, show the date once
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            elif days_diff <= 7:
                # For up to a week, show each day
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            elif days_diff <= 30:
                # For up to a month, show every few days
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:
                # For longer periods, show weekly or monthly
                if days_diff <= 90:
                    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                else:
                    ax.xaxis.set_major_locator(mdates.MonthLocator())
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        else:
            # Fallback to daily plot if datetime column is not available
            # Check for available date/time columns
            date_col = None
            for col in ['date', 'time', 'timestamp']:
                if col in plot_df.columns:
                    date_col = col
                    break
            
            if date_col is None:
                # If no date column found, create a simple index-based plot
                logger.warning("No date/time column found, using index for x-axis")
                sns.lineplot(
                    data=plot_df,
                    x=plot_df.index,
                    y=consumption_col,
                    marker='o',
                    markersize=6,
                    linewidth=2,
                    color=COLORS.get("consumption", "#00897B"),  # Teal color
                    alpha=0.9,
                    ax=ax
                )
                ax.set_xlabel("Data Point Index", fontsize=12)
            else:
                sns.lineplot(
                    data=plot_df,
                    x=date_col,
                    y=consumption_col,
                    marker='o',
                    markersize=6,
                    linewidth=2,
                    color=COLORS.get("consumption", "#00897B"),  # Teal color
                    alpha=0.9,
                    ax=ax
                )
                
                # Format x-axis for daily data
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax.xaxis.set_major_locator(mdates.DayLocator())

      

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Customize the plot
        date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        # Determine appropriate title based on data granularity
        days_diff = (end_date - start_date).days
        if days_diff <= 1:
            title = f"Consumption for {plant_display_name} ({date_range})"
        else:
            title = f"Daily Consumption for {plant_display_name} ({date_range})"
        
        ax.set_title(title, fontsize=16, pad=20)
        ax.set_ylabel("Consumption (kWh)", fontsize=12)
        ax.set_xlabel("Date", fontsize=12)

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Rotate x-axis labels for better readability
        days_diff = (end_date - start_date).days
        if days_diff <= 7:
            # For short periods, rotate less
            plt.xticks(rotation=30, ha='right')
        else:
            # For longer periods, rotate more
            plt.xticks(rotation=45, ha='right')

        # Add grid
        ax.grid(True, axis='both', linestyle='--', alpha=0.5)

        # Add subtle watermark
        fig.text(0.99, 0.01, 'Consumption Data',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)

        # Adjust layout
        plt.tight_layout()
        return fig

    except Exception as e:
        logger.error(f"Error creating daily consumption plot: {e}")
        return create_error_figure(str(e))

def create_daily_comparison_plot(df, plant_name, start_date, end_date):
    """Create a plot comparing daily generation and consumption data with surplus calculations"""
    sns.set_theme(style="whitegrid")

    try:
        # Standardize column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # Determine the generation and consumption column names
        generation_col = None
        consumption_col = None
        
        for possible_col in ['generation_kwh', 'generation', 'energy_kwh']:
            if possible_col in df.columns:
                generation_col = possible_col
                break
                
        for possible_col in ['consumption_kwh', 'consumption', 'energy_kwh']:
            if possible_col in df.columns:
                consumption_col = possible_col
                break
        
        if generation_col is None or consumption_col is None:
            logger.error(f"Required columns not found. Available columns: {df.columns.tolist()}")
            return create_error_figure("Required generation or consumption data columns not found")

        # Calculate surplus generation and demand
        df['surplus_generation'] = df.apply(lambda row: max(0, row[generation_col] - row[consumption_col]), axis=1)
        df['surplus_demand'] = df.apply(lambda row: max(0, row[consumption_col] - row[generation_col]), axis=1)

  
        # Create the figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12), gridspec_kw={'height_ratios': [2, 1]})

        # Top plot: Generation vs Consumption
        # Plot generation (solid green line)
        ax1.plot(df['date'], df[generation_col], color='green', linewidth=3,
                marker='o', markersize=8, label='Generation')

        # Plot consumption (dashed red line) - handle zero consumption gracefully
        # Separate zero and non-zero consumption for better visualization
        zero_consumption_mask = df[consumption_col] == 0
        non_zero_consumption = df[~zero_consumption_mask]
        zero_consumption = df[zero_consumption_mask]

        # Plot non-zero consumption with normal style
        if not non_zero_consumption.empty:
            ax1.plot(non_zero_consumption['date'], non_zero_consumption[consumption_col],
                    color='red', linewidth=3, linestyle='--', marker='s', markersize=8,
                    label='Consumption')

        # Plot zero consumption points with different style (smaller markers, different color)
        if not zero_consumption.empty:
            ax1.scatter(zero_consumption['date'], zero_consumption[consumption_col],
                       color='orange', marker='x', s=100, linewidth=3,
                       label='Zero Consumption', alpha=0.8)

        # Add transparent fill between curves (only for non-zero consumption)
        if len(non_zero_consumption) > 1:
            for i in range(len(non_zero_consumption)-1):
                date_range = [non_zero_consumption.iloc[i]['date'], non_zero_consumption.iloc[i+1]['date']]
                gen_vals = [non_zero_consumption.iloc[i][generation_col], non_zero_consumption.iloc[i+1][generation_col]]
                cons_vals = [non_zero_consumption.iloc[i][consumption_col], non_zero_consumption.iloc[i+1][consumption_col]]

                fill_color = 'green' if gen_vals[0] > cons_vals[0] else 'red'
                ax1.fill_between(date_range, gen_vals, cons_vals, color=fill_color, alpha=0.2)

        # Format x-axis dates for top plot
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        if (end_date - start_date).days > 30:
            ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        else:
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))

        # Format y-axis with K for thousands for top plot
        ax1.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Axes labels for top plot
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Energy (kWh)")

        # Add legend to top plot
        ax1.legend(loc='upper right')

        # Rotate x-axis labels for top plot
        plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')


        # Bottom plot: Surplus Generation and Demand
        # Plot surplus generation (green bars)
        ax2.bar(df['date'], df['surplus_generation'], color='green', alpha=0.6, label='Surplus Generation')

        # Plot surplus demand (red bars)
        ax2.bar(df['date'], df['surplus_demand'], color='red', alpha=0.6, label='Surplus Demand')

        # Format x-axis dates for bottom plot
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        if (end_date - start_date).days > 30:
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        else:
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))

        # Format y-axis with K for thousands for bottom plot
        ax2.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Axes labels for bottom plot
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Energy (kWh)")

        # Add legend to bottom plot
        ax2.legend(loc='upper right')

        # Rotate x-axis labels for bottom plot
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

        # Add grid to bottom plot
        ax2.grid(True, linestyle='--', alpha=0.5)

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Title and layout
        date_range_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        fig.suptitle(f"Daily Energy Generation vs Consumption for {plant_display_name} ({date_range_str})",
                    fontsize=16, y=0.98)
        plt.tight_layout()
        plt.subplots_adjust(top=0.9, hspace=0.3)  # Adjust spacing between subplots

        return fig

    except Exception as e:
        logger.error(f"Error creating daily comparison plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig



def create_tod_binned_plot(df, plant_name, start_date, end_date=None):
    """
    Create a bar plot comparing generation vs consumption with custom ToD bins

    Args:
        df (DataFrame): ToD binned data with generation and consumption
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        Figure: Matplotlib figure object
    """

    try:
      
        

        # For multi-day data, we now get total values across all days per ToD bin
        # For single-day data, we get the actual totals for that day per ToD bin
        
        from backend.config.tod_config import get_tod_bin_labels
        tod_bin_labels = get_tod_bin_labels("full")
        
        # Use the data as-is since it's already properly aggregated
        df_plot = df.copy()
        



        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 7))

        # Set width of bars
        bar_width = 0.35

        # Set positions of the bars on X axis
        r1 = np.arange(len(df_plot))
        r2 = [x + bar_width for x in r1]

        # Create bars using the properly aggregated data
        generation_bars = ax.bar(
            r1,
            df_plot['generation_kwh'],
            width=bar_width,
            label='Generation',
            color=COLORS.get("generation", "#4CAF50"),
            alpha=0.8
        )

        consumption_bars = ax.bar(
            r2,
            df_plot['consumption_kwh'],
            width=bar_width,
            label='Consumption',
            color=COLORS.get("consumption", "#F44336"),
            alpha=0.8
        )

        # Add data labels on top of bars
        add_bar_labels(generation_bars, ax, rotation=90, color='white', position='center')
        add_bar_labels(consumption_bars, ax, rotation=90, color='white', position='center')

        # Add peak/off-peak background shading
        if 'is_peak' in df_plot.columns:
            for i, is_peak in enumerate(df_plot['is_peak']):
                if is_peak:
                    # Light yellow background for peak periods
                    ax.axvspan(i - 0.4, i + 0.8, alpha=0.2, color='#FFF9C4')
        else:
            # Fallback: determine peak periods from tod_bin names
            for i, tod_bin in enumerate(df_plot['tod_bin']):
                if 'Peak' in str(tod_bin):
                    # Light yellow background for peak periods
                    ax.axvspan(i - 0.4, i + 0.8, alpha=0.2, color='#FFF9C4')

      

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Add labels and title with normalization explanation
        if end_date is None or start_date == end_date:
            date_str = start_date.strftime('%Y-%m-%d')
            ax.set_title(f"ToD Generation vs Consumption for {plant_display_name} on {date_str}", fontsize=16, pad=20)
        else:
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            ax.set_title(f"ToD Generation vs Consumption for {plant_display_name} ({date_range})", fontsize=16, pad=20)

        ax.set_ylabel("Energy (kWh)", fontsize=12)
        ax.set_xlabel("Time of Day", fontsize=12)

        # Set x-axis ticks
        ax.set_xticks([r + bar_width/2 for r in range(len(df_plot))])
        
        # Create custom labels with actual time ranges - consistent for both single day and multiple day
        custom_labels = []
        for tod_bin in df_plot['tod_bin']:
            # Map ToD bin name to expected format
            mapped_tod_bin = map_tod_bin_name(tod_bin)
            
            # Use consistent labeling format for both single day and multiple day plots
            if mapped_tod_bin == tod_bin_labels[0]:  # '6 AM - 9 AM (Peak)'
                custom_labels.append('6 AM - 9 AM\n(Peak)')
            elif mapped_tod_bin == tod_bin_labels[1]:  # '9 AM - 6 PM (Off-Peak)'
                custom_labels.append('9 AM - 6 PM\n(Off-Peak)')
            elif mapped_tod_bin == tod_bin_labels[2]:  # '6 PM - 10 PM (Peak)'
                custom_labels.append('6 PM - 10 PM\n(Peak)')
            elif mapped_tod_bin == tod_bin_labels[3]:  # '10 PM - 6 AM (Off-Peak)'
                custom_labels.append('10 PM - 6 AM\n(Off-Peak)')
            else:
                # Fallback to original label if not recognized
                custom_labels.append(str(tod_bin))
                
        ax.set_xticklabels(custom_labels, fontsize=10, ha='center')

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))


        # Add grid for y-axis only
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        # Add legend
        ax.legend(loc='upper right', frameon=True, framealpha=0.9)

        # Add annotations for peak/off-peak periods
        ax.annotate(
            'Peak Periods',
            xy=(0.02, 0.97),
            xycoords='axes fraction',
            backgroundcolor='#FFF9C4',
            fontsize=10,
            bbox=dict(boxstyle="round,pad=0.3", fc="#FFF9C4", ec="gray", alpha=0.8)
        )

        # Calculate maximum bar height for proper Y-axis scaling
        max_bar_height = max(max(df_plot['generation_kwh']), max(df_plot['consumption_kwh']))

        # Set Y-axis limits with extra space for percentage labels (20% padding at top)
        y_margin = max_bar_height * 0.2
        ax.set_ylim(0, max_bar_height + y_margin)

        # Calculate and display replacement percentages
        for i in range(len(df_plot)):
            gen = df_plot['generation_kwh'].iloc[i]
            cons = df_plot['consumption_kwh'].iloc[i]
            if cons > 0:
                # Calculate raw replacement percentage
                raw_replacement = (gen / cons) * 100
                # Cap at 100% for display purposes
                replacement = min(100, raw_replacement)

                # Log both values for debugging
                logger.info(f"ToD bin {df_plot['tod_bin'].iloc[i]} - Raw replacement %: {raw_replacement:.2f}%, Capped: {replacement:.2f}%")

                # Position percentage text safely within the plot area
                text_y_position = max(gen, cons) + (y_margin * 0.3)  # 30% of margin space

                ax.text(
                    i + bar_width/2,
                    text_y_position,
                    f"{replacement:.1f}%",
                    ha='center',
                    va='bottom',
                    fontsize=9,
                    fontweight='bold',
                    color='#1565C0'
                )

        # Add subtle watermark
        fig.text(0.99, 0.01, 'ToD Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)

        # Adjust layout with proper spacing to prevent clipping of multi-line labels
        plt.subplots_adjust(top=0.85, bottom=0.20, left=0.1, right=0.95)
        return fig

    except Exception as e:
        logger.error(f"Error creating ToD binned plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig


def create_generation_only_plot(df, plant_name, start_date, end_date=None):
    """
    Create a generation-only plot (line chart for single day, bar chart for date ranges).

    Args:
        df (DataFrame): Generation data with 'time' and 'generation_kwh' columns
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # DEBUGGING: Log DataFrame structure
        logger.info(f"Generation Only Plot - DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Generation Only Plot - DataFrame shape: {df.shape}")
        
        # Check for the correct generation column name (case-insensitive)
        generation_col = None
        for col in ['generation_kwh', 'generation', 'gen_kwh', 'kwh', 'Generation']:
            if col in df.columns:
                generation_col = col
                break
        
        if generation_col is None:
            raise ValueError(f"No generation column found. Available columns: {df.columns.tolist()}")
        
        # DEBUGGING: Log visualization data for Y-axis consistency verification
        total_generation = df[generation_col].sum()
        max_gen_value = df[generation_col].max()
        logger.info(f"Summary Generation Only Plot - Total Gen: {total_generation:.2f} kWh, Max Gen Value: {max_gen_value:.2f} kWh")
        logger.info(f"Summary Generation Only Plot - Data points: {len(df)}, Generation column: {generation_col}")

        # Determine if single day or date range
        is_single_day = end_date is None or start_date == end_date

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))

        if is_single_day:
            # Line chart for single day (hourly data)
            if 'hour' in df.columns:
                # Use hour for x-axis
                ax.plot(df['hour'], df[generation_col],
                       color=COLORS.get("primary", "#4285F4"),
                       marker='o', markersize=6, linewidth=2)
                ax.set_xlabel("Hour of Day", fontsize=12)
                ax.set_xlim(0, 23)
                ax.set_xticks(range(0, 24, 2))
            else:
                # Use time for x-axis
                ax.plot(df['time'], df[generation_col],
                       color=COLORS.get("primary", "#4285F4"),
                       marker='o', markersize=6, linewidth=2)
                ax.set_xlabel("Time", fontsize=12)

            title = f"Generation - {plant_name}\n{start_date.strftime('%B %d, %Y')}"

        else:
            # Bar chart for date ranges (daily data)
            ax.bar(df['time'], df[generation_col],
                  color=COLORS.get("primary", "#4285F4"),
                  alpha=0.8, width=0.8)
            ax.set_xlabel("Date", fontsize=12)

            # Format x-axis dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            if (end_date - start_date).days > 30:
                ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
            else:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))

            # Rotate x-axis labels for better readability
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

            title = f"Generation - {plant_name}\n{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}"

        # Common formatting
        ax.set_ylabel("Generation (kWh)", fontsize=12)
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

        # Format y-axis with K for thousands
        
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid
        ax.grid(True, linestyle='--', alpha=0.7)

        # Adjust layout
        plt.tight_layout()

        return fig

    except Exception as e:
        logger.error(f"Error creating generation-only plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig


def create_tod_generation_plot(df, plant_name, start_date, end_date=None):
    """
    Create a stacked bar chart of generation data with custom ToD bins
    based on the configuration settings.

    Args:
        df (DataFrame): ToD binned data with generation
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        matplotlib.figure.Figure: The generated plot
    """
    try:
   
        
        # Check if date information is available for multi-day plots
        if end_date is not None and start_date != end_date and 'date' not in df.columns:
            # For multi-day plots without date information, create a simple bar chart
            logger.warning("Date information missing for multi-day ToD generation plot. Creating simple bar chart.")
            
            # Create a copy of the dataframe and map ToD bin names to proper labels
            df_mapped = df.copy()
            df_mapped['tod_bin_mapped'] = df_mapped['tod_bin'].apply(map_tod_bin_name)
            
            # Get proper ToD bin labels for sorting
            
            tod_bin_labels = get_tod_bin_labels("full")
            
            # Sort by the expected ToD order for better visualization
            tod_order = {
                tod_bin_labels[0]: 0,  # Morning Peak (6 AM - 9 AM)
                tod_bin_labels[1]: 1,  # Daytime Off-Peak (9 AM - 6 PM)
                tod_bin_labels[2]: 2,  # Evening Peak (6 PM - 10 PM)
                tod_bin_labels[3]: 3   # Nighttime Off-Peak (10 PM - 6 AM)
            }
            
            df_mapped['sort_order'] = df_mapped['tod_bin_mapped'].map(tod_order)
            df_mapped = df_mapped.sort_values('sort_order', na_position='last')
            
            # Create a simple bar chart with the aggregated data
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Define colors for each ToD category
            tod_colors_mapped = {
                tod_bin_labels[0]: '#FF5722',  # Deep Orange for morning peak
                tod_bin_labels[1]: '#FFC107',  # Amber for daytime off-peak
                tod_bin_labels[2]: '#E91E63',  # Pink for evening peak
                tod_bin_labels[3]: '#3F51B5'   # Indigo for nighttime off-peak
            }
            
            # Create bars with appropriate colors
            bars = []
            for i, (_, row) in enumerate(df_mapped.iterrows()):
                color = tod_colors_mapped.get(row['tod_bin_mapped'], COLORS.get("generation", "#4CAF50"))
                bar = ax.bar(
                    i,
                    row['generation_kwh'],
                    width=0.6,
                    color=color,
                    alpha=0.8,
                    label=row['tod_bin_mapped']
                )
                bars.extend(bar)
            
            # Add data labels on bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2.,
                    height/2,
                    f'{height:.1f}',
                    ha='center',
                    va='center',
                    fontsize=9,
                    rotation=90,
                    color='white',
                    fontweight='bold'
                )
            
            # Set x-axis ticks and labels with proper ToD labels
            ax.set_xticks(range(len(df_mapped)))
            ax.set_xticklabels(df_mapped['tod_bin_mapped'], rotation=45, ha='right')
            
            # Set title and labels
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            ax.set_title(f"ToD Generation for {plant_name} ({date_range})", fontsize=16, pad=20)
            ax.set_ylabel("Generation (kWh)", fontsize=12)
            ax.set_xlabel("Time of Day", fontsize=12)
            
            # Add grid
            ax.grid(True, axis='y', linestyle='--', alpha=0.5)
            
            # Format y-axis with K for thousands
            ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
            
            # Add legend to show ToD categories
            handles, labels = ax.get_legend_handles_labels()
            by_label = dict(zip(labels, handles))  # Remove duplicates
            ax.legend(by_label.values(), by_label.keys(), loc='upper right', frameon=True, framealpha=0.9)
            
            plt.tight_layout()
            return fig
            
        from backend.config.tod_config import get_tod_bin_labels
        tod_bin_labels = get_tod_bin_labels("full")
        
        # Use the data as-is since it's already properly aggregated
        df_plot = df.copy()
        
        # Log values for verification

       
        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 7))
        
        tod_categories = {
            0: tod_bin_labels[0],  # '6 AM - 9 AM (Peak)'
            1: tod_bin_labels[1],  # '9 AM - 6 PM (Off-Peak)'
            2: tod_bin_labels[2],  # '6 PM - 10 PM (Peak)'
            3: tod_bin_labels[3]   # '10 PM - 6 AM (Off-Peak)'
        }

      
        tod_colors = {
            tod_bin_labels[0]: '#FF5722',      # Deep Orange for morning peak (6 AM - 9 AM)
            tod_bin_labels[1]: '#FFC107',      # Amber/Yellow for daytime off-peak (9 AM - 6 PM)
            tod_bin_labels[2]: '#E91E63',      # Pink for evening peak (6 PM - 10 PM)
            tod_bin_labels[3]: '#3F51B5'       # Indigo for nighttime off-peak (10 PM - 6 AM)
        }

        is_single_day = end_date is None or start_date == end_date

        if is_single_day:
            # For single day view, create a stacked bar chart with ToD categories

            # Check if we have the right data structure
            if 'tod_bin' in df.columns and 'generation_kwh' in df.columns:
                

                # Create a new dataframe with the data organized by our ToD categories
                stacked_data = {}

                # Initialize with zeros
                for date_val in df['date'].unique() if 'date' in df.columns else [start_date]:
                    stacked_data[date_val] = {cat: 0 for cat in tod_categories.values()}

                # Fill in the values from our data
                for _, row in df_plot.iterrows():
                    tod_bin = row['tod_bin']
                    # Map ToD bin name to expected format
                    mapped_tod_bin = map_tod_bin_name(tod_bin)
                    gen_kwh = row['generation_kwh']  # This is the actual total generation for this ToD bin
                    date_val = row['date'] if 'date' in df_plot.columns else start_date

                
                    if mapped_tod_bin in tod_categories.values():
                        stacked_data[date_val][mapped_tod_bin] = gen_kwh
                    else:
                        # Log unmapped ToD bin for debugging
                        logger.warning(f"ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') not found in categories: {list(tod_categories.values())}")
                        # Try to find the best match
                        for _, cat in tod_categories.items():
                            if cat == mapped_tod_bin:
                                stacked_data[date_val][cat] = gen_kwh
                                break

                # Convert to DataFrame for plotting
                plot_data = pd.DataFrame(stacked_data).T

                # Create the stacked bar chart
                bottom = np.zeros(len(plot_data))

                # Plot each category as a segment of the stacked bar
                for cat in tod_categories.values():
                    if cat in plot_data.columns:
                        ax.bar(
                            range(len(plot_data)),
                            plot_data[cat],
                            bottom=bottom,
                            label=cat,
                            color=tod_colors[cat],
                            alpha=0.8,
                            width=0.6
                        )
                        bottom += plot_data[cat].values

                # Calculate maximum height for proper Y-axis scaling
                max_height = plot_data.sum(axis=1).max()
                y_margin = max_height * 0.15  # 15% padding at top
                ax.set_ylim(0, max_height + y_margin)

                # Add total value on top of the stacked bar
                for i, total in enumerate(plot_data.sum(axis=1)):
                    ax.text(
                        i,
                        total + (y_margin * 0.2),  # Position within margin
                        f'{total:.1f}',
                        ha='center',
                        va='bottom',
                        fontsize=10,
                        fontweight='bold'
                    )

                # Set x-axis labels
                if 'date' in df.columns:
                    ax.set_xticks(range(len(plot_data)))
                    ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in plot_data.index])
                else:
                    ax.set_xticks([0])
                    ax.set_xticklabels([start_date.strftime('%Y-%m-%d')])
            else:
                # Fallback to simple bar chart if data structure doesn't match
                logger.warning("Data structure doesn't match expected format for stacked bar chart")
                bars = ax.bar(
                    np.arange(len(df)),
                    df['generation_kwh'],
                    width=0.6,
                    color=COLORS.get("generation", "#4CAF50"),
                    alpha=0.8
                )

                # Add data labels on top of bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height / 2,
                        f"{height:.1f}",
                        ha="center",
                        va="center",
                        fontsize=9,
                        rotation=90,
                        color="white",
                    )

                # Set x-axis ticks
                ax.set_xticks(np.arange(len(df)))
                ax.set_xticklabels(df['tod_bin'])
        else:
            # For multiple days view, create a stacked bar chart for each day

            # Check if we have date information
            if 'date' in df.columns:
                # Sort the dataframe by date
                df = df.sort_values('date')

                # Get unique dates
                unique_dates = df['date'].unique()
                num_dates = len(unique_dates)

               
                # Map database ToD bin names to expected format
                tod_bin_to_category = {}
                for _, row in df.iterrows():
                    tod_bin = row['tod_bin']
                    # Map ToD bin name to expected format
                    mapped_tod_bin = map_tod_bin_name(tod_bin)
                    # Direct mapping - mapped tod_bin should match category exactly
                    if mapped_tod_bin in tod_categories.values():
                        tod_bin_to_category[tod_bin] = mapped_tod_bin
                    else:
                        # Log unmapped ToD bins for debugging
                        logger.warning(f"Multi-day generation plot: ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') not found in categories: {list(tod_categories.values())}")

                # Create a new dataframe with data organized by date and ToD category
                plot_data = []

                # Process each date
                for date_val in unique_dates:
                    date_df = df[df['date'] == date_val]

                    # Initialize data for this date with zeros for all categories
                    date_data = {
                        'date': date_val,
                        'date_str': date_val.strftime('%Y-%m-%d')
                    }

                    # Initialize all categories with zero
                    for cat in tod_categories.values():
                        date_data[cat] = 0

                    # Fill in the values from our data
                    for _, row in date_df.iterrows():
                        tod_bin = row['tod_bin']
                        gen_kwh = row['generation_kwh']

                        # Map ToD bin name to expected format
                        mapped_tod_bin = map_tod_bin_name(tod_bin)
                        
                        # Use the mapped ToD bin name
                        if mapped_tod_bin in tod_categories.values():
                            date_data[mapped_tod_bin] = gen_kwh
                        else:
                            logger.warning(f"Multi-day generation plot: Unmapped ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') for date {date_val}")

                    plot_data.append(date_data)

                # Convert to DataFrame for plotting
                plot_df = pd.DataFrame(plot_data)

                # Set up x positions for the bars
                x = np.arange(num_dates)
                width = 0.6

                # Create the stacked bar chart - one stacked bar for each date
                bottom = np.zeros(num_dates)

                # Plot each category as a segment of the stacked bars
                for cat in tod_categories.values():
                    if cat in plot_df.columns:
                        ax.bar(
                            x,
                            plot_df[cat],
                            bottom=bottom,
                            label=cat,
                            color=tod_colors[cat],
                            alpha=0.8,
                            width=width
                        )
                        bottom += plot_df[cat].values

                # Add total value on top of each stacked bar
                for i, (_, row) in enumerate(plot_df.iterrows()):
                    total = sum(row[cat] for cat in tod_categories.values() if cat in row)
                    ax.text(
                        i,
                        total / 2,
                        f'{total:.1f}',
                        ha='center',
                        va='center',
                        fontsize=9,
                        rotation=90,
                        color='white',
                        fontweight='bold'
                    )

                # Set x-axis labels
                ax.set_xticks(x)
                ax.set_xticklabels(plot_df['date_str'], rotation=45, ha='right')
            else:
                # Fallback to simple bar chart if no date information
                logger.warning("No date information available for multi-day stacked bar chart")
                
                # Create a copy of the dataframe and map ToD bin names to proper labels
                df_mapped = df.copy()
                df_mapped['tod_bin_mapped'] = df_mapped['tod_bin'].apply(map_tod_bin_name)
                
                # Sort by the expected ToD order for better visualization
                tod_order = {
                    tod_bin_labels[0]: 0,  # Morning Peak
                    tod_bin_labels[1]: 1,  # Daytime Off-Peak
                    tod_bin_labels[2]: 2,  # Evening Peak
                    tod_bin_labels[3]: 3   # Nighttime Off-Peak
                }
                
                df_mapped['sort_order'] = df_mapped['tod_bin_mapped'].map(tod_order)
                df_mapped = df_mapped.sort_values('sort_order', na_position='last')
                
                # Define colors for each ToD category
                tod_colors_mapped = {
                    tod_bin_labels[0]: '#FF5722',  # Deep Orange for morning peak
                    tod_bin_labels[1]: '#FFC107',  # Amber for daytime off-peak
                    tod_bin_labels[2]: '#E91E63',  # Pink for evening peak
                    tod_bin_labels[3]: '#3F51B5'   # Indigo for nighttime off-peak
                }
                
                # Create bars with appropriate colors
                bars = []
                for i, (_, row) in enumerate(df_mapped.iterrows()):
                    color = tod_colors_mapped.get(row['tod_bin_mapped'], COLORS.get("generation", "#4CAF50"))
                    bar = ax.bar(
                        i,
                        row['generation_kwh'],
                        width=0.6,
                        color=color,
                        alpha=0.8,
                        label=row['tod_bin_mapped']
                    )
                    bars.extend(bar)

                # Add data labels on top of bars
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width()/2.,
                        height + height * 0.01,  # Slightly above the bar
                        f'{height:.1f}',
                        ha='center',
                        va='bottom',
                        fontsize=9,
                        fontweight='bold'
                    )

                # Set x-axis ticks with proper labels
                ax.set_xticks(np.arange(len(df_mapped)))
                ax.set_xticklabels(df_mapped['tod_bin_mapped'], rotation=45, ha='right')
                
                # Add legend to show ToD categories
                handles, labels = ax.get_legend_handles_labels()
                by_label = dict(zip(labels, handles))  # Remove duplicates
                ax.legend(by_label.values(), by_label.keys(), loc='upper right', frameon=True, framealpha=0.9)

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Add labels and title with normalization explanation
        if end_date is None or start_date == end_date:
            date_str = start_date.strftime('%Y-%m-%d')
            ax.set_title(f"ToD Generation for {plant_display_name} on {date_str}", fontsize=16, pad=20)
        else:
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            ax.set_title(f"ToD Generation for {plant_display_name} ({date_range})", fontsize=16, pad=20)

        ax.set_ylabel("Generation (kWh)", fontsize=12)
        ax.set_xlabel("Date", fontsize=12)

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid for y-axis only
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        # Add legend for ToD categories
        ax.legend(loc='upper right', frameon=True, framealpha=0.9)

        # Add subtle watermark
        fig.text(0.99, 0.01, 'ToD Generation',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)

        # Adjust layout with proper spacing to prevent clipping
        plt.subplots_adjust(top=0.85, bottom=0.15, left=0.1, right=0.95)
        return fig

    except Exception as e:
        logger.error(f"Error creating ToD generation plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig




def create_tod_consumption_plot(df, plant_name, start_date, end_date=None):
    """
    Create a stacked bar chart of consumption data with custom ToD bins
    based on the configuration settings.

    Args:
        df (DataFrame): ToD binned data with consumption
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # Import pandas and numpy for DataFrame operations
        import pandas as pd
        import numpy as np
        # Check if date information is available for multi-day plots
        if end_date is not None and start_date != end_date and 'date' not in df.columns:
            # For multi-day plots without date information, create a simple bar chart
            logger.warning("Date information missing for multi-day ToD consumption plot. Creating simple bar chart.")
            
            # Create a copy of the dataframe and map ToD bin names to proper labels
            df_mapped = df.copy()
            df_mapped['tod_bin_mapped'] = df_mapped['tod_bin'].apply(map_tod_bin_name)
            
            # Get proper ToD bin labels for sorting
            tod_bin_labels = get_tod_bin_labels("full")
            
            # Sort by the expected ToD order for better visualization
            tod_order = {
                tod_bin_labels[0]: 0,  # Morning Peak (6 AM - 9 AM)
                tod_bin_labels[1]: 1,  # Daytime Off-Peak (9 AM - 6 PM)
                tod_bin_labels[2]: 2,  # Evening Peak (6 PM - 10 PM)
                tod_bin_labels[3]: 3   # Nighttime Off-Peak (10 PM - 6 AM)
            }
            
            df_mapped['sort_order'] = df_mapped['tod_bin_mapped'].map(tod_order)
            df_mapped = df_mapped.sort_values('sort_order', na_position='last')
            
            # Create a simple bar chart with the aggregated data
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Define colors for each ToD category
            tod_colors_mapped = {
                tod_bin_labels[0]: '#FF5722',  # Deep Orange for morning peak
                tod_bin_labels[1]: '#FFC107',  # Amber for daytime off-peak
                tod_bin_labels[2]: '#E91E63',  # Pink for evening peak
                tod_bin_labels[3]: '#3F51B5'   # Indigo for nighttime off-peak
            }
            
            # Create bars with appropriate colors
            bars = []
            for i, (_, row) in enumerate(df_mapped.iterrows()):
                color = tod_colors_mapped.get(row['tod_bin_mapped'], COLORS.get("consumption", "#F44336"))
                bar = ax.bar(
                    i,
                    row['consumption_kwh'],
                    width=0.6,
                    color=color,
                    alpha=0.8,
                    label=row['tod_bin_mapped']
                )
                bars.extend(bar)
            
            # Add data labels on bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2.,
                    height/2,
                    f'{height:.1f}',
                    ha='center',
                    va='center',
                    fontsize=9,
                    rotation=90,
                    color='white',
                    fontweight='bold'
                )
            
            # Set x-axis ticks and labels with proper ToD labels
            ax.set_xticks(range(len(df_mapped)))
            ax.set_xticklabels(df_mapped['tod_bin_mapped'], rotation=45, ha='right')
            
            # Set title and labels
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            ax.set_title(f"ToD Consumption for {plant_name} ({date_range})", fontsize=16, pad=20)
            ax.set_ylabel("Consumption (kWh)", fontsize=12)
            ax.set_xlabel("Time of Day", fontsize=12)
            
            # Add grid
            ax.grid(True, axis='y', linestyle='--', alpha=0.5)
            
            # Format y-axis with K for thousands
            ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
            
            # Add legend to show ToD categories
            handles, labels = ax.get_legend_handles_labels()
            by_label = dict(zip(labels, handles))  # Remove duplicates
            ax.legend(by_label.values(), by_label.keys(), loc='upper right', frameon=True, framealpha=0.9)
            
            plt.tight_layout()
            return fig
            

        
        from backend.config.tod_config import get_tod_bin_labels
        tod_bin_labels = get_tod_bin_labels("full")
        
        # Use the data as-is since it's already properly aggregated
        df_plot = df.copy()


        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 7))

        # Define ToD categories for stacking - FIXED to match actual ToD bin labels
        tod_categories = {
            0: tod_bin_labels[0],  # '6 AM - 9 AM (Peak)'
            1: tod_bin_labels[1],  # '9 AM - 6 PM (Off-Peak)'
            2: tod_bin_labels[2],  # '6 PM - 10 PM (Peak)'
            3: tod_bin_labels[3]   # '10 PM - 6 AM (Off-Peak)'
        }

        # Define colors for each ToD category - using distinct colors that complement the generation colors
        tod_colors = {
            tod_bin_labels[0]: '#F44336',      # Red for morning peak (6 AM - 9 AM)
            tod_bin_labels[1]: '#00BCD4',      # Cyan for daytime off-peak (9 AM - 6 PM)
            tod_bin_labels[2]: '#9C27B0',      # Purple for evening peak (6 PM - 10 PM)
            tod_bin_labels[3]: '#673AB7'       # Deep Purple for nighttime off-peak (10 PM - 6 AM)
        }

        is_single_day = end_date is None or start_date == end_date

        if is_single_day:
            # For single day view, create a stacked bar chart with ToD categories

            # Check if we have the right data structure
            if 'tod_bin' in df.columns and 'consumption_kwh' in df.columns:
                # Create a mapping from existing tod_bin to our categories
             

                # Create a new dataframe with the data organized by our ToD categories
                stacked_data = {}

                # Initialize with zeros
                for date_val in df['date'].unique() if 'date' in df.columns else [start_date]:
                    stacked_data[date_val] = {cat: 0 for cat in tod_categories.values()}

                # Fill in the values from our data
                for _, row in df_plot.iterrows():
                    tod_bin = row['tod_bin']
                    # Map ToD bin name to expected format
                    mapped_tod_bin = map_tod_bin_name(tod_bin)
                    cons_kwh = row['consumption_kwh']  # This is the actual total consumption for this ToD bin
                    date_val = row['date'] if 'date' in df_plot.columns else start_date

                  
                    # Direct mapping using exact ToD bin strings
                    if mapped_tod_bin in tod_categories.values():
                        stacked_data[date_val][mapped_tod_bin] = cons_kwh
                    else:
                        # Log unmapped ToD bin for debugging
                        logger.warning(f"ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') not found in categories: {list(tod_categories.values())}")
                        # Try to find the best match
                        for _, cat in tod_categories.items():
                            if cat == mapped_tod_bin:
                                stacked_data[date_val][cat] = cons_kwh
                                break

                # Convert to DataFrame for plotting
                plot_data = pd.DataFrame(stacked_data).T

                # Create the stacked bar chart
                bottom = np.zeros(len(plot_data))

                # Plot each category as a segment of the stacked bar
                for cat in tod_categories.values():
                    if cat in plot_data.columns:
                        ax.bar(
                            range(len(plot_data)),
                            plot_data[cat],
                            bottom=bottom,
                            label=cat,
                            color=tod_colors[cat],
                            alpha=0.8,
                            width=0.6
                        )
                        bottom += plot_data[cat].values

                # Calculate maximum height for proper Y-axis scaling
                max_height = plot_data.sum(axis=1).max()
                y_margin = max_height * 0.15  # 15% padding at top
                ax.set_ylim(0, max_height + y_margin)

                # Add total value on top of the stacked bar
                for i, total in enumerate(plot_data.sum(axis=1)):
                    ax.text(
                        i,
                        total + (y_margin * 0.2),  # Position at the top of the bar
                        f'{total:.1f}',
                        ha='center',
                        va='bottom',
                        fontsize=10,
                        fontweight='bold'
                    )

                # Set x-axis labels
                if 'date' in df.columns:
                    ax.set_xticks(range(len(plot_data)))
                    ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in plot_data.index])
                else:
                    ax.set_xticks([0])
                    ax.set_xticklabels([start_date.strftime('%Y-%m-%d')])
            else:
                # Fallback to simple bar chart if data structure doesn't match
                logger.warning("Data structure doesn't match expected format for stacked bar chart")
                bars = ax.bar(
                    np.arange(len(df_plot)),
                    df_plot['consumption_kwh'],
                    width=0.6,
                    color=COLORS.get("consumption", "#F44336"),
                    alpha=0.8
                )

                # Add data labels on top of bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width()/2.,
                        height / 2,
                        f'{height:.1f}',
                        ha='center',
                        va='center',
                        fontsize=9,
                        rotation=90,
                        color='white'
                    )

                # Set x-axis ticks
                ax.set_xticks(np.arange(len(df_plot)))
                ax.set_xticklabels(df_plot['tod_bin'])
        else:
            # For multiple days view, create a stacked bar chart for each day

            # Check if we have date information
            if 'date' in df.columns:
                # Sort the dataframe by date
                df = df.sort_values('date')

                # Get unique dates
                unique_dates = df['date'].unique()
                num_dates = len(unique_dates)

                # FIXED: Create a direct mapping from tod_bin to categories
                # Map database ToD bin names to expected format
                tod_bin_to_category = {}
                for _, row in df.iterrows():
                    tod_bin = row['tod_bin']
                    # Map ToD bin name to expected format
                    mapped_tod_bin = map_tod_bin_name(tod_bin)
                    # Direct mapping - mapped tod_bin should match category exactly
                    if mapped_tod_bin in tod_categories.values():
                        tod_bin_to_category[tod_bin] = mapped_tod_bin
                    else:
                        # Log unmapped ToD bins for debugging
                        logger.warning(f"Multi-day consumption plot: ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') not found in categories: {list(tod_categories.values())}")

                # Create a new dataframe with data organized by date and ToD category
                plot_data = []

                # Process each date
                for date_val in unique_dates:
                    date_df = df[df['date'] == date_val]

                    # Initialize data for this date with zeros for all categories
                    date_data = {
                        'date': date_val,
                        'date_str': date_val.strftime('%Y-%m-%d')
                    }

                    # Initialize all categories with zero
                    for cat in tod_categories.values():
                        date_data[cat] = 0

                    # Fill in the values from our data
                    for _, row in date_df.iterrows():
                        tod_bin = row['tod_bin']
                        cons_kwh = row['consumption_kwh']

                        # Map ToD bin name to expected format
                        mapped_tod_bin = map_tod_bin_name(tod_bin)
                        
                        # Check if the mapped tod_bin is in our categories
                        if mapped_tod_bin in tod_categories.values():
                            date_data[mapped_tod_bin] = cons_kwh
                        # Check if the original tod_bin is in our categories
                        elif tod_bin in tod_categories.values():
                            date_data[tod_bin] = cons_kwh
                        else:
                            logger.warning(f"Multi-day consumption plot: Unmapped ToD bin '{tod_bin}' (mapped to '{mapped_tod_bin}') for date {date_val}")

                    plot_data.append(date_data)

                # Convert to DataFrame for plotting
                plot_df = pd.DataFrame(plot_data)

                # Set up x positions for the bars
                x = np.arange(num_dates)
                width = 0.6

                # Create the stacked bar chart - one stacked bar for each date
                bottom = np.zeros(num_dates)

                # Plot each category as a segment of the stacked bars
                for cat in tod_categories.values():
                    if cat in plot_df.columns:
                        ax.bar(
                            x,
                            plot_df[cat],
                            bottom=bottom,
                            label=cat,
                            color=tod_colors[cat],
                            alpha=0.8,
                            width=width
                        )
                        bottom += plot_df[cat].values

                # Add total value on top of each stacked bar
                for i, (_, row) in enumerate(plot_df.iterrows()):
                    total = sum(row[cat] for cat in tod_categories.values() if cat in row)
                    ax.text(
                        i,
                        total / 2,
                        f'{total:.1f}',
                        ha='center',
                        va='center',
                        fontsize=9,
                        rotation=90,
                        color='white',
                        fontweight='bold'
                    )

                # Set x-axis labels
                ax.set_xticks(x)
                ax.set_xticklabels(plot_df['date_str'], rotation=45, ha='right')
            else:
                # Fallback to simple bar chart if no date information
                logger.warning("No date information available for multi-day stacked bar chart")
                bars = ax.bar(
                    np.arange(len(df)),
                    df['consumption_kwh'],
                    width=0.6,
                    color=COLORS.get("consumption", "#F44336"),
                    alpha=0.8
                )

                # Add data labels on top of bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width()/2.,
                        height / 2,
                        f'{height:.1f}',
                        ha='center',
                        va='center',
                        fontsize=9,
                        rotation=90,
                        color='white'
                    )

                # Set x-axis ticks
                ax.set_xticks(np.arange(len(df)))
                ax.set_xticklabels(df['tod_bin'])

        # Get the display name for the plant
        plant_display_name = get_plant_display_name(plant_name)

        # Add labels and title with normalization explanation
        if end_date is None or start_date == end_date:
            date_str = start_date.strftime('%Y-%m-%d')
            ax.set_title(f"ToD Consumption for {plant_display_name} on {date_str}", fontsize=16, pad=20)
        else:
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            ax.set_title(f"ToD Consumption for {plant_display_name} ({date_range})", fontsize=16, pad=20)

        ax.set_ylabel("Consumption per 15-min Interval (kWh)", fontsize=12)
        ax.set_xlabel("Date", fontsize=12)

        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid for y-axis only
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        # Add legend for ToD categories
        ax.legend(loc='upper right', frameon=True, framealpha=0.9)

        # Add subtle watermark
        fig.text(0.99, 0.01, 'ToD Consumption',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)

        # Adjust layout with proper spacing to prevent clipping
        plt.subplots_adjust(top=0.85, bottom=0.15, left=0.1, right=0.95)
        return fig

    except Exception as e:
        logger.error(f"Error creating ToD consumption plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig






def create_daily_tod_binned_plot(df, plant_name, start_date, end_date):
    """
    Create a comparative bar chart showing Generation vs Consumption across ToD slots aggregated over multiple days.
    
    This function creates a grouped bar chart with 4 ToD slots (Morning, Afternoon, Evening, Night),
    where each slot has two side-by-side bars showing total generation and consumption
    summed across all selected days.

    Args:
        df (DataFrame): Daily ToD binned data with generation and consumption for multiple days
        plant_name (str): Name of the plant
        start_date (datetime): Start date of the data range
        end_date (datetime): End date of the data range

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Import ToD configuration
        from backend.config.tod_config import get_tod_bin_labels
        
        # Define the 4 ToD slots with their time ranges and display names
        tod_slots = {
            'Morning': {'hours': (6, 9), 'label': 'Morning\n(6 AM - 9 AM)'},
            'Afternoon': {'hours': (9, 18), 'label': 'Afternoon\n(9 AM - 6 PM)'},
            'Evening': {'hours': (18, 22), 'label': 'Evening\n(6 PM - 10 PM)'},
            'Night': {'hours': (22, 6), 'label': 'Night\n(10 PM - 6 AM)'}
        }
        
        # Aggregate data across all days for each ToD slot
        aggregated_data = {}
        
        # Initialize aggregated data structure
        for slot_name in tod_slots.keys():
            aggregated_data[slot_name] = {'generation': 0, 'consumption': 0}
        
        # Check if we have tod_bin column for direct aggregation
        if 'tod_bin' in df.columns:
            logger.info("Using tod_bin column for aggregation")
            
            # Get ToD bin labels for mapping
            tod_bin_labels = get_tod_bin_labels("full")
            
            # Aggregate by tod_bin
            for _, row in df.iterrows():
                tod_bin = row['tod_bin']
                mapped_tod_bin = map_tod_bin_name(tod_bin)
                
                # Map to our 4 slots
                if mapped_tod_bin == tod_bin_labels[0]:  # '6 AM - 9 AM (Peak)'
                    slot = 'Morning'
                elif mapped_tod_bin == tod_bin_labels[1]:  # '9 AM - 6 PM (Off-Peak)'
                    slot = 'Afternoon'
                elif mapped_tod_bin == tod_bin_labels[2]:  # '6 PM - 10 PM (Peak)'
                    slot = 'Evening'
                elif mapped_tod_bin == tod_bin_labels[3]:  # '10 PM - 6 AM (Off-Peak)'
                    slot = 'Night'
                else:
                    continue  # Skip unknown bins
                
                aggregated_data[slot]['generation'] += row.get('generation_kwh', 0)
                aggregated_data[slot]['consumption'] += row.get('consumption_kwh', 0)
        
        elif 'hour' in df.columns or 'timestamp' in df.columns:
            
            
            # If we have hourly data, aggregate by hour ranges
            hour_col = 'hour' if 'hour' in df.columns else 'timestamp'
            
            for _, row in df.iterrows():
                if hour_col == 'timestamp':
                    # Extract hour from timestamp
                    if pd.isna(row[hour_col]):
                        continue
                    hour = pd.to_datetime(row[hour_col]).hour
                else:
                    hour = row[hour_col]
                
                # Determine which slot this hour belongs to
                slot = None
                for slot_name, slot_info in tod_slots.items():
                    start_hour, end_hour = slot_info['hours']
                    if end_hour < start_hour:  # Night slot wraps around midnight
                        if hour >= start_hour or hour < end_hour:
                            slot = slot_name
                            break
                    else:
                        if start_hour <= hour < end_hour:
                            slot = slot_name
                            break
                
                if slot:
                    aggregated_data[slot]['generation'] += row.get('generation_kwh', 0)
                    aggregated_data[slot]['consumption'] += row.get('consumption_kwh', 0)
        
        else:
            # If no time information, try to aggregate all data equally across slots
            logger.warning("No time column found, distributing data equally across ToD slots")
            total_gen = df.get('generation_kwh', pd.Series([0])).sum()
            total_cons = df.get('consumption_kwh', pd.Series([0])).sum()
            
            for slot_name in tod_slots.keys():
                aggregated_data[slot_name]['generation'] = total_gen / 4
                aggregated_data[slot_name]['consumption'] = total_cons / 4
        
        # Prepare data for plotting
        slot_names = list(tod_slots.keys())
        generation_values = [aggregated_data[slot]['generation'] for slot in slot_names]
        consumption_values = [aggregated_data[slot]['consumption'] for slot in slot_names]
        slot_labels = [tod_slots[slot]['label'] for slot in slot_names]
        
        # Create grouped bar chart
        x = np.arange(len(slot_names))
        width = 0.35
        
        # Create bars
        generation_bars = ax.bar(
            x - width/2, 
            generation_values, 
            width, 
            label='Generation',
            color=COLORS.get("success", "#4CAF50"),
            alpha=0.8,
            edgecolor='white',
            linewidth=1
        )
        
        consumption_bars = ax.bar(
            x + width/2, 
            consumption_values, 
            width, 
            label='Consumption',
            color=COLORS.get("danger", "#F44336"),
            alpha=0.8,
            edgecolor='white',
            linewidth=1
        )
        
        # Add value labels on bars
        def add_value_labels(bars, values):
            for bar, value in zip(bars, values):
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(values) * 0.01,
                           f'{value:.1f}',
                           ha='center', va='bottom', fontweight='bold', fontsize=10)
        
        add_value_labels(generation_bars, generation_values)
        add_value_labels(consumption_bars, consumption_values)
        
        # Add replacement percentage labels
        for i, (gen, cons) in enumerate(zip(generation_values, consumption_values)):
            if cons > 0:
                replacement_pct = min(100, (gen / cons) * 100)
                max_height = max(gen, cons)
                ax.text(i, max_height + max(generation_values + consumption_values) * 0.08,
                       f'{replacement_pct:.1f}%',
                       ha='center', va='bottom', fontweight='bold', 
                       fontsize=11, color='#1565C0',
                       bbox=dict(boxstyle="round,pad=0.3", facecolor='white', 
                                edgecolor='#1565C0', alpha=0.8))
        
        # Customize the plot
        ax.set_xlabel('Time of Day Slots', fontsize=12, fontweight='bold')
        ax.set_ylabel('Total Energy (kWh)', fontsize=12, fontweight='bold')
        
        # Set x-axis labels
        ax.set_xticks(x)
        ax.set_xticklabels(slot_labels, fontsize=11, fontweight='bold')
        
        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
        
        # Add grid for better readability
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)
        ax.set_axisbelow(True)
        
        # Add legend
        ax.legend(loc='upper right', fontsize=11, framealpha=0.9)
        
        # Set title
        plant_display_name = get_plant_display_name(plant_name)
        if end_date is None or start_date == end_date:
            date_str = start_date.strftime('%Y-%m-%d')
            title = f"ToD Generation vs Consumption for {plant_display_name} on {date_str}"
        else:
            date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            title = f"ToD Generation vs Consumption for {plant_display_name}\n({date_range})"
        
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        
        # Add peak/off-peak background shading
        peak_slots = ['Morning', 'Evening']  # Peak periods
        for i, slot_name in enumerate(slot_names):
            if slot_name in peak_slots:
                ax.axvspan(i - 0.4, i + 0.4, alpha=0.1, color='#FFF9C4', zorder=0)
        
        # Add peak period annotation
        ax.text(0.02, 0.98, 'Peak Periods', transform=ax.transAxes,
               bbox=dict(boxstyle="round,pad=0.3", facecolor='#FFF9C4', 
                        edgecolor='gray', alpha=0.8),
               fontsize=10, verticalalignment='top')
        
        # Set y-axis limits with padding
        max_value = max(generation_values + consumption_values)
        if max_value > 0:
            ax.set_ylim(0, max_value * 1.15)
        
        # Adjust layout
        plt.tight_layout()
        
        # Add subtle watermark
        fig.text(0.99, 0.01, 'ToD Comparative Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating daily ToD binned plot: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12, color='red')
        ax.set_title("Error Creating ToD Comparative Chart", fontsize=14, color='red')
        return fig

def create_combined_wind_solar_plot(df, client_name, start_date, end_date):
    """
    Create a plot showing combined wind and solar generation for a client

    Args:
        df (DataFrame): Combined wind and solar generation data
        client_name (str): Name of the client
        start_date (datetime): Start date of the data
        end_date (datetime): End date of the data

    Returns:
        Figure: Matplotlib figure object
    """
    sns.set_theme(style="whitegrid")

    try:
        # Create figure with two subplots - line chart on left, pie chart on right
        fig = plt.figure(figsize=(16, 8))

        # Create a grid spec to control the width of the subplots
        gs = fig.add_gridspec(1, 2, width_ratios=[2, 1])  # 2:1 ratio for line chart to pie chart

        # Create the two axes
        ax1 = fig.add_subplot(gs[0, 0])  # Line chart
        ax2 = fig.add_subplot(gs[0, 1])  # Pie chart

        # Print dataframe info for debugging
        logger.info(f"Combined wind and solar data shape: {df.shape}")
        logger.info(f"Combined wind and solar data columns: {df.columns.tolist()}")
        logger.info(f"Combined wind and solar data sample: {df.head(2).to_dict()}")
        
        # Additional debugging for time data
        if 'time' in df.columns:
            logger.info(f"Time column data type: {df['time'].dtype}")
            logger.info(f"Time range: {df['time'].min()} to {df['time'].max()}")
            if len(df) > 1:
                time_diff = df['time'].iloc[1] - df['time'].iloc[0]
                logger.info(f"Time interval between first two points: {time_diff}")

        # Create a copy to avoid modifying the original dataframe
        plot_df = df.copy()

        # Ensure all column names are lowercase
        plot_df.columns = [col.lower() for col in plot_df.columns]

        # Make sure we have the required columns
        if 'date' not in plot_df.columns:
            logger.error("Date column not found in dataframe")
            raise ValueError("Date column not found in dataframe")

        if 'source' not in plot_df.columns:
            logger.error("Source column not found in dataframe")
            raise ValueError("Source column not found in dataframe")

        if 'generation_kwh' not in plot_df.columns:
            logger.error("Generation_kwh column not found in dataframe")
            raise ValueError("Generation_kwh column not found in dataframe")

        # Determine if this is single day or date range data
        is_single_day = start_date == end_date

        if is_single_day:
            # For single day, use time column for 15-minute granularity
            if 'time' in plot_df.columns:
                plot_df['time'] = pd.to_datetime(plot_df['time'])
                # Convert timezone-aware timestamps to naive for better matplotlib compatibility
                if plot_df['time'].dt.tz is not None:
                    plot_df['time'] = plot_df['time'].dt.tz_localize(None)
                time_col = 'time'
                logger.info("Using time column for single day 15-minute data")
            else:
                # Fallback to date if time is not available
                plot_df['date'] = pd.to_datetime(plot_df['date'])
                # Convert timezone-aware timestamps to naive for better matplotlib compatibility
                if plot_df['date'].dt.tz is not None:
                    plot_df['date'] = plot_df['date'].dt.tz_localize(None)
                time_col = 'date'
                logger.info("Fallback to date column for single day data")

            # Sort by time to ensure proper chronological order
            plot_df = plot_df.sort_values(by=time_col)
            
            # Group by time and source to aggregate generation
            logger.info(f"Grouping single day data by {time_col} and source")
            grouped_df = plot_df.groupby([time_col, 'source'])['generation_kwh'].sum().reset_index()

            # Pivot the data to get generation by time and source
            logger.info("Pivoting single day data")
            pivot_df = grouped_df.pivot(
                index=time_col,
                columns='source',
                values='generation_kwh'
            ).reset_index()
            
            # Sort pivot_df by time column to ensure proper plotting order
            pivot_df = pivot_df.sort_values(by=time_col)
        else:
            # For date range, use date column for daily aggregation
            plot_df['date'] = pd.to_datetime(plot_df['date'])

            # Group by date and source to aggregate generation
            logger.info("Grouping data by date and source")
            grouped_df = plot_df.groupby([plot_df['date'].dt.date, 'source'])['generation_kwh'].sum().reset_index()

            # Convert date back to datetime for plotting
            grouped_df['date'] = pd.to_datetime(grouped_df['date'])

            # Pivot the data to get generation by date and source
            logger.info("Pivoting data")
            pivot_df = grouped_df.pivot(
                index='date',
                columns='source',
                values='generation_kwh'
            ).reset_index()
            time_col = 'date'

        # Fill NaN values with 0
        if 'Solar' in pivot_df.columns:
            pivot_df['Solar'] = pivot_df['Solar'].fillna(0)
        else:
            pivot_df['Solar'] = 0

        if 'Wind' in pivot_df.columns:
            pivot_df['Wind'] = pivot_df['Wind'].fillna(0)
        else:
            pivot_df['Wind'] = 0

        # Calculate total generation
        pivot_df['Total'] = pivot_df['Solar'] + pivot_df['Wind']

        # Calculate total generation by source for pie chart
        total_solar = pivot_df['Solar'].sum()
        total_wind = pivot_df['Wind'].sum()

        # ===== LINE CHART (LEFT SIDE) =====

        # Plot solar generation on the line chart
        ax1.plot(
            pivot_df[time_col],
            pivot_df['Solar'],
            color=COLORS.get("secondary", "#FBBC05"),  # Yellow for solar
            marker='o',
            markersize=6 if not is_single_day else 3,  # Smaller markers for 15-minute data
            linewidth=2,
            label='Solar Generation'
        )

        # Plot wind generation on the line chart
        ax1.plot(
            pivot_df[time_col],
            pivot_df['Wind'],
            color=COLORS.get("primary", "#4285F4"),  # Blue for wind
            marker='^',
            markersize=6 if not is_single_day else 3,  # Smaller markers for 15-minute data
            linewidth=2,
            label='Wind Generation'
        )

        # Plot total generation on the line chart
        ax1.plot(
            pivot_df[time_col],
            pivot_df['Total'],
            color=COLORS.get("success", "#34A853"),  # Green for total
            marker='s',
            markersize=6 if not is_single_day else 3,  # Smaller markers for 15-minute data
            linewidth=3,
            label='Total Generation'
        )

        # Format x-axis based on data type
        if is_single_day:
            # For single day 15-minute data, format as time
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            # Show every hour for better readability with 15-minute data
            ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))
            # Add minor ticks every 15 minutes for reference
            ax1.xaxis.set_minor_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45]))
        else:
            # For date range data, format as dates
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            if (end_date - start_date).days > 30:
                ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
            else:
                ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))

        # Format y-axis with K for thousands
        ax1.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid to line chart
        ax1.grid(True, linestyle='--', alpha=0.7)

        # Axes labels for line chart
        ax1.set_xlabel("Time" if is_single_day else "Date", fontsize=12)
        ax1.set_ylabel("Generation (kWh)", fontsize=12)

        # ===== PIE CHART (RIGHT SIDE) =====

        # Create pie chart data
        pie_data = [total_solar, total_wind]
        pie_labels = ['Solar', 'Wind']
        pie_colors = [COLORS.get("secondary", "#FBBC05"), COLORS.get("primary", "#4285F4")]

        # Calculate percentages for pie chart labels
        total_generation = total_solar + total_wind
        solar_percentage = (total_solar / total_generation * 100) if total_generation > 0 else 0
        wind_percentage = (total_wind / total_generation * 100) if total_generation > 0 else 0

        # Create pie chart labels with percentages
        pie_labels = [f'Solar: {solar_percentage:.1f}%', f'Wind: {wind_percentage:.1f}%']

        # Plot pie chart
        ax2.pie(
            pie_data,
            labels=pie_labels,
            autopct='%1.1f%%',
            startangle=90,
            colors=pie_colors,
            wedgeprops={'edgecolor': 'w', 'linewidth': 1},
            textprops={'fontsize': 12}
        )

        # Equal aspect ratio ensures that pie is drawn as a circle
        ax2.axis('equal')

        # Add title to pie chart
        ax2.set_title('Distribution of Generation Sources', fontsize=14, pad=20)

        # Print column names for debugging
        logger.info(f"DataFrame columns for plant names: {df.columns.tolist()}")

        # Summary text box removed as requested

        # Add legend to the line chart
        ax1.legend(loc='upper right', frameon=True, framealpha=0.9)

        # Rotate x-axis labels on the line chart
        if is_single_day:
            # For single day, use smaller rotation for time labels
            for label in ax1.get_xticklabels():
                label.set_rotation(30)
                label.set_ha('right')
        else:
            # For date range, use larger rotation for date labels
            for label in ax1.get_xticklabels():
                label.set_rotation(45)
                label.set_ha('right')


        # Title for the entire figure
        if is_single_day:
            date_range_str = f"{start_date.strftime('%Y-%m-%d')}"
        else:
            date_range_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

      
        plt.subplots_adjust(top=0.85, bottom=0.15, hspace=0.3, left=0.1, right=0.9)

        # Add title after adjusting the layout
        fig.suptitle(f"Combined Wind and Solar Generation for {client_name} ({date_range_str})",
                    fontsize=16, y=0.98)

        return fig

    except Exception as e:
        logger.error(f"Error creating combined wind and solar plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig


def create_power_cost_comparison_plot(cost_df, plant_name, start_date, end_date=None):
    """
    Create a comparison plot showing grid cost vs actual cost.

    Args:
        cost_df (DataFrame): DataFrame with cost metrics
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        if cost_df.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, "No data available for cost analysis",
                    ha='center', va='center', fontsize=12)
            return fig

        # Validate required columns
        required_columns = ['grid_cost', 'actual_cost']
        missing_columns = [col for col in required_columns if col not in cost_df.columns]
        if missing_columns:
            logger.error(f"Missing required columns for cost comparison plot: {missing_columns}")
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, f"Missing data columns: {', '.join(missing_columns)}",
                   ha='center', va='center', fontsize=12)
            ax.set_title("Power Cost Comparison - Data Error")
            return fig
        
        # Ensure numeric columns are properly formatted
        for col in required_columns:
            cost_df[col] = pd.to_numeric(cost_df[col], errors='coerce').fillna(0)
        
        # Check if we have any valid data
        if cost_df[required_columns].sum().sum() == 0:
            logger.warning("All cost values are zero or invalid")
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, "No valid cost data available",
                   ha='center', va='center', fontsize=12)
            ax.set_title("Power Cost Comparison - No Data")
            return fig

        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Determine if single day or date range
        is_single_day = end_date is None or start_date == end_date

        if is_single_day:
            # For single day, use time column or create index
            if 'time' in cost_df.columns:
                x_data = cost_df['time']
                x_label = "Time"
            elif 'datetime' in cost_df.columns:
                x_data = cost_df['datetime']
                x_label = "Time"
            else:
                x_data = range(len(cost_df))
                x_label = "Time Period"
            title_date = start_date.strftime('%Y-%m-%d')
        else:
            # For date range, use date column
            if 'date' in cost_df.columns:
                x_data = cost_df['date']
                x_label = "Date"
            elif 'time' in cost_df.columns:
                # Fallback: try to extract date from time column
                try:
                    cost_df['date'] = pd.to_datetime(cost_df['time']).dt.date
                    x_data = cost_df['date']
                    x_label = "Date"
                except:
                    x_data = range(len(cost_df))
                    x_label = "Day"
            else:
                x_data = range(len(cost_df))
                x_label = "Day"
            title_date = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        # Create bar chart comparing grid cost vs actual cost
        width = 0.35
        x_pos = range(len(cost_df))

        bars1 = ax.bar([x - width/2 for x in x_pos], cost_df['grid_cost'],
                      width, label='Grid Cost (Without Solar/Wind)',
                      color=COLORS.get("danger", "#EA4335"), alpha=0.8)

        bars2 = ax.bar([x + width/2 for x in x_pos], cost_df['actual_cost'],
                      width, label='Actual Cost (With Solar/Wind)',
                      color=COLORS.get("success", "#34A853"), alpha=0.8)


        # Customize the plot
        plant_display_name = get_plant_display_name(plant_name)

        ax.set_title(f"Power Cost Comparison - {plant_display_name}\n{title_date}", fontsize=14, pad=20)
        ax.set_xlabel(x_label, fontsize=12)
        ax.set_ylabel("Cost (₹)", fontsize=12)
        ax.legend()

        # Format x-axis
        if is_single_day:
            # For single day data
            step = max(1, len(cost_df) // 8)
            ax.set_xticks(range(0, len(cost_df), step))

            if 'time' in cost_df.columns:
                # Try to format time column
                try:
                    labels = []
                    for i in range(0, len(cost_df), step):
                        time_val = cost_df.iloc[i]['time']
                        if hasattr(time_val, 'strftime'):
                            labels.append(time_val.strftime('%H:%M'))
                        elif hasattr(time_val, 'hour'):  # Handle time objects
                            labels.append(f"{time_val.hour:02d}:{time_val.minute:02d}")
                        else:
                            # Try to convert to datetime
                            try:
                                dt_val = pd.to_datetime(time_val)
                                labels.append(dt_val.strftime('%H:%M'))
                            except:
                                labels.append(f"T{i}")
                    ax.set_xticklabels(labels, rotation=45)
                except Exception as e:
                    logger.warning(f"Error formatting time labels: {e}")
                    # Fallback to simple numbering
                    ax.set_xticklabels([f"T{i}" for i in range(0, len(cost_df), step)], rotation=45)
            else:
                # Use simple time period labels
                ax.set_xticklabels([f"Period {i}" for i in range(0, len(cost_df), step)], rotation=45)
        else:
            # For date range data
            step = max(1, len(cost_df) // 10)
            ax.set_xticks(range(0, len(cost_df), step))

            if 'date' in cost_df.columns:
                try:
                    labels = []
                    for i in range(0, len(cost_df), step):
                        date_val = cost_df.iloc[i]['date']
                        if hasattr(date_val, 'strftime'):
                            labels.append(date_val.strftime('%m/%d'))
                        elif hasattr(date_val, 'year'):  # Handle date objects
                            labels.append(f"{date_val.month:02d}/{date_val.day:02d}")
                        else:
                            labels.append(f"Day {i+1}")
                    ax.set_xticklabels(labels, rotation=45)
                except Exception as e:
                    logger.warning(f"Error formatting date labels: {e}")
                    # Fallback to simple numbering
                    ax.set_xticklabels([f"Day {i+1}" for i in range(0, len(cost_df), step)], rotation=45)
            else:
                # Use simple day labels
                ax.set_xticklabels([f"Day {i+1}" for i in range(0, len(cost_df), step)], rotation=45)

        # Add grid
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        plt.tight_layout()
        return fig

    except Exception as e:
        logger.error(f"Error creating power cost comparison plot: {e}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig


def create_power_savings_plot(cost_df, plant_name, start_date, end_date=None):
    """
    Create a plot showing power savings over time.

    Args:
        cost_df (DataFrame): DataFrame with cost metrics
        plant_name (str): Name of the plant
        start_date (datetime): Start date
        end_date (datetime, optional): End date. If None, only start_date is used.

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        if cost_df.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, "No data available for savings analysis",
                    ha='center', va='center', fontsize=12)
            return fig

        # Validate required columns
        if 'savings' not in cost_df.columns:
            logger.error(f"Missing 'savings' column for power savings plot. Available columns: {cost_df.columns.tolist()}")
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, "Missing savings data column",
                   ha='center', va='center', fontsize=12)
            ax.set_title("Power Savings - Data Error")
            return fig
        
        # Ensure savings column is properly formatted
        cost_df['savings'] = pd.to_numeric(cost_df['savings'], errors='coerce').fillna(0)
        
        # Handle edge case of single data point
        if len(cost_df) == 1:
            logger.info("Single data point for savings plot")
            # Force to use bar chart for single point
            is_single_day = False
        
        # Check if we have any valid savings data
        if cost_df['savings'].sum() == 0 and cost_df['savings'].max() == 0:
            logger.info("All savings values are zero")
            # Still create the plot to show zero savings
            pass

        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Determine if single day or date range
        is_single_day = end_date is None or start_date == end_date

        if is_single_day:
            # For single day, use line plot
            if 'time' in cost_df.columns or 'datetime' in cost_df.columns:
                # Try to use time data for x-axis
                try:
                    time_col = 'time' if 'time' in cost_df.columns else 'datetime'
                    time_data = pd.to_datetime(cost_df[time_col])
                    
                    # Use different colors for positive and negative savings
                    positive_mask = cost_df['savings'] >= 0
                    negative_mask = cost_df['savings'] < 0
                    
                    # Plot the main line
                    ax.plot(range(len(cost_df)), cost_df['savings'], marker='o', linewidth=2,
                           color=COLORS.get("success", "#34A853"), markersize=4)
                    
                    # Highlight negative savings points in red
                    if negative_mask.any():
                        negative_indices = [i for i in range(len(cost_df)) if negative_mask.iloc[i]]
                        negative_values = cost_df.loc[negative_mask, 'savings']
                        ax.scatter(negative_indices, negative_values, 
                                 color=COLORS.get("danger", "#EA4335"), s=30, zorder=5)

                    # Format x-axis with time labels
                    step = max(1, len(cost_df) // 8)
                    ax.set_xticks(range(0, len(cost_df), step))
                    labels = []
                    for i in range(0, len(cost_df), step):
                        time_val = time_data.iloc[i]
                        if hasattr(time_val, 'strftime'):
                            labels.append(time_val.strftime('%H:%M'))
                        else:
                            labels.append(f"T{i}")
                    ax.set_xticklabels(labels, rotation=45)
                except Exception as e:
                    logger.warning(f"Error formatting time data for savings plot: {e}")
                    # Fallback to simple plot with color coding
                    ax.plot(range(len(cost_df)), cost_df['savings'], marker='o', linewidth=2,
                           color=COLORS.get("success", "#34A853"), markersize=4)
                    
                    # Highlight negative savings points in red
                    negative_mask = cost_df['savings'] < 0
                    if negative_mask.any():
                        negative_indices = [i for i in range(len(cost_df)) if negative_mask.iloc[i]]
                        negative_values = cost_df.loc[negative_mask, 'savings']
                        ax.scatter(negative_indices, negative_values, 
                                 color=COLORS.get("danger", "#EA4335"), s=30, zorder=5)
            else:
                # Use simple index-based plot with color coding
                ax.plot(range(len(cost_df)), cost_df['savings'], marker='o', linewidth=2,
                       color=COLORS.get("success", "#34A853"), markersize=4)
                
                # Highlight negative savings points in red
                negative_mask = cost_df['savings'] < 0
                if negative_mask.any():
                    negative_indices = [i for i in range(len(cost_df)) if negative_mask.iloc[i]]
                    negative_values = cost_df.loc[negative_mask, 'savings']
                    ax.scatter(negative_indices, negative_values, 
                             color=COLORS.get("danger", "#EA4335"), s=30, zorder=5)

            x_label = "Time"
            title_date = start_date.strftime('%Y-%m-%d')
        else:
            # For date range, use bar plot with different colors for positive/negative savings
            colors = []
            for saving in cost_df['savings']:
                if saving >= 0:
                    colors.append(COLORS.get("success", "#34A853"))
                else:
                    colors.append(COLORS.get("danger", "#EA4335"))
            
            bars = ax.bar(range(len(cost_df)), cost_df['savings'],
                         color=colors, alpha=0.8)

            # Add value labels on bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                # Only add label if height is significant (avoid cluttering for very small values)
                if abs(height) > 0.1:
                    y_pos = height + (abs(height) * 0.01) if height >= 0 else height - (abs(height) * 0.01)
                    va_pos = 'bottom' if height >= 0 else 'top'
                    ax.text(bar.get_x() + bar.get_width()/2., y_pos,
                           f'₹{height:.0f}', ha='center', va=va_pos, fontsize=9)

            x_label = "Date"
            title_date = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

            # Format x-axis for dates
            step = max(1, len(cost_df) // 10)
            ax.set_xticks(range(0, len(cost_df), step))
            if 'date' in cost_df.columns:
                try:
                    labels = []
                    for i in range(0, len(cost_df), step):
                        date_val = cost_df.iloc[i]['date']
                        if hasattr(date_val, 'strftime'):
                            labels.append(date_val.strftime('%m/%d'))
                        elif hasattr(date_val, 'year'):  # Handle date objects
                            labels.append(f"{date_val.month:02d}/{date_val.day:02d}")
                        else:
                            labels.append(f"Day {i+1}")
                    ax.set_xticklabels(labels, rotation=45)
                except Exception as e:
                    logger.warning(f"Error formatting date labels for savings plot: {e}")
                    # Fallback to simple numbering
                    ax.set_xticklabels([f"Day {i+1}" for i in range(0, len(cost_df), step)], rotation=45)
            elif 'time' in cost_df.columns:
                # Try to extract date from time column
                try:
                    dates = pd.to_datetime(cost_df['time']).dt.date
                    labels = []
                    for i in range(0, len(cost_df), step):
                        date_val = dates.iloc[i]
                        labels.append(f"{date_val.month:02d}/{date_val.day:02d}")
                    ax.set_xticklabels(labels, rotation=45)
                except Exception as e:
                    logger.warning(f"Error formatting date labels in savings plot: {e}")
                    ax.set_xticklabels([f"Day {i+1}" for i in range(0, len(cost_df), step)], rotation=45)
            else:
                ax.set_xticklabels([f"Day {i+1}" for i in range(0, len(cost_df), step)], rotation=45)

        # Customize the plot
        from backend.data.db_data_manager import get_plant_display_name
        plant_display_name = get_plant_display_name(plant_name)

        ax.set_title(f"Power Cost Savings - {plant_display_name}\n{title_date}", fontsize=14, pad=20)
        ax.set_xlabel(x_label, fontsize=12)
        ax.set_ylabel("Savings (₹)", fontsize=12)

        # Format y-axis for better readability
        max_savings = cost_df['savings'].max()
        min_savings = cost_df['savings'].min()
        if max_savings > 1000 or abs(min_savings) > 1000:
            ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))

        # Add grid
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        # Add zero line if there are negative savings
        if cost_df['savings'].min() < 0:
            ax.axhline(y=0, color='red', linestyle='-', alpha=0.5)

        # Add summary annotation for multi-day scenarios
        if not is_single_day and len(cost_df) > 1:
            total_savings = cost_df['savings'].sum()
            avg_savings = cost_df['savings'].mean()
            
            # Add text box with summary
            summary_text = f"Total Savings: ₹{total_savings:.0f}\nAvg Daily: ₹{avg_savings:.0f}"
            ax.text(0.02, 0.98, summary_text, transform=ax.transAxes, 
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7),
                   verticalalignment='top', fontsize=10)

        plt.tight_layout()
        return fig

    except Exception as e:
        logger.error(f"Error creating power savings plot: {e}")
        logger.error(traceback.format_exc())
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12)
        return fig


def create_banking_plot(df, plant_name, banking_type="daily", tod_based=False):
    """
    Create a plot for banking data

    Args:
        df (DataFrame): Banking data
        plant_name (str): Name of the plant
        banking_type (str): Type of banking data (daily, monthly, yearly)
        tod_based (bool): Whether the data is ToD-based

    Returns:
        Figure: Matplotlib figure object
    """
    try:
        # Create the figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Set the title based on the banking type and ToD setting
        tod_text = "ToD-Based" if tod_based else "Non-ToD"
        title = f"{banking_type.capitalize()} Banking ({tod_text}) - {plant_name}"
        ax.set_title(title, fontsize=16, pad=20)

        # Determine the columns to plot based on the banking type and ToD setting
        if tod_based:
            # For ToD-based banking, we have different columns
            if 'origin_slot_name' in df.columns:
                # Create a grouped bar chart for peak and off-peak
                peak_df = df[df['origin_slot_name'] == 'peak']
                offpeak_df = df[df['origin_slot_name'] == 'offpeak']

                # Determine x-axis based on banking type
                if banking_type == "daily":
                    x_col = 'Date'
                elif banking_type == "monthly":
                    x_col = 'Month'
                else:  # yearly
                    x_col = 'Year'

                # Plot peak data
                if not peak_df.empty:
                    sns.barplot(
                        data=peak_df,
                        x=x_col,
                        y='Surplus Generation(After Settlement)',
                        color=COLORS.get("primary", "#1E88E5"),
                        alpha=0.8,
                        label="Peak Surplus Generation",
                        ax=ax
                    )

                    # Plot grid consumption as negative values
                    sns.barplot(
                        data=peak_df,
                        x=x_col,
                        y='Grid Consumption(After Settlement)',
                        color=COLORS.get("consumption", "#00897B"),
                        alpha=0.8,
                        label="Peak Grid Consumption",
                        ax=ax
                    )

                # Plot off-peak data
                if not offpeak_df.empty:
                    sns.barplot(
                        data=offpeak_df,
                        x=x_col,
                        y='Surplus Generation(After Settlement)',
                        color=COLORS.get("secondary", "#5E35B1"),
                        alpha=0.8,
                        label="Off-Peak Surplus Generation",
                        ax=ax
                    )

                    # Plot grid consumption as negative values
                    sns.barplot(
                        data=offpeak_df,
                        x=x_col,
                        y='Grid Consumption(After Settlement)',
                        color=COLORS.get("tertiary", "#00ACC1"),
                        alpha=0.8,
                        label="Off-Peak Grid Consumption",
                        ax=ax
                    )
            else:
                # Fallback if the expected columns aren't found
                ax.text(0.5, 0.5, "No ToD data available", ha='center', va='center', fontsize=14)
        else:
            # For non-ToD banking
            if banking_type == "daily":
                # For daily banking, plot Surplus Generation and Grid Consumption
                if 'Surplus Generation' in df.columns and 'Grid Consumption' in df.columns:
                    # Plot surplus generation
                    sns.barplot(
                        data=df,
                        x='Date',
                        y='Surplus Generation',
                        color=COLORS.get("primary", "#1E88E5"),
                        alpha=0.8,
                        label="Surplus Generation",
                        ax=ax
                    )

                    # Plot grid consumption
                    sns.barplot(
                        data=df,
                        x='Date',
                        y='Grid Consumption',
                        color=COLORS.get("consumption", "#00897B"),
                        alpha=0.8,
                        label="Grid Consumption",
                        ax=ax
                    )
                else:
                    ax.text(0.5, 0.5, "No daily banking data available", ha='center', va='center', fontsize=14)

            elif banking_type == "monthly":
                # For monthly banking, plot Surplus Generation and Grid Consumption
                if 'Surplus Generation' in df.columns and 'Grid Consumption' in df.columns:
                    # Plot surplus generation
                    sns.barplot(
                        data=df,
                        x='Month',
                        y='Surplus Generation',
                        color=COLORS.get("primary", "#1E88E5"),
                        alpha=0.8,
                        label="Surplus Generation",
                        ax=ax
                    )

                    # Plot grid consumption
                    sns.barplot(
                        data=df,
                        x='Month',
                        y='Grid Consumption',
                        color=COLORS.get("consumption", "#00897B"),
                        alpha=0.8,
                        label="Grid Consumption",
                        ax=ax
                    )
                else:
                    ax.text(0.5, 0.5, "No monthly banking data available", ha='center', va='center', fontsize=14)

            elif banking_type == "yearly":
                # For yearly banking, plot Yearly Surplus and Yearly Deficit
                if 'Yearly Surplus' in df.columns and 'Yearly Deficit' in df.columns:
                    # Plot yearly surplus
                    sns.barplot(
                        data=df,
                        x='Year',
                        y='Yearly Surplus',
                        color=COLORS.get("primary", "#1E88E5"),
                        alpha=0.8,
                        label="Yearly Surplus",
                        ax=ax
                    )

                    # Plot yearly deficit
                    sns.barplot(
                        data=df,
                        x='Year',
                        y='Yearly Deficit',
                        color=COLORS.get("consumption", "#00897B"),
                        alpha=0.8,
                        label="Yearly Deficit",
                        ax=ax
                    )
                else:
                    ax.text(0.5, 0.5, "No yearly banking data available", ha='center', va='center', fontsize=14)

        # Add legend
        ax.legend()

        # Add grid lines
        ax.grid(True, linestyle='--', alpha=0.7)

        # Rotate x-axis labels if needed
        plt.xticks(rotation=45)

        # Adjust layout manually instead of using tight_layout()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.15)

        return fig
    except Exception as e:
        logger.error(f"Error creating banking plot: {e}")
        # Create an empty figure with error message
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"Error creating banking plot: {str(e)}", ha='center', va='center', fontsize=14)
        return fig


def create_monthly_before_banking_settlement_plot(df, plant_name):
  
    """
    Create a monthly BEFORE banking settlement stacked bar chart showing generation and consumption by ToD slots.
    Shows initial surplus generation and demand values before any banking settlement process.
    
    Args:
        df: DataFrame with monthly before banking settlement data
        plant_name: Name of the plant
        
    Returns:
        Matplotlib figure object
    """
    try:
        fig, ax = plt.subplots(figsize=(16, 10))
        
        if df.empty:
            ax.text(0.5, 0.5, 'No banking settlement data available', 
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title(f"Monthly Banking Settlement - {get_plant_display_name(plant_name)}", 
                        fontsize=16, fontweight='bold')
            return fig
        
     
        
        # Get unique months and sort them, handle None values
        months = [m for m in df['month'].unique() if m is not None]
        if not months:
            # If all months are None, use a default label
            months = ['N/A']
            # Replace None values with 'N/A' in the dataframe
            df['month'] = df['month'].fillna('N/A')
        else:
            months = sorted(months)
        logger.info(f"Available months: {months}")
        
        # Get unique slots and sort them
        slots = df['slot_name'].nunique()
        unique_slots = sorted(df['slot_name'].unique())
        logger.info(f"Available slots: {unique_slots}")
        
        # If no slots, show error
        if slots == 0:
            ax.text(0.5, 0.5, 'No ToD slot data available', 
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title(f"Monthly Banking Settlement - {get_plant_display_name(plant_name)}", 
                        fontsize=16, fontweight='bold')
            return fig
        
        # Create pivot tables for generation and consumption
        gen_pivot = df.pivot_table(
            index='month', 
            columns='slot_name', 
            values='total_generation', 
            fill_value=0
        )
        
        cons_pivot = df.pivot_table(
            index='month', 
            columns='slot_name', 
            values='total_consumption', 
            fill_value=0
        )
        
        # Ensure both pivots have the same columns
        all_slots = sorted(set(gen_pivot.columns) | set(cons_pivot.columns))
        gen_pivot = gen_pivot.reindex(columns=all_slots, fill_value=0)
        cons_pivot = cons_pivot.reindex(columns=all_slots, fill_value=0)
        
        logger.info(f"Generation pivot shape: {gen_pivot.shape}")
        logger.info(f"Consumption pivot shape: {cons_pivot.shape}")
        
        # Check if pivot tables are empty
        if gen_pivot.empty and cons_pivot.empty:
            ax.text(0.5, 0.5, 'No banking settlement data available for pivot analysis', 
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title(f"Monthly Banking Settlement - {get_plant_display_name(plant_name)}", 
                        fontsize=16, fontweight='bold')
            return fig
        
        # Set up the plot
        n_months = len(months)
        bar_width = 0.35
        x_positions = np.arange(n_months)
        
        # Colors for ToD slots (mapping for different slot names)
        # Using more distinct and consistent colors for better visibility
        tod_colors = {
            'Slot_1': '#FF5722',    # Deep Orange/Red for Slot 1
            'Slot_2': '#4CAF50',    # Green for Slot 2
            'Slot_3': '#2196F3',    # Blue for Slot 3
            'Slot_4': '#FF9800',    # Orange for Slot 4
            'Normal': '#4CAF50',    # Green for Normal slot
            'off-peak': '#2196F3',  # Blue for off-peak
            'Peak-1': '#FF5722',    # Deep Orange/Red for Peak-1
            'Peak-2': '#FF9800',    # Orange for Peak-2
            # Additional common slot names
            '6-9': '#FF5722',       # Morning peak (6 AM - 9 AM)
            '9-18': '#4CAF50',      # Day off-peak (9 AM - 6 PM)
            '18-22': '#FF9800',     # Evening peak (6 PM - 10 PM)
            '22-6': '#2196F3'       # Night off-peak (10 PM - 6 AM)
        }
        
        # Plot generation bars (stacked)
        gen_bottom = np.zeros(n_months)
        for slot in all_slots:
            if slot in gen_pivot.columns and not gen_pivot.empty:
                values = gen_pivot[slot].values
                # Ensure values array has the correct length
                if len(values) == n_months:
                    ax.bar(
                        x_positions - bar_width/2,
                        values,
                        bar_width,
                        bottom=gen_bottom,
                        label=f'Gen {slot}',
                        color=tod_colors.get(slot, '#808080'),
                        alpha=0.9,
                        edgecolor='white',
                        linewidth=1.5
                    )
                    gen_bottom += values
        
        # Plot consumption bars (stacked)
        cons_bottom = np.zeros(n_months)
        for slot in all_slots:
            if slot in cons_pivot.columns and not cons_pivot.empty:
                values = cons_pivot[slot].values
                # Ensure values array has the correct length
                if len(values) == n_months:
                    ax.bar(
                        x_positions + bar_width/2,
                        values,
                        bar_width,
                        bottom=cons_bottom,
                        label=f'Cons {slot}',
                        color=tod_colors.get(slot, '#808080'),
                        alpha=0.9,
                        edgecolor='black',
                        linewidth=1.5,
                        hatch='///'  # Pattern to differentiate from generation
                    )
                    cons_bottom += values
        
        # Add total value labels on top of stacked bars
        for i, month in enumerate(months):
            # Handle case where month might be 'N/A' or None
            try:
                if month in gen_pivot.index:
                    gen_total = gen_pivot.loc[month].sum()
                else:
                    gen_total = 0
                    
                if month in cons_pivot.index:
                    cons_total = cons_pivot.loc[month].sum()
                else:
                    cons_total = 0
            except (KeyError, TypeError):
                gen_total = 0
                cons_total = 0
            
            # Label for generation bar
            if gen_total > 0:
                ax.text(
                    i - bar_width/2,
                    gen_total + max(gen_total, cons_total) * 0.02,
                    f'{gen_total:.0f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=10,
                    color='#2E7D32'
                )
            
            # Label for consumption bar
            if cons_total > 0:
                ax.text(
                    i + bar_width/2,
                    cons_total + max(gen_total, cons_total) * 0.02,
                    f'{cons_total:.0f}',
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=10,
                    color='#C62828'
                )
        
        # Customize the plot
        ax.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax.set_ylabel('Energy (kWh)', fontsize=12, fontweight='bold')
        ax.set_title(f'Monthly ToD Before Banking - {get_plant_display_name(plant_name)}', 
                    fontsize=16, fontweight='bold', pad=20)
        
        # Set x-axis
        ax.set_xticks(x_positions)
        ax.set_xticklabels(months, rotation=45, ha='right', fontsize=10)
        
        # Format y-axis with K for thousands
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
        
        # Add grid for better readability
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)
        ax.set_axisbelow(True)
        
        # Create custom legend
        legend_elements = []
        
        # Add generation legend entries
        for slot in all_slots:
            legend_elements.append(
                plt.Rectangle((0,0),1,1, 
                            facecolor=tod_colors.get(slot, '#808080'), 
                            alpha=0.9,
                            edgecolor='white',
                            linewidth=1.5,
                            label=f'Generation {slot}')
            )
        
        # Add consumption legend entries
        for slot in all_slots:
            legend_elements.append(
                plt.Rectangle((0,0),1,1, 
                            facecolor=tod_colors.get(slot, '#808080'), 
                            alpha=0.9,
                            edgecolor='black',
                            linewidth=1.5,
                            hatch='///',
                            label=f'Consumption {slot}')
            )
        
        ax.legend(handles=legend_elements, 
                 loc='upper left', 
                 bbox_to_anchor=(1.02, 1), 
                 fontsize=10,
                 framealpha=0.9)
        
        # Add summary statistics
        total_gen = gen_pivot.sum().sum()
        total_cons = cons_pivot.sum().sum()
        net_surplus = total_gen - total_cons
        
        summary_text = f"""
        Total Generation: {total_gen:,.0f} kWh
        Total Consumption: {total_cons:,.0f} kWh
        Net Surplus: {net_surplus:,.0f} kWh
        """
        
        ax.text(0.02, 0.98, summary_text.strip(),
               transform=ax.transAxes,
               bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.8),
               fontsize=10,
               verticalalignment='top')
        
        # Set y-axis limits with padding
        max_value = max(gen_pivot.sum(axis=1).max(), cons_pivot.sum(axis=1).max())
        if max_value > 0:
            ax.set_ylim(0, max_value * 1.15)
        
        # Adjust layout
        plt.tight_layout()
        
        # Add watermark
        fig.text(0.99, 0.01, 'Monthly Banking Settlement Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating monthly banking settlement plot: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Return a simple error figure
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(0.5, 0.5, f"Error creating plot: {str(e)}",
                ha='center', va='center', fontsize=12, color='red')
        ax.set_title("Error Creating Monthly Banking Settlement Chart", fontsize=14, color='red')
        return fig



def create_monthly_banking_settlement_chart(df: pd.DataFrame, plant_name: str) -> tuple[plt.Figure, pd.DataFrame]:
    """
    Create a chart showing monthly banking settlement data with only settlement metrics.
    Shows total settlement, settlement with banking, and settlement without banking.
    Includes a pie chart showing overall percentage breakdown.
    
    Args:
        df: DataFrame with monthly banking settlement data
        plant_name: Name of the plant
        
    Returns:
        Tuple of (matplotlib figure object, processed monthly data DataFrame)
    """
    try:
        # Create subplot layout: line chart on left, pie chart on right
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10), gridspec_kw={'width_ratios': [2, 1]})
        
        if df.empty:
            ax1.text(0.5, 0.5, 'No banking settlement data available', 
                    ha='center', va='center', fontsize=14, color='red')
            ax1.set_title(f"Monthly Banking Settlement - {get_plant_display_name(plant_name)}", 
                         fontsize=16, fontweight='bold')
            ax2.text(0.5, 0.5, 'No data for pie chart', 
                    ha='center', va='center', fontsize=14, color='red')
            ax2.set_title("Overall Breakdown", fontsize=16, fontweight='bold')
            return fig, pd.DataFrame()
        
        logger.info(f"Banking settlement line chart data shape: {df.shape}")
        logger.info(f"Banking settlement line chart data columns: {df.columns.tolist()}")
        
        # Aggregate data by month using the correct column names
        monthly_data = df.groupby('month').agg({
            'total_generation': 'sum',  # Settlement without banking
            'settled_units_with_banking': 'sum',  # Settlement with banking (partial)
            'intra_settlement': 'sum', 
            'inter_settlement': 'sum',
            'total_consumption': 'sum',  # Grid consumption without banking
            'surplus_demand_after_banking': 'sum'  # Grid consumption with banking
        }).reset_index()
        
        # Calculate settlement with banking (settled_units_with_banking + intra + inter)
        monthly_data['settlement_with_banking'] = (
            monthly_data['settled_units_with_banking'] + 
            monthly_data['intra_settlement'] + 
            monthly_data['inter_settlement']
        )
        
        # Calculate total settlement (settlement with banking + settlement without banking)
        monthly_data['total_settlement'] = (
            monthly_data['settlement_with_banking'] + monthly_data['total_generation']
        )
        
        # Rename columns for clarity
        monthly_data = monthly_data.rename(columns={
            'total_generation': 'settlement_without_banking',
            'total_consumption': 'grid_consumption_without_banking',
            'surplus_demand_after_banking': 'grid_consumption_with_banking'
        })
        
        # Calculate replacement percentage
        monthly_data['replacement_percentage'] = (
            (monthly_data['settlement_with_banking'] / monthly_data['settlement_without_banking']) * 100
        ).fillna(0)
        
        # Sort by month
        monthly_data = monthly_data.sort_values('month')
        
        # Get months for x-axis
        months = monthly_data['month'].tolist()
        x_positions = range(len(months))
        
        # Define colors for the three settlement lines only
        colors = {
            'total_settlement': '#9C27B0',                 # Purple
            'settlement_with_banking': '#4CAF50',          # Green
            'settlement_without_banking': '#2196F3',       # Blue
        }
        
        # Plot only the three settlement lines on ax1 (left subplot)
        ax1.plot(x_positions, monthly_data['total_settlement'], 
                color=colors['total_settlement'], linewidth=3, marker='o', markersize=8,
                label='Total Settlement', alpha=0.9)
        
        ax1.plot(x_positions, monthly_data['settlement_with_banking'], 
                color=colors['settlement_with_banking'], linewidth=3, marker='s', markersize=8,
                label='Total Settlement with Banking', alpha=0.9)
        
        ax1.plot(x_positions, monthly_data['settlement_without_banking'], 
                color=colors['settlement_without_banking'], linewidth=3, marker='^', markersize=8,
                label='Total Settlement without Banking', alpha=0.9)
        
        # Add replacement percentage labels above the lines
        for i, (x, pct) in enumerate(zip(x_positions, monthly_data['replacement_percentage'])):
            if pct > 0:  # Only show if percentage is meaningful
                # Find the highest point among the three settlement lines at this x position
                max_y = max(
                    monthly_data.iloc[i]['total_settlement'],
                    monthly_data.iloc[i]['settlement_with_banking'],
                    monthly_data.iloc[i]['settlement_without_banking']
                )
                
                # Place percentage label above the highest point
                ax1.text(x, max_y * 1.05, f'{pct:.1f}%', 
                        ha='center', va='bottom', fontweight='bold', 
                        fontsize=10, color='#333333',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='yellow', alpha=0.7))
        
        # Customize the line chart (ax1)
        ax1.set_xlabel('Month', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Energy (kWh)', fontsize=14, fontweight='bold')
        ax1.set_title(f'Monthly Banking Settlement - {get_plant_display_name(plant_name)}', 
                     fontsize=16, fontweight='bold', pad=20)
        
        # Set x-axis
        ax1.set_xticks(x_positions)
        ax1.set_xticklabels(months, rotation=45, ha='right', fontsize=12)
        
        # Format y-axis with thousands separator
        ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Add grid for better readability
        ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        ax1.set_axisbelow(True)
        
        # Add legend
        ax1.legend(loc='upper right', 
                  framealpha=0.9, fontsize=10)
        
        # Set y-axis limits with padding for only the three settlement lines
        all_values = (
            monthly_data['total_settlement'].tolist() + 
            monthly_data['settlement_with_banking'].tolist() + 
            monthly_data['settlement_without_banking'].tolist()
        )
        max_value = max(all_values) if all_values else 1
        ax1.set_ylim(0, max_value * 1.2)  # Extra padding for percentage labels
        
        # Calculate totals for pie chart (only with and without banking)
        total_settlement_with = monthly_data['settlement_with_banking'].sum()
        total_settlement_without = monthly_data['settlement_without_banking'].sum()
        
        # Create pie chart (ax2) with only two categories: with and without banking
        pie_labels = [
            'Settlement with Banking',
            'Settlement without Banking'
        ]
        pie_values = [total_settlement_with, total_settlement_without]
        pie_colors = [
            colors['settlement_with_banking'],
            colors['settlement_without_banking']
        ]
        
        # Only include non-zero values in pie chart
        filtered_data = [(label, value, color) for label, value, color in zip(pie_labels, pie_values, pie_colors) if value > 0]
        if filtered_data:
            labels, values, chart_colors = zip(*filtered_data)
            
            # Calculate percentages
            total_sum = sum(values)
            percentages = [value/total_sum * 100 for value in values]
            
            # Create pie chart
            wedges, texts, autotexts = ax2.pie(values, labels=None, colors=chart_colors, autopct='%1.1f%%',
                                              startangle=90, textprops={'fontsize': 8}, 
                                              pctdistance=1.1)
            
            # Enhance the pie chart appearance - make percentages vertical and outside
            for autotext in autotexts:
                autotext.set_color('black')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(9)
                autotext.set_rotation(90)  # Make text vertical
            
            # Add legend to pie chart
            ax2.legend(wedges, labels, title="Banking Settlement", loc="upper right", 
                      bbox_to_anchor=(1, 1), fontsize=9)
            
            ax2.set_title('Banking vs Non-Banking Settlement', fontsize=14, fontweight='bold', pad=20)
        else:
            ax2.text(0.5, 0.5, 'No data available for pie chart', 
                    ha='center', va='center', fontsize=12, color='red')
            ax2.set_title('Banking vs Non-Banking Settlement', fontsize=14, fontweight='bold', pad=20)
        
        # Add summary statistics to line chart (only settlement metrics)
        avg_replacement = monthly_data['replacement_percentage'].mean()
        total_combined = total_settlement_with + total_settlement_without
        
        summary_text = f"""Summary Statistics:
Avg Replacement %: {avg_replacement:.1f}%
Total Combined Settlement: {total_combined:,.0f} kWh
Settlement w/ Banking: {total_settlement_with:,.0f} kWh
Settlement w/o Banking: {total_settlement_without:,.0f} kWh"""
        
        ax1.text(0.02, 0.98, summary_text.strip(),
                transform=ax1.transAxes,
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.8),
                fontsize=9,
                verticalalignment='top',
                fontfamily='monospace')
        
        # Ensure tight layout
        plt.tight_layout()
        
        # Add watermark
        fig.text(0.99, 0.01, 'Banking Settlement Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)
        
        return fig, monthly_data
        
    except Exception as e:
        logger.error(f"Error creating monthly banking settlement chart: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Return a simple error figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10), gridspec_kw={'width_ratios': [2, 1]})
        ax1.text(0.5, 0.5, f"Error creating line chart: {str(e)}",
                ha='center', va='center', fontsize=12, color='red')
        ax1.set_title("Error Creating Monthly Banking Settlement", fontsize=14, color='red')
        ax2.text(0.5, 0.5, f"Error creating pie chart: {str(e)}",
                ha='center', va='center', fontsize=12, color='red')
        ax2.set_title("Error Creating Overall Breakdown", fontsize=14, color='red')
        return fig, pd.DataFrame()

# New clean consumption visualization functions following ToD pattern
def create_consumption_plot_db_clean(df: pd.DataFrame, plant_name: str, selected_date) -> plt.Figure:
    """
    Create a clean consumption plot using database data.
    This function follows the ToD consumption pattern for consistency.
    
    Args:
        df: DataFrame with columns: datetime, consumption
        plant_name: Name of the plant
        selected_date: Selected date for the plot
        
    Returns:
        matplotlib Figure object
    """
    try:
        fig, ax = plt.subplots(figsize=(14, 8))
        
        if df.empty:
            ax.text(0.5, 0.5, 'No consumption data available', 
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title(f"Consumption - {get_plant_display_name(plant_name)}", 
                        fontsize=16, fontweight='bold')
            return fig
        
        # Ensure datetime column is properly formatted
        df = df.copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Sort by datetime
        df = df.sort_values('datetime')
        
        # Create line plot
        ax.plot(df['datetime'], df['consumption'], 
               color='#00897B', linewidth=2.5, marker='o', markersize=4,
               label='Consumption', alpha=0.9)
        
        # Customize the plot
        plant_display_name = get_plant_display_name(plant_name)
        ax.set_title(f'Consumption - {plant_display_name}\n{selected_date.strftime("%B %d, %Y")}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Time', fontsize=12, fontweight='bold')
        ax.set_ylabel('Consumption (kWh)', fontsize=12, fontweight='bold')
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Format y-axis with K for thousands (consistent with ToD consumption)
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        ax.set_axisbelow(True)
        
        # Add statistics
        if not df.empty:
            total_consumption = df['consumption'].sum()
            avg_consumption = df['consumption'].mean()
            max_consumption = df['consumption'].max()
            
            stats_text = f"""Statistics:
Total: {total_consumption:,.0f} kWh
Average: {avg_consumption:.1f} kWh
Peak: {max_consumption:.1f} kWh"""
            
            ax.text(0.02, 0.98, stats_text,
                   transform=ax.transAxes,
                   bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.8),
                   fontsize=10,
                   verticalalignment='top',
                   fontfamily='monospace')
        
        # Add legend
        ax.legend(loc='upper right', framealpha=0.9)
        
        # Ensure tight layout
        plt.tight_layout()
        
        # Add watermark
        fig.text(0.99, 0.01, 'Consumption Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating consumption plot: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return create_error_figure(f"Error creating consumption plot: {str(e)}")

def create_daily_consumption_plot_db_clean(df: pd.DataFrame, plant_name: str, start_date, end_date) -> plt.Figure:
    """
    Create a clean daily consumption plot using database data.
    This function follows the ToD consumption pattern for consistency.
    
    Args:
        df: DataFrame with columns: datetime, consumption
        plant_name: Name of the plant
        start_date: Start date
        end_date: End date
        
    Returns:
        matplotlib Figure object
    """
    try:
        fig, ax = plt.subplots(figsize=(16, 8))
        
        if df.empty:
            ax.text(0.5, 0.5, 'No consumption data available', 
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title(f"Daily Consumption - {get_plant_display_name(plant_name)}", 
                        fontsize=16, fontweight='bold')
            return fig
        
        # Ensure datetime column is properly formatted
        df = df.copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Sort by datetime
        df = df.sort_values('datetime')
        
        # Create line plot
        ax.plot(df['datetime'], df['consumption'], 
               color='#00897B', linewidth=2, marker='o', markersize=3,
               label='Consumption', alpha=0.9)
        
        # Customize the plot
        plant_display_name = get_plant_display_name(plant_name)
        date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        ax.set_title(f'Daily Consumption - {plant_display_name}\n{date_range}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date & Time', fontsize=12, fontweight='bold')
        ax.set_ylabel('Consumption (kWh)', fontsize=12, fontweight='bold')
        
        # Format x-axis based on date range
        days_diff = (end_date - start_date).days
        if days_diff <= 7:
            # For week or less, show daily ticks
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        else:
            # For longer periods, show fewer ticks
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, days_diff//10)))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Format y-axis with K for thousands (consistent with ToD consumption)
        ax.yaxis.set_major_formatter(FuncFormatter(format_thousands))
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        ax.set_axisbelow(True)
        
        # Add statistics
        if not df.empty:
            total_consumption = df['consumption'].sum()
            avg_consumption = df['consumption'].mean()
            max_consumption = df['consumption'].max()
            
            stats_text = f"""Statistics:
Total: {total_consumption:,.0f} kWh
Daily Avg: {avg_consumption:.1f} kWh
Peak: {max_consumption:.1f} kWh"""
            
            ax.text(0.02, 0.98, stats_text,
                   transform=ax.transAxes,
                   bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.8),
                   fontsize=10,
                   verticalalignment='top',
                   fontfamily='monospace')
        
        # Add legend
        ax.legend(loc='upper right', framealpha=0.9)
        
        # Ensure tight layout
        plt.tight_layout()
        
        # Add watermark
        fig.text(0.99, 0.01, 'Daily Consumption Analysis',
                fontsize=8, color='gray', ha='right', va='bottom', alpha=0.5)
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating daily consumption plot: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return create_error_figure(f"Error creating daily consumption plot: {str(e)}")

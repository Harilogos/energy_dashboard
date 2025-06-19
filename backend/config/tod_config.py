"""
Time-of-Day (ToD) configuration module.

This module contains the ToD slot definitions and helper functions used throughout the application.
All ToD-related settings should be defined here to ensure consistency across the application.
"""


# Configure logging
import os
import sys
# Add the project root to the path to ensure imports work correctly
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.logs.logger_setup import setup_logger
logger = setup_logger('tod_config', 'tod_config.log')

# Define ToD slots with updated time ranges and colors as specified by user
TOD_SLOTS = [
    {
        "start_hour": 22,
        "end_hour": 6,
        "name": "Off-Peak",
        "description": "Off-Peak period (10pm to 6am)",
        "color": "#6A1B9A"  # Deep Purple / Violet
    },
    {
        "start_hour": 6,
        "end_hour": 9,
        "name": "Morning Peak",
        "description": "Morning peak demand period (6am to 9am)",
        "color": "#FB8C00"  # Bright Orange
    },
    {
        "start_hour": 9,
        "end_hour": 18,
        "name": "Day (Normal)",
        "description": "Day normal period (9am to 6pm)",
        "color": "#0288D1"  # Sky Blue
    },
    {
        "start_hour": 18,
        "end_hour": 22,
        "name": "Evening Peak",
        "description": "Evening peak demand period (6pm to 10pm)",
        "color": "#C62828"  # Crimson Red
    }
]

def get_tod_slot(hour):
    """
    Get the ToD slot for a given hour.

    Args:
        hour (int): Hour of the day (0-23)

    Returns:
        dict: ToD slot information
    """
    try:
        # Convert hour to integer if it's a string
        if isinstance(hour, str):
            if ':' in hour:
                hour = int(hour.split(':')[0])
            else:
                hour = int(hour)

        for slot in TOD_SLOTS:
            start_hour = slot["start_hour"]
            end_hour = slot["end_hour"]

            # Handle slots that wrap around midnight
            if end_hour < start_hour:
                if hour >= start_hour or hour < end_hour:
                    return slot
            else:
                if hour >= start_hour and hour < end_hour:
                    return slot

        # Default to the last slot if no match is found
        return TOD_SLOTS[-1]
    except Exception as e:
        logger.error(f"Error in get_tod_slot for hour {hour}: {e}")
        # Return the first slot as a fallback
        return TOD_SLOTS[0]

def is_peak_hour(hour):
    """
    Check if a given hour is in a peak period.

    Args:
        hour (int): Hour of the day (0-23)

    Returns:
        bool: True if the hour is in a peak period, False otherwise
    """
    slot = get_tod_slot(hour)
    return slot["name"].lower() == "peak"

def get_tod_slots_formatted():
    """
    Get a formatted string representation of ToD slots.

    Returns:
        str: Formatted string representation of ToD slots
    """
    result = []
    for slot in TOD_SLOTS:
        start_hour = slot["start_hour"]
        end_hour = slot["end_hour"]
        name = slot["name"]

        # Format hours in 12-hour format with AM/PM
        start_str = f"{start_hour if start_hour <= 12 else start_hour - 12}{'AM' if start_hour < 12 else 'PM'}"
        end_str = f"{end_hour if end_hour <= 12 else end_hour - 12}{'AM' if end_hour < 12 else 'PM'}"

        result.append(f"{start_str} - {end_str}: {name}")

    return ", ".join(result)

def get_tod_slots_html_table():
    """
    Get an HTML table representation of ToD slots.

    Returns:
        str: HTML table representation of ToD slots
    """
    # Create a simpler, more robust HTML table that's less likely to be escaped
    css = """
    <style>
    .tod-table {
        border-collapse: collapse;
        width: 100%;
        max-width: 500px;
        margin: 15px 0;
        font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    }
    .tod-table th {
        background-color: #f0f2f6;
        font-weight: bold;
        text-align: left;
        padding: 12px;
        border: 1px solid #e1e4e8;
    }
    .tod-table td {
        padding: 12px;
        border: 1px solid #e1e4e8;
        text-align: left;
    }
    .tod-table tr:nth-child(even) {
        background-color: #f8f9fa;
    }
    .tod-peak {
        color: #D83B01;
        font-weight: bold;
    }
    .tod-offpeak {
        color: #107C10;
        font-weight: bold;
    }
    </style>
    """

    # Start the table
    html = css + '<h4>Time-of-Day (ToD) Classification</h4><table class="tod-table"><tr><th>Time Period</th><th>Classification</th></tr>'

    # Add each row
    for slot in TOD_SLOTS:
        start_hour = slot["start_hour"]
        end_hour = slot["end_hour"]
        name = slot["name"]

        # Format hours in 12-hour format with AM/PM
        start_str = f"{start_hour if start_hour <= 12 else start_hour - 12} {'AM' if start_hour < 12 else 'PM'}"
        end_str = f"{end_hour if end_hour <= 12 else end_hour - 12} {'AM' if end_hour < 12 else 'PM'}"

        # Determine CSS class based on slot name
        css_class = "tod-peak" if name.lower() == "peak" else "tod-offpeak"

        # Add the row
        html += f'<tr><td>{start_str} - {end_str}</td><td class="{css_class}">{name}</td></tr>'

    # Close the table
    html += '</table>'

    return html

def get_tod_bin_labels(format_type="full"):
    """
    Get ToD bin labels in the specified format.

    Args:
        format_type (str): Format type for the labels. Options:
            - "full": Full format with AM/PM and spaces (e.g., "10 PM - 6 AM (Off-Peak)")
            - "compact": Compact format without spaces (e.g., "22-6PM (Off-Peak)")

    Returns:
        list: List of ToD bin labels in the specified format
    """
    bin_labels = []

    for slot in TOD_SLOTS:
        start_hour = slot["start_hour"]
        end_hour = slot["end_hour"]
        name = slot["name"]

        if format_type == "full":
            # Format hours in 12-hour format with AM/PM
            if start_hour == 0:
                start_str = "12 AM"
            elif start_hour < 12:
                start_str = f"{start_hour} AM"
            elif start_hour == 12:
                start_str = "12 PM"
            else:
                start_str = f"{start_hour - 12} PM"
                
            if end_hour == 0:
                end_str = "12 AM"
            elif end_hour < 12:
                end_str = f"{end_hour} AM"
            elif end_hour == 12:
                end_str = "12 PM"
            else:
                end_str = f"{end_hour - 12} PM"
                
            bin_labels.append(f"{start_str} - {end_str} ({name})")
        elif format_type == "compact":
            # Compact format for visualization
            start_str = f"{start_hour}"
            end_str = f"{end_hour}"
            bin_labels.append(f"{start_str}-{end_str}{'AM' if start_hour < 12 else 'PM'} ({name})")

    return bin_labels

def get_tod_slots():
    """
    Get ToD slots as a dictionary for easy lookup.
    
    Returns:
        dict: Dictionary with slot names as keys and slot info as values
    """
    slots_dict = {}
    for i, slot in enumerate(TOD_SLOTS):
        # Create unique keys for slots with same name
        key = slot["name"]
        if key in slots_dict:
            key = f"{slot['name']}_{i}"
        
        slots_dict[key] = {
            'start_hour': slot["start_hour"],
            'end_hour': slot["end_hour"],
            'name': slot["name"],
            'description': slot["description"],
            'color': slot.get("color", "#808080")  # Default gray if no color specified
        }
    
    return slots_dict

def get_tod_slot_color(slot_name):
    """
    Get the color for a specific ToD slot.
    
    Args:
        slot_name (str): Name of the ToD slot
        
    Returns:
        str: Hex color code for the slot
    """
    for slot in TOD_SLOTS:
        if slot["name"] == slot_name:
            return slot.get("color", "#808080")  # Default gray if no color specified
    return "#808080"  # Default gray for unknown slots

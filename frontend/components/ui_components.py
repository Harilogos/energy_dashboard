"""
UI components for the Energy Generation Dashboard.
"""
import streamlit as st


from backend.logs.logger_setup import setup_logger

logger = setup_logger('ui_components', 'ui_components.log')

def load_client_data():
    """Load client data from database"""
    try:
        from backend.data.db_data_manager import get_plants
        plants_data = get_plants()
        
        # Convert to the expected format for compatibility
        client_data = {"solar": {}, "wind": {}}
        
        for client_name, client_plants in plants_data.items():
            if client_plants.get('solar'):
                client_data['solar'][client_name] = client_plants['solar']
            if client_plants.get('wind'):
                client_data['wind'][client_name] = client_plants['wind']
        
        return client_data
    except Exception as e:
        logger.error(f"Error loading client data from database: {e}")
        return {"solar": {}, "wind": {}}

def create_client_plant_filters():
    """Create and return client and plant selection filters"""
    # Load client data from JSON file
    with st.spinner("Loading client data..."):
        client_data = load_client_data()

    # Create sidebar filters with better styling
    st.sidebar.markdown('<div class="sidebar-header">Dashboard Controls</div>', unsafe_allow_html=True)
    st.sidebar.markdown("---")

    # Add a client selection header with better styling
    st.sidebar.markdown('<div style="font-weight: bold; margin-bottom: 10px; color: #424242;">Client Selection</div>', unsafe_allow_html=True)

    # Client selection (first level) with improved UI
    all_clients = sorted(list(set(list(client_data['solar'].keys()) + list(client_data['wind'].keys()))))
    selected_client = st.sidebar.selectbox(
        "Select Client",
        options=all_clients,
        help="Choose a client to view their energy data"
    )

    # Get solar and wind plants for the selected client
    solar_plants_data = []  # Full plant objects (dicts)
    wind_plants_data = []   # Full plant objects (dicts)
    solar_plants = []       # Plant names for display
    wind_plants = []        # Plant names for display
    all_plants = []

    if selected_client in client_data['solar']:
        solar_plants_data = client_data['solar'][selected_client]
        solar_plants = [plant.get('name', 'Unknown Plant') for plant in solar_plants_data]
        all_plants.extend([(plant, "Solar") for plant in solar_plants_data])

    if selected_client in client_data['wind']:
        wind_plants_data = client_data['wind'][selected_client]
        wind_plants = [plant.get('name', 'Unknown Plant') for plant in wind_plants_data]
        all_plants.extend([(plant, "Wind") for plant in wind_plants_data])

    # Check if client has only one plant total
    has_only_one_plant = (len(solar_plants) == 1 and not wind_plants) or (len(wind_plants) == 1 and not solar_plants)

    # Define callback functions for plant selection
    def on_solar_plant_select():
        # If a solar plant is selected, reset wind plant selection to default
        if st.session_state.selected_solar_plant != "Select Solar Plant" and st.session_state.selected_solar_plant != "No solar plants":
            st.session_state.selected_wind_plant = "Select Wind Plant"

    def on_wind_plant_select():
        # If a wind plant is selected, reset solar plant selection to default
        if st.session_state.selected_wind_plant != "Select Wind Plant" and st.session_state.selected_wind_plant != "No wind plants":
            st.session_state.selected_solar_plant = "Select Solar Plant"

    # Initialize session state for plant selections if they don't exist
    if 'selected_solar_plant' not in st.session_state:
        # If there's only one solar plant and no wind plants, select it by default
        if len(solar_plants) == 1 and not wind_plants:
            st.session_state.selected_solar_plant = solar_plants[0]  # Now this is a plant name string
        else:
            st.session_state.selected_solar_plant = "Select Solar Plant"

    if 'selected_wind_plant' not in st.session_state:
        # If there's only one wind plant and no solar plants, select it by default
        if len(wind_plants) == 1 and not solar_plants:
            st.session_state.selected_wind_plant = wind_plants[0]  # Now this is a plant name string
        else:
            st.session_state.selected_wind_plant = "Select Wind Plant"

    # Only show plant selection UI if client has more than one plant
    if not has_only_one_plant:
        # Add a plant selection header with better styling
        st.sidebar.markdown("---")
        st.sidebar.markdown('<div style="font-weight: bold; margin-bottom: 10px; color: #424242;">Plant Selection</div>', unsafe_allow_html=True)

        # Create two columns for plant selection
        col1, col2 = st.sidebar.columns(2)

        # Add a header to explain the plant selection
        if solar_plants and wind_plants:
            st.sidebar.markdown("""
            <div style="background-color: #e8f0fe; padding: 10px; border-radius: 5px; margin-bottom: 15px; border-left: 4px solid #1E88E5;">
                <span style="font-weight: bold;">Tip:</span> Select a specific plant or leave both dropdowns at default to view combined data
            </div>
            """, unsafe_allow_html=True)

        # Solar plant selection
        with col1:
            if solar_plants:
                # Add a "Select Solar Plant" option as the default
                solar_options = ["Select Solar Plant"] + solar_plants
                selected_solar_plant = st.selectbox(
                    "Solar Plants",
                    options=solar_options,
                    index=solar_options.index(st.session_state.selected_solar_plant) if st.session_state.selected_solar_plant in solar_options else 0,
                    key="selected_solar_plant",
                    on_change=on_solar_plant_select
                )
            else:
                selected_solar_plant = st.selectbox(
                    "Solar Plants",
                    options=["No solar plants"],
                    disabled=True,
                    key="selected_solar_plant_disabled"
                )

        # Wind plant selection
        with col2:
            if wind_plants:
                # Add a "Select Wind Plant" option as the default
                wind_options = ["Select Wind Plant"] + wind_plants
                selected_wind_plant = st.selectbox(
                    "Wind Plants",
                    options=wind_options,
                    index=wind_options.index(st.session_state.selected_wind_plant) if st.session_state.selected_wind_plant in wind_options else 0,
                    key="selected_wind_plant",
                    on_change=on_wind_plant_select
                )
            else:
                selected_wind_plant = st.selectbox(
                    "Wind Plants",
                    options=["No wind plants"],
                    disabled=True,
                    key="selected_wind_plant_disabled"
                )
    else:
        # For clients with only one plant, set the selected plant directly
        if len(solar_plants) == 1:
            selected_solar_plant = solar_plants[0]  # This is now a plant name string
            selected_wind_plant = "No wind plants"
        else:  # len(wind_plants) == 1
            selected_wind_plant = wind_plants[0]  # This is now a plant name string
            selected_solar_plant = "No solar plants"

    # Helper function to find plant object by name
    def find_plant_object_by_name(plant_name):
        """Find the full plant object (dict) by plant name"""
        # Search in solar plants
        for plant_obj in solar_plants_data:
            if plant_obj.get('name') == plant_name:
                return plant_obj
        # Search in wind plants
        for plant_obj in wind_plants_data:
            if plant_obj.get('name') == plant_name:
                return plant_obj
        # If not found, return the name as-is (for backward compatibility)
        return plant_name

    # Determine which plant to use based on selection
    selected_plant = None
    selected_plant_type = None

    # If both solar and wind plants are available
    if solar_plants and wind_plants:
        # If both are set to default options, show combined view
        if selected_solar_plant == "Select Solar Plant" and selected_wind_plant == "Select Wind Plant":
            selected_plant = "Combined View"
            selected_plant_type = "Combined"
        # If solar is selected but wind is at default
        elif selected_solar_plant != "Select Solar Plant" and selected_wind_plant == "Select Wind Plant":
            selected_plant = find_plant_object_by_name(selected_solar_plant)
            selected_plant_type = "Solar"
        # If wind is selected but solar is at default
        elif selected_wind_plant != "Select Wind Plant" and selected_solar_plant == "Select Solar Plant":
            selected_plant = find_plant_object_by_name(selected_wind_plant)
            selected_plant_type = "Wind"
        # This case should no longer occur due to our callbacks, but handle it just in case
        elif selected_solar_plant != "Select Solar Plant" and selected_wind_plant != "Select Wind Plant":
            # Default to the most recently selected plant (which should be solar in this case)
            selected_plant = find_plant_object_by_name(selected_solar_plant)
            selected_plant_type = "Solar"
            # Reset the wind plant selection silently
            st.session_state.selected_wind_plant = "Select Wind Plant"
    # If only solar plants are available
    elif solar_plants:
        # If there's only one solar plant, always select it
        if len(solar_plants) == 1:
            selected_plant = find_plant_object_by_name(solar_plants[0])
            selected_plant_type = "Solar"
        # Otherwise, use the selected plant from the dropdown
        elif selected_solar_plant != "Select Solar Plant" and selected_solar_plant != "No solar plants":
            selected_plant = find_plant_object_by_name(selected_solar_plant)
            selected_plant_type = "Solar"
        else:
            selected_plant = "No plant selected"
            selected_plant_type = None
    # If only wind plants are available
    elif wind_plants:
        # If there's only one wind plant, always select it
        if len(wind_plants) == 1:
            selected_plant = find_plant_object_by_name(wind_plants[0])
            selected_plant_type = "Wind"
        # Otherwise, use the selected plant from the dropdown
        elif selected_wind_plant != "Select Wind Plant" and selected_wind_plant != "No wind plants":
            selected_plant = find_plant_object_by_name(selected_wind_plant)
            selected_plant_type = "Wind"
        else:
            selected_plant = "No plant selected"
            selected_plant_type = None
    # If no plants are available
    else:
        selected_plant = "No plants available"
        selected_plant_type = None

    # Show the selected plant and type with better styling
    st.sidebar.markdown("---")
    st.sidebar.markdown('<div style="font-weight: bold; margin-bottom: 10px; color: #424242;">Current Selection</div>', unsafe_allow_html=True)

    # Import the helper function to get plant display name
    from backend.data.db_data_manager import get_plant_display_name

    # Get the display name for the plant
    plant_display_name = get_plant_display_name(selected_plant) if isinstance(selected_plant, dict) else selected_plant

    # Create a styled info box for the selected plant
    plant_color = "#1E88E5" if selected_plant != "No plant selected" and selected_plant != "No plants available" else "#757575"
    st.sidebar.markdown(f"""
    <div style="background-color: #e8f0fe; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid {plant_color};">
        <span style="font-weight: bold;">Selected Plant:</span> {plant_display_name}
    </div>
    """, unsafe_allow_html=True)

    # Show plant type if available
    if selected_plant_type:
        type_color = {
            "Solar": "#FFA000",  # Amber for solar
            "Wind": "#1E88E5",   # Blue for wind
            "Combined": "#4CAF50"  # Green for combined
        }.get(selected_plant_type, "#757575")

        st.sidebar.markdown(f"""
        <div style="background-color: #e8f0fe; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid {type_color};">
            <span style="font-weight: bold;">Plant Type:</span> {selected_plant_type}
        </div>
        """, unsafe_allow_html=True)

    # Add a reset button to go back to combined view if a specific plant is selected
    if solar_plants and wind_plants and selected_plant != "Combined View" and selected_plant_type != "Combined":
        # Define a callback function for the reset button
        def reset_to_combined_view():
            st.session_state.selected_solar_plant = "Select Solar Plant"
            st.session_state.selected_wind_plant = "Select Wind Plant"

        # Add the reset button with the callback and better styling
        st.sidebar.markdown("""
        <style>
        div.stButton > button {
            background-color: #1E88E5;
            color: white;
            font-weight: bold;
            border: none;
            border-radius: 4px;
            padding: 0.5rem 1rem;
            width: 100%;
        }
        div.stButton > button:hover {
            background-color: #1565C0;
        }
        </style>
        """, unsafe_allow_html=True)
        st.sidebar.button("Reset to Combined View", on_click=reset_to_combined_view, help="Click to view combined data for all plants")

    return {
        "selected_client": selected_client,
        "selected_plant": selected_plant,
        "selected_plant_type": selected_plant_type,
        "has_solar": bool(solar_plants),
        "has_wind": bool(wind_plants),
        "all_plants": all_plants
    }

def create_date_filters():
    """Create and return date range filters"""
    from datetime import datetime, timedelta

    # Add a date selection header with better styling
    st.sidebar.markdown("---")
    st.sidebar.markdown('<div style="font-weight: bold; margin-bottom: 10px; color: #424242;">Date Selection</div>', unsafe_allow_html=True)

    # Set default dates to today, but define yesterday for button functionality
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Calculate min and max values (1 year before and after today)
    min_date = today - timedelta(days=365)
    max_date = today + timedelta(days=365)



    # Initialize session state for date range if it doesn't exist
    if 'date_range' not in st.session_state:
        st.session_state.date_range = (today, today)

    # Use a single date_input with 'start' and 'end' values
    date_range = st.sidebar.date_input(
        "Select Custom Date Range",
        value=st.session_state.date_range,
        min_value=min_date,
        max_value=max_date,
        help="Select a custom date range for your data"
    )

    # Extract start and end dates from the tuple
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        # Fallback if only one date is selected
        start_date = end_date = date_range[0]

    return start_date, end_date

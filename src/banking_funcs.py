import pandas as pd

def process_daily_banking_without_tod(df, banking_fee=0.1):
    """
    Processes the given DataFrame for daily banking logic without TOD and returns the updated DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.
        banking_fee (float): Banking fee as a decimal (default is 0.1 for 10%).

    Returns:
        pd.DataFrame: Updated DataFrame with calculated columns and daily banking logic applied.
    """
    # Make a copy of the DataFrame to avoid modifying the original
    df = df.copy()

    # Convert numeric columns to float to avoid dtype warnings
    numeric_cols = ["Lapsed", "Leftover Demand (kWh)"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Ensure the Date column is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    # Sort the DataFrame by Date and Time
    df = df.sort_values(by=["Date", "Time"]).reset_index(drop=True)

    # Create new columns with float data type
    df["Daily Leftover Demand Sum"] = 0.0
    df["Daily Lapsed Sum"] = 0.0
    df["Daily Difference"] = 0.0
    df["Surplus Generation"] = 0.0
    df["Grid Consumption"] = 0.0

    # Group by Date and calculate the sums
    daily_leftover_sum = df.groupby("Date")["Leftover Demand (kWh)"].sum()
    daily_lapsed_sum = df.groupby("Date")["Lapsed"].sum()

    # Create a new DataFrame for the daily summary to avoid modifying the original
    dates = daily_leftover_sum.index.unique()
    daily_summary_data = []

    for date in dates:
        leftover_sum = daily_leftover_sum[date]
        lapsed_sum = daily_lapsed_sum[date]

        # Calculate the difference
        difference = lapsed_sum - ((1 + banking_fee) * leftover_sum)

        # Determine surplus generation and grid consumption
        surplus_gen = difference if difference > 0 else 0.0
        grid_cons = abs(difference) if difference < 0 else 0.0

        # Add to the summary data
        daily_summary_data.append({
            "Date": date,
            "Daily Leftover Demand Sum": leftover_sum,
            "Daily Lapsed Sum": lapsed_sum,
            "Surplus Generation": surplus_gen,
            "Grid Consumption": grid_cons
        })

    # Create the summary DataFrame
    daily_summary_df = pd.DataFrame(daily_summary_data)

    # Sort the summary DataFrame by Date
    daily_summary_df = daily_summary_df.sort_values(by="Date").reset_index(drop=True)

    # Convert the Date column back to a string in the original format
    daily_summary_df["Date"] = daily_summary_df["Date"].dt.strftime("%d/%m/%Y")

    # Return the summary DataFrame
    return daily_summary_df

def process_monthly_banking_without_tod(df, banking_fee=0.1):
    """
    Processes the given DataFrame for monthly banking logic without TOD and returns the summarized DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.
        banking_fee (float): Banking fee as a decimal (default is 0.1 for 10%).

    Returns:
        pd.DataFrame: Summarized DataFrame with monthly calculations.
    """
    # Make a copy of the DataFrame to avoid modifying the original
    df = df.copy()

    # Convert numeric columns to float to avoid dtype warnings
    numeric_cols = ["Lapsed", "Leftover Demand (kWh)"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Ensure the Date column is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    # Group by month and calculate the sums
    monthly_leftover_sum = df.groupby(df["Date"].dt.to_period("M"))["Leftover Demand (kWh)"].sum()
    monthly_lapsed_sum = df.groupby(df["Date"].dt.to_period("M"))["Lapsed"].sum()

    # Create a new DataFrame for the monthly summary
    monthly_summary_data = []

    for month in monthly_leftover_sum.index:
        leftover_sum = monthly_leftover_sum[month]
        lapsed_sum = monthly_lapsed_sum[month]

        # Calculate the difference
        difference = lapsed_sum - ((1 + banking_fee) * leftover_sum)

        # Determine surplus generation and grid consumption
        surplus_gen = difference if difference > 0 else 0.0
        grid_cons = abs(difference) if difference < 0 else 0.0

        # Add to the summary data
        monthly_summary_data.append({
            "Month": month,
            "Monthly Leftover Demand Sum": leftover_sum,
            "Monthly Lapsed Sum": lapsed_sum,
            "Surplus Generation": surplus_gen,
            "Grid Consumption": grid_cons
        })

    # Create the summary DataFrame
    monthly_summary_df = pd.DataFrame(monthly_summary_data)

    # Convert the Month column to string format for output
    monthly_summary_df["Month"] = monthly_summary_df["Month"].dt.strftime("%Y-%m")

    # Return the summarized DataFrame
    return monthly_summary_df

def process_yearly_banking_without_tod(df, banking_fee=0.1):
    """
    Processes the given DataFrame for yearly banking logic without TOD and returns the summarized DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.
        banking_fee (float): Banking fee as a decimal (default is 0.1 for 10%).

    Returns:
        pd.DataFrame: Summarized DataFrame with yearly calculations.
    """
    # Make a copy of the DataFrame to avoid modifying the original
    df = df.copy()

    # Convert numeric columns to float to avoid dtype warnings
    numeric_cols = ["Lapsed", "Leftover Demand (kWh)"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Ensure the Date column is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    # Group by year and calculate the sums
    yearly_leftover_sum = df.groupby(df["Date"].dt.to_period("Y"))["Leftover Demand (kWh)"].sum()
    yearly_lapsed_sum = df.groupby(df["Date"].dt.to_period("Y"))["Lapsed"].sum()

    # Create a new DataFrame for the yearly summary
    yearly_summary_data = []

    for year in yearly_leftover_sum.index:
        leftover_sum = yearly_leftover_sum[year]
        lapsed_sum = yearly_lapsed_sum[year]

        # Calculate the difference
        difference = lapsed_sum - ((1 + banking_fee) * leftover_sum)

        # Determine yearly surplus and deficit
        yearly_surplus = difference if difference > 0 else 0.0
        yearly_deficit = abs(difference) if difference < 0 else 0.0

        # Add to the summary data
        yearly_summary_data.append({
            "Year": year,
            "Yearly Leftover Demand Sum": leftover_sum,
            "Yearly Lapsed Sum": lapsed_sum,
            "Yearly Surplus": yearly_surplus,
            "Yearly Deficit": yearly_deficit
        })

    # Create the summary DataFrame
    yearly_summary_df = pd.DataFrame(yearly_summary_data)

    # Convert the Year column to string format for output
    yearly_summary_df["Year"] = yearly_summary_df["Year"].dt.strftime("%Y")

    # Return the summarized DataFrame
    return yearly_summary_df
def process_tod_banking_daily(df):
    """
    Processes the given DataFrame for TOD-based banking logic and returns the updated DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated columns and settlement logic applied.
    """
    # Ensure Date is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    df = df.sort_values(by=["Date", "Time"]).reset_index(drop=True)

    # Map slots to hours
    slot_mapping = {
        0: "offpeak", 1: "offpeak", 2: "offpeak", 3: "offpeak", 4: "offpeak", 5: "offpeak",
        6: "peak", 7: "peak", 8: "peak", 9: "peak",
        10: "offpeak", 11: "offpeak", 12: "offpeak", 13: "offpeak", 14: "offpeak", 15: "offpeak",
        16: "offpeak", 17: "offpeak",
        18: "peak", 19: "peak", 20: "peak", 21: "peak",
        22: "offpeak", 23: "offpeak",
    }

    # Assign origin_slot_name based on Time
    df["origin_slot_name"] = df["Time"].apply(lambda x: slot_mapping[int(x.split(":")[0])])

    # Group by Date and origin_slot_name, and calculate the sum of Lapsed and Leftover Demand (kWh)
    grouped_df = df.groupby(["Date", "origin_slot_name"]).agg({
        "Lapsed": "sum",
        "Leftover Demand (kWh)": "sum"
    }).reset_index()

    # Rename columns for clarity
    grouped_df.rename(columns={
        "Lapsed": "Sum of Lapsed",
        "Leftover Demand (kWh)": "Sum of Leftover Demand (kWh)"
    }, inplace=True)

    # Calculate Difference, Grid Consumption, and Surplus Generation
    grouped_df["Difference"] = grouped_df["Sum of Leftover Demand (kWh)"] - grouped_df["Sum of Lapsed"]
    grouped_df["Grid Consumption"] = grouped_df["Difference"].apply(lambda x: x if x > 0 else 0)
    grouped_df["Surplus Generation"] = grouped_df["Difference"].apply(lambda x: -x if x < 0 else 0)

    # Sort by Date and slot order (peak first, then offpeak)
    slot_order = {"peak": 0, "offpeak": 1}
    grouped_df["slot_order"] = grouped_df["origin_slot_name"].map(slot_order)
    grouped_df = grouped_df.sort_values(by=["Date", "slot_order"]).reset_index(drop=True)
    grouped_df.drop(columns=["slot_order"], inplace=True)

    # Add settlement columns and description
    grouped_df["Grid Consumption(After Settlement)"] = grouped_df["Grid Consumption"]
    grouped_df["Surplus Generation(After Settlement)"] = grouped_df["Surplus Generation"]
    grouped_df["Description"] = "No peak to Offpeak Settlement"

    # Apply settlement logic for each date
    for date in grouped_df["Date"].unique():
        # Filter rows for the current date
        date_rows = grouped_df[grouped_df["Date"] == date]

        # Check if there is a positive "Difference" in "offpeak" and a negative "Difference" in "peak"
        peak_row = date_rows[date_rows["origin_slot_name"] == "peak"]
        offpeak_row = date_rows[date_rows["origin_slot_name"] == "offpeak"]

        if not peak_row.empty and not offpeak_row.empty:
            peak_difference = peak_row["Difference"].values[0]
            offpeak_difference = offpeak_row["Difference"].values[0]

            if peak_difference < 0 and offpeak_difference > 0:
                # Calculate the maximum transferable energy
                surplus_generation = peak_row["Surplus Generation"].values[0]
                grid_consumption = offpeak_row["Grid Consumption"].values[0]

                if surplus_generation > grid_consumption:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation
                    grid_consumption_after_settlement = 0
                    surplus_generation_after_settlement = surplus_generation - max_required_settlement

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                else:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation / 1.08
                    grid_consumption_after_settlement = grid_consumption - max_available_settlement
                    surplus_generation_after_settlement = 0

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"
    # Sort by Date and slot order (peak first, then offpeak)
    slot_order = {"peak": 0, "offpeak": 1}
    grouped_df["slot_order"] = grouped_df["origin_slot_name"].map(slot_order)
    grouped_df = grouped_df.sort_values(by=["Date", "slot_order"]).reset_index(drop=True)
    grouped_df.drop(columns=["slot_order"], inplace=True)
    grouped_df["Date"] = grouped_df["Date"].dt.strftime("%d/%m/%Y")
    # Return the updated DataFrame
    return grouped_df
def process_tod_banking_monthly(df):
    """
    Processes the given DataFrame for TOD-based banking logic on a monthly basis and returns the updated DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated columns and settlement logic applied on a monthly basis.
    """
    # Ensure Date is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    # Extract the month and year for grouping
    df["Month"] = df["Date"].dt.to_period("M")

    # Map slots to hours
    slot_mapping = {
        0: "offpeak", 1: "offpeak", 2: "offpeak", 3: "offpeak", 4: "offpeak", 5: "offpeak",
        6: "peak", 7: "peak", 8: "peak", 9: "peak",
        10: "offpeak", 11: "offpeak", 12: "offpeak", 13: "offpeak", 14: "offpeak", 15: "offpeak",
        16: "offpeak", 17: "offpeak",
        18: "peak", 19: "peak", 20: "peak", 21: "peak",
        22: "offpeak", 23: "offpeak",
    }

    # Assign origin_slot_name based on Time
    df["origin_slot_name"] = df["Time"].apply(lambda x: slot_mapping[int(x.split(":")[0])])

    # Group by Month and origin_slot_name, and calculate the sum of Lapsed and Leftover Demand (kWh)
    grouped_df = df.groupby(["Month", "origin_slot_name"]).agg({
        "Lapsed": "sum",
        "Leftover Demand (kWh)": "sum"
    }).reset_index()

    # Rename columns for clarity
    grouped_df.rename(columns={
        "Lapsed": "Sum of Lapsed",
        "Leftover Demand (kWh)": "Sum of Leftover Demand (kWh)"
    }, inplace=True)

    # Calculate Difference, Grid Consumption, and Surplus Generation
    grouped_df["Difference"] = grouped_df["Sum of Leftover Demand (kWh)"] - grouped_df["Sum of Lapsed"]
    grouped_df["Grid Consumption"] = grouped_df["Difference"].apply(lambda x: x if x > 0 else 0)
    grouped_df["Surplus Generation"] = grouped_df["Difference"].apply(lambda x: -x if x < 0 else 0)

    # Sort by Month and slot order (peak first, then offpeak)
    slot_order = {"peak": 0, "offpeak": 1}
    grouped_df["slot_order"] = grouped_df["origin_slot_name"].map(slot_order)
    grouped_df = grouped_df.sort_values(by=["Month", "slot_order"]).reset_index(drop=True)
    grouped_df.drop(columns=["slot_order"], inplace=True)

    # Add settlement columns and description
    grouped_df["Grid Consumption(After Settlement)"] = grouped_df["Grid Consumption"]
    grouped_df["Surplus Generation(After Settlement)"] = grouped_df["Surplus Generation"]
    grouped_df["Description"] = "No peak to Offpeak Settlement"

    # Apply settlement logic for each month
    for month in grouped_df["Month"].unique():
        # Filter rows for the current month
        month_rows = grouped_df[grouped_df["Month"] == month]

        # Check if there is a positive "Difference" in "offpeak" and a negative "Difference" in "peak"
        peak_row = month_rows[month_rows["origin_slot_name"] == "peak"]
        offpeak_row = month_rows[month_rows["origin_slot_name"] == "offpeak"]

        if not peak_row.empty and not offpeak_row.empty:
            peak_difference = peak_row["Difference"].values[0]
            offpeak_difference = offpeak_row["Difference"].values[0]

            if peak_difference < 0 and offpeak_difference > 0:
                # Calculate the maximum transferable energy
                surplus_generation = peak_row["Surplus Generation"].values[0]
                grid_consumption = offpeak_row["Grid Consumption"].values[0]

                if surplus_generation > grid_consumption:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation
                    grid_consumption_after_settlement = 0
                    surplus_generation_after_settlement = surplus_generation - max_required_settlement

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                else:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation / 1.08
                    grid_consumption_after_settlement = grid_consumption - max_available_settlement
                    surplus_generation_after_settlement = 0

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"

    # Format Month back to string for output
    grouped_df["Month"] = grouped_df["Month"].dt.strftime("%Y-%m")

    # Return the updated DataFrame
    return grouped_df

def process_tod_banking_yearly(df):
    """
    Processes the given DataFrame for TOD-based banking logic on a yearly basis and returns the updated DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with columns "Date", "Time", "Lapsed", "Leftover Demand (kWh)", etc.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated columns and settlement logic applied on a yearly basis.
    """
    # Ensure Date is in datetime format
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    # Extract the year for grouping
    df["Year"] = df["Date"].dt.year

    # Map slots to hours
    slot_mapping = {
        0: "offpeak", 1: "offpeak", 2: "offpeak", 3: "offpeak", 4: "offpeak", 5: "offpeak",
        6: "peak", 7: "peak", 8: "peak", 9: "peak",
        10: "offpeak", 11: "offpeak", 12: "offpeak", 13: "offpeak", 14: "offpeak", 15: "offpeak",
        16: "offpeak", 17: "offpeak",
        18: "peak", 19: "peak", 20: "peak", 21: "peak",
        22: "offpeak", 23: "offpeak",
    }

    # Assign origin_slot_name based on Time
    df["origin_slot_name"] = df["Time"].apply(lambda x: slot_mapping[int(x.split(":")[0])])

    # Group by Year and origin_slot_name, and calculate the sum of Lapsed and Leftover Demand (kWh)
    grouped_df = df.groupby(["Year", "origin_slot_name"]).agg({
        "Lapsed": "sum",
        "Leftover Demand (kWh)": "sum"
    }).reset_index()

    # Rename columns for clarity
    grouped_df.rename(columns={
        "Lapsed": "Sum of Lapsed",
        "Leftover Demand (kWh)": "Sum of Leftover Demand (kWh)"
    }, inplace=True)

    # Calculate Difference, Grid Consumption, and Surplus Generation
    grouped_df["Difference"] = grouped_df["Sum of Leftover Demand (kWh)"] - grouped_df["Sum of Lapsed"]
    grouped_df["Grid Consumption"] = grouped_df["Difference"].apply(lambda x: x if x > 0 else 0)
    grouped_df["Surplus Generation"] = grouped_df["Difference"].apply(lambda x: -x if x < 0 else 0)

    # Sort by Year and slot order (peak first, then offpeak)
    slot_order = {"peak": 0, "offpeak": 1}
    grouped_df["slot_order"] = grouped_df["origin_slot_name"].map(slot_order)
    grouped_df = grouped_df.sort_values(by=["Year", "slot_order"]).reset_index(drop=True)
    grouped_df.drop(columns=["slot_order"], inplace=True)

    # Add settlement columns and description
    grouped_df["Grid Consumption(After Settlement)"] = grouped_df["Grid Consumption"]
    grouped_df["Surplus Generation(After Settlement)"] = grouped_df["Surplus Generation"]
    grouped_df["Description"] = "No peak to Offpeak Settlement"

    # Apply settlement logic for each year
    for year in grouped_df["Year"].unique():
        # Filter rows for the current year
        year_rows = grouped_df[grouped_df["Year"] == year]

        # Check if there is a positive "Difference" in "offpeak" and a negative "Difference" in "peak"
        peak_row = year_rows[year_rows["origin_slot_name"] == "peak"]
        offpeak_row = year_rows[year_rows["origin_slot_name"] == "offpeak"]

        if not peak_row.empty and not offpeak_row.empty:
            peak_difference = peak_row["Difference"].values[0]
            offpeak_difference = offpeak_row["Difference"].values[0]

            if peak_difference < 0 and offpeak_difference > 0:
                # Calculate the maximum transferable energy
                surplus_generation = peak_row["Surplus Generation"].values[0]
                grid_consumption = offpeak_row["Grid Consumption"].values[0]

                if surplus_generation > grid_consumption:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation
                    grid_consumption_after_settlement = 0
                    surplus_generation_after_settlement = surplus_generation - max_required_settlement

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_required_settlement}"
                else:
                    max_required_settlement = grid_consumption * 1.08
                    max_available_settlement = surplus_generation / 1.08
                    grid_consumption_after_settlement = grid_consumption - max_available_settlement
                    surplus_generation_after_settlement = 0

                    grouped_df.loc[offpeak_row.index, "Grid Consumption(After Settlement)"] = grid_consumption_after_settlement
                    grouped_df.loc[peak_row.index, "Surplus Generation(After Settlement)"] = surplus_generation_after_settlement
                    grouped_df.loc[offpeak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"
                    grouped_df.loc[peak_row.index, "Description"] = f"Peak to Offpeak Settlement: {max_available_settlement}"

    # Return the updated DataFrame
    return grouped_df


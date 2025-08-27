import pandas as pd


def sync_weather_gas(
    weather_df: pd.DataFrame,
    gas_df: pd.DataFrame,
    time_weather: str,
    time_gas: str,
    weather_prop: str,
    municipality: str,
    accum_method="mean",
):
    # Dropping "id" as it will be a hurdle when aggregating
    weather_df = weather_df.drop(labels="id", axis=1)
    gas_df = gas_df.drop(labels="_id", axis=1)

    # Filter weather_df w.r.t. municipality
    weather_df = weather_df.query(f"municipality == '{municipality}'")

    # Sort the DataFrames in temporal order
    weather_df = weather_df.sort_values(time_weather, ascending=True)
    gas_df = gas_df.sort_values(time_gas, ascending=True)

    # Rename the time_gas column to time_weather
    gas_df = gas_df.rename(columns={time_gas: time_weather})

    # Set the index to their time column
    weather_df = weather_df.set_index(time_weather)
    gas_df = gas_df.set_index(time_weather)

    # Only have mean implemented, but it could be further extended
    if accum_method == "mean":
        weather_df = weather_df.groupby("municipality").resample("D").mean()

    # Merging the two DataFrames on their respective time column
    combined_df = pd.merge(weather_df, gas_df, on=time_weather)

    return combined_df[[weather_prop, "purchasepricedkk_kwh"]]

# %%
import sys
import os

sys.path.append(os.path.abspath(".."))

import pandas as pd
from helper.utils import mongodb_setup, spark_setup

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# PySpark<4.0.0
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from helper.utils import retrieve_from_cas

import geopy

mdb_client = mongodb_setup()
spark = spark_setup()
cas_keyspace = "rewrite320"

# %% [markdown]
# ## Imputation


# %%
def nan_overview(df):
    df_raw = df.isna().sum()[df.isna().sum() > 0]
    df_pct = df.isna().sum()[df.isna().sum() > 0] / df.shape[0]
    final_df = pd.concat([df_raw, df_pct], axis=1)
    final_df.columns = ["num", "pct"]
    final_df = final_df.sort_values(by="num", ascending=False)
    # return df.isna().sum()[ df.isna().sum() > 0 ] / df.shape[0]
    return final_df


# %% [markdown]
# ### Gas Daily Balancing Price
# [https://energidataservice.dk/tso-electricity/GasDailyBalancingPrice](https://energidataservice.dk/tso-electricity/GasDailyBalancingPrice)

# %%
df_gas = pd.DataFrame(list(mdb_client[cas_keyspace]["gas"].find()))
df_gas = df_gas.sort_values("gasday")
df_gas.head()

# %%
nan_overview(df_gas)

# %% [markdown]
# `exhighestpricepurchasedkk_kwh`/`eexlowestpricesaledkk_kwh` - Energinet's highest purchase/lowest sale price on EEX DKK/MWh.
#

# %%
df_gas.eexlowestpricesaledkk_kwh = df_gas.eexlowestpricesaledkk_kwh.fillna(0)
df_gas.eexhighestpricepurchasedkk_kwh = (
    df_gas.eexhighestpricepurchasedkk_kwh.fillna(0)
)

# %% [markdown]
# Fill in the NaNs with 0 to indicate no highest/lowest purchase/sale price from Energinet.

# %% [markdown]
# ### Production per Municipality per Hour
# [https://www.energidataservice.dk/tso-electricity/ProductionMunicipalityHour](https://www.energidataservice.dk/tso-electricity/ProductionMunicipalityHour)

# %%
df_prod = retrieve_from_cas(spark, "production", cas_keyspace)
df_prod = df_prod.sort_values("hourutc")
df_prod.head()

# %%
nan_overview(df_prod)

# %% [markdown]
# We can assume that the large portion of missing values in offshore wind production is due to the dataset. It tracks all the municipalities in Denmark, but not every municipality does not have offshord wind farms. According to this [website](https://turbines.dk/), as of 2022, there are only 630 offshore wind farms compared to 5666 onshore wind farms. We can set the NaNs to 0 in this case.

# %%
df_prod.offshorewindge100mw_mwh = df_prod.offshorewindge100mw_mwh.fillna(0)
df_prod.offshorewindlt100mw_mwh = df_prod.offshorewindlt100mw_mwh.fillna(0)

# %% [markdown]
# `solarmwh`

# %%
print(df_prod[df_prod.municipalityno == 999].shape)
print(df_prod[df_prod.solarmwh.isna()].municipalityno.unique())

# %% [markdown]
# 999 is a non-stated municipality and it lacks any measurements of solar production throughout the entire year, perhaps indicating no solar power production at all. We can set the NaNs to 0 as well.

# %%
df_prod.solarmwh = df_prod.solarmwh.fillna(0)

# %% [markdown]
# `thermalpowermwh`

# %%
thermalpowermwh_muns = df_prod[
    df_prod.thermalpowermwh.isna()
].municipalityno.unique()
print(thermalpowermwh_muns)

for mun_no in thermalpowermwh_muns:
    temp_df = nan_overview(df_prod[df_prod.municipalityno == mun_no])
    print(f"Municipality no. {mun_no}: {temp_df.values}")

# %%
df_prod[df_prod.municipalityno == 411].shape[0] / df_prod[
    df_prod.municipalityno == 163
].shape[0]

# %%
df_prod_360 = df_prod[df_prod.municipalityno == 360]
df_prod_360[df_prod_360.thermalpowermwh.isna()]

# %% [markdown]
# There are multiple municipalities here which lack all the data available for `thermalpowermwh`, in similarity to `solarmwh`. Municipality no. 411 and 360 stick out from the bunch for different reasons. 411 only has 47% of the data available, meanwhile, 360 misses 66.8% of its `thermalpower` measurements.
#
# Upon further inspection of municipality 360, they may have not started producing (or tracking) thermal power production before 2022-09-01 22:00:00.

# %%
df_prod.thermalpowermwh = df_prod.thermalpowermwh.fillna(0)

# %% [markdown]
# ### Production and Consumption - Settlement
#
# [https://www.energidataservice.dk/tso-electricity/productionconsumptionsettlement](https://www.energidataservice.dk/tso-electricity/productionconsumptionsettlement)

# %%
df_prodcons = retrieve_from_cas(spark, "prodcons", cas_keyspace)
df_prodcons = df_prodcons.sort_values("hourutc")
df_prodcons.head()

# %%
nan_overview(df_prodcons)

# %%
print(df_prodcons[df_prodcons.exchangenl_mwh.isna()].pricearea.unique())
print(df_prodcons[df_prodcons.exchangeno_mwh.isna()].pricearea.unique())

# %% [markdown]
# The missing values for the exchange between Denmark and the Netherlands and Norway can be explained by the fact that the two countries only exchange electricity with price area DK2 (Sjælland og Øerne).
#
# No exchange is made between Denmark and the Great Britain in any of the price areas.

# %%
df_prodcons = df_prodcons.fillna(0)

# %% [markdown]
# ### Consumption per Industry, Public and Private, Municipality and Hour
# [https://www.energidataservice.dk/tso-electricity/ConsumptionIndustry](https://www.energidataservice.dk/tso-electricity/ConsumptionIndustry)

# %%
df_cons = retrieve_from_cas(spark, "consumption", cas_keyspace)
df_cons = df_cons.sort_values("hourutc")
df_cons.head()

# %%
nan_overview(df_cons)

# %% [markdown]
# No missing values!

# %% [markdown]
# ### Weather data

# %%
df_weather = retrieve_from_cas(spark, "weather", cas_keyspace)
df_weather = df_weather.sort_values("time")
df_weather.head()

# %%
# For some reason, the data types of these columns did not get converted properly
# when stored into Cassandra
df_weather[
    [
        "coco",
        "dwpt",
        "prcp",
        "pres",
        "rhum",
        "snow",
        "temp",
        "tsun",
        "wdir",
        "wpgt",
        "wspd",
    ]
] = df_weather[
    [
        "coco",
        "dwpt",
        "prcp",
        "pres",
        "rhum",
        "snow",
        "temp",
        "tsun",
        "wdir",
        "wpgt",
        "wspd",
    ]
].astype(float)

# %%
df_weather.dtypes

# %%
nan_overview(df_weather)

# %% [markdown]
# ##### `snow`
#
# Lets first address the `snow`-column. We can check which municipalities actually do snow.

# %%
df_weather[df_weather["snow"].notna()].municipality.unique()

# %%
for mun in ["Sønderborg", "Ærø", "Tønder"]:
    print(
        mun,
        nan_overview(
            df_weather[df_weather.municipality.isin([mun])]
        ).num.values,
    )

# %% [markdown]
# Across the three municipalities only 10 entries are missing. But they are only missing Sønderborg and Ærø

# %%
df_sønd = df_weather[df_weather.municipality == "Sønderborg"]
df_sønd[df_sønd.snow.isna()]

# %%
df_ærø = df_weather[df_weather.municipality == "Ærø"]
df_ærø[df_ærø.snow.isna()]

# %%
df_weather.loc[df_weather.municipality == "Ærø", "snow"] = df_weather.loc[
    df_weather.municipality == "Ærø", "snow"
].ffill()
df_weather.loc[df_weather.municipality == "Sønderborg", "snow"] = (
    df_weather.loc[df_weather.municipality == "Sønderborg", "snow"].ffill()
)

# %% [markdown]
# We will leave the rest as NaNs as snow is not measured at all.

# %% [markdown]
# ##### `tsun`

# %%
df_weather.groupby("municipality")["tsun"].apply(
    lambda x: x.isna().mean() * 100
).sort_values(ascending=False)

# %%
for idx, muni in enumerate(
    df_weather[df_weather["tsun"].notna()].municipality.unique().tolist(), 1
):
    temp_df = (
        df_weather[df_weather["tsun"].notna()]
        .query(f"municipality == '{muni}'")
        .sort_values("time", ascending=True)
    )
    print(idx, muni, temp_df.time.min())

# %% [markdown]
# There look to be a systemic pattern with the `tsun` data. Obviously, we won't be able to impute the data where 100% is missing. But we cannot impute the data where 60% is missing either. This is due to the fact that measurements don't start before sometime in August, hence the large proportion missing.
#
# We will have to leave it be.

# %% [markdown]
# ##### `prcp`

# %%
missing_prcp_pct = df_weather.groupby("municipality")["prcp"].apply(
    lambda x: x.isna().mean() * 100
)
missing_prcp_pct = missing_prcp_pct.sort_values(ascending=False)
missing_prcp_pct[missing_prcp_pct > 0]

# %% [markdown]
# Again we see a systematic pattern for the missing values.


# %%
def find_time_based_gaps(df, time_col="time", value_col="prcp"):
    """
    Find NaN gaps with actual time duration calculations
    """
    df_sorted = df.sort_values(time_col).reset_index(drop=True)

    if not pd.api.types.is_datetime64_any_dtype(df_sorted[time_col]):
        df_sorted[time_col] = pd.to_datetime(df_sorted[time_col])

    is_nan = df_sorted[value_col].isna()
    groups = (is_nan != is_nan.shift()).cumsum()

    gap_info = []
    for _, group_data in df_sorted.groupby(groups):
        if group_data[value_col].isna().all():
            start_time = group_data[time_col].iloc[0]
            end_time = group_data[time_col].iloc[-1]
            duration = end_time - start_time

            gap_info.append(
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "duration_days": duration.total_seconds() / (24 * 3600),
                    "gap_length_records": len(group_data),
                    "start_index": group_data.index[0],
                    "end_index": group_data.index[-1],
                }
            )

    return pd.DataFrame(gap_info)


df_billund = df_weather[df_weather.municipality == "Billund"]
time_gaps_39 = find_time_based_gaps(
    df_billund, time_col="time", value_col="prcp"
)
print(time_gaps_39)


# %%
# Analyzing the time gaps for the municipalities wth 30-something percent missing data
for mun in missing_prcp_pct[
    (missing_prcp_pct < 100) & (missing_prcp_pct > 1)
].index.to_list():
    temp_df_coco = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="prcp",
    )
    print(f"{mun}, max gap: {temp_df_coco.gap_length_records.max()} hours")

# %% [markdown]
# For the municipalities missing 39.8972602739726% of data, imputation would not be advisable.

# %%
time_gaps_36 = find_time_based_gaps(
    df_weather[df_weather.municipality == "Syddjurs"],
    time_col="time",
    value_col="prcp",
)
time_gaps_36

# %%
df_weather.loc[df_weather.municipality == "Syddjurs", "prcp"].reset_index(
    drop=True
).plot()

# %% [markdown]
# Taking one example where 39% of the data is missing, we can see that there is a large gap of missing values. Imputing these would not be feasible. For 36% missing, the largest gap is 35 hours.
#
# When plotted, we see the gaps are very much present in the first 1/3 of the dataset. We can use interpolation or a fill strategy, but it wouldn't be representative. If we were to assume that all the NaNs are due to no precipitation being mesured, despite 0.0 being prevalent in the data, we can fill it with 0 as a quick fix (as seen below) but we get two periods consisting of no percepitation which look to be highly unlikely.
#
# We could attempt a rolling average + backfill as an imputation strategy here to ensure that no missing values are present at all. But again, this may not be fully representative for the actual underlying data but it simplifies using the data in the long run.

# %%
time_gaps_021 = find_time_based_gaps(
    df_weather[df_weather.municipality == "Bornholm"],
    time_col="time",
    value_col="prcp",
)
time_gaps_021


# %%
def rolling_average(df, municipality, column, window_size=24):
    mask = df.loc[df.municipality == municipality, column].isna()
    rolling_avg = (
        df.loc[df.municipality == municipality, column]
        .rolling(window=window_size, min_periods=1)
        .mean()
    )
    rolling_avg = (
        rolling_avg.bfill()
    )  # Extra measure to ensure that all NaNs are accounted for
    return mask, rolling_avg[mask]


# %%
mask, r_avg = rolling_average(df_weather, "Syddjurs", "prcp")
df_weather.loc[(df_weather.municipality == "Syddjurs") & mask, "prcp"] = r_avg

# %%
prcp_municipalities = [
    "Billund",
    "Horsens",
    "Samsø",
    "Kerteminde",
    "Frederikssund",
    "Odense",
    "Assens",
    "Esbjerg",
    "Varde",
    "Fanø",
    "Nyborg",
    "Nordfyn",
    "Hedensted",
    "Syddjurs",
    "Norddjurs",
    "Odder",
    "Skanderborg",
    "Aarhus",
    "Favrskov",
    "Rebild",
    "Aalborg",
    "Ikast-Brande",
    "Silkeborg",
    "Holstebro",
    "Jammerbugt",
    "Viborg",
    "Frederikshavn",
    "Bornholm",
    "Faaborg-Midtfyn",
    "Fredensborg",
    "Fredericia",
    "Ringkøbing-Skjern",
    "Herning",
    "Middelfart",
    "Roskilde",
    "Vejen",
    "Langeland",
    "Vejle",
    "Kolding",
    "Greve",
    "Solrød",
    "Svendborg",
    "Guldborgsund",
    "Haderslev",
    "Hjørring",
    "Hillerød",
    "Brøndby",
    "Copenhagen",
    "Ballerup",
    "Allerød",
    "Rødovre",
    "Rudersdal",
    "Vallensbæk",
    "Albertslund",
    "Brønderslev",
    "Herlev",
    "Glostrup",
    "Gladsaxe",
    "Høje-Taastrup",
    "Hørsholm",
    "Ishøj",
    "Egedal",
    "Hvidovre",
    "Gentofte",
    "Furesø",
    "Lyngby-Taarbæk",
    "Frederiksberg",
    "Tårnby",
    "Dragør",
    "Læsø",
    "Kalundborg",
    "Slagelse",
    "Sorø",
]

for mun in prcp_municipalities:
    mask, roll_avg = rolling_average(df_weather, mun, "prcp")
    df_weather.loc[(df_weather.municipality == mun) & mask, "prcp"] = roll_avg

# %%
missing_prcp_check = df_weather.groupby("municipality")["prcp"].apply(
    lambda x: x.isna().mean() * 100
)
missing_prcp_check = missing_prcp_check.sort_values(ascending=False)
missing_prcp_check[missing_prcp_check > 0]

# %%
nan_overview(df_weather).index

# %% [markdown]
# #### `coco`

# %%
missing_coco_pct = df_weather.groupby("municipality")["coco"].apply(
    lambda x: x.isna().mean() * 100
)
missing_coco_pct = missing_coco_pct.sort_values(ascending=False)
missing_coco_pct[missing_coco_pct > 0]

# %% [markdown]
# Lets look how large the gaps are for the municipalities missing large portions of data.

# %%
for mun in missing_coco_pct[missing_coco_pct > 40].index.to_list():
    temp_df_coco = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="coco",
    )
    print(
        f"{mun}, max gap: {temp_df_coco.gap_length_records.max()} hours, avg. gap: {temp_df_coco.gap_length_records.mean():.1f}"
    )

# %% [markdown]
# For municipalities missing 40-something percent of data would not be feasible to impute due to the large time gaps with our current configuration. We could increase the window of our rolling average to take a week into account, but there might be smaller, but comparable, time gaps preceeding or following the largest gaps.

# %%
df_weather.loc[df_weather.municipality == "Svendborg", "coco"].reset_index(
    drop=True
).plot()

# %%
for mun in missing_coco_pct[missing_coco_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "coco")
    df_weather.loc[(df_weather.municipality == mun) & mask, "coco"] = roll_avg

# %%
mask_coco = df_weather.loc[:, "coco"].isna()
df_weather.loc[~mask_coco, "coco"] = df_weather.loc[~mask_coco, "coco"].astype(
    int
)
# df_weather.coco = df_weather.coco.astype('Int64')  # Use Int64 to handle NaN values

# %% [markdown]
# #### `wpgt`

# %%
missing_wpgt_pct = df_weather.groupby("municipality")["wpgt"].apply(
    lambda x: x.isna().mean() * 100
)
missing_wpgt_pct = missing_wpgt_pct.sort_values(ascending=False)
missing_wpgt_pct[missing_wpgt_pct > 0]

# %%
for mun in missing_wpgt_pct[missing_wpgt_pct > 0].index.to_list():
    temp_df_wpgt = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="wpgt",
    )
    print(
        f"{mun}, max gap: {temp_df_wpgt.gap_length_records.max()} hours, avg. gap: {temp_df_wpgt.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_wpgt_pct[missing_wpgt_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "wpgt", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "wpgt"] = roll_avg

# %% [markdown]
# #### `wdir`

# %%
missing_wdir_pct = df_weather.groupby("municipality")["wdir"].apply(
    lambda x: x.isna().mean() * 100
)
missing_wdir_pct = missing_wdir_pct.sort_values(ascending=False)
missing_wdir_pct[missing_wdir_pct > 0]

# %%
for mun in missing_wdir_pct[missing_wdir_pct > 0].index.to_list():
    temp_df_wdir = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="wdir",
    )
    print(
        f"{mun}, max gap: {temp_df_wdir.gap_length_records.max()} hours, avg. gap: {temp_df_wdir.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_wdir_pct[missing_wdir_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(
        df_weather, mun, "wdir", window_size=24 * 7
    )
    df_weather.loc[(df_weather.municipality == mun) & mask, "wdir"] = roll_avg

# %% [markdown]
# #### `dwpt`

# %%
missing_dwpt_pct = df_weather.groupby("municipality")["dwpt"].apply(
    lambda x: x.isna().mean() * 100
)
missing_dwpt_pct = missing_dwpt_pct.sort_values(ascending=False)
missing_dwpt_pct[missing_dwpt_pct > 0]

# %%
for mun in missing_dwpt_pct[missing_dwpt_pct > 0].index.to_list():
    temp_df_dwpt = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="dwpt",
    )
    print(
        f"{mun}, max gap: {temp_df_dwpt.gap_length_records.max()} hours, avg. gap: {temp_df_dwpt.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_dwpt_pct[missing_dwpt_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "dwpt", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "dwpt"] = roll_avg

# %% [markdown]
# #### `pres`

# %%
missing_pres_pct = df_weather.groupby("municipality")["pres"].apply(
    lambda x: x.isna().mean() * 100
)
missing_pres_pct = missing_pres_pct.sort_values(ascending=False)
missing_pres_pct[missing_pres_pct > 0]

# %%
for mun in missing_pres_pct[missing_pres_pct > 0].index.to_list():
    temp_df_pres = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="pres",
    )
    print(
        f"{mun}, max gap: {temp_df_pres.gap_length_records.max()} hours, avg. gap: {temp_df_pres.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_pres_pct[missing_pres_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "pres", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "pres"] = roll_avg

# %% [markdown]
# #### `rhum`

# %%
missing_rhum_pct = df_weather.groupby("municipality")["rhum"].apply(
    lambda x: x.isna().mean() * 100
)
missing_rhum_pct = missing_rhum_pct.sort_values(ascending=False)
missing_rhum_pct[missing_rhum_pct > 0]

# %%
for mun in missing_rhum_pct[missing_rhum_pct > 0].index.to_list():
    temp_df_rhum = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="rhum",
    )
    print(
        f"{mun}, max gap: {temp_df_rhum.gap_length_records.max()} hours, avg. gap: {temp_df_rhum.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_rhum_pct[missing_rhum_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "rhum", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "rhum"] = roll_avg

# %% [markdown]
# #### `temp`

# %%
missing_temp_pct = df_weather.groupby("municipality")["temp"].apply(
    lambda x: x.isna().mean() * 100
)
missing_temp_pct = missing_temp_pct.sort_values(ascending=False)
missing_temp_pct[missing_temp_pct > 0]

# %%
for mun in missing_temp_pct[missing_temp_pct > 0].index.to_list():
    temp_df_temp = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="temp",
    )
    print(
        f"{mun}, max gap: {temp_df_temp.gap_length_records.max()} hours, avg. gap: {temp_df_temp.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_temp_pct[missing_temp_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "temp", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "temp"] = roll_avg

# %% [markdown]
# #### `wspd`

# %%
missing_wspd_pct = df_weather.groupby("municipality")["wspd"].apply(
    lambda x: x.isna().mean() * 100
)
missing_wspd_pct = missing_wspd_pct.sort_values(ascending=False)
missing_wspd_pct[missing_wspd_pct > 0]

# %%
for mun in missing_wspd_pct[missing_wspd_pct > 0].index.to_list():
    wspd_df_wspd = find_time_based_gaps(
        df_weather[df_weather.municipality == mun],
        time_col="time",
        value_col="wspd",
    )
    print(
        f"{mun}, max gap: {wspd_df_wspd.gap_length_records.max()} hours, avg. gap: {wspd_df_wspd.gap_length_records.mean():.1f}"
    )

# %%
for mun in missing_wspd_pct[missing_wspd_pct < 40].index.tolist():
    mask, roll_avg = rolling_average(df_weather, mun, "wspd", window_size=24)
    df_weather.loc[(df_weather.municipality == mun) & mask, "wspd"] = roll_avg

# %%
nan_overview(df_weather)

# %% [markdown]
# ### Inserting imputed data back into the database

# %%
import uuid


def overwrite_cas_table(
    df: pd.DataFrame,
    table_name: str,
    keyspace_name: str,
    add_timeuuid: bool = False,
):
    """Dedicated overwrite function"""

    def generate_timeuuid():
        return str(uuid.uuid1())

    spark_df = spark.createDataFrame(df)

    if add_timeuuid:
        timeuuid_udf = udf(generate_timeuuid, StringType())
        spark_df = spark_df.withColumn("id", timeuuid_udf())

    spark_df.write.format("org.apache.spark.sql.cassandra").mode(
        "overwrite"
    ).option("confirm.truncate", "true").options(
        table=table_name, keyspace=keyspace_name
    ).save()
    print(f"Successfully written the table {keyspace_name}.{table_name}")


# %%
# overwrite_cas_table(df_prod, "production", cas_keyspace)
# overwrite_cas_table(df_prodcons, "prodcons", cas_keyspace)
# overwrite_cas_table(df_weather, "weather", cas_keyspace)

# %%
df_prod = retrieve_from_cas(spark, "production", cas_keyspace)
df_prod = df_prod.sort_values("hourutc")
df_cons = retrieve_from_cas(spark, "consumption", cas_keyspace)
df_cons = df_cons.sort_values("hourutc")

# %%
df_prod.head()

# %%
df_cons.head()

# %%
df_mun = pd.DataFrame(list(mdb_client[cas_keyspace]["municipalities"].find()))
df_mun

# %%
# Create a temporary dataframe with just the columns we need
mun_names = df_mun[["municipalityNo", "municipality"]]

# Merge df_prod with mun_names on municipalityNo
df_prod_e = df_prod.merge(
    mun_names,
    left_on="municipalityno",
    right_on="municipalityNo",
    # how='left'
).drop("municipalityNo", axis=1)  # Drop the redundant column

df_prod_e.head()

# %%
from cassandra.cluster import Cluster

cluster = Cluster(["localhost"], port=9042)
session = cluster.connect()

# %%
cas_keyspace = "rewrite320"
# session.execute(f"ALTER TABLE {cas_keyspace}.weather RENAME time TO hourutc")
session.execute(f"ALTER TABLE {cas_keyspace}.weather DROP hourutc")
# session.execute(f"UPDATE {cas_keyspace}.weather SET hourutc = time")
# ALTER TABLE keyspace_name.table_name RENAME old_column_name TO new_column_name;

# %%
# Step 1: Add the new column
session.execute(f"ALTER TABLE {cas_keyspace}.weather ADD hourutc timestamp")

# Step 2: Copy data from time column to hourutc column
rows = session.execute(f"SELECT id, time FROM {cas_keyspace}.weather")

# Build the query string with f-string for table name
update_query = f"UPDATE {cas_keyspace}.weather SET hourutc = ? WHERE id = ?"

for row in rows:
    # Execute with the pre-built query and parameters separately
    session.execute(update_query, [row.time, row.id])

# Step 3: Drop the old column (optional)
session.execute(f"ALTER TABLE {cas_keyspace}.weather DROP time")

# %%
# session.execute(f"ALTER TABLE {cas_keyspace}.production ADD municipality text;")
# Get the table description
result = session.execute(f"DESCRIBE TABLE {cas_keyspace}.weather")

# Print each row of the result
for row in result:
    print(row)

# %%
overwrite_cas_table(df_prod_e, "production", cas_keyspace)

# %%
# Merge df_prod with mun_names on municipalityNo
df_cons_e = df_cons.merge(
    mun_names,
    left_on="municipalityno",
    right_on="municipalityNo",
    # how='left'
).drop("municipalityNo", axis=1)  # Drop the redundant column

df_cons_e.head()

# %%
overwrite_cas_table(df_cons_e, "consumption", cas_keyspace)

# %%

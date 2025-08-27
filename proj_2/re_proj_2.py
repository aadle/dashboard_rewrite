# %%
import sys
import os

sys.path.append(os.path.abspath(".."))

import pandas as pd

# from helper.utils import mongodb_setup, spark_setup
from helper.utils import *

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# PySpark<4.0.0
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import requests
from bs4 import BeautifulSoup
import geopy

mdb_client = mongodb_setup()
spark = spark_setup()
cas_keyspace = "rewrite320"

# %% [markdown]
# ## Geographical locations of municipalities

# %%
df_mun = pd.DataFrame(list(mdb_client[cas_keyspace]["municipalities"].find()))
df_mun.head()

# %%
df_mun_ = df_mun[["municipality", "region"]]
df_mun_.head()

# %%
df_mun["municipality"] = df_mun["municipality"].replace(
    "Bornholm (merged 2003)", "Bornholm"
)
df_mun["municipality"] = df_mun["municipality"].replace(
    "Ærø (merged 2006)", "Ærø"
)

# %%
# "DK1 is west of the Great Belt and DK2 is east of the Great Belt."
# https://www.energidataservice.dk/tso-electricity/productionconsumptionsettlement
price_areas = {
    "North": "DK1",
    "Central": "DK1",
    "South": "DK1",
    "Zealand": "DK2",
    "Capital": "DK2",
}
df_mun["price_area"] = df_mun["region"].map(price_areas)

# %%
# nominatim = geopy.geocoders.Nominatim(user_agent="Danske kommuner in lon, lat")
# mun_coords = {}
# for idx, municipality in enumerate(df_mun["municipality"].tolist()):
#     geolocation = nominatim.geocode(f"{municipality}, Denmark")
#     lon = geolocation.longitude
#     lat = geolocation.latitude
#     mun_coords[municipality] = {"longitude": lon, "latitude": lat}
#     print(idx, municipality, lon, lat)


# %%
# df_mun["lon"] = df_mun["municipality"].map(lambda x: mun_coords[x]["longitude"])
# df_mun["lat"] = df_mun["municipality"].map(lambda x: mun_coords[x]["latitude"])

# %%
df_mun.head()

# %% [markdown]
# ## Weather data

# %%
from datetime import datetime
import meteostat

t_start = datetime(2021, 12, 31, 23, 00)
t_end = datetime(2022, 12, 31, 22, 00)


def get_weather_data(lat, lon, municipality):
    # Get distance to nearest station
    nearby_stations = meteostat.Stations().nearby(lat, lon).fetch()
    dist_to_station = nearby_stations["distance"].iloc[
        0
    ]  # distance is in meters

    # Narrow down nearby_stations to the ones from which we can retrieve data from
    next_best_stations = nearby_stations[
        (nearby_stations["hourly_start"] <= t_start)
        & (nearby_stations["hourly_end"] >= t_end)
    ]

    # Get location
    try:
        # Try nearest station
        loc = meteostat.Point(
            lat, lon
        )  # Point points to the nearest weather station
        weather_data = meteostat.Hourly(loc, t_start, t_end).fetch()
    except AttributeError:
        # If the nearest station does not have data available,
        # narrow down to nearby stations with data available and pick the best available
        station_id = next_best_stations.index[0]
        weather_data = meteostat.Hourly(station_id, t_start, t_end).fetch()
    weather_data["dist_to_station"] = dist_to_station
    weather_data["municipality"] = municipality

    return weather_data


weather_dfs = []
for _, row in df_mun.iterrows():
    municipality = row["municipality"]
    lon = round(row["lon"], 4)
    lat = round(row["lat"], 4)
    weather_df = get_weather_data(lat, lon, municipality)
    weather_dfs.append(weather_df)

df_weather_all = pd.concat(weather_dfs)
df_weather_all.reset_index(inplace=True)

# %% [markdown]
# ### Inserting into Cassandra

# %%
# def create_cassandra_table_query(df, keyspace, table_name):
#     # Define mapping between Pandas and Cassandra datatypes
#     dtype_mapping = {
#         'int64': 'int',
#         'float64': 'double',
#         'object': 'text',
#         'bool': 'boolean',
#         'datetime64[ns]': 'timestamp'
#     }

#     # Start constructing the CREATE TABLE query
#     query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (\n"

#     # Add primary key column with timeuuid
#     columns = ["id timeuuid"]

#     # Add remaining columns with mapped Cassandra data types
#     for col, dtype in df.dtypes.items():
#         if col != "id":  # Exclude 'id' to avoid duplication
#             cassandra_type = dtype_mapping.get(str(dtype), 'text')  # Default to 'text' if type is unrecognized
#             columns.append(f"{col} {cassandra_type}")

#     # Join columns with commas and specify primary key as 'id'
#     columns_str = ",\n    ".join(columns)
#     query += f"    {columns_str},\n"
#     query += "    PRIMARY KEY (id)\n);"

#     return query


# %%
# from cassandra.cluster import Cluster
# cluster = Cluster(['localhost'], port=9042)
# session = cluster.connect()
# weather_query = create_cassandra_table_query(df_weather_all, cas_keyspace, "weather")
# session.execute(weather_query)

# %%
# import uuid
# def insert_to_cas(df: pd.DataFrame , table_name: str, keyspace_name: str):
# # Prompt: How can I add a timeuuid 'id' column when inserting into Cassandra with PySpark
#     def generate_timeuuid():
#         return str(uuid.uuid1())

#     timeuuid_udf = udf(generate_timeuuid, StringType())
#     spark.createDataFrame(df).withColumn("id", timeuuid_udf())\
#         .write.format("org.apache.spark.sql.cassandra")\
#         .mode("append")\
#         .options(table=table_name, keyspace=keyspace_name)\
#         .save()

# insert_to_cas(df_weather_all, "weather", cas_keyspace)

# %% [markdown]
# ## Smoothing outliers/anomalies

# %% [markdown]
# ### Smoothing with DCT

# %%
df_gas = pd.DataFrame(list(mdb_client[cas_keyspace]["gas"].find()))
df_gas.head()

# %%
from scipy.fft import dct, idct
import matplotlib.pyplot as plt
import numpy as np

dct_purchaseprice = dct(df_gas.purchasepricedkk_kwh.values)
W = np.arange(0, dct_purchaseprice.shape[0])

fig_dct, ax_dct = plt.subplots(figsize=(4, 3), dpi=100)
ax_dct.plot(np.abs(dct_purchaseprice))
ax_dct.set_xticks(np.arange(0, 16, step=2))
ax_dct.set_xlim(0, 15)
plt.show()

# %%
filtered_purchaseprice = dct_purchaseprice.copy()
filtered_purchaseprice[(W > 3)] = 0
fig_gas, ax_gas = plt.subplots(figsize=(7, 5), dpi=100)
ax_gas.fill_between(
    df_gas.gasday,
    df_gas.purchasepricedkk_kwh,
    color="grey",
    alpha=0.6,
    label="Underlying purchase price",
)
ax_gas.plot(
    df_gas.gasday,
    idct(filtered_purchaseprice),
    color="purple",
    linewidth=5,
    label="DCT-filtered purchase price",
)
ax_gas.legend(loc="lower right")
ax_gas.set_title("Gas purchase price over time")
plt.show()

# %% [markdown]
# ### Outlier/anomaly detection

# %%
df_prodcons = retrieve_from_cas(spark, "prodcons", cas_keyspace)
df_prodcons.head()

# %%
from scipy.stats import trim_mean, median_abs_deviation

df_no = df_prodcons[["hourutc", "exchangeno_mwh"]]
df_no = df_no.groupby("hourutc").agg({"exchangeno_mwh": "sum"})
print(10 * "-" + " Standard statistics " + 10 * "-")
print("Mean:", round(df_no.exchangeno_mwh.mean(), 3))
print("Standard deviation:", round(df_no.exchangeno_mwh.std(), 3))
print()

print(10 * "-" + " Robust statistics " + 10 * "-")
print("Trimmed mean:", round(trim_mean(df_no.exchangeno_mwh, 0.05), 3))
print(
    "Median absolute deviation (MAD):",
    round(median_abs_deviation(df_no.exchangeno_mwh), 3),
)
print(
    "Adjusted MAD:",
    round(median_abs_deviation(df_no.exchangeno_mwh) * 1.4826, 3),
)


# %% [markdown]
# The change of effect comes from removing observations we consider as outliers in either direction, where we make the range of value smaller. As a result of doing so we get a larger trimmed mean value compared to the standard mean.
#
# MAD differs from standard deviation as the equivalent to the mean in SD is the median for MAD. But if we assume that our data is normally distributed, we can multiply the MAD by a factor $k = 1.4826$ to approximate the standard deviation. Here, we see the approximation being relatively good.

# %% [markdown]
# ## Imputation

# %% [markdown]
# ### Inspection of the data

# %% [markdown]
# #### "production" table

# %%
df_prod = retrieve_from_cas(spark, "production", cas_keyspace)
df_prod = df_prod.sort_values("hourdk", ascending=True)
df_prod.head()

# %%
df_prod.isna().sum()[df_prod.isna().sum() > 0] / df_prod.shape[0]

# %% [markdown]
# _offshorewindlt100mw_mwh_, _offshorewindge100mw_mwh_ are missing more than 90% of the data making it not feasible to impute its missing values. _solarmwh_ and _thermalpowermwh_ look to be more managable to impute.

# %%
df_solar = df_prod.groupby("municipalityno")["solarmwh"].apply(
    lambda x: x.isna().mean() * 100
)
df_solar[df_solar > 0].sort_values(ascending=False)

# %%
df_thermal = df_prod.groupby("municipalityno")["thermalpowermwh"].apply(
    lambda x: x.isna().mean() * 100
)
df_thermal[df_thermal > 0].sort_values(ascending=False)

# %%
df_lt = df_prod.groupby("municipalityno")["offshorewindlt100mw_mwh"].apply(
    lambda x: x.isna().mean() * 100
)
df_lt.sort_values(ascending=False)

# %%
df_ge = df_prod.groupby("municipalityno")["offshorewindge100mw_mwh"].apply(
    lambda x: x.isna().mean() * 100
)
df_ge.sort_values(ascending=False)

# %% [markdown]
# With reasonability, we will just assume that where data is missing is due to lack of activity thus we will set the missing values to 0 to indicate this.

# %% [markdown]
# #### "prodcons" table

# %%
df_prodcons = retrieve_from_cas(spark, "prodcons", cas_keyspace)
df_prodcons = df_prodcons.sort_values("hourdk", ascending=True)
df_prodcons.head()

# %%
df_prodcons.exchangegb_mwh = df_prodcons.exchangegb_mwh.astype(float)

# %%
df_prodcons.isna().sum()[df_prodcons.isna().sum() > 0] / df_prodcons.shape[0]

# %% [markdown]
# _exchangegb_mwh_ misses all the data which may indicate that there is no exchange between Denmark and the UK during this period. _exchangenl_mwh_, _exchangeno_mwh_ are both missing 50% of the data which we should investigate further.

# %%
df_prodcons[df_prodcons["exchangenl_mwh"].isna()].head()

# %%
print(df_prodcons[df_prodcons["exchangenl_mwh"].isna()].pricearea.unique())
print(df_prodcons[df_prodcons["exchangeno_mwh"].isna()].pricearea.unique())

# %% [markdown]
# Upon further inspection, Denmark exchanges electricity with the Netherlands and Norway from the DK1 area – explaining that they are missing 50% of their data.

# %% [markdown]
# #### "consumption" table

# %%
df_cons = retrieve_from_cas(spark, "consumption", cas_keyspace)
df_cons = df_cons.sort_values("hourdk", ascending=True)
df_cons.head()

# %%
(df_cons.isna().sum()[df_cons.isna().sum() > 0] / df_cons.shape[0]).head()

# %% [markdown]
# The "consumption" table have no missing data at all.

# %% [markdown]
# #### "weather" table

# %%
df_weather = retrieve_from_cas(spark, "weather", cas_keyspace)
df_weather = df_weather.sort_values("time", ascending=True)
df_weather.head()

# %%
cols_to_float = [
    col
    for col in df_weather.columns
    if col not in ["id", "municipality", "time"]
]
df_weather[cols_to_float] = df_weather[cols_to_float].apply(
    pd.to_numeric, errors="coerce"
)

# %%
(
    df_weather.isna().sum()[df_weather.isna().sum() > 0] / df_weather.shape[0]
).sort_values(ascending=False)

# %% [markdown]
# Didn't catch this earlier, but there is a systemic pattern to the missing values of _dwpt, pres, rhum, themp_ and _wspd_ where we may find the missing data in the same rows.

# %%
df_weather[df_weather["snow"].notna()].municipality.unique()

# %% [markdown]
# _snow_ is only measured at three stations during this time period, explaining why 96% of the data is missing.

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
# Of the municipalities where _tsun_ is measured, measurements start in august though this is not alway the case (e.g. Tønder). Knowing this, imputing the data of this particular column becomes not feasible. This is further semented when looking at the missing values for each municipality.

# %%
df_weather.groupby("municipality")["tsun"].apply(
    lambda x: x.isna().mean() * 100
).sort_values(ascending=False)

# %% [markdown]
# For _tsun_ it would be wrong to set 0 in place of the NaNs, where the data is missing completly, as it would not be representable for what the column is attempting to measure. Imputing the rest would not be advisable either as those measurements start from some time in August.

# %%
missing_prcp_pct = df_weather.groupby("municipality")["prcp"].apply(
    lambda x: x.isna().mean() * 100
)
missing_prcp_pct = missing_prcp_pct.sort_values(ascending=False)
missing_prcp_pct[missing_prcp_pct > 0]

# %%
for idx, muni in enumerate(
    df_weather[df_weather["prcp"].notna()].municipality.unique().tolist(), 1
):
    temp_df = (
        df_weather[df_weather["prcp"].notna()]
        .query(f"municipality == '{muni}'")
        .sort_values("time", ascending=True)
    )
    print(idx, muni, temp_df.time.min())

# %% [markdown]
# In terms of _prcp_, the missing data look to be spread among most of the municipalities. Multiple groups of the municipalities share the same portion of missing values.
#
# Looking at the 9 municipalities missing all of its values, some of them happen to be closely situated to one another (e.g. Halsnæs, Odsherred, Gribskov). This could indicate that the data could come from the same station, or that stations close to each other measure the same set of data.
#
# Imputing where 100% is missing would be nonsensical as no data is available. For the remainder imputation should be more feasible.
#
# Upon further inspection, the systematic pattern could be due to the time when the measurements start, or measurements are made available from.

# %% [markdown]
# ### Imputation of the data

# %% [markdown]
# #### "production" table

# %%
df_prod = df_prod.sort_values("hourdk", ascending=True)
df_prod.isna().sum()[df_prod.isna().sum() > 0] / df_prod.shape[0]

# %%
df_prod[["solarmwh"]] = df_prod[["solarmwh"]].fillna(0)
df_prod[["thermalpowermwh"]] = df_prod[["thermalpowermwh"]].fillna(0)
df_prod[["offshorewindlt100mw_mwh"]] = df_prod[
    ["offshorewindlt100mw_mwh"]
].fillna(0)
df_prod[["offshorewindge100mw_mwh"]] = df_prod[
    ["offshorewindge100mw_mwh"]
].fillna(0)

# %% [markdown]
# NaN = no activity which would be equivalent to set it to 0.

# %% [markdown]
# #### "production-consumption" table

# %%
df_prodcons = df_prodcons.sort_values("hourdk", ascending=True)
df_prodcons.isna().sum()[df_prodcons.isna().sum() > 0] / df_prodcons.shape[0]

# %%
df_prodcons = df_prodcons.fillna(0)

# %% [markdown]
# #### "weather" table

# %% [markdown]
# Here we need to be weary of how we impute onwards here and to do it municipality by municipality.

# %%
(
    df_weather.isna().sum()[df_weather.isna().sum() > 0] / df_weather.shape[0]
).sort_values(ascending=False)

# %%
df_prcp = df_weather.groupby("municipality")["prcp"].apply(
    lambda x: x.isna().mean() * 100
)
df_prcp[df_prcp > 0].sort_values(ascending=False)

# %%
incl_prcp = [
    "Ringkøbing-Skjern",
    "Bornholm",
    "Roskilde",
    "Faaborg-Midtfyn",
    "Assens",
    "Vordingborg",
    "Vejen",
    "Langeland",
    "Vejle",
    "Kolding",
    "Svendborg",
    "Fredericia",
    "Fredensborg",
    "Frederikshavn",
    "Frederikssund",
    "Haderslev",
    "Hillerød",
    "Guldborgsund",
    "Hjørring",
    "Herlev",
    "Rudersdal",
    "Vallensbæk",
    "Rødovre",
    "Ishøj",
    "Furesø",
    "Allerød",
    "Albertslund",
    "Greve",
    "Glostrup",
    "Ballerup",
    "Gentofte",
    "Brøndby",
    "Frederiksberg",
    "Hørsholm",
    "Høje-Taastrup",
    "Hvidovre",
    "Brønderslev",
    "Gladsaxe",
    "Lyngby-Taarbæk",
    "Egedal",
    "Copenhagen",
    "Tårnby",
    "Dragør",
    "Læsø",
    "Slagelse",
    "Sorø",
    "Kalundborg",
]
for mun in incl_prcp:
    mask = df_weather["municipality"] == mun
    # df_weather.loc[mask, "prcp"] = df_weather.loc[mask, "prcp"].ffill()
    df_weather.loc[mask, "prcp"] = df_weather.loc[mask, "prcp"].interpolate(
        method="linear"
    )

# %%
(
    df_weather.isna().sum()[df_weather.isna().sum() > 0] / df_weather.shape[0]
).sort_values(ascending=False)

# %% [markdown]
# Upon further inspection, municipalities with 39% missing data is due to recording starting late. For the municipalities with 36% missing values, data is missing throughout the entirety of the period. With this being the case, imputation is not feasible.
#
# This leaves us with the remained where 0.0021%, and less, missing which should be non-problematic. With these, we will just use .ffill() as the number of missing values are very small.

# %%
df_coco = df_weather.groupby("municipality")["coco"].apply(
    lambda x: x.isna().mean() * 100
)
df_coco[df_coco > 0].sort_values(ascending=False)

# %%
df_weather[df_weather["coco"].isna()].query("municipality == 'Aabenraa'")[
    "time"
]

# %%
(
    df_weather[df_weather["coco"].isna()]
    .query("municipality == 'Aabenraa'")["time"]
    .max()
    - df_weather[df_weather["coco"].isna()]
    .query("municipality == 'Aabenraa'")["time"]
    .min()
)

# %% [markdown]
# Smallest gap corresponds to 10 days of missing data consecutively, imputing this gap and anything larger than it would be unreasonable. We will refrain from imputing _coco_

# %%
df_wpgt = df_weather.groupby("municipality")["wpgt"].apply(
    lambda x: x.isna().mean() * 100
)
df_wpgt[df_wpgt > 0].sort_values(ascending=False)

# %% [markdown]
# When insepcting the data further, the gaps of data are continuous and not spread throughout the time series.
#
# With this in mind, 3% of data corresponds roughly 10-11 days and imputing such a gap with data that would not be advisable. Yes, forecasting techniques such as STL and ARIMA could be used here, but we will keep the solution simple.
#
# A gap of 0.2% is more managable to impute with e.g. linear interpolation is good enough for visualization purposes.

# %%
# Vesthimmerland (263 rows ~ 3%)
# (263 rows ~ 0.2%)

incl_wpgt = [
    "Svendborg",
    "Ringkøbing-Skjern",
    "Assens",
    "Fredericia",
    "Langeland",
    "Hjørring",
    "Læsø",
    "Slagelse",
    "Sorø",
    "Kalundborg",
]

for mun in incl_wpgt:
    mask = df_weather["municipality"] == mun
    # df_weather.loc[mask, "wpgt"] = df_weather.loc[mask, "wpgt"].ffill()
    df_weather.loc[mask, "wpgt"] = df_weather.loc[mask, "wpgt"].interpolate(
        method="linear"
    )

# %%
# df_weather.query("municipality == 'Svendborg' and time >= '2022-05-25 00:00:00' and time <= '2022-05-27 00:00:00'")
# df_weather[df_weather["wpgt"].isna()].query("municipality == 'Svendborg'")
df_weather[df_weather["wpgt"].isna()].query("municipality == 'Vesthimmerland'")

# %%
df_wdir = df_weather.groupby("municipality")["wdir"].apply(
    lambda x: x.isna().mean() * 100
)
df_wdir[df_wdir > 0].sort_values(ascending=False)

# %%
incl_wdir = [
    "Hjørring",
    "Ringkøbing-Skjern",
    "Langeland",
    "Svendborg",
    "Kalundborg",
    "Slagelse",
    "Sorø",
]

for mun in incl_wdir:
    mask = df_weather["municipality"] == mun
    # df_weather.loc[mask, "wdir"] = df_weather.loc[mask, "wdir"].ffill()
    df_weather.loc[mask, "wdir"] = df_weather.loc[mask, "wdir"].interpolate(
        method="linear"
    )

# %%
df_dwpt = df_weather.groupby("municipality")["dwpt"].apply(
    lambda x: x.isna().mean() * 100
)
df_dwpt[df_dwpt > 0].sort_values(ascending=False)

# %% [markdown]
# This distribution of missing values for _dwpt_ is the same for _pres_, _rhum_, _temp_ and _wspd_

# %%
incl_rest = [
    "Hjørring",
    "Ringkøbing-Skjern",
    "Langeland",
    "Svendborg",
]  # municipalities to impute over "dwpt", "pres", "rhum", "temp" and "wspd"

for mun in incl_wdir:
    mask = df_weather["municipality"] == mun
    for col in ["dwpt", "pres", "rhum", "temp", "wspd"]:
        df_weather.loc[mask, col] = df_weather.loc[mask, col].interpolate(
            method="linear"
        )

# %%
df_pres = df_weather.groupby("municipality")["pres"].apply(
    lambda x: x.isna().mean() * 100
)
df_pres[df_pres > 0].sort_values(ascending=False)

# %%
df_rhum = df_weather.groupby("municipality")["rhum"].apply(
    lambda x: x.isna().mean() * 100
)
df_rhum[df_rhum > 0].sort_values(ascending=False)

# %%
df_temp = df_weather.groupby("municipality")["temp"].apply(
    lambda x: x.isna().mean() * 100
)
df_temp[df_temp > 0].sort_values(ascending=False)

# %%
df_wspd = df_weather.groupby("municipality")["wspd"].apply(
    lambda x: x.isna().mean() * 100
)
df_wspd[df_wspd > 0].sort_values(ascending=False)

# %%
(
    df_weather.isna().sum()[df_weather.isna().sum() > 0] / df_weather.shape[0]
).sort_values(ascending=False)

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


overwrite_cas_table(df_prod, "production", cas_keyspace)
overwrite_cas_table(df_prodcons, "prodcons", cas_keyspace)
overwrite_cas_table(df_weather, "weather", cas_keyspace)

# %% [markdown]
# ## Synchronization

# %%
df_gas = pd.DataFrame(list(mdb_client[cas_keyspace]["gas"].find()))
df_gas.head()

# %%
df_weather = retrieve_from_cas(spark, "weather", cas_keyspace)
cols_to_float = [
    col
    for col in df_weather.columns
    if col not in ["id", "municipality", "time"]
]
df_weather[cols_to_float] = df_weather[cols_to_float].apply(
    pd.to_numeric, errors="coerce"
)
df_weather["time_dk"] = df_weather["time"] + pd.Timedelta(hours=1)
df_weather = df_weather.drop(columns=["time"])
df_weather = df_weather.sort_values("time_dk", ascending=True)
df_weather.head()

# %%
# def create_cassandra_table_query(df, keyspace, table_name):
#     # Define mapping between Pandas and Cassandra datatypes
#     dtype_mapping = {
#         'int64': 'int',
#         'float64': 'double',
#         'object': 'text',
#         'bool': 'boolean',
#         'datetime64[ns]': 'timestamp'
#     }

#     # Start constructing the CREATE TABLE query
#     query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (\n"

#     # Add primary key column with timeuuid
#     columns = ["id timeuuid"]

#     # Add remaining columns with mapped Cassandra data types
#     for col, dtype in df.dtypes.items():
#         if col != "id":  # Exclude 'id' to avoid duplication
#             cassandra_type = dtype_mapping.get(str(dtype), 'text')  # Default to 'text' if type is unrecognized
#             columns.append(f"{col} {cassandra_type}")

#     # Join columns with commas and specify primary key as 'id'
#     columns_str = ",\n    ".join(columns)
#     query += f"    {columns_str},\n"
#     query += "    PRIMARY KEY (id)\n);"

#     return query

# weather_query = create_cassandra_table_query(df_weather, cas_keyspace, "weather")

# %%
# import uuid
# def insert_to_cas(df: pd.DataFrame , table_name: str, keyspace_name: str):
# # Prompt: How can I add a timeuuid 'id' column when inserting into Cassandra with PySpark
#     def generate_timeuuid():
#         return str(uuid.uuid1())

#     timeuuid_udf = udf(generate_timeuuid, StringType())
#     spark.createDataFrame(df).withColumn("id", timeuuid_udf())\
#         .write.format("org.apache.spark.sql.cassandra")\
#         .mode("append")\
#         .options(table=table_name, keyspace=keyspace_name)\
#         .save()

# insert_to_cas(df_weather, "weather", cas_keyspace)

# %%
df_weather.dtypes

# %%
df_c = df_weather.copy()

# %%
df_c.set_index("time_dk", inplace=True)
df_c.head()


# %%
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


# %%
import numpy

numpy.random.seed(1234341)  # Setting the seed for reproducability

# Choose a random municipality
rand_municipality = numpy.random.choice(df_weather["municipality"].unique())
df_random = sync_weather_gas(
    df_weather, df_gas, "time_dk", "gasday", "temp", rand_municipality
)

fig, ax1 = plt.subplots(figsize=(10, 5), dpi=100)

# Plotting the temperatures
ax1.plot(df_random.index, df_random["temp"], color="b", label="Temperature")
ax1.set_xlabel("time")
ax1.set_ylabel("Temperature", color="b")
ax1.tick_params(axis="y", labelcolor="b")

# Plotting the gas purchase prices
ax2 = ax1.twinx()
ax2.plot(
    df_random.index,
    df_random["purchasepricedkk_kwh"],
    color="r",
    label="Gas purchase price",
)
ax2.set_ylabel("purchasepricedkk_kwh", color="r")
ax2.tick_params(axis="y", labelcolor="r")

fig.legend(loc="upper right")
plt.title(f"Temperature in {rand_municipality} and gas purchase prices")

plt.show()

# %%
